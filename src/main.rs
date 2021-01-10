#![feature(async_closure)]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate diesel;

#[macro_use]
extern crate diesel_migrations;

mod config;
mod database;
mod drive_client;
mod filesystem;
mod indexing;

use crate::config::Config;
use crate::database::{EntryType, FilesystemEntry, FilesystemRepository, RemoteType};
use crate::drive_client::{ChangeList, DriveClient};
use crate::filesystem::GdriveFs;
use anyhow::Result;
use diesel_migrations::run_pending_migrations;
use indexing::{DriveIndex, IndexWriter};
use std::io::{stdin, SeekFrom};
use std::sync::Arc;
use std::{io::prelude::*, path::Path};

lazy_static! {
    static ref CREDENTIALS_PATH: &'static str = "clientCredentials.json";
}

embed_migrations!("migrations/");

#[tokio::main]
async fn main() -> Result<()> {
    simple_logger::init_with_level(log::Level::Info).unwrap();

    let config_dir = dirs::config_dir().unwrap().join("StreamDrive");

    if !config_dir.exists() {
        std::fs::create_dir(&config_dir)?;
    }

    log::info!(
        "Using config directory: {}",
        &config_dir.to_str().expect("Str")
    );

    let config: Config = confy::load_path(&config_dir.join("config.toml"))
        .expect("Unable to read configuration file.");
    let state_file_name = config_dir.join("state.txt");

    let data_dir = Path::new(&config.cache.data_path);
    if !data_dir.exists() {
        std::fs::create_dir(data_dir)?;
    }

    let mount_dir = Path::new(&config.general.mount_path);
    if !mount_dir.exists() {
        std::fs::create_dir(mount_dir)?;
    }

    log::info!("Using data directory: {}", data_dir.to_str().unwrap());

    let blocking_client = tokio::task::spawn_blocking(|| reqwest::blocking::Client::new()).await?;
    let drive_client =
        Arc::new(DriveClient::create(*CREDENTIALS_PATH, blocking_client, config.clone()).await?);
    let connection_manager =
        diesel::r2d2::ConnectionManager::new(data_dir.join("filesystem.db").to_str().unwrap());
    let connection_pool = diesel::r2d2::Pool::builder()
        .max_size(10)
        .connection_customizer(Box::new(database::connection::ConnectionOptions {
            enable_wal: true,
            enable_foreign_keys: false,
            busy_timeout: Some(chrono::Duration::seconds(15)),
        }))
        .build(connection_manager)
        .expect("Failed to create connection pool");

    run_pending_migrations(&connection_pool.get().unwrap())?;

    let repository = Arc::new(FilesystemRepository::new(connection_pool.clone()));
    let root_inode = create_root(&repository);
    let shared_drives_inode = create_shared_drives(&repository, root_inode);
    let my_drives_inode = create_my_drives(&repository, root_inode);

    let filesystem = GdriveFs::new(
        Arc::clone(&repository),
        Arc::clone(&drive_client),
        root_inode,
        shared_drives_inode,
        my_drives_inode,
    );

    log::info!("Starting indexing.");

    let mut drive_index = DriveIndex::new(
        Arc::clone(&drive_client),
        connection_pool.clone(),
        config.clone(),
        shared_drives_inode,
        state_file_name,
    );
    drive_index.update_drives().await?;
    drive_index.refresh_full().await?;

    log::info!(
        "Indexing finished, mounting filesystem on {}.",
        &config.general.mount_path
    );

    let mount_path = config.general.mount_path.clone();
    let handle = tokio::task::spawn_blocking(|| {
        fuse::mount(filesystem, mount_path, &[]).expect("unable to mount FUSE filesystem");
    });

    // Make sure we get live updates about all the changes to the drive
    let background_index_handle = drive_index.start_background_indexing();

    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();

    unmount(&config.general.mount_path);

    handle.await?;
    background_index_handle.await?;

    Ok(())
}

#[cfg(target_os = "linux")]
fn unmount(path: &str) {
    let mut command = std::process::Command::new("fusermount");
    command.arg("-u").arg(path);

    command.spawn().unwrap().wait().expect("Failed to unmount.");
}

#[cfg(target_os = "macos")]
fn unmount(path: &str) {
    let mut command = std::process::Command::new("umount");
    command.arg(path);

    command.spawn().unwrap().wait().expect("Failed to unmount.");
}

fn create_root(repository: &FilesystemRepository) -> u64 {
    let date_time = chrono::Utc::now().naive_local();
    match repository.find_inode_by_remote_id("root") {
        Some(inode) => inode as u64,
        None => {
            let inode = repository.get_largest_inode() + 1;
            repository.create_entry(&FilesystemEntry {
                id: "root".to_string(),
                parent_id: None,
                name: "StreamDrive".to_string(),
                entry_type: EntryType::Directory,
                created_at: date_time.clone(),
                last_modified_at: date_time,
                remote_type: Some(RemoteType::Directory),
                inode,
                size: 0,
                parent_inode: None,
            });

            inode as u64
        }
    }
}

fn create_shared_drives(repository: &FilesystemRepository, root_inode: u64) -> u64 {
    let date_time = chrono::Utc::now().naive_local();
    match repository.find_inode_by_remote_id("shared_drives") {
        Some(inode) => inode as u64,
        None => {
            let inode = repository.get_largest_inode() + 1;
            repository.create_entry(&FilesystemEntry {
                id: "shared_drives".to_string(),
                parent_id: Some("root".to_string()),
                name: "Shared Drives".to_string(),
                entry_type: EntryType::Directory,
                created_at: date_time,
                last_modified_at: date_time,
                remote_type: Some(RemoteType::Directory),
                inode,
                size: 0,
                parent_inode: Some(root_inode as i64),
            });

            inode as u64
        }
    }
}

fn create_my_drives(repository: &FilesystemRepository, root_inode: u64) -> u64 {
    let date_time = chrono::Utc::now().naive_local();
    match repository.find_inode_by_remote_id("my_drives") {
        Some(inode) => inode as u64,
        None => {
            let inode = repository.get_largest_inode() + 1;
            repository.create_entry(&FilesystemEntry {
                id: "my_drives".to_string(),
                parent_id: Some("root".to_string()),
                name: "My Drives".to_string(),
                entry_type: EntryType::Directory,
                created_at: date_time,
                last_modified_at: date_time,
                remote_type: Some(RemoteType::Directory),
                inode,
                size: 0,
                parent_inode: Some(root_inode as i64),
            });

            inode as u64
        }
    }
}

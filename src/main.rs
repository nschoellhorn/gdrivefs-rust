#![feature(async_closure)]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate diesel;

#[macro_use]
extern crate diesel_migrations;

mod database;
mod drive_client;
mod filesystem;
mod indexing;
mod config;

use crate::database::{EntryType, FilesystemEntry, FilesystemRepository, RemoteType};
use crate::drive_client::{ChangeList, DriveClient};
use crate::filesystem::GdriveFs;
use anyhow::Result;
use indexing::IndexWriter;
use std::{io::prelude::*, path::Path};
use std::io::{stdin, SeekFrom};
use std::sync::Arc;
use diesel_migrations::run_pending_migrations;
use crate::config::Config;

lazy_static! {
    static ref CREDENTIALS_PATH: &'static str = "clientCredentials.json";
}

embed_migrations!("migrations/");

async fn create_drive_client(config: Config) -> Result<DriveClient> {
    let blocking_client = tokio::task::spawn_blocking(|| reqwest::blocking::Client::new()).await?;
    Ok(DriveClient::create(*CREDENTIALS_PATH, blocking_client, config).await?)
}

#[tokio::main]
async fn main() -> Result<()> {
    simple_logger::init_with_level(log::Level::Info).unwrap();

    let config_dir = dirs::config_dir().unwrap().join("StreamDrive");

    if !config_dir.exists() {
        std::fs::create_dir(&config_dir)?;
    }

    log::info!("Using config directory: {}", &config_dir.to_str().unwrap());

    let config: Config = confy::load_path(&config_dir.join("config.toml")).expect("Unable to read configuration file.");
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

    let drive_client = Arc::new(create_drive_client(config.clone()).await?);
    let connection_manager = diesel::r2d2::ConnectionManager::new(data_dir.join("filesystem.db").to_str().unwrap());
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

    let mut drives = drive_client.get_drives().await?;
    let test_drive = drives.remove(1);

    if let None = repository.find_inode_by_remote_id(test_drive.id.as_str()) {
        repository.create_entry(&FilesystemEntry {
            id: test_drive.id.clone(),
            parent_id: Some("shared_drives".to_string()),
            name: test_drive.name.clone(),
            entry_type: EntryType::Directory,
            created_at: test_drive.created_time.naive_local(),
            last_modified_at: test_drive.created_time.naive_local(),
            remote_type: Some(RemoteType::Directory),
            inode: repository.get_largest_inode() + 1,
            size: 0,
            parent_inode: Some(shared_drives_inode as i64),
        });
    }

    let filesystem = GdriveFs::new(
        Arc::clone(&repository),
         Arc::clone(&drive_client),
         root_inode,
         shared_drives_inode,
         my_drives_inode
    );

    log::info!("Starting indexing.");
    let mut state_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(&state_file_name)?;

    let mut current_token = String::new();
    state_file.read_to_string(&mut current_token)?;

    state_file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&state_file_name)?;

    current_token = current_token.trim().to_string();

    if current_token.is_empty() {
        current_token = "1".to_string();
    }

    let indexing_worker = IndexWriter::new(connection_pool.clone(), config.clone());
    let (indexing_handle, mut publisher) = indexing_worker.launch();

    let mut has_more = true;
    let drive_id = test_drive.id.as_str();
    while has_more {
        let current_token_str = current_token.as_str();

        let result = drive_client
            .get_change_list(current_token_str, drive_id)
            .await;

        let change_list = if let Err(error) = result {
            println!("Got error while fetching changes");
            dbg!(error);
            ChangeList {
                next_page_token: None,
                new_start_page_token: None,
                changes: vec![],
            }
        } else {
            result.unwrap()
        };

        let start_page_token = change_list.new_start_page_token.clone();
        let next_page_token = change_list.next_page_token.clone();

        for change in change_list.changes.into_iter() {
            publisher.send(change).await?;
        }

        has_more = next_page_token.is_some();
        if has_more {
            current_token = next_page_token.unwrap();
            state_file
                .write_all(current_token.as_bytes())
                .expect("Unable to write new state to file");
        } else {
            state_file
                .write_all(
                    start_page_token
                        .expect("Next Start Page Token Not Found, this seems like an logic bug.")
                        .as_bytes(),
                )
                .expect("Unable to write new state to file");
        }

        state_file.flush().expect("Unable to flush state file");
        state_file
            .seek(SeekFrom::Start(0))
            .expect("Failed to reset write pointer to start of file.");
    }

    log::info!("Indexing finished, mounting filesystem on {}.", &config.general.mount_path);

    let mount_path = config.general.mount_path.clone();
    let handle = tokio::task::spawn_blocking(|| {
        fuse::mount(filesystem, mount_path, &[])
            .expect("unable to mount FUSE filesystem");
    });

    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();

    unmount(&config.general.mount_path);

    handle.await?;
    indexing_handle.await?;

    Ok(())
}

#[cfg(target_os="linux")]
fn unmount(path: &str)  {
    let mut command = std::process::Command::new("fusermount");
    command.arg("-u").arg(path);

    command.spawn().unwrap().wait().expect("Failed to unmount.");
}

#[cfg(target_os="macos")]
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

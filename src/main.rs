#[macro_use]
extern crate diesel;
#[macro_use]
extern crate diesel_migrations;
#[macro_use]
extern crate lazy_static;

use std::io::stdin;
use std::path::Path;
use std::sync::{Arc, RwLock};

use anyhow::Result;
use diesel_migrations::run_pending_migrations;
use simple_logger::SimpleLogger;

use cache::DataCache;
use database::index_state::IndexStateRepository;
use indexing::DriveIndex;

use crate::config::Config;
use crate::database::entity::{EntryType, FilesystemEntry, RemoteType};
use crate::database::filesystem::FilesystemRepository;
use crate::drive_client::DriveClient;
use crate::filesystem::GdriveFs;

mod cache;
mod config;
mod database;
mod drive_client;
mod filesystem;
mod indexing;

lazy_static! {
    static ref CREDENTIALS_PATH: &'static str = "clientCredentials.json";
}

embed_migrations!("migrations/");

#[tokio::main]
async fn main() -> Result<()> {
    SimpleLogger::new()
        .with_level(log::LevelFilter::Info) // Everything from the libs shall be relatively silent
        //.with_module_level("fuse", log::LevelFilter::Debug) // We want to debug native filesystem stuff
        .with_module_level(module_path!(), log::LevelFilter::Debug) // For our own crate, we want to "DEBUG"
        .init()
        .unwrap();

    let config_dir = dirs::config_dir().unwrap().join("StreamDrive");

    if !config_dir.exists() {
        std::fs::create_dir(&config_dir)?;
    }

    log::info!(
        "Using config directory: {}",
        &config_dir.to_str().expect("Str")
    );

    let lock_file = config_dir.join("streamdrive.lock");
    let is_first_launch = !lock_file.exists();

    let config: Config = confy::load_path(&config_dir.join("config.toml"))
        .expect("Unable to read configuration file.");

    let data_dir = Path::new(&config.cache.data_path);
    if !data_dir.exists() {
        std::fs::create_dir(data_dir)?;
    }

    let mount_dir = Path::new(&config.general.mount_path);
    if !mount_dir.exists() {
        std::fs::create_dir(mount_dir)?;
    }

    log::info!("Using data directory: {}", data_dir.to_str().unwrap());

    let blocking_client = tokio::task::spawn_blocking(reqwest::blocking::Client::new).await?;
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
    let state_repository = IndexStateRepository::new(connection_pool.clone());
    let root_inode = create_root(&repository);
    let shared_drives_inode = create_shared_drives(&repository, root_inode);

    // We need to figure out the drive id for "My Drive" (which is the user ID as well, I guess?!)
    let my_drive_meta = drive_client.get_file_meta("root").await?;

    let _ = create_my_drive(&repository, root_inode, my_drive_meta.id.as_str());
    // We ignore the result here because we will receive UniqueConstraintViolations on each but the first launch
    let _ = state_repository.init_state(my_drive_meta.id.as_str(), RemoteType::OwnDrive);

    let cache = Arc::new(DataCache::new(
        Arc::clone(&drive_client),
        connection_pool.clone(),
        config.clone(),
    ));
    let filesystem = GdriveFs::new(
        Arc::clone(&repository),
        Arc::clone(&drive_client),
        Arc::clone(&cache),
    );

    log::info!("Starting indexing.");

    let mut drive_index = DriveIndex::new(
        Arc::clone(&drive_client),
        connection_pool.clone(),
        config.clone(),
        shared_drives_inode,
    );

    cache::run_download_worker(cache);

    if is_first_launch {
        tokio::spawn(async move {
            // We are starting up the full indexing procedure, so create the lock file (and close it again)
            let _ = std::fs::File::create(lock_file);

            drive_index
                .update_drives()
                .await
                .expect("Updating drives failed.");
            drive_index
                .refresh_full()
                .await
                .expect("Full indexing failed.");

            // Make sure we get live updates about all the changes to the drive
            drive_index
                .start_background_indexing()
                .await
                .expect("Failed to incrementally index the drives.");
        });
    } else {
        // Make sure we get live updates about all the changes to the drive
        drive_index.start_background_indexing();
    }

    log::info!(
        "Indexing finished, mounting filesystem on {}.",
        &config.general.mount_path
    );

    let mount_path = config.general.mount_path.clone();
    let handle = tokio::task::spawn_blocking(|| {
        fuse::mount(filesystem, mount_path, &[]).expect("unable to mount FUSE filesystem");
    });

    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();

    unmount(&config.general.mount_path);

    handle.await?;

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
    match repository.find_inode_by_remote_id("fs_root") {
        Some(inode) => inode as u64,
        None => {
            let inode = repository.get_largest_inode() + 1;
            repository.create_entry(&FilesystemEntry {
                id: "fs_root".to_string(),
                parent_id: None,
                name: "StreamDrive".to_string(),
                entry_type: EntryType::Directory,
                created_at: date_time,
                last_modified_at: date_time,
                remote_type: Some(RemoteType::Directory),
                inode,
                size: 0,
                parent_inode: None,
                last_accessed_at: date_time,
                mode: 0o700,
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
                parent_id: Some("fs_root".to_string()),
                name: "Shared Drives".to_string(),
                entry_type: EntryType::Directory,
                created_at: date_time,
                last_modified_at: date_time,
                remote_type: Some(RemoteType::Directory),
                inode,
                size: 0,
                parent_inode: Some(root_inode as i64),
                last_accessed_at: date_time,
                mode: 0o700,
            });

            inode as u64
        }
    }
}

fn create_my_drive(repository: &FilesystemRepository, root_inode: u64, drive_id: &str) -> u64 {
    let date_time = chrono::Utc::now().naive_local();
    match repository.find_inode_by_remote_id(drive_id) {
        Some(inode) => inode as u64,
        None => {
            let inode = repository.get_largest_inode() + 1;
            repository.create_entry(&FilesystemEntry {
                id: drive_id.to_string(),
                parent_id: Some("fs_root".to_string()),
                name: "My Drive".to_string(),
                entry_type: EntryType::Drive,
                created_at: date_time,
                last_modified_at: date_time,
                remote_type: Some(RemoteType::OwnDrive),
                inode,
                size: 0,
                parent_inode: Some(root_inode as i64),
                last_accessed_at: date_time,
                mode: 0o700,
            });

            inode as u64
        }
    }
}

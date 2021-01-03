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
use ::config::{File, Config};
use diesel_migrations::run_pending_migrations;

lazy_static! {
    static ref CREDENTIALS_PATH: &'static str = "clientCredentials.json";
    static ref BATCH_SIZE: usize = 512;
    static ref FETCH_SIZE: usize = 512;
    static ref BUFFER_SIZE: usize = 2048;
}

embed_migrations!("migrations/");

async fn create_drive_client() -> Result<DriveClient> {
    let blocking_client = tokio::task::spawn_blocking(|| reqwest::blocking::Client::new()).await?;
    Ok(DriveClient::create(*CREDENTIALS_PATH, blocking_client).await?)
}

#[tokio::main]
async fn main() -> Result<()> {
    simple_logger::init_with_level(log::Level::Info).unwrap();

    let config_dir = dirs::config_dir().unwrap().join("StreamDrive");

    if !config_dir.exists() {
        std::fs::create_dir(config_dir)?;
    }

    let drive_client = Arc::new(create_drive_client().await?);

    let connection_manager = diesel::r2d2::ConnectionManager::new("/Users/nschoellhorn/IdeaProjects/gdrivefs-rust/filesystem.db");
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
            remote_type: Some(RemoteType::TeamDrive),
            inode: repository.get_largest_inode() + 1,
            size: 0,
            parent_inode: Some(shared_drives_inode),
        });
    }

    let filesystem = GdriveFs::new(Arc::clone(&repository), Arc::clone(&drive_client));

    log::info!("Starting indexing.");
    let mut state_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open("/Users/nschoellhorn/IdeaProjects/gdrivefs-rust/state.txt")?;

    let mut current_token = String::new();
    state_file.read_to_string(&mut current_token)?;

    state_file = std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open("/Users/nschoellhorn/IdeaProjects/gdrivefs-rust/state.txt")?;

    current_token = current_token.trim().to_string();

    if current_token.is_empty() {
        current_token = "1".to_string();
    }

    let indexing_worker = IndexWriter::new(connection_pool.clone());
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

    log::info!("Indexing finished, mounting filesystem.");

    let handle = tokio::task::spawn_blocking(|| {
        fuse::mount(filesystem, "/Users/nschoellhorn/testfs", &[])
            .expect("unable to mount FUSE filesystem");
    });

    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();

    let mut command = std::process::Command::new("fusermount");
    command.arg("-u").arg("/Users/nschoellhorn/testfs");

    command.spawn().unwrap().wait().expect("Filesystem wasn't unmounted successfully, try running this yourself: fusermount -u /home/nschoellhorn/testfs");

    handle.await?;
    indexing_handle.await;

    Ok(())
}

fn create_root(repository: &FilesystemRepository) -> i64 {
    let date_time = chrono::Utc::now().naive_local();
    match repository.find_inode_by_remote_id("root") {
        Some(inode) => inode,
        None => {
            let inode = repository.get_largest_inode() + 1;
            repository.create_entry(&FilesystemEntry {
                id: "root".to_string(),
                parent_id: None,
                name: "Shared Drives".to_string(),
                entry_type: EntryType::Directory,
                created_at: date_time.clone(),
                last_modified_at: date_time,
                remote_type: Some(RemoteType::Directory),
                inode: inode,
                size: 0,
                parent_inode: None,
            });

            inode
        }
    }
}

fn create_shared_drives(repository: &FilesystemRepository, root_inode: i64) -> i64 {
    let date_time = chrono::Utc::now().naive_local();
    match repository.find_inode_by_remote_id("shared_drives") {
        Some(inode) => inode,
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
                inode: inode,
                size: 0,
                parent_inode: Some(root_inode),
            });

            inode
        }
    }
}

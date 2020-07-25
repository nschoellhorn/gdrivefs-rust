#![feature(async_closure)]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate diesel;

mod database;
mod drive_client;
mod filesystem;

use crate::database::{EntryType, FilesystemEntry, FilesystemRepository, RemoteType};
use crate::drive_client::{DriveClient, File};
use crate::filesystem::GdriveFs;
use anyhow::Result;
use chrono::naive::NaiveDateTime;
use diesel::prelude::*;
use std::io::stdin;
use std::sync::Arc;

lazy_static! {
    static ref CREDENTIALS_PATH: &'static str = "clientCredentials.json";
}

#[tokio::main]
async fn main() -> Result<()> {
    let drive_client = Arc::new(DriveClient::create(*CREDENTIALS_PATH).await?);
    let mut drives = drive_client.get_drives().await?;
    let test_drive = drives.remove(2);

    dbg!(&test_drive);

    let connection = SqliteConnection::establish("filesystem.db").unwrap();
    let repository = Arc::new(FilesystemRepository::new(connection));
    if let None = repository.find_inode_by_remote_id(test_drive.id.as_str()) {
        repository.create_entry(&FilesystemEntry {
            inode: repository.get_largest_inode() + 1,
            parent_inode: filesystem::SHARED_DRIVES_INODE as i64,
            name: test_drive.name,
            entry_type: EntryType::Directory,
            created_at: test_drive.created_time.naive_local(),
            last_modified_at: test_drive.created_time.naive_local(),
            remote_type: Some(RemoteType::TeamDrive),
            remote_id: Some(test_drive.id.clone()),
            size: 0,
        });
    }

    let filesystem = GdriveFs::new(Arc::clone(&repository), Arc::clone(&drive_client));

    println!("Indexing the drive.");
    let repository_ref = &repository;
    drive_client
        .process_folder_recursively(test_drive.id, &async move |file| {
            let indexing_repo = Arc::clone(repository_ref);
            let parent_id = file.parents.first().unwrap();
            let remote_id = file.id;

            if let None = indexing_repo.find_inode_by_remote_id(remote_id.as_str()) {
                indexing_repo.create_entry(&FilesystemEntry {
                    inode: indexing_repo.get_largest_inode() + 1,
                    parent_inode: indexing_repo
                        .find_inode_by_remote_id(parent_id.as_str())
                        .unwrap(),
                    name: file.name,
                    entry_type: match file.mime_type.as_str() {
                        "application/vnd.google-apps.folder" => EntryType::Directory,
                        _ => EntryType::File,
                    },
                    created_at: file.created_time.naive_local(),
                    last_modified_at: file.modified_time.naive_local(),
                    remote_type: Some(match file.mime_type.as_str() {
                        "application/vnd.google-apps.folder" => RemoteType::Directory,
                        _ => RemoteType::File,
                    }),
                    remote_id: Some(remote_id),
                    size: file
                        .size
                        .unwrap_or("0".to_string())
                        .as_str()
                        .parse()
                        .unwrap_or(0),
                })
            }

            Ok(())
        })
        .await?;
    println!("Indexing finished.");

    let mount_handle =
        unsafe { fuse::spawn_mount(filesystem, "/home/nschoellhorn/testfs", &[]) }.unwrap();

    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();

    Ok(())
}

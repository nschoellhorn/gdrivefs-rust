#![feature(async_closure)]

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate diesel;

mod database;
mod drive_client;
mod filesystem;

use crate::database::{EntryType, FilesystemEntry, FilesystemRepository, RemoteType};
use crate::drive_client::{Change, ChangeList, DriveClient, File};
use crate::filesystem::GdriveFs;
use anyhow::Result;
use diesel::prelude::*;
use std::io::prelude::*;
use std::io::stdin;
use std::sync::Arc;

lazy_static! {
    static ref CREDENTIALS_PATH: &'static str = "clientCredentials.json";
}

#[tokio::main]
async fn main() -> Result<()> {
    simple_logger::init_with_level(log::Level::Info).unwrap();

    let blocking_client = tokio::task::spawn_blocking(|| {
        reqwest::blocking::Client::new()
    }).await?;

    let drive_client = Arc::new(DriveClient::create(*CREDENTIALS_PATH, blocking_client).await?);
    let mut drives = drive_client.get_drives().await?;
    let test_drive = drives.remove(1);

    dbg!(&test_drive);

    let connection = SqliteConnection::establish("filesystem.db").unwrap();
    let repository = Arc::new(FilesystemRepository::new(connection));
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
        });
    }

    let filesystem = GdriveFs::new(Arc::clone(&repository), Arc::clone(&drive_client));

    println!("Indexing drive changes...");
    let mut state_file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open("state.txt")?;

    let mut current_token = String::new();
    state_file.read_to_string(&mut current_token)?;

    state_file.seek(std::io::SeekFrom::Start(0))?;

    current_token = current_token.trim().to_string();

    if current_token.is_empty() {
        current_token = "1".to_string();
    }

    println!("Starting with page token {}", &current_token);

    async_scoped::scope_and_block(|scope| {
        let mut has_more = true;
        let repo_ref = &repository;
        let drive_id = test_drive.id.as_str();
        while has_more {
            let current_token_str = current_token.as_str();
            let (_, mut change_list) = async_scoped::scope_and_block(|inner_scope| {
                let drive_client = Arc::clone(&drive_client);
                inner_scope.spawn(async move {
                    let result = drive_client
                        .get_change_list(current_token_str, drive_id)
                        .await;

                    if let Err(error) = result {
                        println!("Got error while fetching changes");
                        dbg!(error);
                        return ChangeList {
                            next_page_token: None,
                            new_start_page_token: None,
                            changes: vec![],
                        };
                    }

                    result.unwrap()
                });
            });
            let change_list = change_list.remove(0);
            let changes = change_list.changes;

            scope.spawn(async move {
                let mut changes = changes
                    .into_iter()
                    .filter(|change| change.r#type == "file")
                    .collect::<Vec<_>>();
                changes.sort_by(|a, b| {
                    let datetime_a = match a.file {
                        Some(ref f) => f.created_time,
                        None => a.time,
                    };

                    let datetime_b = match b.file {
                        Some(ref f) => f.created_time,
                        None => b.time,
                    };

                    datetime_a.cmp(&datetime_b)
                });

                changes
                    .into_iter()
                    .for_each(|change| process_change(change, Arc::clone(repo_ref)))
            });

            has_more = change_list.next_page_token.is_some();
            if has_more {
                current_token = change_list.next_page_token.unwrap();
                state_file
                    .write_all(current_token.as_bytes())
                    .expect("Unable to write new state to file");
            } else {
                state_file
                    .write_all(
                        change_list
                            .new_start_page_token
                            .expect(
                                "Next Start Page Token Not Found, this seems like an logic bug.",
                            )
                            .as_bytes(),
                    )
                    .expect("Unable to write new state to file");
            }

            state_file.flush().expect("Unable to flush state file");
        }
    });

    println!("Finished indexing, mounting file system.");

    let handle = tokio::task::spawn_blocking(|| {
        fuse::mount(filesystem, "/home/nschoellhorn/testfs", &[])
            .expect("unable to mount FUSE filesystem");
    });

    let mut input = String::new();
    stdin().read_line(&mut input).unwrap();

    let mut command = std::process::Command::new("fusermount");
    command.arg("-u").arg("/home/nschoellhorn/testfs");

    command.spawn().unwrap().wait().expect("Filesystem wasn't unmounted successfully, try running this yourself: fusermount -u /home/nschoellhorn/testfs");

    handle.await?;

    Ok(())
}

fn process_change(change: Change, indexing_repo: Arc<FilesystemRepository>) {
    if change.removed {
        process_delete(change.fileId.unwrap(), indexing_repo);
    } else {
        process_create(change.file.unwrap(), indexing_repo);
    }
}

fn process_delete(remote_id: String, indexing_repo: Arc<FilesystemRepository>) {
    indexing_repo
        .remove_entry_by_remote_id(remote_id.as_str())
        .expect("Unable to execute delete.");
}

fn process_create(file: File, indexing_repo: Arc<FilesystemRepository>) {

    let remote_id = file.id;

    if let None = indexing_repo.find_inode_by_remote_id(remote_id.as_str()) {
        indexing_repo.create_entry(&FilesystemEntry {
            inode: indexing_repo.get_largest_inode() + 1,
            parent_id: file.parents.first().map(|item| item.clone()),
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
            id: remote_id,
            size: file
                .size
                .unwrap_or("0".to_string())
                .as_str()
                .parse()
                .unwrap_or(0),
        })
    }
}

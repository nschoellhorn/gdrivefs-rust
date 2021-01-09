use anyhow::Result;
use diesel::{
    r2d2::{ConnectionManager, Pool},
    SqliteConnection,
};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinHandle,
};

use crate::config::Config;
use crate::{
    database::{EntryType, FilesystemEntry, FilesystemRepository, RemoteType},
    drive_client::{Change, ChangeList, DriveClient, File},
};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) struct IndexWriter {
    publisher: Sender<Change>,
    worker: IndexWorker,
}

impl IndexWriter {
    pub(crate) fn new(pool: Pool<ConnectionManager<SqliteConnection>>, config: Config) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(config.indexing.buffer_size);
        IndexWriter {
            publisher: tx,
            worker: IndexWorker::new(rx, FilesystemRepository::new(pool), config),
        }
    }

    pub(crate) fn launch(self) -> (JoinHandle<()>, Sender<Change>) {
        (self.worker.launch(), self.publisher)
    }
}

struct IndexWorker {
    receiver: Receiver<Change>,
    repository: FilesystemRepository,
    config: Config,
}

impl IndexWorker {
    fn new(receiver: Receiver<Change>, repository: FilesystemRepository, config: Config) -> Self {
        IndexWorker {
            receiver,
            repository,
            config,
        }
    }

    fn launch(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            self.worker_loop().await;
        })
    }

    async fn worker_loop(&mut self) {
        let mut batch = Vec::with_capacity(self.config.indexing.batch_size);
        while let Some(change) = self.receiver.recv().await {
            batch.push(change);

            // We collect a batch of 1000 changes and execute all of them in one transaction
            if batch.len() == self.config.indexing.batch_size {
                self.process_batch(batch);
                batch = Vec::with_capacity(self.config.indexing.batch_size);
            }
        }
    }

    fn process_batch(&self, batch: Vec<Change>) -> Result<(), anyhow::Error> {
        self.repository.transaction::<(), anyhow::Error, _>(|| {
            batch
                .into_iter()
                .filter(|change| change.r#type == "file")
                .for_each(|change| self.process_change(change));

            Ok(())
        })
    }

    fn process_change(&self, change: Change) {
        if change.removed {
            self.process_delete(change.fileId.unwrap());
        } else {
            self.process_create(change.file.unwrap());
        }
    }

    fn process_delete(&self, remote_id: String) {
        self.repository
            .remove_entry_by_remote_id(remote_id.as_str())
            .expect("Unable to execute delete.");
    }

    fn process_create(&self, file: File) {
        let remote_id = file.id;
        let parent_id = file.parents.first().map(|item| item.clone());

        let parent_inode = if let Some(parent_remote) = parent_id.as_ref() {
            self.repository
                .find_inode_by_remote_id(parent_remote.as_str())
        } else {
            None
        };

        if let None = self.repository.find_inode_by_remote_id(remote_id.as_str()) {
            // First, we create the new entry itself
            let inode = self.repository.get_largest_inode() + 1;
            self.repository.create_entry(&FilesystemEntry {
                inode,
                parent_id: parent_id,
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
                id: remote_id.clone(),
                size: file
                    .size
                    .unwrap_or("0".to_string())
                    .as_str()
                    .parse()
                    .unwrap_or(0),
                parent_inode,
            });

            // but then, we also update all entries that have the current remote id as the parent remote
            // to make sure they know about their parent inode as well
            self.repository
                .set_parent_inode_by_parent_id(&remote_id, inode)
                .expect("Failed to update parent inodes");
        }
    }
}

struct IndexFetcher {
    drive_client: Arc<DriveClient>,
}

impl IndexFetcher {
    fn new(drive_client: Arc<DriveClient>) -> Self {
        IndexFetcher { drive_client }
    }
}

pub struct DriveIndex {
    drive_client: Arc<DriveClient>,
    change_publisher: Sender<Change>,
    writer_handle: JoinHandle<()>,
    current_change_token: String,
    repository: FilesystemRepository,
    shared_drives_inode: u64,
    config: Config,
    state_file_name: PathBuf,
}

impl DriveIndex {
    pub fn new(
        drive_client: Arc<DriveClient>,
        pool: Pool<ConnectionManager<SqliteConnection>>,
        config: Config,
        shared_drives_inode: u64,
        state_file_name: PathBuf,
    ) -> Self {
        let (handle, publisher) = IndexWriter::new(pool.clone(), config.clone()).launch();

        let mut state_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&state_file_name)
            .expect("Unable to read current change token.");

        let mut current_token = String::new();
        state_file
            .read_to_string(&mut current_token)
            .expect("Unable to read current change token.");

        Self {
            drive_client,
            change_publisher: publisher,
            writer_handle: handle,
            repository: FilesystemRepository::new(pool),
            shared_drives_inode,
            config,
            state_file_name,
            current_change_token: current_token,
        }
    }

    pub async fn refresh_full(&mut self) -> Result<()> {
        log::info!("Starting full index refresh.");

        self.current_change_token = String::from("1");
        self.process_pending_changes().await?;

        log::info!("Full index refresh finished.");

        Ok(())
    }

    async fn process_pending_changes(&mut self) -> Result<()> {
        let mut has_more = true;
        let drive_id = self.config.general.drive_id.as_str();

        let mut state_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&self.state_file_name)?;

        let mut current_token = String::new();
        state_file.read_to_string(&mut current_token)?;

        let mut state_file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.state_file_name)?;

        current_token = current_token.trim().to_string();

        if current_token.is_empty() {
            current_token = "1".to_string();
        }

        dbg!(&current_token);
        dbg!(drive_id);

        while has_more {
            let current_token_str = current_token.as_str();

            let result = self
                .drive_client
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

            dbg!(&change_list);

            let start_page_token = change_list.new_start_page_token.clone();
            let next_page_token = change_list.next_page_token.clone();

            for change in change_list.changes.into_iter() {
                println!("Sending change");
                self.change_publisher.send(change).await?;
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
                            .expect(
                                "Next Start Page Token Not Found, this seems like an logic bug.",
                            )
                            .as_bytes(),
                    )
                    .expect("Unable to write new state to file");
            }

            state_file.flush().expect("Unable to flush state file");
            state_file
                .seek(SeekFrom::Start(0))
                .expect("Failed to reset write pointer to start of file.");
        }

        Ok(())
    }

    pub async fn update_drives(&self) -> Result<()> {
        // Get all drives that we currently have access to
        let drives = self.drive_client.get_drives().await?;
        let accessible_drive_ids = drives
            .iter()
            .map(|drive| drive.id.clone())
            .collect::<Vec<_>>();
        let known_drive_ids = self.repository.get_all_drive_ids();

        dbg!(&known_drive_ids, &accessible_drive_ids);

        self.repository.transaction::<_, anyhow::Error, _>(|| {
            // Remove all known drives (from db) that we don't find in the received list (from google) anymore
            known_drive_ids
                .iter()
                .filter(|drive_id| !accessible_drive_ids.contains(drive_id))
                .for_each(|drive_id| {
                    self.repository
                        .remove_entry_by_remote_id(drive_id)
                        .expect("Failed to remove entry")
                });

            // add all drives that we currently don't know about
            drives
                .into_iter()
                .filter(|drive| !known_drive_ids.contains(&drive.id))
                .for_each(|drive| {
                    let inode = self.repository.get_largest_inode() + 1;

                    // TODO: This should probably go through the index writer instead, but it currently accepts only stuff of type "Change"
                    //  Maybe we could introduce some ADT "Change" which contains DriveChange and FileChange or something like that.
                    self.repository.create_entry(&FilesystemEntry {
                        id: drive.id.clone(),
                        parent_id: Some("shared_drives".to_string()),
                        name: drive.name.clone(),
                        entry_type: EntryType::Drive,
                        created_at: drive.created_time.naive_local(),
                        last_modified_at: drive.created_time.naive_local(),
                        remote_type: Some(RemoteType::TeamDrive),
                        inode: self.repository.get_largest_inode() + 1,
                        size: 0,
                        parent_inode: Some(self.shared_drives_inode as i64),
                    });
                });

            Ok(())
        })?;

        Ok(())
    }
}

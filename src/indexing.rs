#![feature(async_closure)]

use anyhow::Result;
use chrono::{DateTime, Utc};
use diesel::{
    r2d2::{ConnectionManager, Pool},
    SqliteConnection,
};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        Mutex,
    },
    task::JoinHandle,
};

use crate::{
    config::Config,
    database::{entity::IndexState, index_state::IndexStateRepository},
};
use crate::{
    database::entity::{EntryType, FilesystemEntry, RemoteType},
    database::filesystem::FilesystemRepository,
    drive_client::{Change, ChangeList, DriveClient, File},
};
use core::panic;
use std::cell::RefCell;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) enum IndexChange {
    RemoteChange(Change),
    FileCreate(FilesystemEntry),
    InstantChange(Box<IndexChange>),
}

pub(crate) struct IndexWriter {
    publisher: Sender<IndexChange>,
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

    pub(crate) fn launch(self) -> (JoinHandle<()>, Sender<IndexChange>) {
        (self.worker.launch(), self.publisher)
    }

    pub(crate) fn process_create_immediately(file: File, repository: &FilesystemRepository) {
        IndexWorker::process_update(file, repository);
    }
}

struct ChangeBatch {
    changes: Vec<IndexChange>,
    latest_change_received: Option<DateTime<Utc>>,
}

struct IndexWorker {
    receiver: Receiver<IndexChange>,
    repository: FilesystemRepository,
    config: Config,
    batch: Arc<Mutex<RefCell<ChangeBatch>>>,
}

impl IndexWorker {
    fn new(
        receiver: Receiver<IndexChange>,
        repository: FilesystemRepository,
        config: Config,
    ) -> Self {
        IndexWorker {
            receiver,
            repository,
            batch: Arc::new(Mutex::new(RefCell::new(IndexWorker::init_batch(
                config.indexing.batch_size,
            )))),
            config,
        }
    }

    fn launch(mut self) -> JoinHandle<()> {
        let batch_arc = Arc::clone(&self.batch);
        let flush_interval = self.config.indexing.batch_flush_interval;
        let batch_size = self.config.indexing.batch_size;
        let repository = self.repository.clone();
        tokio::spawn(async move {
            Self::flush_loop(batch_arc, &repository, flush_interval, batch_size).await;
        });

        tokio::spawn(async move {
            self.worker_loop().await;
        })
    }

    async fn flush_loop(
        batch: Arc<Mutex<RefCell<ChangeBatch>>>,
        repository: &FilesystemRepository,
        flush_interval: u8,
        batch_size: usize,
    ) {
        loop {
            let current_time = Utc::now();
            let batch_time_option = {
                let batch_lock = batch.lock().await;
                let batch_ref = (*batch_lock).borrow();

                batch_ref.latest_change_received
            };

            if let Some(batch_time) = batch_time_option {
                if current_time.signed_duration_since(batch_time).num_seconds()
                    >= (flush_interval as i64)
                {
                    Self::flush(Arc::clone(&batch), repository, batch_size)
                        .await
                        .expect("Unable to flush in flush loop.");
                }
            }

            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    async fn worker_loop(&mut self) {
        while let Some(change) = self.receiver.recv().await {
            if let IndexChange::InstantChange(actual_change) = change {
                // If the change type is InstantChange, we want to process it instantly instead of batching it.
                Self::process_change(*actual_change, &self.repository);
                continue;
            }

            let changes_length = {
                let batch = self.batch.lock().await;
                let mut mut_batch = batch.borrow_mut();

                mut_batch.changes.push(change);
                mut_batch.latest_change_received = Some(Utc::now());

                mut_batch.changes.len()
            };

            // We collect a batch of 1000 changes and execute all of them in one transaction
            if changes_length == self.config.indexing.batch_size {
                Self::flush(
                    Arc::clone(&self.batch),
                    &self.repository,
                    self.config.indexing.batch_size,
                )
                .await
                .expect("Unable to flush batch");
            }
        }
    }

    async fn flush(
        batch: Arc<Mutex<RefCell<ChangeBatch>>>,
        repository: &FilesystemRepository,
        batch_size: usize,
    ) -> Result<(), anyhow::Error> {
        let batch = batch.lock().await;
        let finished_batch = batch.replace(IndexWorker::init_batch(batch_size));

        log::info!("Flushing batch of {} items.", finished_batch.changes.len());

        Self::process_batch(finished_batch, repository)
    }

    fn init_batch(capacity: usize) -> ChangeBatch {
        ChangeBatch {
            changes: Vec::with_capacity(capacity),
            latest_change_received: None,
        }
    }

    fn process_batch(
        batch: ChangeBatch,
        repository: &FilesystemRepository,
    ) -> Result<(), anyhow::Error> {
        repository.transaction::<(), anyhow::Error, _>(|| {
            batch
                .changes
                .into_iter()
                .filter(|change| match change {
                    IndexChange::FileCreate(_) | IndexChange::InstantChange(_) => true,
                    IndexChange::RemoteChange(remote_change) => remote_change.r#type == "file",
                })
                .for_each(|change| Self::process_change(change, repository));

            Ok(())
        })
    }

    fn process_change(change: IndexChange, repository: &FilesystemRepository) {
        match change {
            IndexChange::RemoteChange(change) => {
                if change.removed || change.file.is_some() && change.file.clone().unwrap().trashed {
                    Self::process_delete(change.fileId.unwrap(), repository);
                } else {
                    Self::process_update(change.file.unwrap(), repository);
                }
            }
            IndexChange::FileCreate(mut file) => {
                file.inode = repository.get_largest_inode() + 1;
                repository.create_entry(&file);
            }
            IndexChange::InstantChange(actual_change) => {
                if let IndexChange::InstantChange(_) = actual_change.as_ref() {
                    // This could probably lead to recursion and should not be the case, so break out of it - better safe than sorry.
                    panic!("Logical flaw detected. InstantChange contains InstantChange.");
                }
            }
        }
    }

    fn process_delete(remote_id: String, repository: &FilesystemRepository) {
        repository
            .remove_entry_by_remote_id(remote_id.as_str())
            .expect("Unable to execute delete.");
    }

    pub fn process_update(file: File, repository: &FilesystemRepository) {
        let remote_id = file.id;
        let parent_id = file.parents.first().map(|item| item.clone());

        let parent_inode = if let Some(parent_remote) = parent_id.as_ref() {
            repository.find_inode_by_remote_id(parent_remote.as_str())
        } else {
            None
        };

        match repository.find_inode_by_remote_id(remote_id.as_str()) {
            Some(inode) => {
                // Update the existing entry. For now, we just update the size and parent.
                match repository.update_entry_by_inode(
                    inode,
                    file.size
                        .unwrap_or("0".to_string())
                        .as_str()
                        .parse()
                        .unwrap_or(0),
                    parent_id,
                ) {
                    Ok(_) => (),
                    Err(err) => log::warn!("Unable to process update: {:?}", err),
                }
            }
            None => {
                // First, we create the new entry itself
                let inode = repository.get_largest_inode() + 1;
                repository.create_entry(&FilesystemEntry {
                    inode,
                    parent_id,
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
                    last_accessed_at: file.modified_time.naive_local(),
                    mode: 0o700,
                });

                // but then, we also update all entries that have the current remote id as the parent remote
                // to make sure they know about their parent inode as well
                repository
                    .set_parent_inode_by_parent_id(&remote_id, inode)
                    .expect("Failed to update parent inodes");
            }
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
    change_publisher: Sender<IndexChange>,
    writer_handle: JoinHandle<()>,
    fs_repository: FilesystemRepository,
    shared_drives_inode: u64,
    config: Config,
    state_repository: IndexStateRepository,
}

impl DriveIndex {
    pub fn new(
        drive_client: Arc<DriveClient>,
        pool: Pool<ConnectionManager<SqliteConnection>>,
        config: Config,
        shared_drives_inode: u64,
    ) -> Self {
        let (handle, publisher) = IndexWriter::new(pool.clone(), config.clone()).launch();

        Self {
            drive_client,
            change_publisher: publisher,
            writer_handle: handle,
            fs_repository: FilesystemRepository::new(pool.clone()),
            shared_drives_inode,
            config,
            state_repository: IndexStateRepository::new(pool),
        }
    }

    pub async fn refresh_full(&mut self) -> Result<()> {
        log::info!("Starting full index refresh.");

        self.state_repository.reset_all()?;

        for state in self.state_repository.get_all_states().into_iter() {
            self.process_pending_changes(state).await?;
        }

        log::info!("Full index refresh finished.");

        Ok(())
    }

    pub fn start_background_indexing(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(
                self.config.indexing.background_refresh_interval as u64,
            ));
            loop {
                interval.tick().await;
                log::info!("Fetching new changes from Google Drive");

                for state in self.state_repository.get_all_states().into_iter() {
                    self.process_pending_changes(state)
                        .await
                        .expect("Background indexing tick failed.");
                }
            }
        })
    }

    async fn process_pending_changes(&mut self, index_state: IndexState) -> Result<()> {
        let mut has_more = true;
        let drive_id = index_state.drive_id.as_str();

        let mut current_token = index_state.page_token.to_string();

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

            let start_page_token = change_list.new_start_page_token.clone();
            let next_page_token = change_list.next_page_token.clone();

            for change in change_list.changes.into_iter() {
                log::trace!("Sending change from [process_pending_changes()] to [worker_loop()]");
                self.change_publisher
                    .send(IndexChange::RemoteChange(change))
                    .await?;
            }

            has_more = next_page_token.is_some();
            if has_more {
                current_token = next_page_token.unwrap();

                // TODO: This needs to change because we can't be sure yet, that the changes from the last page really have been saved.
                let _ = self
                    .state_repository
                    .update_token_for_drive(drive_id, &current_token);
            } else {
                let _ = self.state_repository.update_token_for_drive(
                    drive_id,
                    &start_page_token
                        .expect("Next Start Page Token Not Found, this seems like an logic bug."),
                );
            }
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
        let known_drive_ids = self.fs_repository.get_all_drive_ids();

        dbg!(&known_drive_ids, &accessible_drive_ids);

        // Remove all known drives (from db) that we don't find in the received list (from google) anymore
        known_drive_ids
            .iter()
            .filter(|drive_id| !accessible_drive_ids.contains(drive_id))
            .for_each(|drive_id| {
                self.fs_repository
                    .remove_entry_by_remote_id(drive_id)
                    .expect("Failed to remove entry")
            });

        // add all drives that we currently don't know about
        for drive in drives
            .into_iter()
            .filter(|drive| !known_drive_ids.contains(&drive.id))
        {
            // We can just silently fail here
            let _ = self.state_repository.init_state(&drive.id);

            self.change_publisher
                .send(IndexChange::FileCreate(FilesystemEntry {
                    id: drive.id.clone(),
                    parent_id: Some("shared_drives".to_string()),
                    name: drive.name.clone(),
                    entry_type: EntryType::Drive,
                    created_at: drive.created_time.naive_local(),
                    last_modified_at: drive.created_time.naive_local(),
                    remote_type: Some(RemoteType::TeamDrive),
                    inode: -1, // This gets overwritten by the worker to make sure we always get the latest inode
                    size: 0,
                    parent_inode: Some(self.shared_drives_inode as i64),
                    last_accessed_at: drive.created_time.naive_local(),
                    mode: 0o700,
                }))
                .await?;
        }

        Ok(())
    }
}

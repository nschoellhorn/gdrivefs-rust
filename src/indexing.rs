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
                .set_parent_inode_by_parent_id(&remote_id, inode);
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
    fetcher: IndexFetcher,
    writer: IndexWriter,
}

impl DriveIndex {
    pub fn new(drive_client: Arc<DriveClient>, pool: Pool<ConnectionManager<SqliteConnection>>, config: Config) -> Self {
        Self {
            fetcher: IndexFetcher::new(drive_client),
            writer: IndexWriter::new(pool, config),
        }
    }
}

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::{collections::HashMap, path::Path};

use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use diesel::{
    r2d2::{ConnectionManager, Pool},
    SqliteConnection,
};
use sha2::{Digest, Sha256};
use tokio::{sync::Notify, task::JoinHandle};

use crate::{
    config::Config,
    database::{
        entity::{FilesystemEntry, NewObjectChunk, ObjectChunk},
        objectchunk::ObjectCacheChunkRepository,
    },
    drive_client::DriveClient,
};
use std::cell::RefCell;

pub(crate) struct DataCache {
    object_dir: Box<Path>,
    capacity: usize,
    pub chunk_repository: ObjectCacheChunkRepository,
    pub drive_client: Arc<DriveClient>,
    config: Config,
    pub waiting_chunks: Mutex<RefCell<HashMap<String, Arc<(Mutex<bool>, Condvar)>>>>,
    pub chunk_notifier: Arc<Notify>,
}

pub(crate) fn run_download_worker(cache: Arc<DataCache>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let notifier = Arc::clone(&cache.chunk_notifier);

        loop {
            log::info!("Processing chunk download requests");
            // Prio no 1 are the chunks someone's waiting for, so do them first
            for key in cache.get_waiting_chunk_keys() {
                let chunk = cache
                    .chunk_repository
                    .find_chunk_by_object_name(key.as_str())
                    .expect("Chunk not found");

                let _ = cache.update_cache_content(chunk).await;
            }

            // after we've downloaded all the waiting chunks, we can now keep downloading the
            // backlog of chunks we've accumulated. We do that one by one since it's possible that
            // while we're downloading them, there will be chunks queued into `waiting_chunks`
            if let Some(chunk) = cache.chunk_repository.find_next_incomplete_chunk() {
                let _ = cache.update_cache_content(chunk).await;
            }

            let has_incomplete_chunks = cache.chunk_repository.has_incomplete_chunks();
            dbg!(has_incomplete_chunks);
            if !has_incomplete_chunks {
                // Instead of spin-looping, wait until someone has a new request that we can process
                log::info!("Processed all open chunk download requests, waiting for new ones.");
                notifier.notified().await;
            }
        }
    })
}

impl DataCache {
    pub fn new(
        drive_client: Arc<DriveClient>,
        connection: Pool<ConnectionManager<SqliteConnection>>,
        config: Config,
    ) -> Self {
        dbg!(config.cache.data_path.as_str());

        let this = Self {
            object_dir: Path::new(config.cache.data_path.as_str())
                .join("objects")
                .into_boxed_path(),
            capacity: config.cache.capacity * 1024 * 1024, // Capacity in config is in Megabytes
            chunk_repository: ObjectCacheChunkRepository::new(connection),
            drive_client,
            config,
            waiting_chunks: Mutex::new(RefCell::new(HashMap::new())),
            chunk_notifier: Arc::new(Notify::new()),
        };

        dbg!(&this.object_dir);

        // make sure the cache directory exists
        if !this.object_dir.exists() {
            let _ = std::fs::create_dir(&this.object_dir);
        }

        this
    }

    pub fn get_waiting_chunk_keys(&self) -> Vec<String> {
        self.waiting_chunks
            .lock()
            .unwrap()
            .borrow()
            .keys()
            .cloned()
            .collect()
    }

    pub async fn update_cache_content(&self, chunk: ObjectChunk) -> Result<()> {
        log::info!(
            "Downloading chunk data for chunk {} / file {}",
            chunk.id,
            chunk.file_id.as_str()
        );

        let result = self
            .drive_client
            .get_file_content(
                chunk.file_id.as_str(),
                chunk.byte_from as u64,
                chunk.byte_to as u64,
            )
            .await;

        match result {
            Ok(bytes) => {
                // overwrite (or create) the chunk file with the data we've fetched
                let object_file_path = self.object_dir.join(&chunk.object_name);
                dbg!(&object_file_path);

                // TODO: Maybe we could store the file handles/open files somewhere in memory for some time instead of always opening new ones
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(object_file_path)
                    .expect("Unable to open cache chunk file for write.");

                file.write_all(bytes.as_ref())
                    .expect("Failed to write to chunk file.");
                self.chunk_repository
                    .set_chunk_complete(chunk.id)
                    .expect("Failed to update chunk metadata");

                let notifier_tuple_option = (*self.waiting_chunks.lock().unwrap())
                    .borrow_mut()
                    .remove(&chunk.object_name);

                if let Some(notifier_tuple) = notifier_tuple_option {
                    let (mutex, condvar) = &*notifier_tuple;

                    let mut is_complete = mutex.lock().unwrap();
                    *is_complete = true;

                    condvar.notify_all();
                }

                log::info!("Successfully downloaded a chunk!")
            }
            Err(error) => log::error!("Failed to download chunk: {:?}", error),
        }

        Ok(())
    }

    pub fn initialize_chunks(&self, entry: FilesystemEntry) {
        let size = entry.size as usize;

        let chunk_size = self.config.cache.chunk_size;
        let chunk_amount = (size as f32 / chunk_size as f32).ceil() as usize;

        // First, we check if we maybe already have the chunks
        let existing_chunks = self
            .chunk_repository
            .find_chunks_by_file_id(entry.id.as_str());

        // if there are already chunks in the database, we expect the layout to be correct since
        // we regularly update them when updates from Google come in or the files are changed
        // locally, so we can just return what we found in the database.
        if existing_chunks.len() == chunk_amount {
            return;
        }

        // if there are no chunks, we create them
        let mut chunks = Vec::with_capacity(chunk_amount);
        for chunk_index in 0..chunk_amount {
            let byte_from = chunk_index * chunk_size;
            let byte_to = std::cmp::min(byte_from + chunk_size, size);

            let mut hasher = Sha256::new();
            hasher.update(format!("{}_c{}", entry.id, chunk_index));

            let object_name = hex::encode(hasher.finalize());

            let new_chunk = NewObjectChunk {
                file_id: entry.id.clone(),
                chunk_sequence: chunk_index as i32,
                cached_size: (byte_to - byte_from + 1) as i64,
                byte_from: byte_from as i64,
                byte_to: byte_to as i64,
                object_name,
            };

            chunks.push(new_chunk);
        }

        self.chunk_repository.insert_chunks(chunks);
        self.chunk_notifier.notify_one();
    }

    pub fn get_bytes_blocking(
        &self,
        file_id: String,
        byte_from: i64,
        byte_to: i64,
    ) -> Result<Bytes> {
        // to read bytes, we first need to figure out in which chunks the requested range is saved
        let chunks =
            self.chunk_repository
                .find_chunks_for_range(file_id.as_str(), byte_from, byte_to);

        let mut buffer = BytesMut::with_capacity((byte_to - byte_from + 1) as usize);
        // zero out the buffer to make sure its fully initialized (length == capacity)
        buffer.resize(buffer.capacity(), 0);

        let mut buffer_pointer = 0;
        for chunk in chunks {
            // if the download of the chunk has not been completed, we need to wait on it
            if !chunk.is_complete {
                self.wait_for_chunk(&chunk);
            }

            let end = (std::cmp::min(byte_to, chunk.byte_to) + 1) as usize - byte_from as usize;

            let relative_start = if byte_from > chunk.byte_from {
                byte_from - chunk.byte_from
            } else {
                0
            } as u64;

            let sub_slice = &mut buffer[buffer_pointer..end];
            let mut object_file = File::open(self.object_dir.join(chunk.object_name))
                .context("Unable to open cache file")?;

            object_file
                .seek(SeekFrom::Start(relative_start))
                .context("Failed to seek to relative start")?;

            object_file
                .read_exact(sub_slice)
                .context("Unable to read from cache file.")?;

            buffer_pointer += end;
        }

        Ok(buffer.freeze())
    }

    fn wait_for_chunk(&self, chunk: &ObjectChunk) {
        // maybe other parts of the program already wait for this chunk,
        // so make sure that we don't create a new condvar if there's one already
        let result = {
            self.waiting_chunks
                .lock()
                .unwrap()
                .borrow()
                .get(&chunk.object_name)
                .cloned()
        };
        let arc = if let Some(tuple_arc) = result {
            Arc::clone(&tuple_arc)
        } else {
            let mutex = Mutex::new(false);
            let condvar = Condvar::new();
            let arc = Arc::new((mutex, condvar));

            // if nobody was waiting, we insert the chunk into the list of awaited chunks ourselves :)
            self.waiting_chunks
                .lock()
                .unwrap()
                .borrow_mut()
                .insert(chunk.object_name.clone(), Arc::clone(&arc));

            arc
        };

        self.chunk_notifier.notify_one();

        let (mutex, condvar) = &*arc;
        let mut is_complete = mutex.lock().unwrap();

        while !*is_complete {
            is_complete = condvar
                .wait(is_complete)
                .expect("Got poisoned lock while waiting for chunk download.");
        }
    }
}

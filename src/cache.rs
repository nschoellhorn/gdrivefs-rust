use std::fs::{File, OpenOptions};
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
    drive_client::{DriveClient, DriveError},
};
use std::cell::RefCell;

pub(crate) struct DataCache {
    object_dir: Box<Path>,
    capacity: usize,
    pub chunk_repository: ObjectCacheChunkRepository,
    pub drive_client: Arc<DriveClient>,
    config: Config,
    pub waiting_chunks: Mutex<RefCell<HashMap<String, Arc<(Mutex<bool>, Condvar)>>>>,
    pub download_notifier: Arc<Notify>,
    pub upload_notifier: Arc<Notify>,
}

pub(crate) fn run_upload_worker(cache: Arc<DataCache>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let notifier = Arc::clone(&cache.upload_notifier);

        loop {
            log::info!("Uploading changed files to Google Drive");

            for dirty_file in cache.chunk_repository.find_dirty_files() {
                // For each dirty file, we need to prepare a resumable upload, then upload *all* chunks (not only the dirty ones),
                // and then mark the file as complete & not dirty
                let upload = cache
                    .drive_client
                    .prepare_resumable_upload(
                        dirty_file.as_str(),
                        cache.get_size_on_disk(dirty_file.as_str()),
                    )
                    .await
                    .expect("Failed to prepare resumable upload.");

                for chunk in cache
                    .chunk_repository
                    .find_chunks_by_file_id(dirty_file.as_str())
                {
                    log::info!("Uploading chunk {}", chunk.id);
                    let mut file =
                        std::fs::File::open(cache.object_dir.join(chunk.object_name)).unwrap();
                    let mut data = Vec::with_capacity(file.metadata().unwrap().len() as usize);
                    file.read_to_end(&mut data)
                        .expect("Failed to read chunk file for uploading");
                    cache
                        .drive_client
                        .write_file_resumable(&upload, chunk.byte_from as u64, data.as_slice())
                        .await
                        .expect("Failed to upload chunk");

                    cache.chunk_repository.mark_chunk_clean(chunk.id);
                }
            }

            let has_dirty_chunk = cache.chunk_repository.has_dirty_chunks();

            if !has_dirty_chunk {
                // Instead of spin-looping, wait until someone has a new request that we can process
                log::info!("Processed all outstanding uploads. Waiting for new ones.");
                notifier.notified().await;
            }
        }
    })
}

pub(crate) fn run_download_worker(cache: Arc<DataCache>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let notifier = Arc::clone(&cache.download_notifier);

        loop {
            log::info!("Processing chunk download requests");
            // Prio no 1 are the chunks someone's waiting for, so do them first
            for key in cache.get_waiting_chunk_keys() {
                let chunk = cache
                    .chunk_repository
                    .find_chunk_by_object_name(key.as_str())
                    .expect("Chunk not found");

                let _ = cache.update_cache_content_from_remote(chunk).await;
            }

            // after we've downloaded all the waiting chunks, we can now keep downloading the
            // backlog of chunks we've accumulated. We do that one by one since it's possible that
            // while we're downloading them, there will be chunks queued into `waiting_chunks`
            if let Some(chunk) = cache.chunk_repository.find_next_incomplete_chunk() {
                let _ = cache.update_cache_content_from_remote(chunk).await;
            }

            let has_incomplete_chunks = cache.chunk_repository.has_incomplete_chunks();

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
            download_notifier: Arc::new(Notify::new()),
            upload_notifier: Arc::new(Notify::new()),
        };

        dbg!(&this.object_dir);

        // make sure the cache directory exists
        if !this.object_dir.exists() {
            let _ = std::fs::create_dir(&this.object_dir);
        }

        this
    }

    pub fn is_fully_cached(&self, file_id: &str) -> bool {
        !self.chunk_repository.has_incomplete_chunks_by_file(file_id)
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

    pub fn get_size_on_disk(&self, file_id: &str) -> usize {
        let chunks = self.chunk_repository.find_chunks_by_file_id(file_id);
        if chunks.len() == 0 {
            return 0;
        }

        chunks
            .into_iter()
            .map(|chunk| {
                std::fs::metadata(self.object_dir.join(&chunk.object_name))
                    .map(|meta| meta.len())
                    .unwrap_or(0) as usize
            })
            .reduce(|a, b| a + b)
            .unwrap()
    }

    pub fn get_chunk_size_on_disk(&self, chunk_id: i64) -> usize {
        let chunk = self.chunk_repository.find_chunk_by_id(chunk_id);
        match chunk {
            Option::Some(chunk) => std::fs::metadata(self.object_dir.join(&chunk.object_name))
                .unwrap()
                .len() as usize,
            Option::None => 0usize,
        }
    }

    pub async fn update_cache_content_from_remote(&self, chunk: ObjectChunk) -> Result<()> {
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
            Err(error) => {
                if error.is::<DriveError>() {
                    match error.downcast_ref::<DriveError>().unwrap() {
                        DriveError::NotFound => {
                            log::warn!(
                                "Didn't find the file for this chunk on Google Drive. Deleting chunk."
                            );
                            self.chunk_repository.delete_chunk(chunk.id);
                        }
                        _ => log::error!("Failed to download chunk: {:?}", error),
                    }
                } else {
                    log::error!("Failed to download chunk: {:?}", error);
                }
            }
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
            let byte_to = std::cmp::min(byte_from + chunk_size - 1, size);

            let mut hasher = Sha256::new();
            hasher.update(format!("{}_c{}", entry.id, chunk_index));

            let object_name = hex::encode(hasher.finalize());

            let new_chunk = NewObjectChunk {
                file_id: entry.id.clone(),
                chunk_sequence: chunk_index as i32,
                byte_from: byte_from as i64,
                byte_to: byte_to as i64,
                object_name: object_name.clone(),
                is_dirty: false,
                is_complete: false,
            };

            // Ensure the chunk file exists on disk
            let _ = OpenOptions::new()
                .create(true)
                .write(true)
                .append(true)
                .open(self.object_dir.join(object_name));

            chunks.push(new_chunk);
        }

        self.chunk_repository.insert_chunks(chunks);
        self.download_notifier.notify_one();
    }

    pub fn write_bytes(&self, file_id: &str, byte_from: i64, data: &[u8]) -> Result<()> {
        // Writing bytes to the chunk files is not as easy as it seems. It is possible that:
        //  - either the bytes to be written span multiple files, which is semi-trivial
        //  - or the bytes just belong to one specific chunk, which is trivial
        //  - or the bytes exceed the last chunk's current size, which is semi-trivial
        //  - or the bytes exceed the last chunk's current size and also the max chunk size, which is non-trivial

        log::debug!("write_bytes()");
        dbg!(byte_from);

        let byte_to = byte_from + data.len() as i64 - 1;
        dbg!(byte_to);

        // Check if we find matching chunk(s) that fit all the bytes we want to write
        let chunks = self
            .chunk_repository
            .find_exact_chunks_for_range(file_id, byte_from, byte_to);

        dbg!(chunks.len());
        // if the length of the result is 1, we are in luck: we can fit all the bytes in one chunk, so this is pretty straight-forward
        if chunks.len() == 1 {
            let chunk = chunks.get(0).unwrap();
            dbg!(&chunk);
            let mut object_file = OpenOptions::new()
                .write(true)
                .open(self.object_dir.join(chunk.object_name.as_str()))
                .context("Unable to open chunk file for writing")?;

            object_file
                .seek(SeekFrom::Start((byte_from - chunk.byte_from) as u64))
                .context("Unable to seek through cache file")?;
            object_file
                .write_all(data)
                .context("Unable to write to cache file.")?;

            log::debug!("2: Wrote {} bytes.", data.len());

            return Ok(());
        }

        // The bytes to be written span multiple chunks, but all of them exist and have a size that fits the bytes
        if chunks.len() > 1 {
            let mut data_cursor = 0usize;
            for chunk in chunks {
                dbg!(data_cursor, chunk.byte_to);

                let relative_start = if byte_from > chunk.byte_from {
                    byte_from - chunk.byte_from
                } else {
                    0
                } as u64;

                let mut object_file = OpenOptions::new()
                    .write(true)
                    .open(self.object_dir.join(chunk.object_name))
                    .context("Unable to open cache file")?;

                object_file
                    .seek(SeekFrom::Start(relative_start))
                    .context("Failed to seek to relative start")?;

                let data_end =
                    std::cmp::min(data.len(), chunk.byte_to as usize - byte_from as usize);
                dbg!(data_end);
                object_file
                    .write_all(&data[data_cursor..data_end])
                    .context("Unable to write to cache file.")?;

                log::debug!("3: Wrote {} bytes.", &data[data_cursor..data_end].len());

                data_cursor += data_end;
            }

            return Ok(());
        }

        // We have to write bytes that don't fit in the current chunks, so we either need to extend an existing one
        //  or create a new one or even both.
        if chunks.len() == 0 {
            // We need all chunks from the starting point up to the currently last one
            let mut chunks = self
                .chunk_repository
                .find_chunks_from_byte(file_id, byte_from);

            // there is an edge case where we still dont find any chunks. This can happen when the
            // starting point for our writing is after the the end of the currently last chunk.
            // This should not happen very often, so for now we just violently fail here to monitor
            // what the impact is.
            if chunks.len() == 0 {
                let new_byte_from = byte_from;
                let new_byte_to = new_byte_from + self.config.cache.chunk_size as i64 - 1;

                let chunk_index = self.chunk_repository.get_max_sequence_for_file(file_id) + 1;

                let mut hasher = Sha256::new();
                hasher.update(format!("{}_c{}", file_id, chunk_index));

                let object_name = hex::encode(hasher.finalize());

                let new_chunk = NewObjectChunk {
                    file_id: String::from(file_id),
                    chunk_sequence: chunk_index,
                    byte_from: new_byte_from,
                    byte_to: new_byte_to,
                    object_name: object_name.clone(),
                    is_dirty: true,
                    is_complete: true,
                };
                self.chunk_repository.insert_chunks(vec![new_chunk]);

                chunks.push(ObjectChunk {
                    id: -1,
                    last_read: None,
                    last_write: None,
                    is_complete: true,
                    is_dirty: true,
                    file_id: String::from(file_id),
                    chunk_sequence: chunk_index,
                    byte_from: new_byte_from,
                    byte_to: new_byte_to,
                    object_name,
                });
            }

            // We can re-use nearly the same logic as above, but instead of stopping at the current
            // chunk boundary, we allow extending the chunk boundary up to the maximum chunk size.
            let mut data_cursor = 0usize;
            let mut previous_chunk = chunks.first().unwrap().clone();
            for chunk in &chunks {
                dbg!(data_cursor);

                let relative_start = if byte_from > chunk.byte_from {
                    byte_from - chunk.byte_from
                } else {
                    0
                } as u64;

                let mut object_file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(self.object_dir.join(&chunk.object_name))
                    .context("Unable to open cache file")?;

                object_file
                    .seek(SeekFrom::Start(relative_start))
                    .context("Failed to seek to relative start")?;

                let chunk_max_byte_to = std::cmp::max(
                    chunk.byte_to as usize,
                    chunk.byte_from as usize + self.config.cache.chunk_size,
                );
                dbg!(chunk_max_byte_to);
                let data_end = std::cmp::min(data.len(), chunk_max_byte_to - byte_from as usize);
                dbg!(data_end);
                object_file
                    .write_all(&data[data_cursor..data_end])
                    .context("Unable to write to cache file.")?;

                log::debug!("4: Wrote {} bytes.", &data[data_cursor..data_end].len());

                // Since we possibly extended the chunk, we need to recalculate the `byte_to` value
                // for this chunk
                let new_byte_to = chunk.byte_from + object_file.metadata().unwrap().len() as i64;
                dbg!(chunk, new_byte_to);
                if new_byte_to != chunk.byte_to {
                    self.chunk_repository
                        .update_chunk_byte_to(chunk.id, new_byte_to);
                }

                data_cursor += &data[data_cursor..data_end].len();
                previous_chunk = chunk.clone();
            }

            // We have written to (and possibly extended) all chunks that currently exist. Now we
            // need to check if there's data left to write.
            while data_cursor < (data.len() - 1) {
                let new_byte_from = previous_chunk.byte_to + 1;
                let new_byte_to = std::cmp::min(
                    new_byte_from + self.config.cache.chunk_size as i64,
                    byte_from + data.len() as i64,
                ) - 1;

                let chunk_index = previous_chunk.chunk_sequence + 1;

                let mut hasher = Sha256::new();
                hasher.update(format!("{}_c{}", file_id, chunk_index));

                let object_name = hex::encode(hasher.finalize());
                let new_chunk = NewObjectChunk {
                    file_id: String::from(file_id),
                    chunk_sequence: chunk_index,
                    byte_from: new_byte_from,
                    byte_to: new_byte_to,
                    object_name: object_name.clone(),
                    is_dirty: true,
                    is_complete: true,
                };
                self.chunk_repository.insert_chunks(vec![new_chunk]);

                let mut object_file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(self.object_dir.join(&object_name))
                    .unwrap();

                let data_end = std::cmp::min(data.len(), new_byte_to as usize - byte_from as usize);
                dbg!(
                    previous_chunk,
                    byte_from,
                    byte_to,
                    new_byte_from,
                    new_byte_to,
                    data_cursor,
                    data_end
                );
                eprintln!();
                object_file
                    .write_all(&data[data_cursor..data_end])
                    .context("Unable to write to cache file.")?;

                log::debug!("1: Wrote {} bytes.", &data[data_cursor..data_end].len());

                data_cursor += &data[data_cursor..data_end].len();
                previous_chunk = ObjectChunk {
                    id: -1,
                    last_read: None,
                    last_write: None,
                    is_complete: false,
                    is_dirty: true,
                    file_id: String::from(file_id),
                    chunk_sequence: chunk_index,
                    byte_from: new_byte_from,
                    byte_to: new_byte_to,
                    object_name,
                }
            }
        }

        Ok(())
    }

    pub fn get_bytes_blocking(&self, file_id: &str, byte_from: i64, byte_to: i64) -> Result<Bytes> {
        // to read bytes, we first need to figure out in which chunks the requested range is saved
        let chunks = self
            .chunk_repository
            .find_exact_chunks_for_range(file_id, byte_from, byte_to);

        let mut buffer = BytesMut::with_capacity((byte_to - byte_from + 1) as usize);
        // zero out the buffer to make sure its fully initialized (length == capacity)
        buffer.resize(buffer.capacity(), 0);

        let mut buffer_cursor = 0;
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

            let mut object_file = File::open(self.object_dir.join(chunk.object_name.as_str()))
                .context("Unable to open cache file")?;
            let real_end = std::cmp::min(end, object_file.metadata().unwrap().len() as usize);
            let sub_slice = &mut buffer[buffer_cursor..real_end];

            object_file
                .seek(SeekFrom::Start(relative_start))
                .context("Failed to seek to relative start")?;

            log::debug!(
                "rel_start = {}, end = {}, real_end = {}, chunk = {:?}",
                relative_start,
                end,
                real_end,
                &chunk
            );

            object_file
                .read_exact(sub_slice)
                .context("Unable to read from cache file.")?;

            buffer_cursor += sub_slice.len();
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

        self.download_notifier.notify_one();

        let (mutex, condvar) = &*arc;
        let mut is_complete = mutex.lock().unwrap();

        while !*is_complete {
            is_complete = condvar
                .wait(is_complete)
                .expect("Got poisoned lock while waiting for chunk download.");
        }
    }
}

use anyhow::{anyhow, Result};
use std::{
    cell::{Cell, RefCell},
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    unimplemented,
};

use bytes::Bytes;
use tokio::{
    sync::{
        mpsc::UnboundedSender,
        oneshot::{error::TryRecvError, Sender},
    },
    task::JoinHandle,
};

use crate::drive_client::DriveClient;

lazy_static! {
    static ref CHUNK_SIZE: usize = 1024 * 1024;
    static ref BASE_OFFSET: usize = 0;
}

#[derive(Debug)]
pub(crate) struct DownloadRequest {
    pub file_id: String,
    pub offset: u64,
    pub size: u32,
    pub response_channel: Option<tokio::sync::oneshot::Sender<()>>,
}

pub(crate) struct DataCache {
    drive_client: Arc<DriveClient>,
    cache_storage: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    sender: Mutex<RefCell<Option<UnboundedSender<DownloadRequest>>>>,
}

impl DataCache {
    pub fn new(drive_client: Arc<DriveClient>) -> Self {
        Self {
            drive_client,
            cache_storage: Arc::new(RwLock::new(HashMap::new())),
            sender: Mutex::new(RefCell::new(None)),
        }
    }

    pub fn run_download_worker(&self) -> JoinHandle<()> {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<DownloadRequest>();

        {
            self.sender.lock().unwrap().replace(Some(sender.clone()));
        }

        let drive_client = Arc::clone(&self.drive_client);
        let cache = Arc::clone(&self.cache_storage);
        tokio::spawn(async move {
            log::info!("Waiting for download requests");
            while let Some(download_request) = receiver.recv().await {
                log::info!("Got download request: {:?}", download_request);
                let data = drive_client
                    .get_file_content(
                        download_request.file_id.as_str(),
                        download_request.offset,
                        download_request.offset + (download_request.size as u64) - 1,
                    )
                    .await;

                // This is a very naive form of implementing a retry mechanism.
                // Instead of stupidly retrying on every error, we should decide based on the exact error type.
                if data.is_err() {
                    log::warn!("Download request failed, retrying: {:?}", data);
                    sender.send(download_request);
                    continue;
                }

                let data = data.unwrap();

                log::info!("Got {} bytes.", data.len());

                let mut write_lock = cache
                    .write()
                    .expect("Failed to acquire write lock on cache.");

                log::info!("Got write lock on cache.");

                let required_len = download_request.offset + (download_request.size as u64);
                let mut currently_cached_data = write_lock
                    .get_mut(download_request.file_id.as_str())
                    .cloned()
                    .unwrap_or_else(|| vec![0; required_len as usize]);
                let mut cache_slice = currently_cached_data.as_mut_slice();

                dbg!(cache_slice.len(), required_len, data.len());

                if (cache_slice.len() as u64) < required_len {
                    log::info!("Cache slice too small, reallocating.");
                    currently_cached_data = vec![0; required_len as usize];
                    cache_slice = currently_cached_data.as_mut_slice();
                }

                cache_slice[(download_request.offset as usize)
                    ..(download_request.offset as usize + data.len())]
                    .copy_from_slice(data.as_ref());

                dbg!(
                    cache_slice.len(),
                    download_request.offset,
                    download_request.offset as usize + data.len(),
                );

                write_lock.insert(download_request.file_id, cache_slice.to_vec());

                log::info!("Downloaded and cached the requested chunk.");

                if let Some(response_channel) = download_request.response_channel {
                    log::info!("The caller requested a response, sending it now.");
                    let _ = response_channel.send(());
                }
            }
        })
    }

    pub fn warmup_key(&self, file_id: String) -> Result<()> {
        let lock = self.sender.lock().unwrap();
        let borrow = lock.borrow();
        let sender = borrow.as_ref();
        if sender.is_none() {
            return Err(anyhow!("The download has not been started yet."));
        }

        log::info!("Sending warmup download request for file {}", &file_id);
        sender.as_ref().unwrap().send(DownloadRequest {
            file_id,
            offset: *BASE_OFFSET as u64,
            size: *CHUNK_SIZE as u32,
            response_channel: None,
        });

        Ok(())
    }

    pub fn get_chunk(&self, file_id: String, offset: u64, size: u32) -> Result<Bytes> {
        let lock = self.sender.lock().unwrap();
        let borrow = lock.borrow();
        let sender = borrow.as_ref();
        if sender.is_none() {
            return Err(anyhow!("The download has not been started yet."));
        }

        let read_lock = self
            .cache_storage
            .read()
            .expect("Failed to acquire read lock on cache.");

        let cache_value = read_lock.get(file_id.as_str());

        // If we encounter a cache miss, then we need to download a chunk of data and wait for this to finish
        if cache_value.is_none() || cache_value.unwrap().len() < (offset as usize + size as usize) {
            log::info!("Cache miss, sending immediate download request.");

            drop(cache_value);
            drop(read_lock);

            let (response_channel, mut receiver) = tokio::sync::oneshot::channel();

            let _ = sender.unwrap().send(DownloadRequest {
                file_id: file_id.clone(),
                offset,
                size: std::cmp::max(*CHUNK_SIZE as u32, size),
                response_channel: Some(response_channel),
            });

            loop {
                let recv_result = receiver.try_recv();

                if let Ok(_) = recv_result {
                    log::info!("Got a response from the download worker.");
                    break;
                }

                if let Err(TryRecvError::Closed) = recv_result {
                    return Err(anyhow!("The download worker unexpectedly closed the response channel without responding."));
                }
            }
        }

        log::info!("Deliverying chunk from cache");

        let read_lock = self
            .cache_storage
            .read()
            .expect("Failed to acquire read lock on cache.");

        let cache_value = read_lock.get(file_id.as_str());

        let cache_value = cache_value.unwrap();
        dbg!(cache_value.len(), offset, offset as usize + size as usize,);
        let chunk = cache_value[(offset as usize)..(offset as usize + size as usize)].to_owned();

        Ok(Bytes::from(chunk))
    }
}

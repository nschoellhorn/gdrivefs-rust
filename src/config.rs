use dirs::cache_dir;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Default, Clone)]
pub struct Config {
    pub indexing: IndexingConfig,
    pub cache: CacheConfig,
    pub general: GeneralConfig,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct IndexingConfig {
    pub batch_size: usize,
    pub fetch_size: usize,
    pub buffer_size: usize,
    pub batch_flush_interval: u8,
    pub background_refresh_interval: u8,
}

impl Default for IndexingConfig {
    fn default() -> Self {
        IndexingConfig {
            batch_size: 512,
            fetch_size: 1000,
            buffer_size: 4096,
            batch_flush_interval: 10,
            background_refresh_interval: 30,
        }
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct CacheConfig {
    pub data_path: String,
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            data_path: cache_dir()
                .unwrap()
                .join("StreamDrive")
                .to_str()
                .unwrap()
                .to_string(),
        }
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct GeneralConfig {
    pub mount_path: String,
}

impl Default for GeneralConfig {
    fn default() -> Self {
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        unimplemented!("StreamDrive currently only supports Windows and MacOS");

        GeneralConfig {
            #[cfg(target_os = "linux")]
            mount_path: String::from("/media/StreamDrive"),
            #[cfg(target_os = "macos")]
            mount_path: String::from("/Volumes/StreamDrive"),
        }
    }
}

use serde::{Deserialize, Serialize};
use dirs::cache_dir;

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
}

impl Default for IndexingConfig {
    fn default() -> Self {
        IndexingConfig {
            batch_size: 512,
            fetch_size: 1000,
            buffer_size: 4096,
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
            data_path: cache_dir().unwrap().join("StreamDrive").to_str().unwrap().to_string(),
        }
    }
}

#[derive(Deserialize, Serialize, Clone)]
pub struct GeneralConfig {
    pub mount_path: String,
}

impl Default for GeneralConfig {
    fn default() -> Self {
        #[cfg(not(any(target_os="linux", target_os="macos")))]
        unimplemented!("StreamDrive currently only supports Windows and MacOS");

        GeneralConfig {
            #[cfg(target_os="linux")]
            mount_path: String::from("/media/StreamDrive"),
            #[cfg(target_os="macos")]
            mount_path: String::from("/Volumes/StreamDrive"),
        }
    }
}

use serde::{Deserialize, Serialize};
use dirs::cache_dir;

#[derive(Deserialize, Serialize, Default, Clone)]
pub struct Config {
    pub indexing: IndexingConfig,
    pub cache: CacheConfig,
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

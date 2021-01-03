use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Default)]
struct Config {

}

struct IndexingConfig {
    batch_size: usize,
    fetch_size: usize,
    buffer_size: usize,
}

impl Default for IndexingConfig {
    fn default() -> Self {
        IndexingConfig {
            batch_size: 512,
            fetch_size: 512,
            buffer_size: 512,
        }
    }
}
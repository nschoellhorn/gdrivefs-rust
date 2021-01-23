use std::path::Path;

use crate::database::entity::ObjectCacheMetadata;

pub(crate) struct DataCache {
    object_dir: Box<Path>,
    capacity: usize,
    meta_repository: Repos
}

impl DataCache {

}
use diesel::{SqliteConnection, r2d2::{ConnectionManager, Pool}};

pub(crate) struct ObjectCacheMetadataRepository {
    connection: Pool<ConnectionManager<SqliteConnection>>,
}

impl ObjectCacheMetadataRepository {

}
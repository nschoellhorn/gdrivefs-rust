use chrono::NaiveDateTime;
use diesel_derive_enum::DbEnum;

use crate::database::schema::filesystem;
use crate::database::schema::index_state;
use crate::database::schema::object_chunk;

#[derive(Debug, DbEnum, Hash, Eq, PartialEq)]
pub enum EntryType {
    Drive,
    File,
    Directory,
}

#[derive(Debug, DbEnum, Clone)]
pub enum RemoteType {
    OwnDrive,
    TeamDrive,
    Directory,
    File,
}

#[derive(Debug, Queryable, Insertable, QueryableByName)]
#[table_name = "filesystem"]
pub struct FilesystemEntry {
    pub id: String,
    pub name: String,
    pub entry_type: EntryType,
    pub created_at: NaiveDateTime,
    pub last_modified_at: NaiveDateTime,
    pub last_accessed_at: NaiveDateTime,
    pub mode: i32,
    pub remote_type: Option<RemoteType>,
    pub inode: i64,
    pub size: i64,
    pub parent_id: Option<String>,
    pub parent_inode: Option<i64>,
}

#[derive(Debug, Queryable, Insertable)]
#[table_name = "index_state"]
pub struct IndexState {
    pub drive_id: String,
    pub page_token: i64,
    pub remote_type: RemoteType,
}

#[derive(Debug, Queryable, QueryableByName)]
#[table_name = "object_chunk"]
pub struct ObjectChunk {
    pub id: i64,
    pub file_id: String,
    pub chunk_sequence: i32,
    pub last_read: Option<NaiveDateTime>,
    pub last_write: Option<NaiveDateTime>,
    pub cached_size: i64,
    pub byte_from: i64,
    pub byte_to: i64,
    pub is_complete: bool,
    pub object_name: String,
}

#[derive(Debug, Insertable)]
#[table_name = "object_chunk"]
pub struct NewObjectChunk {
    pub file_id: String,
    pub chunk_sequence: i32,
    pub cached_size: i64,
    pub byte_from: i64,
    pub byte_to: i64,
    pub object_name: String,
}

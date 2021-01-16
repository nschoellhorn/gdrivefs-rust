use diesel_derive_enum::DbEnum;
use chrono::NaiveDateTime;

use crate::database::schema::filesystem;
use crate::database::schema::index_state;

#[derive(Debug, DbEnum, Hash, Eq, PartialEq)]
pub enum EntryType {
    Drive,
    File,
    Directory,
}

#[derive(Debug, DbEnum)]
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

#[derive(Debug, Queryable, Insertable, QueryableByName)]
#[table_name = "index_state"]
pub struct IndexState {
    pub drive_id: String,
    pub page_token: i64,
}
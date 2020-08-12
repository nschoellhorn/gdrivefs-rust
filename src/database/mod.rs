use crate::database::schema::filesystem;
use crate::database::schema::filesystem::dsl::*;
use crate::filesystem as fs;

use chrono::NaiveDateTime;
use diesel::dsl::{exists, max};
use diesel::{select, SqliteConnection};
use diesel_derive_enum::DbEnum;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::sync::Mutex;
use anyhow::Result;

use diesel::prelude::*;

mod schema;

#[derive(Debug, DbEnum, Hash, Eq, PartialEq)]
pub enum EntryType {
    File,
    Directory,
}

#[derive(Debug, DbEnum)]
pub enum RemoteType {
    TeamDrive,
    Directory,
    File,
}

#[derive(Debug, Queryable, Insertable)]
#[table_name = "filesystem"]
pub struct FilesystemEntry {
    pub id: String,
    pub name: String,
    pub entry_type: EntryType,
    pub created_at: NaiveDateTime,
    pub last_modified_at: NaiveDateTime,
    pub remote_type: Option<RemoteType>,
    pub inode: Option<i64>,
    pub size: i64,
    pub parent_id: Option<String>,
}

pub struct FilesystemRepository {
    connection: Mutex<SqliteConnection>,
}

impl FilesystemRepository {
    pub(crate) fn new(connection: SqliteConnection) -> Self {
        Self {
            connection: Mutex::new(connection),
        }
    }

    pub(crate) fn get_largest_inode(&self) -> i64 {
        let largest_inode: Option<i64> = filesystem
            .select(max(inode))
            .first(&*self.connection.lock().unwrap())
            .unwrap();

        largest_inode.unwrap_or(0)
    }

    pub(crate) fn find_parent_id(&self, i: String) -> Option<String> {
        filesystem
            .select(parent_id)
            .filter(id.eq(i))
            .first::<Option<String>>(&*self.connection.lock().unwrap())
            .optional()
            .expect("Unable to search for parent inode")
            .flatten()
    }

    pub(crate) fn find_inode_by_remote_id(&self, rid: &str) -> Option<i64> {
        filesystem
            .select(inode)
            .filter(remote_id.eq(rid))
            .limit(1)
            .first::<i64>(&*self.connection.lock().unwrap())
            .optional()
            .expect("Error searching filesystem entry")
    }

    pub(crate) fn get_remote_inode_mapping(&self) -> HashMap<String, i64> {
        filesystem
            .select((remote_id, inode))
            .filter(remote_id.is_not_null())
            .load::<(Option<String>, i64)>(&*self.connection.lock().unwrap())
            .expect("Unable to load cached remote inode mapping")
            .into_iter()
            .filter(|(rid, _)| rid.is_some())
            .map(|(rid, i)| (rid.unwrap(), i))
            .collect()
    }

    pub(crate) fn find_entry_as_child(
        &self,
        parent_i: u64,
        entry_name: &OsStr,
    ) -> Option<FilesystemEntry> {
        let real_str = entry_name.to_str().unwrap();

        filesystem
            .filter(parent_inode.eq(parent_i as i64))
            .filter(name.eq(real_str))
            .first::<FilesystemEntry>(&*self.connection.lock().unwrap())
            .optional()
            .expect("Error searching filesystem entry")
    }

    pub(crate) fn find_entry_for_inode(&self, i: u64) -> Option<FilesystemEntry> {
        filesystem
            .filter(inode.eq(i as i64))
            .first::<FilesystemEntry>(&*self.connection.lock().unwrap())
            .optional()
            .expect("Error searching filesystem entry")
    }

    pub(crate) fn find_entry_by_name_and_parent_inode(
        &self,
        _name: &str,
        _parent_inode: i64,
    ) -> Option<FilesystemEntry> {
        filesystem
            .filter(name.eq(&_name))
            .filter(parent_inode.eq(_parent_inode))
            .limit(1)
            .first::<FilesystemEntry>(&*self.connection.lock().unwrap())
            .optional()
            .expect("Error searching filesystem entry")
    }

    pub(crate) fn find_all_entries_in_parent(&self, parent_i: u64) -> Vec<FilesystemEntry> {
        filesystem
            .filter(parent_inode.eq(parent_i as i64))
            .order_by(inode)
            .load::<FilesystemEntry>(&*self.connection.lock().unwrap())
            .expect("Error searching filesystem entry")
    }

    pub(crate) fn inode_exists(&self, i: u64) -> bool {
        if i == fs::ROOT_INODE {
            return true;
        }

        select(exists(filesystem.filter(inode.eq(i as i64))))
            .get_result(&*self.connection.lock().unwrap())
            .expect("SQL Query went sideways.")
    }

    pub(crate) fn create_entry(&self, fs_entry: &FilesystemEntry) {
        diesel::insert_into(filesystem::table)
            .values(fs_entry)
            .execute(&*self.connection.lock().unwrap())
            .expect("Unable to insert new entry");
    }

    pub(crate) fn remove_entry_by_remote_id(&self, rid: &str) -> Result<()> {
        diesel::delete(filesystem::table)
            .filter(remote_id.eq(rid))
            .execute(&*self.connection.lock().unwrap())?;

        Ok(())
    }
}

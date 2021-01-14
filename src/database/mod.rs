use std::collections::HashMap;
use std::ffi::OsStr;

use anyhow::Result;
use chrono::NaiveDateTime;
use diesel::dsl::{exists, max};
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::{select, SqliteConnection};
use diesel_derive_enum::DbEnum;

use crate::database::schema::filesystem::dsl::*;
use crate::{database::schema::filesystem, drive_client::File};

pub(crate) mod connection;
mod schema;

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
    pub remote_type: Option<RemoteType>,
    pub inode: i64,
    pub size: i64,
    pub parent_id: Option<String>,
    pub parent_inode: Option<i64>,
}

#[derive(Clone)]
pub struct FilesystemRepository {
    connection: Pool<ConnectionManager<SqliteConnection>>,
}

impl FilesystemRepository {
    pub(crate) fn new(pool: Pool<ConnectionManager<SqliteConnection>>) -> Self {
        Self { connection: pool }
    }

    pub(crate) fn get_largest_inode(&self) -> i64 {
        let largest_inode: Option<i64> = filesystem
            .select(max(inode))
            .first(&self.connection.get().unwrap())
            .unwrap();

        largest_inode.unwrap_or(0)
    }

    pub(crate) fn transaction<T, E, F>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Result<T, E>,
        E: From<diesel::result::Error>,
    {
        let connection = self.connection.get().unwrap();
        connection.transaction(f)
    }

    pub(crate) fn get_all_drive_ids(&self) -> Vec<String> {
        filesystem
            .select(id)
            .filter(entry_type.eq(EntryType::Drive))
            .load::<String>(&self.connection.get().unwrap())
            .expect("Unable to fetch known drives")
    }

    pub(crate) fn find_parent_id(&self, i: String) -> Option<String> {
        filesystem
            .select(parent_id)
            .filter(id.eq(i))
            .first::<Option<String>>(&self.connection.get().unwrap())
            .optional()
            .expect("Unable to search for parent inode")
            .flatten()
    }

    pub(crate) fn find_inode_by_remote_id(&self, rid: &str) -> Option<i64> {
        filesystem
            .select(inode)
            .filter(id.eq(rid))
            .limit(1)
            .first::<i64>(&self.connection.get().unwrap())
            .optional()
            .expect("Error searching filesystem entry")
    }

    pub(crate) fn get_remote_inode_mapping(&self) -> HashMap<String, i64> {
        filesystem
            .select((id, inode))
            .load::<(String, i64)>(&self.connection.get().unwrap())
            .expect("Unable to load cached remote inode mapping")
            .into_iter()
            .collect()
    }

    pub(crate) fn find_parent_inode(&self, child_i: i64) -> Option<i64> {
        diesel::sql_query(
            "select parent.* from filesystem as parent
                                join filesystem as child on (child.parent_id = parent.id)
                                where child.inode = ?",
        )
        .bind::<diesel::sql_types::BigInt, _>(child_i)
        .load::<FilesystemEntry>(&self.connection.get().unwrap())
        .optional()
        .expect("Error searching filesystem entry")
        .map(|vec| vec.into_iter().nth(0))
        .flatten()
        .map(|entry| entry.inode)
    }

    pub(crate) fn update_entry_by_inode(
        &self,
        current_inode: i64,
        new_size: i64,
        parent_remote_id: Option<String>,
    ) -> Result<usize> {
        Ok(diesel::update(filesystem.filter(inode.eq(current_inode)))
            .set((size.eq(new_size), parent_id.eq(parent_remote_id)))
            .execute(&self.connection.get().unwrap())?)
    }

    pub(crate) fn find_entry_as_child(
        &self,
        parent_i: i64,
        entry_name: &OsStr,
    ) -> Option<FilesystemEntry> {
        let real_str = entry_name.to_str().unwrap();

        diesel::sql_query("SELECT * FROM filesystem WHERE parent_inode = ? AND name = ?")
            .bind::<diesel::sql_types::BigInt, _>(parent_i)
            .bind::<diesel::sql_types::Text, _>(real_str)
            .load::<FilesystemEntry>(&self.connection.get().unwrap())
            .optional()
            .expect("Error searching filesystem entry")
            .map(|vec| vec.into_iter().nth(0))
            .flatten()
    }

    pub(crate) fn find_entry_for_inode(&self, i: u64) -> Option<FilesystemEntry> {
        filesystem
            .filter(inode.eq(i as i64))
            .first::<FilesystemEntry>(&self.connection.get().unwrap())
            .optional()
            .expect("Error searching filesystem entry")
    }

    pub(crate) fn find_all_entries_in_parent(&self, parent_i: u64) -> Vec<FilesystemEntry> {
        diesel::sql_query(
            "select child.* from filesystem as child
                                where child.parent_inode = ?",
        )
        .bind::<diesel::sql_types::BigInt, _>(parent_i as i64)
        .load::<FilesystemEntry>(&self.connection.get().unwrap())
        .expect("Error searching filesystem entry")
    }

    pub(crate) fn inode_exists(&self, i: u64) -> bool {
        select(exists(filesystem.filter(inode.eq(i as i64))))
            .get_result(&self.connection.get().unwrap())
            .expect("SQL Query went sideways.")
    }

    pub(crate) fn create_entry(&self, fs_entry: &FilesystemEntry) {
        diesel::insert_into(filesystem::table)
            .values(fs_entry)
            .execute(&self.connection.get().unwrap())
            .expect(&format!(
                "Unable to insert new entry: {}#{}",
                fs_entry.name, fs_entry.id
            ));
    }

    pub(crate) fn find_entry_by_id(&self, rid: &str) -> Option<FilesystemEntry> {
        filesystem
            .filter(id.eq(rid))
            .first::<FilesystemEntry>(&self.connection.get().unwrap())
            .optional()
            .unwrap()
    }

    pub(crate) fn set_parent_inode_by_parent_id(&self, p_id: &str, p_inode: i64) -> Result<usize> {
        let target = filesystem::table
            .filter(parent_id.eq(p_id))
            .filter(parent_inode.is_null());
        let affected_rows = diesel::update(target)
            .set(parent_inode.eq(p_inode))
            .execute(&self.connection.get().unwrap())?;

        Ok(affected_rows)
    }

    pub(crate) fn remove_entry_by_remote_id(&self, rid: &str) -> Result<()> {
        diesel::delete(filesystem::table)
            .filter(id.eq(rid))
            .execute(&self.connection.get().unwrap())?;

        Ok(())
    }
}

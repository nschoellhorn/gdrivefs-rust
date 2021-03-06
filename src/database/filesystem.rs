use std::ffi::OsStr;

use anyhow::Result;
use chrono::NaiveDateTime;
use diesel::dsl::{exists, max};
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::{select, SqliteConnection};

use crate::database::schema::filesystem;
use crate::database::schema::filesystem::dsl::*;

use super::entity::{FilesystemEntry, RemoteType};

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

    pub(crate) fn get_all_shared_drive_ids(&self) -> Vec<String> {
        filesystem
            .select(id)
            .filter(remote_type.eq(RemoteType::TeamDrive))
            .load::<String>(&self.connection.get().unwrap())
            .expect("Unable to fetch known drives")
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
            .map(|vec| vec.into_iter().next())
            .flatten()
    }

    pub(crate) fn update_mode_by_inode(&self, ino: i64, new_mode: i32) -> Result<usize> {
        Ok(diesel::update(filesystem.filter(inode.eq(ino)))
            .set(mode.eq(new_mode))
            .execute(&self.connection.get().unwrap())?)
    }

    pub(crate) fn update_size_by_inode(&self, ino: i64, new_size: i64) -> Result<usize> {
        Ok(diesel::update(filesystem.filter(inode.eq(ino)))
            .set(size.eq(new_size))
            .execute(&self.connection.get().unwrap())?)
    }

    pub(crate) fn update_last_access_by_inode(
        &self,
        ino: i64,
        access_time: NaiveDateTime,
    ) -> Result<usize> {
        Ok(diesel::update(filesystem.filter(inode.eq(ino)))
            .set(last_accessed_at.eq(access_time))
            .execute(&self.connection.get().unwrap())?)
    }

    pub(crate) fn update_last_modification_by_inode(
        &self,
        ino: i64,
        modification_time: NaiveDateTime,
    ) -> Result<usize> {
        Ok(diesel::update(filesystem.filter(inode.eq(ino)))
            .set(last_modified_at.eq(modification_time))
            .execute(&self.connection.get().unwrap())?)
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
        diesel::insert_into(filesystem)
            .values(fs_entry)
            .execute(&self.connection.get().unwrap())
            .unwrap_or_else(|_| {
                panic!(
                    "Unable to insert new entry: {}#{}",
                    fs_entry.name, fs_entry.id
                )
            });
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

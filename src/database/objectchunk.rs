use anyhow::Result;
use diesel::dsl::{exists, max};
use diesel::prelude::*;
use diesel::{
    r2d2::{ConnectionManager, Pool},
    select, SqliteConnection,
};

use super::entity::{NewObjectChunk, ObjectChunk};
use super::schema::object_chunk::dsl::*;

pub(crate) struct ObjectCacheChunkRepository {
    connection: Pool<ConnectionManager<SqliteConnection>>,
}

impl ObjectCacheChunkRepository {
    pub fn new(connection: Pool<ConnectionManager<SqliteConnection>>) -> Self {
        Self { connection }
    }

    pub fn delete_chunk(&self, chunk_id: i64) {
        diesel::delete(object_chunk.filter(id.eq(chunk_id)))
            .execute(&self.connection.get().unwrap())
            .expect("Failed to delete chunk");
    }

    pub fn mark_all_chunks_dirty_for_file(&self, rid: &str) {
        diesel::update(object_chunk.filter(file_id.eq(rid)))
            .set((is_dirty.eq(true), is_complete.eq(true)))
            .execute(&self.connection.get().unwrap())
            .expect("Failed to update chunks");
    }

    pub fn find_chunks_by_file_id(&self, rid: &str) -> Vec<ObjectChunk> {
        object_chunk
            .filter(file_id.eq(rid))
            .load::<ObjectChunk>(&self.connection.get().unwrap())
            .expect("Unable to fetch chunks for given file id.")
    }

    pub fn find_next_incomplete_chunk(&self) -> Option<ObjectChunk> {
        object_chunk
            .filter(is_complete.eq(false))
            .first::<ObjectChunk>(&self.connection.get().unwrap())
            .optional()
            .expect("Failed to fetch incomplete chunks")
    }

    pub fn mark_chunk_clean(&self, chunk_id: i64) {
        diesel::update(object_chunk.filter(id.eq(chunk_id)))
            .set(is_dirty.eq(false))
            .execute(&self.connection.get().unwrap())
            .expect("Failed to mark chunk dirty.");
    }

    pub fn has_incomplete_chunks(&self) -> bool {
        select(exists(object_chunk.filter(is_complete.eq(false))))
            .get_result(&self.connection.get().unwrap())
            .expect("Failed to check for incomplete chunks")
    }

    pub fn has_incomplete_chunks_by_file(&self, rid: &str) -> bool {
        select(exists(
            object_chunk
                .filter(is_complete.eq(false))
                .filter(file_id.eq(rid)),
        ))
        .get_result(&self.connection.get().unwrap())
        .expect("Failed to check for incomplete chunks")
    }

    pub fn find_chunk_by_id(&self, chunk_id: i64) -> Option<ObjectChunk> {
        object_chunk
            .filter(id.eq(chunk_id))
            .first::<ObjectChunk>(&self.connection.get().unwrap())
            .optional()
            .unwrap()
    }

    pub fn has_dirty_chunks(&self) -> bool {
        select(exists(object_chunk.filter(is_dirty.eq(true))))
            .get_result(&self.connection.get().unwrap())
            .expect("Failed to check for incomplete chunks")
    }

    pub(crate) fn set_chunk_complete(&self, chunk_id: i64) -> Result<usize> {
        Ok(diesel::update(object_chunk.filter(id.eq(chunk_id)))
            .set(is_complete.eq(true))
            .execute(&self.connection.get().unwrap())?)
    }

    pub fn find_dirty_files(&self) -> Vec<String> {
        object_chunk
            .filter(is_dirty.eq(true))
            .select(file_id)
            .distinct()
            .load::<String>(&self.connection.get().unwrap())
            .expect("Failed to find dirty files.")
    }

    pub fn get_max_sequence_for_file(&self, rid: &str) -> i32 {
        let largest_index: Option<i32> = object_chunk
            .filter(file_id.eq(rid))
            .select(max(chunk_sequence))
            .first(&self.connection.get().unwrap())
            .unwrap();

        largest_index.unwrap_or(0)
    }

    pub fn insert_chunks(&self, chunks: Vec<NewObjectChunk>) {
        let connection = self.connection.get().unwrap();
        let _ = connection.transaction::<_, anyhow::Error, _>(|| {
            chunks.into_iter().for_each(|chunk| {
                diesel::insert_into(object_chunk)
                    .values(chunk)
                    .execute(&connection)
                    .expect("Failed to insert chunk");
            });

            Ok(())
        });
    }

    pub fn update_chunk_byte_to(&self, chunk_id: i64, to: i64) {
        diesel::update(object_chunk.filter(id.eq(chunk_id)))
            .set(byte_to.eq(to))
            .execute(&self.connection.get().unwrap())
            .expect("Failed to update byte_to for chunk");
    }

    pub fn find_chunks_from_byte(&self, rid: &str, from: i64) -> Vec<ObjectChunk> {
        diesel::sql_query(
            r#"
select * from object_chunk where byte_from >= (
	select byte_from from object_chunk
	    where file_id = ?
	    and byte_from <= ? 
	    and (byte_from - ?) >= 0
	    order by abs(byte_from - ?) limit 1
) and file_id = ?;
        "#,
        )
        .bind::<diesel::sql_types::Text, _>(rid)
        .bind::<diesel::sql_types::BigInt, _>(from)
        .bind::<diesel::sql_types::BigInt, _>(from)
        .bind::<diesel::sql_types::BigInt, _>(from)
        .bind::<diesel::sql_types::Text, _>(rid)
        .load::<ObjectChunk>(&self.connection.get().unwrap())
        .expect("Unable to find chunks for given range.")
    }

    pub fn find_exact_chunks_for_range(&self, rid: &str, from: i64, to: i64) -> Vec<ObjectChunk> {
        diesel::sql_query(
            r#"
select * from object_chunk where id between (
  select id from object_chunk where byte_from <= ? and file_id = ? order by abs(byte_from - ?) asc
) and (
  select id from object_chunk where byte_to >= ? and file_id = ? order by abs(byte_to - ?) asc
);
        "#,
        )
        .bind::<diesel::sql_types::BigInt, _>(from)
        .bind::<diesel::sql_types::Text, _>(rid)
        .bind::<diesel::sql_types::BigInt, _>(from)
        .bind::<diesel::sql_types::BigInt, _>(to)
        .bind::<diesel::sql_types::Text, _>(rid)
        .bind::<diesel::sql_types::BigInt, _>(to)
        .load::<ObjectChunk>(&self.connection.get().unwrap())
        .expect("Unable to find chunks for given range.")
    }

    pub fn find_chunk_by_object_name(&self, name: &str) -> Option<ObjectChunk> {
        object_chunk
            .filter(object_name.eq(name))
            .first(&self.connection.get().unwrap())
            .optional()
            .expect("Unable to fetch chunk from database")
    }
}

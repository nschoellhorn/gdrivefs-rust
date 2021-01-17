use anyhow::Result;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};

use crate::database::schema::index_state;
use crate::database::schema::index_state::dsl::*;

use super::entity::IndexState;

#[derive(Clone)]
pub struct IndexStateRepository {
    connection: Pool<ConnectionManager<SqliteConnection>>,
}

impl IndexStateRepository {
    pub fn new(pool: Pool<ConnectionManager<SqliteConnection>>) -> Self {
        Self { connection: pool }
    }

    pub fn init_state(&self, _drive_id: &str) -> Result<usize> {
        Ok(diesel::insert_into(index_state)
            .values(IndexState {
                drive_id: _drive_id.to_string(),
                page_token: 1,
            })
            .execute(&self.connection.get().unwrap())?)
    }

    pub fn reset_all(&self) -> Result<usize> {
        Ok(diesel::update(index_state)
            .set(page_token.eq(1))
            .execute(&self.connection.get().unwrap())?)
    }

    pub fn update_token_for_drive(&self, _drive_id: &str, new_token: &str) -> Result<usize> {
        Ok(diesel::update(index_state.filter(drive_id.eq(_drive_id)))
            .set(page_token.eq(new_token.parse::<i64>().unwrap()))
            .execute(&self.connection.get().unwrap())?)
    }

    pub fn get_all_states(&self) -> Vec<IndexState> {
        index_state
            .load::<IndexState>(&self.connection.get().unwrap())
            .unwrap()
    }
}

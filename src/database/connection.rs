use diesel::connection::SimpleConnection;
#[derive(Debug)]
pub struct ConnectionOptions {
    pub enable_wal: bool,
    pub enable_foreign_keys: bool,
    pub busy_timeout: Option<chrono::Duration>,
}

impl diesel::r2d2::CustomizeConnection<diesel::SqliteConnection, diesel::r2d2::Error>
    for ConnectionOptions
{
    fn on_acquire(&self, conn: &mut diesel::SqliteConnection) -> Result<(), diesel::r2d2::Error> {
        (|| {
            if self.enable_wal {
                conn.batch_execute("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
            }
            if self.enable_foreign_keys {
                conn.batch_execute("PRAGMA foreign_keys = ON;")?;
            }
            if let Some(d) = self.busy_timeout {
                conn.batch_execute(&format!("PRAGMA busy_timeout = {};", d.num_milliseconds()))?;
            }
            Ok(())
        })()
        .map_err(diesel::r2d2::Error::QueryError)
    }
}

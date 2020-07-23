#[macro_use]
extern crate lazy_static;

use crate::drive_client::DriveClient;
use anyhow::Result;

mod drive_client;

lazy_static! {
    static ref CREDENTIALS_PATH: &'static str = "clientCredentials.json";
}

#[tokio::main]
async fn main() -> Result<()> {
    let drive_client = DriveClient::create(*CREDENTIALS_PATH).await?;

    let full_index_task = drive_client.process_all_files(|file| {
        dbg!(file);
    });

    full_index_task.await?;

    Ok(())
}

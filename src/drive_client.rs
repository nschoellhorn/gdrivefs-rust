use std::cell::Cell;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use reqwest::{header, Client, RequestBuilder};
use serde::{Deserialize, Serialize};
use yup_oauth2::authenticator::Authenticator;
use yup_oauth2::{InstalledFlowAuthenticator, InstalledFlowReturnMethod};

use crate::{config::Config, database::entity::RemoteType};

lazy_static! {
    static ref SCOPES: [&'static str; 1] = ["https://www.googleapis.com/auth/drive",];
    static ref GDRIVE_BASE_URL: &'static str = "https://www.googleapis.com/drive/v3";
    static ref GDRIVE_UPLOAD_BASE_URL: &'static str = "https://www.googleapis.com/upload/drive/v3";
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FileList {
    pub files: Vec<File>,
    pub next_page_token: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct DriveList {
    pub drives: Vec<Drive>,
    pub next_page_token: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Change {
    pub r#type: String,
    pub change_type: String,
    pub time: DateTime<Utc>,
    pub removed: bool,
    pub file_id: Option<String>,
    pub file: Option<File>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ChangeList {
    pub next_page_token: Option<String>,
    pub new_start_page_token: Option<String>,
    pub changes: Vec<Change>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct File {
    pub id: String,
    pub name: String,
    #[serde(default = "Vec::new")]
    pub parents: Vec<String>,
    pub created_time: DateTime<Utc>,
    pub modified_time: DateTime<Utc>,
    pub mime_type: String,
    pub size: Option<String>,
    pub trashed: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FileCreateRequest {
    pub created_time: DateTime<Utc>,
    pub modified_time: DateTime<Utc>,
    pub name: String,
    pub parents: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PrepareResumableUploadRequest {
    id: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Drive {
    pub id: String,
    pub name: String,
    #[serde(alias = "colorRgb")]
    pub color: String,
    pub created_time: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
struct StartPageToken {
    start_page_token: String,
}

pub struct DriveClient {
    http_client: Client,
    blocking_client: reqwest::blocking::Client,
    authenticator: Authenticator<HttpsConnector<HttpConnector>>,
    token: Arc<Mutex<Cell<String>>>,
    config: Config,
}

impl DriveClient {
    pub async fn create(
        credentials_path: &str,
        blocking_client: reqwest::blocking::Client,
        config: Config,
    ) -> Result<Self> {
        // Read the application secret
        let secret = yup_oauth2::read_application_secret(credentials_path)
            .await
            .context("Unable to read credentials file.")?;

        let auth =
            InstalledFlowAuthenticator::builder(secret, InstalledFlowReturnMethod::HTTPRedirect)
                .persist_tokens_to_disk("tokencache.json")
                .build()
                .await
                .context("Unable to write token to disk for caching purposes")?;

        Ok(Self {
            http_client: Client::new(),
            blocking_client,
            authenticator: auth,
            token: Arc::new(Mutex::new(Cell::new(String::new()))),
            config,
        })
    }

    async fn get_token(&self) -> Result<String> {
        let token = self
            .authenticator
            .token(&*SCOPES)
            .await
            .context("Unable to get auth token")?;

        let token = String::from(token.as_str());

        self.token.lock().unwrap().set(token.clone());

        Ok(token)
    }

    async fn get_authenticated(&self, url: &str) -> Result<RequestBuilder> {
        let token = self.get_token().await?;
        Ok(self
            .http_client
            .get(format!("{}{}", *GDRIVE_BASE_URL, url).as_str())
            .header(header::AUTHORIZATION, format!("Bearer {}", token)))
    }

    fn post_authenticated_blocking<T>(
        &self,
        url: &str,
        body: &T,
    ) -> reqwest::blocking::RequestBuilder
    where
        T: Serialize + ?Sized,
    {
        let lock = self.token.lock().unwrap();
        let token = lock.take();
        lock.set(token.clone());

        self.blocking_client
            .post(format!("{}{}", *GDRIVE_BASE_URL, url).as_str())
            .json(body)
            .header(header::AUTHORIZATION, format!("Bearer {}", token))
    }

    fn upload_authenticated_blocking(
        &self,
        url: &str,
        body: Vec<u8>,
    ) -> reqwest::blocking::RequestBuilder {
        let lock = self.token.lock().unwrap();
        let token = lock.take();
        lock.set(token.clone());

        self.blocking_client
            .patch(format!("{}{}", *GDRIVE_UPLOAD_BASE_URL, url).as_str())
            .body(body)
            .header(header::AUTHORIZATION, format!("Bearer {}", token))
    }

    fn delete_authenticated_blocking(&self, url: &str) -> reqwest::blocking::RequestBuilder {
        let lock = self.token.lock().unwrap();
        let token = lock.take();
        lock.set(token.clone());

        self.blocking_client
            .delete(format!("{}{}", *GDRIVE_BASE_URL, url).as_str())
            .header(header::AUTHORIZATION, format!("Bearer {}", token))
    }

    pub fn create_file(&self, create_request: FileCreateRequest) -> Result<File> {
        Ok(self
            .post_authenticated_blocking("/files", &create_request)
            .query(&vec![
                ("supportsAllDrives", "true"),
                (
                    "fields",
                    "id, name, mimeType, parents, createdTime, modifiedTime, size, trashed",
                ),
            ])
            .send()?
            .json::<File>()?)
    }

    pub fn prepare_resumable_upload(&self, file_id: &str) -> Result<String> {
        let lock = self.token.lock().unwrap();
        let token = lock.take();
        lock.set(token.clone());

        // Make sure to free the lock before we send the request to avoid deadlocks or performance
        // issues
        drop(lock);

        let request = self
            .blocking_client
            .patch(format!("{}{}/{}", *GDRIVE_UPLOAD_BASE_URL, "/files", file_id).as_str())
            .header(header::AUTHORIZATION, format!("Bearer {}", token))
            .query(&vec![
                ("uploadType", "resumable"),
                ("supportsAllDrives", "true"),
            ])
            .header(header::CONTENT_LENGTH, "0");

        let response = request.send()?;

        let response_headers = response.headers();

        let upload_url = response_headers
            .get(header::LOCATION)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        // we use a very primitive way of extracing the upload id. Should be fine and we dont need
        // a new crate for this. Might be smart to change this to be more reliable when it first
        // breaks.
        //
        // URL format is something like this: https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable&upload_id=xa298sd_sdlkj2
        let upload_id = upload_url.rsplit("=").next().unwrap();
        Ok(upload_id.to_string())
    }

    pub fn write_file_resumable(&self, upload_id: &str, offset: u64, data: &[u8]) -> Result<()> {
        let lock = self.token.lock().unwrap();
        let token = lock.take();
        lock.set(token.clone());

        // Make sure to free the lock before we send the request to avoid deadlocks or performance
        // issues
        drop(lock);

        let response = self
            .blocking_client
            .put(format!("{}{}", *GDRIVE_UPLOAD_BASE_URL, "/files").as_str())
            .query(&vec![("uploadType", "resumable"), ("upload_id", upload_id)])
            .header(header::CONTENT_LENGTH, &format!("{}", data.len()))
            .header(
                header::RANGE,
                &format!("bytes {}-{}/*", offset, offset + data.len() as u64 - 1),
            )
            .header(header::AUTHORIZATION, format!("Bearer {}", token))
            .query(&vec![
                ("uploadType", "resumable"),
                ("supportsAllDrives", "true"),
            ])
            .body(data.to_vec())
            .send()?;

        Ok(())
    }

    pub fn delete_file(&self, file_id: &str) -> Result<()> {
        self.delete_authenticated_blocking(&format!("/files/{}", file_id))
            .query(&vec![("supportsAllDrives", "true")])
            .send()?;

        Ok(())
    }

    pub async fn get_change_list(
        &self,
        page_token: &str,
        drive_id: &str,
        remote_type: RemoteType,
    ) -> Result<ChangeList> {
        let response = self
            .get_authenticated("/changes")
            .await?
            .query(&self.get_change_list_params(page_token, drive_id, remote_type.clone()))
            .send()
            .await;

        if response.is_err() {
            let err = response.unwrap_err();
            dbg!(&err);
            return Err(anyhow::Error::new(err));
        }

        let response = response.unwrap();

        if !response.status().is_success() {
            dbg!(response.text().await?);
            return Err(anyhow::Error::msg("Something went wrong"));
        }

        let parsed_response = response.json::<ChangeList>().await?;

        // This is another quirk of the Google Drive API. If items are trashed, no matter if they were in
        //  a team drive or the own drive, they will be displayed for the changes to "My Drive". So we need to filter
        //  out everything that's in trash right now. This needs to be changed if we want to support trash folder
        //  some day, but for now, that's not a problem.
        let result = match remote_type {
            RemoteType::OwnDrive => {
                let filtered_changes = parsed_response
                    .changes
                    .into_iter()
                    .filter(|change| {
                        change.file.is_some() && !change.file.as_ref().unwrap().trashed
                    })
                    .collect();

                ChangeList {
                    changes: filtered_changes,
                    ..parsed_response
                }
            }
            _ => parsed_response,
        };

        Ok(result)
    }

    fn get_change_list_params<'a>(
        &self,
        page_token: &'a str,
        drive_id: &'a str,
        remote_type: RemoteType,
    ) -> Vec<(&'a str, String)> {
        match remote_type {
            // To get the changes for "My Drive", we need to call the API slightly differently
            RemoteType::OwnDrive => vec![
                ("pageToken", page_token.to_string()),
                ("restrictToMyDrive", "true".to_string()),
                ("spaces", "drive".to_string()),
                ("fields", "nextPageToken, newStartPageToken, changes(type, changeType, time, removed, fileId, file(id, name, mimeType, parents, createdTime, modifiedTime, size, trashed))".to_string()),
                ("pageSize", format!("{}", self.config.indexing.fetch_size)),
            ],
            _ => vec![
                ("pageToken", page_token.to_string()),
                ("includeItemsFromAllDrives", "true".to_string()),
                ("supportsAllDrives", "true".to_string()),
                ("driveId", drive_id.to_string()),
                ("fields", "nextPageToken, newStartPageToken, changes(type, changeType, time, removed, fileId, file(id, name, mimeType, parents, createdTime, modifiedTime, size, trashed))".to_string()),
                ("pageSize", format!("{}", self.config.indexing.fetch_size)),
            ]
        }
    }

    pub async fn get_file_meta(&self, file_id: &str) -> Result<File> {
        Ok(self
            .get_authenticated(&format!("/files/{}", file_id))
            .await?
            .query(&vec![(
                "fields",
                "id, name, mimeType, parents, createdTime, modifiedTime, size, trashed",
            )])
            .send()
            .await?
            .json()
            .await?)
    }

    pub async fn get_file_content(
        &self,
        file_id: &str,
        byte_from: u64,
        byte_to: u64,
    ) -> Result<bytes::Bytes> {
        let request_url = format!("/files/{}", file_id);
        dbg!(&request_url);
        let mut request = self.get_authenticated(request_url.as_str()).await?;
        let params = vec![
            ("alt".to_string(), "media".to_string()),
            ("supportsAllDrives".to_string(), "true".to_string()),
        ];

        request = request
            .header("Range", format!("bytes={}-{}", byte_from, byte_to).as_str())
            .query(&params);

        let response = request
            .send()
            .await
            .context("Network failure or something")?;
        if !response.status().is_success() {
            dbg!(response.text().await?);
            return Err(anyhow::Error::msg("Something went wrong"));
        }

        Ok(response.bytes().await?)
    }

    pub async fn get_drives(&self) -> Result<Vec<Drive>> {
        let request = self.get_authenticated("/drives").await?;
        let params = vec![
            ("pageSize".to_string(), "100".to_string()),
            ("fields".to_string(), "*".to_string()),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let drive_list: DriveList = request.query(&params).send().await?.json().await?;

        Ok(drive_list.drives)
    }
}

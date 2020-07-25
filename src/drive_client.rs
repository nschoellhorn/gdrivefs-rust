use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::future::{FutureExt, LocalBoxFuture};
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use reqwest::{header, Client, RequestBuilder};
use std::collections::HashMap;
use std::future::Future;
use yup_oauth2::authenticator::Authenticator;
use yup_oauth2::{InstalledFlowAuthenticator, InstalledFlowReturnMethod};

use serde::{Deserialize, Serialize};

lazy_static! {
    static ref SCOPES: [&'static str; 1] = ["https://www.googleapis.com/auth/drive",];
    static ref GDRIVE_BASE_URL: &'static str = "https://www.googleapis.com/drive/v3";
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FileList {
    pub files: Vec<File>,
    #[serde(alias = "nextPageToken")]
    pub next_page_token: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DriveList {
    pub drives: Vec<Drive>,
    #[serde(alias = "nextPageToken")]
    pub next_page_token: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct File {
    pub id: String,
    pub name: String,
    pub parents: Vec<String>,
    #[serde(alias = "createdTime")]
    pub created_time: DateTime<Utc>,
    #[serde(alias = "modifiedTime")]
    pub modified_time: DateTime<Utc>,
    #[serde(alias = "mimeType")]
    pub mime_type: String,
    pub size: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Drive {
    pub id: String,
    pub name: String,
    #[serde(alias = "colorRgb")]
    pub color: String,
    #[serde(alias = "createdTime")]
    pub created_time: DateTime<Utc>,
}

pub struct DriveClient {
    http_client: Client,
    authenticator: Authenticator<HttpsConnector<HttpConnector>>,
}

impl DriveClient {
    pub async fn create(credentials_path: &str) -> Result<Self> {
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
            authenticator: auth,
        })
    }

    async fn get_token(&self) -> Result<String> {
        let token = self
            .authenticator
            .token(&*SCOPES)
            .await
            .context("Unable to get auth token")?;
        Ok(String::from(token.as_str()))
    }

    async fn get_authenticated(&self, url: &str) -> Result<RequestBuilder> {
        let token = self.get_token().await?;
        //dbg!(&token);
        Ok(self
            .http_client
            .get(format!("{}{}", *GDRIVE_BASE_URL, url).as_str())
            .header(header::AUTHORIZATION, format!("Bearer {}", token)))
    }

    pub async fn get_file_content(
        &self,
        file_id: &str,
        byte_from: u64,
        byte_to: u64,
    ) -> Result<bytes::Bytes> {
        let mut request = self
            .get_authenticated(format!("/files/{}", file_id).as_str())
            .await?;
        let params = vec![
            ("alt".to_string(), "media".to_string()),
            ("supportsAllDrives".to_string(), "true".to_string()),
        ];

        request = request
            .header("Range", format!("bytes={}-{}", byte_from, byte_to).as_str())
            .query(&params);

        Ok(request.send().await?.bytes().await?)
    }

    pub async fn get_files_in_parent(&self, parent_id: &str) -> Result<Vec<File>> {
        let mut all_files = vec![];

        let mut has_more = true;
        let mut page_token: Option<String> = None;

        let query = format!("\"{}\" in parents", parent_id);

        // instead of making the user responsible of fetching all pages, we just give him a vector with all the files.
        // this means, that we need to do the page handling ourselves.
        while has_more {
            let mut request = self.get_authenticated("/files").await?;
            let mut params = vec![
                ("supportsAllDrives".to_string(), "true".to_string()),
                ("corpora".to_string(), "allDrives".to_string()),
                ("includeItemsFromAllDrives".to_string(), "true".to_string()),
                (
                    "orderBy".to_string(),
                    "folder desc, createdTime".to_string(),
                ),
                (
                    "fields".to_string(),
                    "files(id, name, mimeType, parents, createdTime, modifiedTime, size)"
                        .to_string(),
                ),
                ("pageSize".to_string(), "1000".to_string()),
                ("q".to_string(), query.clone()),
            ]
            .into_iter()
            .collect::<HashMap<_, _>>();

            if let Some(tok) = page_token.clone() {
                params.insert("pageToken".to_string(), tok);
            }

            request = request.query(&params);
            let mut files: FileList = request.send().await?.json().await?;
            all_files.append(files.files.as_mut());

            if let Some(new_page_token) = files.next_page_token {
                has_more = true;
                page_token = Some(new_page_token);
            } else {
                has_more = false;
            }
        }

        Ok(all_files)
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

    pub async fn process_all_files<'a, F>(&'a self, callback: impl Fn(File) -> F) -> Result<()>
    where
        F: Future<Output = Result<()>>,
    {
        let drives = self.get_drives().await?;
        let callback_ref = &callback;

        let mut tasks = vec![];
        for drive in drives {
            println!("reading drive {}", &drive.id);
            tasks.push(self.process_folder_recursively(drive.id.clone(), callback_ref));
        }

        for task in tasks {
            task.await?;
        }

        Ok(())
    }

    pub(crate) fn process_folder_recursively<'a, F>(
        &'a self,
        parent_id: String,
        callback: &'a impl Fn(File) -> F,
    ) -> LocalBoxFuture<'a, Result<()>>
    where
        F: Future<Output = Result<()>>,
    {
        async move {
            let files = self
                .get_files_in_parent(&parent_id)
                .await
                .expect("Unable to get files in drive.");
            let mut tasks = vec![];
            let mut tasks2 = vec![];
            files.into_iter().for_each(|file| {
                tasks.push(callback(file.clone()));

                if file.mime_type == "application/vnd.google-apps.folder" {
                    tasks2.push(self.process_folder_recursively(file.id, callback));
                }
            });

            for task in tasks {
                task.await?;
            }

            for task in tasks2 {
                task.await?;
            }

            Ok(())
        }
        .boxed_local()
    }
}

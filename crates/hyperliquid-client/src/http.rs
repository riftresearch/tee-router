//! Tiny reqwest wrapper pinned to a user-supplied base URL. Exists so the
//! signing/client layer doesn't bake in HL's enum of {Localhost, Testnet,
//! Mainnet} — we point it at whatever URL the caller configures (including
//! the devnet mock).

use reqwest::Client;
use serde::de::DeserializeOwned;

use crate::error::Error;

#[derive(Debug, Clone)]
pub struct HttpClient {
    client: Client,
    base_url: String,
}

impl HttpClient {
    #[must_use]
    pub fn new(client: Client, base_url: String) -> Self {
        Self { client, base_url }
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// POST a JSON body to `{base_url}{path}`. Returns the raw body; callers
    /// handle decoding.
    pub async fn post_raw(&self, path: &str, body: &str) -> Result<String, Error> {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .map_err(|source| Error::HttpRequest { source })?;

        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|source| Error::HttpRequest { source })?;

        if status.is_success() {
            Ok(text)
        } else {
            Err(Error::HttpStatus {
                status: status.as_u16(),
                body: text,
            })
        }
    }

    /// POST a JSON-serializable body and deserialize the response.
    pub async fn post_json<Req, Res>(&self, path: &str, body: &Req) -> Result<Res, Error>
    where
        Req: serde::Serialize,
        Res: DeserializeOwned,
    {
        let body = serde_json::to_string(body).map_err(|source| Error::Json { source })?;
        let text = self.post_raw(path, &body).await?;
        serde_json::from_str(&text).map_err(|source| Error::Json { source })
    }
}

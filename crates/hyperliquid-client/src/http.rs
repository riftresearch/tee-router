//! Tiny reqwest wrapper pinned to a user-supplied base URL. Exists so the
//! signing/client layer doesn't bake in HL's enum of {Localhost, Testnet,
//! Mainnet} — we point it at whatever URL the caller configures (including
//! the devnet mock).

use reqwest::Client;
use serde::de::DeserializeOwned;
use std::time::{Duration, Instant};

use crate::error::Error;

const MAX_HYPERLIQUID_RESPONSE_BODY_BYTES: usize = 256 * 1024;

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
        let endpoint = hyperliquid_endpoint_label(path);
        let started = Instant::now();
        let response = match self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await
        {
            Ok(response) => response,
            Err(source) => {
                record_venue_request("POST", endpoint, "transport_error", started.elapsed());
                return Err(Error::HttpRequest { source });
            }
        };

        let status = response.status();
        let status_class = status_class(status.as_u16());
        let body =
            match read_limited_response_text(response, MAX_HYPERLIQUID_RESPONSE_BODY_BYTES).await {
                Ok(body) => body,
                Err(source) => {
                    record_venue_request("POST", endpoint, status_class, started.elapsed());
                    return Err(Error::HttpRequest { source });
                }
            };
        record_venue_request("POST", endpoint, status_class, started.elapsed());
        if body.truncated {
            return Err(Error::ResponseBodyTooLarge {
                max_bytes: MAX_HYPERLIQUID_RESPONSE_BODY_BYTES,
            });
        }
        let text = body.text;

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

fn record_venue_request(
    method: &'static str,
    endpoint: &'static str,
    status_class: &'static str,
    duration: Duration,
) {
    observability::upstream::record_upstream_request(
        observability::upstream::UpstreamKind::TradingVenue,
        "hyperliquid",
        method,
        endpoint,
        status_class,
        duration,
    );
}

fn hyperliquid_endpoint_label(path: &str) -> &'static str {
    match path {
        "/info" => "/info",
        "/exchange" => "/exchange",
        _ => "unknown",
    }
}

fn status_class(status: u16) -> &'static str {
    match status {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "unknown",
    }
}

struct LimitedResponseBody {
    text: String,
    truncated: bool,
}

async fn read_limited_response_text(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> Result<LimitedResponseBody, reqwest::Error> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if !append_limited_body_chunk(&mut body, chunk.as_ref(), max_bytes) {
            return Ok(LimitedResponseBody {
                text: String::new(),
                truncated: true,
            });
        }
    }

    Ok(LimitedResponseBody {
        text: String::from_utf8_lossy(&body).into_owned(),
        truncated: false,
    })
}

fn append_limited_body_chunk(body: &mut Vec<u8>, chunk: &[u8], max_bytes: usize) -> bool {
    if body.len().saturating_add(chunk.len()) > max_bytes {
        return false;
    }
    body.extend_from_slice(chunk);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_limited_body_chunk_rejects_chunks_past_the_limit_without_mutating() {
        let mut body = b"abcd".to_vec();

        assert!(!append_limited_body_chunk(&mut body, b"ef", 5));
        assert_eq!(body, b"abcd");
    }

    #[test]
    fn append_limited_body_chunk_accepts_chunks_at_the_limit() {
        let mut body = b"abcd".to_vec();

        assert!(append_limited_body_chunk(&mut body, b"ef", 6));
        assert_eq!(body, b"abcdef");
    }
}

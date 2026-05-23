//! Focused read-only client for Circle's Iris CCTP attestation API.
//!
//! Sauron's typed CCTP-burn observer needs only one thing from Iris: "is the
//! burn attestation ready yet?". `router-core`'s `CctpProvider` already speaks
//! the same `/v2/messages/:source_domain` endpoint, but it is constructed for
//! quoting/execution (it needs an `AssetRegistry` plus TokenMessenger /
//! MessageTransmitter addresses) and its response structs are private. Rather
//! than refactor `router-core` to expose them, this module is a small `reqwest`
//! GET that decodes the same byte-for-byte Iris JSON shape and answers the
//! single readiness question.
//!
//! Endpoint (matches `CctpProvider::fetch_messages`):
//!   `GET {base_url}/v2/messages/{source_domain}?transactionHash={burn_tx_hash}`

use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;

use crate::error::{Error, Result};

/// Canonical Iris production base URL.
///
/// Matches `CCTP_IRIS_DEFAULT_BASE_URL_FOR_CONFIG` in the temporal worker's
/// `production.rs`; keep the two in sync.
pub const CCTP_IRIS_DEFAULT_BASE_URL: &str = "https://iris-api.circle.com";

const CCTP_IRIS_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const CCTP_IRIS_MAX_RESPONSE_BODY_BYTES: usize = 512 * 1024;

/// Readiness verdict for a single CCTP burn attestation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CctpAttestationStatus {
    /// Iris has no message for this burn yet, or the message is still pending.
    Pending,
    /// Iris reports the attestation is complete and the `message` +
    /// `attestation` bytes are available.
    Ready,
    /// Iris reports the burn message terminally failed.
    Failed,
}

/// Minimal read-only client against the Iris CCTP attestation API.
#[derive(Clone)]
pub struct CctpIrisClient {
    base_url: String,
    http: reqwest::Client,
}

impl CctpIrisClient {
    /// Builds a client for the given Iris base URL. A trailing slash is
    /// trimmed so request paths join cleanly.
    pub fn new(base_url: impl Into<String>) -> Result<Self> {
        let base_url = base_url.into();
        let trimmed = base_url.trim().trim_end_matches('/');
        if trimmed.is_empty() {
            return Err(Error::InvalidConfiguration {
                message: "CCTP_API_URL must not be empty".to_string(),
            });
        }
        if !trimmed.starts_with("http://") && !trimmed.starts_with("https://") {
            return Err(Error::InvalidConfiguration {
                message: "CCTP_API_URL must use http or https".to_string(),
            });
        }
        let http = reqwest::Client::builder()
            .timeout(CCTP_IRIS_REQUEST_TIMEOUT)
            .build()
            .map_err(|source| Error::InvalidConfiguration {
                message: format!("failed to build CCTP Iris HTTP client: {source}"),
            })?;
        Ok(Self {
            base_url: trimmed.to_string(),
            http,
        })
    }

    /// Polls Iris for the attestation status of a single CCTP burn.
    ///
    /// `source_domain` is the CCTP domain of the burn chain; `burn_tx_hash` is
    /// the on-chain burn transaction hash. A `404` (no message indexed yet) is
    /// treated as [`CctpAttestationStatus::Pending`] rather than an error.
    pub async fn attestation_status(
        &self,
        source_domain: u32,
        burn_tx_hash: &str,
    ) -> Result<CctpAttestationStatus> {
        let url = format!("{}/v2/messages/{source_domain}", self.base_url);
        let response = self
            .http
            .get(&url)
            .query(&[("transactionHash", burn_tx_hash)])
            .send()
            .await
            .map_err(|source| Error::CctpIris {
                message: format!("iris messages request failed: {}", source.without_url()),
            })?;

        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(CctpAttestationStatus::Pending);
        }

        let body = read_limited_body(response).await?;
        if !status.is_success() {
            return Err(Error::CctpIris {
                message: format!("iris messages request failed with {status}"),
            });
        }

        let parsed: CctpMessagesResponse =
            serde_json::from_slice(&body).map_err(|source| Error::CctpIris {
                message: format!("iris messages response was not valid JSON: {source}"),
            })?;
        Ok(classify_messages(&parsed.messages))
    }
}

async fn read_limited_body(mut response: reqwest::Response) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|source| Error::CctpIris {
        message: format!("iris messages response body failed: {source}"),
    })? {
        if body.len() + chunk.len() > CCTP_IRIS_MAX_RESPONSE_BODY_BYTES {
            return Err(Error::CctpIris {
                message: format!(
                    "iris messages response body exceeded {CCTP_IRIS_MAX_RESPONSE_BODY_BYTES} bytes"
                ),
            });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Iris `/v2/messages` response envelope. Mirrors `CctpMessagesResponse` in
/// `router-core`'s `action_providers.rs`.
#[derive(Debug, Clone, Deserialize)]
struct CctpMessagesResponse {
    #[serde(default)]
    messages: Vec<CctpMessageEntry>,
}

/// A single Iris message entry. Field shape mirrors `router-core`'s private
/// `CctpMessageEntry` (camelCase, optional `message`/`attestation`).
///
/// `decoded_message_body` is nested inside `decoded_message` per the real Iris
/// V2 response (`messages[].decodedMessage.decodedMessageBody`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CctpMessageEntry {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    attestation: Option<String>,
    status: String,
    #[serde(default)]
    #[allow(dead_code)]
    event_nonce: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    decoded_message: Option<CctpDecodedMessage>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
struct CctpDecodedMessage {
    #[serde(default)]
    decoded_message_body: Option<Value>,
}

/// Reduces a set of Iris message entries to a single readiness verdict.
///
/// A `failed` entry wins (terminal); otherwise a `complete` entry with
/// non-empty `message` and `attestation` is `Ready`; anything else is
/// `Pending`. Matches `CctpProvider::observe_bridge_operation`'s logic.
fn classify_messages(messages: &[CctpMessageEntry]) -> CctpAttestationStatus {
    if messages.iter().any(|message| status_is_failed(&message.status)) {
        return CctpAttestationStatus::Failed;
    }
    let ready = messages.iter().any(|message| {
        message.status.eq_ignore_ascii_case("complete")
            && message
                .message
                .as_deref()
                .is_some_and(|value| !value.is_empty())
            && message
                .attestation
                .as_deref()
                .is_some_and(|value| !value.is_empty())
    });
    if ready {
        CctpAttestationStatus::Ready
    } else {
        CctpAttestationStatus::Pending
    }
}

/// Iris terminal failure statuses. Mirrors `cctp_message_status_is_failed` in
/// `router-core`.
fn status_is_failed(status: &str) -> bool {
    matches!(
        status.to_ascii_lowercase().as_str(),
        "failed" | "failure" | "error"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(status: &str, message: Option<&str>, attestation: Option<&str>) -> CctpMessageEntry {
        CctpMessageEntry {
            message: message.map(str::to_string),
            attestation: attestation.map(str::to_string),
            status: status.to_string(),
            event_nonce: None,
            decoded_message: None,
        }
    }

    #[test]
    fn complete_message_with_attestation_is_ready() {
        let messages = vec![entry("complete", Some("0xmsg"), Some("0xatt"))];
        assert_eq!(classify_messages(&messages), CctpAttestationStatus::Ready);
    }

    #[test]
    fn complete_message_without_attestation_is_pending() {
        let messages = vec![entry("complete", Some("0xmsg"), None)];
        assert_eq!(classify_messages(&messages), CctpAttestationStatus::Pending);
    }

    #[test]
    fn pending_status_is_pending() {
        let messages = vec![entry("pending_confirmations", None, None)];
        assert_eq!(classify_messages(&messages), CctpAttestationStatus::Pending);
    }

    #[test]
    fn empty_messages_are_pending() {
        assert_eq!(classify_messages(&[]), CctpAttestationStatus::Pending);
    }

    #[test]
    fn failed_message_wins_over_complete() {
        let messages = vec![
            entry("complete", Some("0xmsg"), Some("0xatt")),
            entry("failed", None, None),
        ];
        assert_eq!(classify_messages(&messages), CctpAttestationStatus::Failed);
    }

    #[test]
    fn iris_failure_statuses_are_terminal() {
        for status in ["failed", "FAILURE", "Error"] {
            assert!(status_is_failed(status));
        }
        for status in ["complete", "pending", "pending_confirmations"] {
            assert!(!status_is_failed(status));
        }
    }

    #[test]
    fn iris_messages_response_decodes_camelcase_iris_shape() {
        // Mirrors the real Iris V2 response: decodedMessageBody is nested
        // inside decodedMessage, not at the top level of the message entry.
        let body = serde_json::json!({
            "messages": [{
                "message": "0xmsg",
                "attestation": "0xatt",
                "status": "complete",
                "eventNonce": "0xnonce",
                "decodedMessage": {
                    "decodedMessageBody": {"amount": "1000000"},
                },
            }]
        });
        let parsed: CctpMessagesResponse =
            serde_json::from_value(body).expect("iris response decodes");
        assert_eq!(
            classify_messages(&parsed.messages),
            CctpAttestationStatus::Ready
        );
        let body = parsed.messages[0]
            .decoded_message
            .as_ref()
            .and_then(|dm| dm.decoded_message_body.as_ref())
            .expect("decodedMessageBody present");
        assert_eq!(body.get("amount").and_then(Value::as_str), Some("1000000"));
    }

    #[test]
    fn rejects_non_http_base_url() {
        assert!(CctpIrisClient::new("ftp://iris.example").is_err());
        assert!(CctpIrisClient::new("   ").is_err());
        assert!(CctpIrisClient::new("https://iris-api.circle.com/").is_ok());
    }

    /// Differential test: deserialize a real Iris V2 response captured from
    /// production (Arbitrum domain 3 → Base domain 6, burn tx
    /// 0x2d77d9667cc326816cd0ddc736a2222829e2087c7073362f16e0043141e43c96).
    /// Pins the on-the-wire shape so a future field rename / re-nesting fails
    /// loudly instead of silently no-op'ing the receive-amount invariant.
    #[test]
    fn real_iris_response_deserializes_with_nested_decoded_message_body() {
        let body: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/cctp/iris_v2_complete_real.json"
        ))
        .expect("fixture parses");
        let parsed: CctpMessagesResponse =
            serde_json::from_value(body).expect("real iris response decodes");
        assert_eq!(
            classify_messages(&parsed.messages),
            CctpAttestationStatus::Ready
        );
        let entry = &parsed.messages[0];
        assert_eq!(entry.status, "complete");
        let body = entry
            .decoded_message
            .as_ref()
            .and_then(|dm| dm.decoded_message_body.as_ref())
            .expect("decodedMessageBody present at the real nested path");
        assert_eq!(
            body.get("amount").and_then(Value::as_str),
            Some("40000000"),
            "real burn amount in the captured response"
        );
        assert_eq!(
            body.get("mintRecipient").and_then(Value::as_str),
            Some("0x33f65788aca48d733c2c2444ac9f79b18206aa92")
        );
    }
}

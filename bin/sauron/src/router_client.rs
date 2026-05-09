use std::time::Duration;

use metrics::histogram;
use reqwest::StatusCode;
use router_server::api::{
    DetectorHintEnvelope, DetectorHintRequest, ProviderOperationHintEnvelope,
    ProviderOperationHintRequest, VaultFundingHintEnvelope, VaultFundingHintRequest,
};
use snafu::ResultExt;
use uuid::Uuid;

use crate::{
    config::{normalize_router_detector_api_key, SauronArgs},
    error::{
        Error, Result, RouterRejectedSnafu, RouterRequestSnafu, RouterResponseBodyTooLargeSnafu,
        RouterResponseJsonSnafu, RouterUrlSnafu,
    },
};

const SAURON_ROUTER_SUBMISSION_DURATION_SECONDS: &str = "sauron_router_submission_duration_seconds";
const MAX_ROUTER_RESPONSE_BODY_BYTES: usize = 256 * 1024;
const MAX_ROUTER_REJECTION_BODY_BYTES: usize = 16 * 1024;

#[derive(Clone)]
pub struct RouterClient {
    client: reqwest::Client,
    base_url: reqwest::Url,
    detector_api_key: String,
}

impl RouterClient {
    pub fn new(args: &SauronArgs) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context(RouterRequestSnafu)?;

        let base_url = normalize_router_internal_base_url(&args.router_internal_base_url)?;

        Ok(Self {
            client,
            base_url,
            detector_api_key: normalize_router_detector_api_key(&args.router_detector_api_key)
                .ok_or_else(|| Error::InvalidConfiguration {
                    message: "ROUTER_DETECTOR_API_KEY must be at least 32 characters".to_string(),
                })?
                .to_string(),
        })
    }

    pub async fn submit_provider_operation_hint(
        &self,
        request: &ProviderOperationHintRequest,
    ) -> Result<ProviderOperationHintEnvelope> {
        self.submit_hint("/api/v1/provider-operations/hints", request)
            .await
    }

    pub async fn submit_detector_hint(
        &self,
        request: &DetectorHintRequest,
    ) -> Result<DetectorHintEnvelope> {
        self.submit_hint("/api/v1/hints", request).await
    }

    pub async fn submit_vault_funding_hint(
        &self,
        vault_id: Uuid,
        request: &VaultFundingHintRequest,
    ) -> Result<VaultFundingHintEnvelope> {
        self.submit_hint(&format!("/api/v1/vaults/{vault_id}/funding-hints"), request)
            .await
    }

    async fn submit_hint<TRequest, TResponse>(
        &self,
        path: &str,
        request: &TRequest,
    ) -> Result<TResponse>
    where
        TRequest: serde::Serialize + ?Sized,
        TResponse: serde::de::DeserializeOwned,
    {
        let started = std::time::Instant::now();
        let endpoint = self.base_url.join(path).context(RouterUrlSnafu {
            base_url: self.base_url.to_string(),
        })?;

        let response = self
            .client
            .post(endpoint)
            .bearer_auth(&self.detector_api_key)
            .json(request)
            .send()
            .await
            .context(RouterRequestSnafu)?;

        if router_response_status_is_success(response.status()) {
            histogram!(SAURON_ROUTER_SUBMISSION_DURATION_SECONDS, "status" => response.status().as_str().to_string())
                .record(started.elapsed().as_secs_f64());
            read_router_response_json(response).await
        } else {
            let status = response.status();
            histogram!(SAURON_ROUTER_SUBMISSION_DURATION_SECONDS, "status" => status.as_str().to_string())
                .record(started.elapsed().as_secs_f64());
            let body = read_router_rejection_body(response).await?;
            RouterRejectedSnafu { status, body }.fail()
        }
    }
}

fn router_response_status_is_success(status: StatusCode) -> bool {
    status.is_success()
}

pub(crate) fn normalize_router_internal_base_url(value: &str) -> Result<reqwest::Url> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidConfiguration {
            message: "ROUTER_INTERNAL_BASE_URL is required".to_string(),
        });
    }

    let mut base_url =
        reqwest::Url::parse(trimmed).map_err(|error| Error::InvalidConfiguration {
            message: format!("ROUTER_INTERNAL_BASE_URL must be a valid absolute URL: {error}"),
        })?;
    if base_url.scheme() != "http" && base_url.scheme() != "https" {
        return Err(Error::InvalidConfiguration {
            message: "ROUTER_INTERNAL_BASE_URL must use http or https".to_string(),
        });
    }
    if base_url.host().is_none() {
        return Err(Error::InvalidConfiguration {
            message: "ROUTER_INTERNAL_BASE_URL must include a host".to_string(),
        });
    }
    if !base_url.username().is_empty() || base_url.password().is_some() {
        return Err(Error::InvalidConfiguration {
            message: "ROUTER_INTERNAL_BASE_URL must not include credentials".to_string(),
        });
    }
    if base_url.query().is_some() || base_url.fragment().is_some() {
        return Err(Error::InvalidConfiguration {
            message: "ROUTER_INTERNAL_BASE_URL must not include a query string or fragment"
                .to_string(),
        });
    }
    if !base_url.path().trim_matches('/').is_empty() {
        return Err(Error::InvalidConfiguration {
            message: "ROUTER_INTERNAL_BASE_URL must not include a path prefix".to_string(),
        });
    }
    base_url.set_path("");
    Ok(base_url)
}

async fn read_router_response_json<T>(response: reqwest::Response) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let body = read_limited_router_body(response, MAX_ROUTER_RESPONSE_BODY_BYTES).await?;
    if body.truncated {
        return RouterResponseBodyTooLargeSnafu {
            max_bytes: MAX_ROUTER_RESPONSE_BODY_BYTES,
        }
        .fail();
    }
    serde_json::from_slice(&body.bytes).context(RouterResponseJsonSnafu {
        body: response_body_preview(&body.bytes),
    })
}

async fn read_router_rejection_body(mut response: reqwest::Response) -> Result<String> {
    let mut body = Vec::new();
    let mut truncated = false;
    while let Some(chunk) = response.chunk().await.context(RouterRequestSnafu)? {
        if append_bounded_body_chunk(&mut body, &chunk, MAX_ROUTER_REJECTION_BODY_BYTES) {
            truncated = true;
            break;
        }
    }

    let mut text = String::from_utf8_lossy(&body).into_owned();
    if truncated {
        text.push_str("...[truncated]");
    }
    Ok(text)
}

struct LimitedRouterBody {
    bytes: Vec<u8>,
    truncated: bool,
}

async fn read_limited_router_body(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> Result<LimitedRouterBody> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.context(RouterRequestSnafu)? {
        if append_bounded_body_chunk(&mut body, &chunk, max_bytes) {
            return Ok(LimitedRouterBody {
                bytes: body,
                truncated: true,
            });
        }
    }
    Ok(LimitedRouterBody {
        bytes: body,
        truncated: false,
    })
}

fn append_bounded_body_chunk(body: &mut Vec<u8>, chunk: &[u8], max_bytes: usize) -> bool {
    let remaining = max_bytes.saturating_sub(body.len());
    if remaining == 0 {
        return true;
    }
    if chunk.len() > remaining {
        body.extend_from_slice(&chunk[..remaining]);
        return true;
    }
    body.extend_from_slice(chunk);
    false
}

fn response_body_preview(body: &[u8]) -> String {
    let truncated = body.len() > MAX_ROUTER_REJECTION_BODY_BYTES;
    let preview = if truncated {
        &body[..MAX_ROUTER_REJECTION_BODY_BYTES]
    } else {
        body
    };
    let mut text = String::from_utf8_lossy(preview).into_owned();
    if truncated {
        text.push_str("...[truncated]");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn router_rejection_body_chunks_are_bounded() {
        let mut body = Vec::new();
        assert!(!append_bounded_body_chunk(
            &mut body,
            b"router failed",
            MAX_ROUTER_REJECTION_BODY_BYTES
        ));
        assert_eq!(body, b"router failed");

        let mut body = Vec::new();
        let truncated = append_bounded_body_chunk(
            &mut body,
            &vec![b'a'; MAX_ROUTER_REJECTION_BODY_BYTES + 1],
            MAX_ROUTER_REJECTION_BODY_BYTES,
        );
        assert!(truncated);
        assert_eq!(body.len(), MAX_ROUTER_REJECTION_BODY_BYTES);
    }

    #[test]
    fn router_success_status_accepts_all_2xx_responses() {
        assert!(router_response_status_is_success(StatusCode::OK));
        assert!(router_response_status_is_success(StatusCode::CREATED));
        assert!(router_response_status_is_success(StatusCode::ACCEPTED));
        assert!(!router_response_status_is_success(StatusCode::BAD_REQUEST));
        assert!(!router_response_status_is_success(
            StatusCode::INTERNAL_SERVER_ERROR
        ));
    }
}

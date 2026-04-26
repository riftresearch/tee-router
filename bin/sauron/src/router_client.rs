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
    config::SauronArgs,
    error::{Result, RouterRejectedSnafu, RouterRequestSnafu, RouterUrlSnafu},
};

const SAURON_ROUTER_SUBMISSION_DURATION_SECONDS: &str = "sauron_router_submission_duration_seconds";

#[derive(Clone)]
pub struct RouterClient {
    client: reqwest::Client,
    base_url: String,
    detector_api_key: String,
}

impl RouterClient {
    pub fn new(args: &SauronArgs) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context(RouterRequestSnafu)?;

        Ok(Self {
            client,
            base_url: args.router_internal_base_url.clone(),
            detector_api_key: args.router_detector_api_key.clone(),
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
        let endpoint = reqwest::Url::parse(&self.base_url)
            .context(RouterUrlSnafu {
                base_url: self.base_url.clone(),
            })?
            .join(path)
            .context(RouterUrlSnafu {
                base_url: self.base_url.clone(),
            })?;

        let response = self
            .client
            .post(endpoint)
            .bearer_auth(&self.detector_api_key)
            .json(request)
            .send()
            .await
            .context(RouterRequestSnafu)?;

        match response.status() {
            StatusCode::OK | StatusCode::ACCEPTED => {
                histogram!(SAURON_ROUTER_SUBMISSION_DURATION_SECONDS, "status" => response.status().as_str().to_string())
                    .record(started.elapsed().as_secs_f64());
                response.json().await.context(RouterRequestSnafu)
            }
            _ => {
                let status = response.status();
                histogram!(SAURON_ROUTER_SUBMISSION_DURATION_SECONDS, "status" => status.as_str().to_string())
                    .record(started.elapsed().as_secs_f64());
                let body = response.text().await.unwrap_or_default();
                RouterRejectedSnafu { status, body }.fail()
            }
        }
    }
}

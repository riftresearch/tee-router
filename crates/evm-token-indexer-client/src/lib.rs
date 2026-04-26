use alloy::primitives::{Address, B256, U256};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to build HTTP client: {source:?}"))]
    BuildClient { source: reqwest::Error },

    #[snafu(display("Failed to send request: {source:?} at {loc}"))]
    Request {
        source: reqwest::Error,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display("Failed to parse response: {source:?}"))]
    ParseResponse {
        source: reqwest::Error,
        #[snafu(implicit)]
        loc: snafu::Location,
    },

    #[snafu(display("Invalid base URL: {source:?}"))]
    InvalidUrl {
        source: url::ParseError,
        #[snafu(implicit)]
        loc: snafu::Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferEvent {
    pub id: String,
    pub amount: String, // BigInt serialized as string
    pub timestamp: u64,
    pub from: Address,
    pub to: Address,
    pub token_address: Option<Address>,
    pub transaction_hash: B256,
    pub block_number: String, // BigInt serialized as string
    pub block_hash: B256,
    pub log_index: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Pagination {
    pub page: u32,
    pub limit: u32,
    pub total: u64,
    #[serde(rename = "totalPages")]
    pub total_pages: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TransfersResponse {
    pub transfers: Vec<TransferEvent>,
    pub pagination: Pagination,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenIndexerWatch {
    pub watch_id: String,
    pub watch_target: String,
    pub token_address: Address,
    pub deposit_address: Address,
    pub min_amount: String,
    pub max_amount: String,
    pub required_amount: String,
    pub created_at: String,
    pub updated_at: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncWatchesRequest {
    pub watches: Vec<TokenIndexerWatch>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncWatchesResponse {
    pub synced: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DepositCandidate {
    pub id: String,
    pub watch_id: String,
    pub watch_target: String,
    pub chain_id: u64,
    pub token_address: Address,
    pub deposit_address: Address,
    pub amount: String,
    pub required_amount: String,
    pub transaction_hash: B256,
    pub transfer_index: u64,
    pub block_number: String,
    pub block_hash: B256,
    pub block_timestamp: String,
    pub status: String,
    pub attempt_count: u64,
    pub last_error: Option<String>,
    pub created_at: String,
    pub delivered_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingCandidatesResponse {
    pub candidates: Vec<DepositCandidate>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ReleaseCandidateRequest {
    error: Option<String>,
}

pub struct TokenIndexerClient {
    client: Client,
    base_url: Url,
}

impl TokenIndexerClient {
    pub fn new(base_url: impl AsRef<str>) -> Result<Self> {
        let client = Client::builder()
            .use_rustls_tls()
            .build()
            .context(BuildClientSnafu)?;

        tracing::info!(
            "Creating TokenIndexerClient with base URL: {}",
            base_url.as_ref()
        );

        let base_url = Url::parse(base_url.as_ref()).context(InvalidUrlSnafu)?;

        Ok(Self { client, base_url })
    }

    pub async fn get_transfers_to(
        &self,
        address: Address,
        token: Option<Address>,
        page: Option<u32>,
        min_amount: Option<U256>,
    ) -> Result<TransfersResponse> {
        let mut url = self
            .base_url
            .join(&format!("transfers/to/{address:?}"))
            .context(InvalidUrlSnafu)?;

        {
            let mut query_pairs = url.query_pairs_mut();

            if let Some(page) = page {
                query_pairs.append_pair("page", &page.to_string());
            }

            if let Some(token) = token {
                query_pairs.append_pair("token", &format!("{token:?}"));
            }

            if let Some(amount) = min_amount {
                query_pairs.append_pair("amount", &amount.to_string());
            }
        }

        let response = self.client.get(url).send().await.context(RequestSnafu)?;

        let response = response
            .json::<TransfersResponse>()
            .await
            .context(ParseResponseSnafu)?;

        Ok(response)
    }

    pub async fn sync_watches(
        &self,
        watches: Vec<TokenIndexerWatch>,
    ) -> Result<SyncWatchesResponse> {
        let url = self.base_url.join("watches").context(InvalidUrlSnafu)?;
        self.client
            .put(url)
            .json(&SyncWatchesRequest { watches })
            .send()
            .await
            .context(RequestSnafu)?
            .json::<SyncWatchesResponse>()
            .await
            .context(ParseResponseSnafu)
    }

    pub async fn materialize_candidates(&self) -> Result<()> {
        let url = self
            .base_url
            .join("candidates/materialize")
            .context(InvalidUrlSnafu)?;
        self.client
            .post(url)
            .send()
            .await
            .context(RequestSnafu)?
            .error_for_status()
            .context(RequestSnafu)?;
        Ok(())
    }

    pub async fn pending_candidates(&self, limit: Option<u32>) -> Result<Vec<DepositCandidate>> {
        let mut url = self
            .base_url
            .join("candidates/pending")
            .context(InvalidUrlSnafu)?;
        if let Some(limit) = limit {
            url.query_pairs_mut()
                .append_pair("limit", &limit.to_string());
        }
        let response = self
            .client
            .get(url)
            .send()
            .await
            .context(RequestSnafu)?
            .json::<PendingCandidatesResponse>()
            .await
            .context(ParseResponseSnafu)?;
        Ok(response.candidates)
    }

    pub async fn mark_candidate_submitted(&self, candidate_id: &str) -> Result<()> {
        let url = self.candidate_action_url(candidate_id, "mark-submitted")?;
        self.client
            .post(url)
            .send()
            .await
            .context(RequestSnafu)?
            .error_for_status()
            .context(RequestSnafu)?;
        Ok(())
    }

    pub async fn release_candidate(&self, candidate_id: &str, error: Option<String>) -> Result<()> {
        let url = self.candidate_action_url(candidate_id, "release")?;
        self.client
            .post(url)
            .json(&ReleaseCandidateRequest { error })
            .send()
            .await
            .context(RequestSnafu)?
            .error_for_status()
            .context(RequestSnafu)?;
        Ok(())
    }

    fn candidate_action_url(&self, candidate_id: &str, action: &str) -> Result<Url> {
        let mut url = self.base_url.join("candidates").context(InvalidUrlSnafu)?;
        url.path_segments_mut()
            .expect("HTTP base URL supports path segments")
            .push(candidate_id)
            .push(action);
        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_creation() {
        let client = TokenIndexerClient::new("http://localhost:3000");
        assert!(client.is_ok());
    }

    #[test]
    fn test_path_building() {
        let client = TokenIndexerClient::new("http://localhost:3000/erc20-indexer/").unwrap();

        let url = client.base_url.join("candidates/pending").unwrap();
        assert_eq!(
            url.to_string(),
            "http://localhost:3000/erc20-indexer/candidates/pending"
        );
    }

    #[test]
    fn candidate_action_path_escapes_candidate_id() {
        let client = TokenIndexerClient::new("http://localhost:3000/erc20-indexer/").unwrap();

        let url = client
            .candidate_action_url("watch:chain:tx:0", "mark-submitted")
            .unwrap();

        assert_eq!(
            url.to_string(),
            "http://localhost:3000/erc20-indexer/candidates/watch:chain:tx:0/mark-submitted"
        );
    }
}

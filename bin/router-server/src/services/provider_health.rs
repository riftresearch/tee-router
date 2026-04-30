use crate::{
    db::Database,
    error::{RouterServerError, RouterServerResult},
    models::{ProviderHealthCheck, ProviderHealthStatus},
};
use chrono::Utc;
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE},
    Client, Method, Proxy, StatusCode,
};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc, time::Duration, time::Instant};
use tokio::sync::RwLock;
use tracing::warn;

#[derive(Debug, Clone)]
pub struct ProviderHealthSnapshot {
    checks: HashMap<String, ProviderHealthCheck>,
}

impl ProviderHealthSnapshot {
    #[must_use]
    pub fn status(&self, provider: &str) -> ProviderHealthStatus {
        self.checks
            .get(provider)
            .map(|check| check.status)
            .unwrap_or(ProviderHealthStatus::Unknown)
    }

    #[must_use]
    pub fn allows_new_routes(&self, provider: &str) -> bool {
        self.status(provider).allows_new_routes()
    }
}

#[derive(Debug, Clone)]
struct ProviderHealthCache {
    fetched_at: Instant,
    snapshot: ProviderHealthSnapshot,
}

#[derive(Clone)]
pub struct ProviderHealthService {
    db: Database,
    cache_ttl: Duration,
    cache: Arc<RwLock<Option<ProviderHealthCache>>>,
}

impl ProviderHealthService {
    #[must_use]
    pub fn new(db: Database) -> Self {
        Self {
            db,
            cache_ttl: Duration::from_secs(5),
            cache: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn snapshot(&self) -> RouterServerResult<ProviderHealthSnapshot> {
        {
            let cache = self.cache.read().await;
            if let Some(cache) = cache.as_ref() {
                if cache.fetched_at.elapsed() < self.cache_ttl {
                    return Ok(cache.snapshot.clone());
                }
            }
        }

        let checks = self.db.provider_health().list().await?;
        let snapshot = ProviderHealthSnapshot {
            checks: checks
                .into_iter()
                .map(|check| (check.provider.clone(), check))
                .collect(),
        };
        let mut cache = self.cache.write().await;
        *cache = Some(ProviderHealthCache {
            fetched_at: Instant::now(),
            snapshot: snapshot.clone(),
        });
        Ok(snapshot)
    }

    pub async fn list(&self) -> RouterServerResult<Vec<ProviderHealthCheck>> {
        self.db.provider_health().list().await
    }

    pub async fn upsert(
        &self,
        check: &ProviderHealthCheck,
    ) -> RouterServerResult<ProviderHealthCheck> {
        let stored = self.db.provider_health().upsert(check).await?;
        let mut cache = self.cache.write().await;
        *cache = None;
        Ok(stored)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ProviderHealthPollSummary {
    pub checked: usize,
    pub healthy: usize,
    pub down: usize,
}

#[derive(Clone)]
pub struct ProviderHealthPoller {
    client: Client,
    health: Arc<ProviderHealthService>,
    probes: Vec<ProviderHealthProbe>,
    updated_by: String,
}

impl ProviderHealthPoller {
    pub fn new(
        health: Arc<ProviderHealthService>,
        probes: Vec<ProviderHealthProbe>,
        updated_by: impl Into<String>,
        timeout: Duration,
    ) -> RouterServerResult<Self> {
        let client = Client::builder().timeout(timeout).build().map_err(|err| {
            RouterServerError::InvalidData {
                message: format!("failed to build provider health HTTP client: {err}"),
            }
        })?;
        let probes = probes
            .into_iter()
            .map(|probe| probe.prepare(timeout))
            .collect::<RouterServerResult<Vec<_>>>()?;
        Ok(Self {
            client,
            health,
            probes,
            updated_by: updated_by.into(),
        })
    }

    #[must_use]
    pub fn probes(&self) -> &[ProviderHealthProbe] {
        &self.probes
    }

    pub async fn poll_once(&self) -> RouterServerResult<ProviderHealthPollSummary> {
        let mut summary = ProviderHealthPollSummary::default();
        for probe in &self.probes {
            let check = match self.check_provider(probe).await {
                Ok(check) => check,
                Err(err) => {
                    summary.down += 1;
                    warn!(
                        provider = %probe.provider,
                        error = %err,
                        "provider health check failed before persistence"
                    );
                    continue;
                }
            };
            match check.status {
                ProviderHealthStatus::Ok => {
                    summary.healthy += 1;
                }
                ProviderHealthStatus::Down | ProviderHealthStatus::Unknown => {
                    summary.down += 1;
                }
            }
            self.health.upsert(&check).await?;
            summary.checked += 1;
        }
        Ok(summary)
    }

    async fn check_provider(
        &self,
        probe: &ProviderHealthProbe,
    ) -> RouterServerResult<ProviderHealthCheck> {
        let started = Instant::now();
        let now = Utc::now();
        let response = probe.send(&self.client).await;
        let latency_ms = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);

        let check = match response {
            Ok((status, body)) => {
                let healthy = provider_http_status_reachable(status);
                ProviderHealthCheck {
                    provider: probe.provider.clone(),
                    status: if healthy {
                        ProviderHealthStatus::Ok
                    } else {
                        ProviderHealthStatus::Down
                    },
                    checked_at: now,
                    latency_ms: Some(latency_ms),
                    http_status: Some(i32::from(status.as_u16())),
                    error: if healthy {
                        None
                    } else {
                        Some(format!("HTTP {status}: {}", truncate_error_body(&body)))
                    },
                    updated_by: self.updated_by.clone(),
                    created_at: now,
                    updated_at: now,
                }
            }
            Err(err) => ProviderHealthCheck {
                provider: probe.provider.clone(),
                status: ProviderHealthStatus::Down,
                checked_at: now,
                latency_ms: Some(latency_ms),
                http_status: None,
                error: Some(err.to_string()),
                updated_by: self.updated_by.clone(),
                created_at: now,
                updated_at: now,
            },
        };
        Ok(check)
    }
}

#[derive(Clone)]
pub struct ProviderHealthProbe {
    provider: String,
    method: Method,
    url: String,
    headers: Vec<(String, String)>,
    json_body: Option<Value>,
    proxy_url: Option<String>,
    proxy_client: Option<Client>,
}

impl ProviderHealthProbe {
    #[must_use]
    pub fn get(provider: impl Into<String>, url: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            method: Method::GET,
            url: url.into(),
            headers: Vec::new(),
            json_body: None,
            proxy_url: None,
            proxy_client: None,
        }
    }

    #[must_use]
    pub fn post_json(
        provider: impl Into<String>,
        url: impl Into<String>,
        json_body: Value,
    ) -> Self {
        Self {
            provider: provider.into(),
            method: Method::POST,
            url: url.into(),
            headers: Vec::new(),
            json_body: Some(json_body),
            proxy_url: None,
            proxy_client: None,
        }
    }

    #[must_use]
    pub fn with_bearer_token(mut self, token: impl Into<String>) -> Self {
        self.headers.push((
            AUTHORIZATION.as_str().to_string(),
            format!("Bearer {}", token.into()),
        ));
        self
    }

    #[must_use]
    pub fn with_proxy_url(mut self, proxy_url: Option<String>) -> Self {
        self.proxy_url = proxy_url
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        self
    }

    fn prepare(mut self, timeout: Duration) -> RouterServerResult<Self> {
        let Some(proxy_url) = self.proxy_url.as_deref() else {
            return Ok(self);
        };
        let parsed = url::Url::parse(proxy_url).map_err(|err| RouterServerError::InvalidData {
            message: format!(
                "invalid provider health proxy URL for {}: {err}",
                self.provider
            ),
        })?;
        if parsed.scheme() != "socks5" {
            return Err(RouterServerError::InvalidData {
                message: format!(
                    "unsupported provider health proxy scheme for {}; expected socks5",
                    self.provider
                ),
            });
        }
        let proxy = Proxy::all(proxy_url).map_err(|err| RouterServerError::InvalidData {
            message: format!(
                "failed to configure provider health proxy for {}: {err}",
                self.provider
            ),
        })?;
        self.proxy_client = Some(
            Client::builder()
                .timeout(timeout)
                .use_rustls_tls()
                .proxy(proxy)
                .build()
                .map_err(|err| RouterServerError::InvalidData {
                    message: format!(
                        "failed to build proxied provider health HTTP client for {}: {err}",
                        self.provider
                    ),
                })?,
        );
        Ok(self)
    }

    async fn send(&self, client: &Client) -> reqwest::Result<(StatusCode, String)> {
        let client = self.proxy_client.as_ref().unwrap_or(client);
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        for (name, value) in &self.headers {
            let Ok(name) = HeaderName::from_bytes(name.as_bytes()) else {
                continue;
            };
            let Ok(value) = HeaderValue::from_str(value) else {
                continue;
            };
            headers.insert(name, value);
        }
        let mut request = client
            .request(self.method.clone(), &self.url)
            .headers(headers);
        if let Some(body) = &self.json_body {
            request = request.header(CONTENT_TYPE, "application/json").json(body);
        }

        let response = request.send().await?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Ok((status, body))
    }
}

fn provider_http_status_reachable(status: StatusCode) -> bool {
    status.is_success() || matches!(status.as_u16(), 400 | 404)
}

fn truncate_error_body(body: &str) -> String {
    const MAX: usize = 256;
    if body.len() <= MAX {
        body.to_string()
    } else {
        format!("{}...", &body[..MAX])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_http_status_reachable_treats_validation_errors_as_reachable() {
        assert!(provider_http_status_reachable(StatusCode::OK));
        assert!(provider_http_status_reachable(StatusCode::BAD_REQUEST));
        assert!(provider_http_status_reachable(StatusCode::NOT_FOUND));
        assert!(!provider_http_status_reachable(StatusCode::UNAUTHORIZED));
        assert!(!provider_http_status_reachable(
            StatusCode::TOO_MANY_REQUESTS
        ));
        assert!(!provider_http_status_reachable(
            StatusCode::INTERNAL_SERVER_ERROR
        ));
    }
}

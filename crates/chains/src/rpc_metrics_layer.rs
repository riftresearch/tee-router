use alloy::rpc::json_rpc::{RequestPacket, ResponsePacket};
use metrics::{counter, histogram};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use tower::{Layer, Service};
use tracing::warn;

#[derive(Clone)]
pub struct RpcMetricsLayer {
    pub chain: String,
}

impl RpcMetricsLayer {
    pub fn new(chain: String) -> Self {
        Self { chain }
    }
}

impl<S> Layer<S> for RpcMetricsLayer {
    type Service = RpcMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RpcMetricsService {
            inner,
            chain: self.chain.clone(),
        }
    }
}

#[derive(Clone)]
pub struct RpcMetricsService<S> {
    inner: S,
    chain: String,
}

impl<S> Service<RequestPacket> for RpcMetricsService<S>
where
    S: Service<RequestPacket, Response = ResponsePacket> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: std::fmt::Display + Send + 'static, // Relaxed bound for Error if possible, but alloy errors are usually Display
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: RequestPacket) -> Self::Future {
        // Extract method name
        let method = match &req {
            RequestPacket::Single(call) => call.method().to_string(),
            RequestPacket::Batch(calls) => {
                if calls.is_empty() {
                    "empty_batch".to_string()
                } else {
                    "batch".to_string()
                }
            }
        };

        let start = Instant::now();
        let chain = self.chain.clone();

        let fut = self.inner.call(req);

        Box::pin(async move {
            let result = fut.await;
            let duration = start.elapsed();

            let rpc_error_message = result
                .as_ref()
                .ok()
                .and_then(ResponsePacket::first_error_message)
                .map(ToOwned::to_owned);
            let transport_error_message = result.as_ref().err().map(ToString::to_string);
            let (status, error_kind, error_message) = match &result {
                Ok(response) if response.is_error() => {
                    let message = rpc_error_message.as_deref().unwrap_or("");
                    ("rpc_error", classify_rpc_error(message), message)
                }
                Ok(_) => ("success", "none", ""),
                Err(_) => {
                    let message = transport_error_message.as_deref().unwrap_or("");
                    ("transport_error", classify_rpc_error(message), message)
                }
            };

            counter!("ethereum_rpc_requests_total", "method" => method.clone(), "status" => status, "chain" => chain.clone()).increment(1);
            histogram!("ethereum_rpc_duration_seconds", "method" => method.clone(), "status" => status, "chain" => chain.clone()).record(duration.as_secs_f64());
            counter!(
                "tee_router_chain_rpc_requests_total",
                "rpc_method" => method.clone(),
                "status" => status,
                "chain" => chain.clone(),
            )
            .increment(1);
            histogram!(
                "tee_router_chain_rpc_request_duration_seconds",
                "rpc_method" => method.clone(),
                "status" => status,
                "chain" => chain.clone(),
            )
            .record(duration.as_secs_f64());

            if status != "success" {
                warn!(
                    chain = %chain,
                    rpc_method = %method,
                    status,
                    error_kind,
                    error = %error_message,
                    "EVM RPC request failed"
                );
                counter!(
                    "ethereum_rpc_errors_total",
                    "method" => method.clone(),
                    "status" => status,
                    "error_kind" => error_kind,
                    "chain" => chain.clone(),
                )
                .increment(1);
                counter!(
                    "tee_router_chain_rpc_errors_total",
                    "rpc_method" => method,
                    "status" => status,
                    "error_kind" => error_kind,
                    "chain" => chain,
                )
                .increment(1);
            }

            result
        })
    }
}

fn classify_rpc_error(message: &str) -> &'static str {
    let message = message.to_ascii_lowercase();
    if message.contains("429")
        || message.contains("rate limit")
        || message.contains("rate-limited")
        || message.contains("too many requests")
    {
        "rate_limited"
    } else if message.contains("timeout") || message.contains("timed out") {
        "timeout"
    } else if message.contains("temporarily unavailable")
        || message.contains("502")
        || message.contains("503")
        || message.contains("504")
    {
        "upstream_unavailable"
    } else if message.contains("connection reset")
        || message.contains("connection closed")
        || message.contains("connection refused")
    {
        "connection"
    } else {
        "other"
    }
}

use axum::{extract::Path, http::StatusCode, response::IntoResponse, routing::get, Json, Router};
use serde_json::json;
use std::sync::Arc;

use crate::mock_integrators::error_response;

/// Per-venue state for the Coinbase spot-price mock. The mock currently serves
/// only static prices, so there is no venue-specific state yet; this unit
/// struct exists for uniformity with the other venue states and gives a home
/// for future Coinbase tunables.
#[derive(Default)]
pub(crate) struct CoinbaseMockState;

/// Builds the Coinbase mock router. Mounted under `/coinbase`; receives its own
/// [`CoinbaseMockState`] substate at nest time.
pub(crate) fn router() -> Router<Arc<CoinbaseMockState>> {
    Router::new().route(
        "/v2/prices/:currency_pair/spot",
        get(mock_coinbase_spot_price),
    )
}

pub(crate) async fn mock_coinbase_spot_price(
    Path(currency_pair): Path<String>,
) -> impl IntoResponse {
    let (base, amount) = match currency_pair.as_str() {
        "ETH-USD" => ("ETH", "3000"),
        "BTC-USD" => ("BTC", "100000"),
        "USDC-USD" => ("USDC", "1"),
        "USDT-USD" => ("USDT", "1"),
        _ => {
            return error_response(
                StatusCode::NOT_FOUND,
                format!("mock Coinbase spot price not found for {currency_pair}"),
            );
        }
    };
    (
        StatusCode::OK,
        Json(json!({
            "data": {
                "amount": amount,
                "base": base,
                "currency": "USD"
            }
        })),
    )
        .into_response()
}

use axum::{
    extract::Path,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde_json::json;

use crate::mock_integrators::error_response;

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

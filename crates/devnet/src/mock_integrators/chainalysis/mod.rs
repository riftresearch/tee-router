use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};

/// Per-venue state for the Chainalysis address-screening mock: the per-address
/// screening rules tests inject to control risk responses.
pub(crate) struct ChainalysisMockState {
    pub(crate) address_screening_rules: BTreeMap<String, MockAddressScreeningRule>,
}

/// Builds the Chainalysis mock router. Mounted under `/chainalysis`; receives
/// its own [`ChainalysisMockState`] substate at nest time.
pub(crate) fn router() -> Router<Arc<ChainalysisMockState>> {
    Router::new().route(
        "/api/risk/v2/entities/:address",
        get(mock_chainalysis_address_risk),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MockAddressRiskLevel {
    Low,
    Medium,
    High,
    Severe,
    Unknown,
}

impl MockAddressRiskLevel {
    fn chainalysis_label(self) -> &'static str {
        match self {
            Self::Low => "Low",
            Self::Medium => "Medium",
            Self::High => "High",
            Self::Severe => "Severe",
            Self::Unknown => "Unknown",
        }
    }
}

#[derive(Debug, Clone)]
pub enum MockAddressScreeningRule {
    Risk {
        risk: MockAddressRiskLevel,
        reason: Option<String>,
    },
    HttpError {
        status: u16,
        body: String,
    },
}

pub(crate) async fn mock_chainalysis_address_risk(
    State(state): State<Arc<ChainalysisMockState>>,
    Path(address): Path<String>,
) -> axum::response::Response {
    let normalized = normalize_mock_screening_address(&address);
    match state.address_screening_rules.get(&normalized) {
        Some(MockAddressScreeningRule::HttpError { status, body }) => {
            let status = StatusCode::from_u16(*status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            (status, body.clone()).into_response()
        }
        Some(MockAddressScreeningRule::Risk { risk, reason }) => Json(json!({
            "address": address,
            "risk": risk.chainalysis_label(),
            "cluster": null,
            "riskReason": reason,
            "addressType": "PRIVATE_WALLET",
            "addressIdentifications": [],
            "exposures": [],
            "triggers": [],
            "status": "COMPLETE",
            "poolMetadata": null,
        }))
        .into_response(),
        None => Json(json!({
            "address": address,
            "risk": MockAddressRiskLevel::Low.chainalysis_label(),
            "cluster": null,
            "riskReason": null,
            "addressType": "PRIVATE_WALLET",
            "addressIdentifications": [],
            "exposures": [],
            "triggers": [],
            "status": "COMPLETE",
            "poolMetadata": null,
        }))
        .into_response(),
    }
}

pub(crate) fn normalize_mock_screening_address(address: &str) -> String {
    address.trim().to_ascii_lowercase()
}

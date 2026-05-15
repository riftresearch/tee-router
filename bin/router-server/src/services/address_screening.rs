use chainalysis_address_screener::{
    ChainalysisAddressScreener, Error as ChainalysisError, RiskLevel,
};
use router_core::protocol::ChainId;
use snafu::Snafu;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressScreeningPurpose {
    Recipient,
    Refund,
}

impl std::fmt::Display for AddressScreeningPurpose {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Recipient => f.write_str("recipient"),
            Self::Refund => f.write_str("refund"),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum AddressScreeningError {
    #[snafu(display(
        "address screening request failed for {purpose} address {address} on {chain}: {source}"
    ))]
    Request {
        purpose: AddressScreeningPurpose,
        chain: ChainId,
        address: String,
        source: ChainalysisError,
    },

    #[snafu(display(
        "address screening blocked {purpose} address {address} on {chain}: risk={risk}{reason_suffix}"
    ))]
    Blocked {
        purpose: AddressScreeningPurpose,
        chain: ChainId,
        address: String,
        risk: &'static str,
        reason_suffix: String,
    },
}

pub type AddressScreeningResult<T> = Result<T, AddressScreeningError>;
const MAX_ADDRESS_SCREENING_REASON_CHARS: usize = 256;

#[derive(Clone)]
pub struct AddressScreeningService {
    chainalysis: ChainalysisAddressScreener,
}

impl AddressScreeningService {
    pub fn new(
        host: impl Into<String>,
        token: impl Into<String>,
    ) -> Result<Self, ChainalysisError> {
        Ok(Self {
            chainalysis: ChainalysisAddressScreener::new(host, token)?,
        })
    }

    pub async fn screen_address(
        &self,
        purpose: AddressScreeningPurpose,
        chain: &ChainId,
        address: &str,
    ) -> AddressScreeningResult<()> {
        let response = self
            .chainalysis
            .get_address_risk(address)
            .await
            .map_err(|source| AddressScreeningError::Request {
                purpose,
                chain: chain.clone(),
                address: address.to_string(),
                source,
            })?;

        if address_risk_is_blocked(&response.risk) {
            return Err(AddressScreeningError::Blocked {
                purpose,
                chain: chain.clone(),
                address: address.to_string(),
                risk: risk_label(&response.risk),
                reason_suffix: risk_reason_suffix(response.risk_reason),
            });
        }

        Ok(())
    }
}

fn address_risk_is_blocked(risk: &RiskLevel) -> bool {
    matches!(
        risk,
        RiskLevel::High | RiskLevel::Severe | RiskLevel::Unknown
    )
}

fn risk_label(risk: &RiskLevel) -> &'static str {
    match risk {
        RiskLevel::Low => "low",
        RiskLevel::Medium => "medium",
        RiskLevel::High => "high",
        RiskLevel::Severe => "severe",
        RiskLevel::Unknown => "unknown",
    }
}

fn risk_reason_suffix(reason: Option<String>) -> String {
    let Some(reason) = reason.map(|reason| reason.trim().to_string()) else {
        return String::new();
    };
    if reason.is_empty() {
        return String::new();
    }

    let mut chars = reason.chars();
    let bounded: String = chars
        .by_ref()
        .take(MAX_ADDRESS_SCREENING_REASON_CHARS)
        .collect();
    if chars.next().is_some() {
        format!(", reason={bounded}...")
    } else {
        format!(", reason={bounded}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocks_high_severe_and_unknown_risk() {
        assert!(address_risk_is_blocked(&RiskLevel::High));
        assert!(address_risk_is_blocked(&RiskLevel::Severe));
        assert!(address_risk_is_blocked(&RiskLevel::Unknown));
    }

    #[test]
    fn allows_low_and_medium_risk() {
        assert!(!address_risk_is_blocked(&RiskLevel::Low));
        assert!(!address_risk_is_blocked(&RiskLevel::Medium));
    }

    #[test]
    fn risk_reason_suffix_is_trimmed_and_bounded_without_splitting_utf8() {
        assert_eq!(risk_reason_suffix(None), "");
        assert_eq!(risk_reason_suffix(Some("  ".to_string())), "");
        assert_eq!(
            risk_reason_suffix(Some("  sanctions exposure  ".to_string())),
            ", reason=sanctions exposure"
        );

        let reason = "é".repeat(MAX_ADDRESS_SCREENING_REASON_CHARS + 1);
        let suffix = risk_reason_suffix(Some(reason));

        assert!(suffix.ends_with("..."));
        assert_eq!(
            suffix
                .trim_start_matches(", reason=")
                .trim_end_matches("...")
                .chars()
                .count(),
            MAX_ADDRESS_SCREENING_REASON_CHARS
        );
    }
}

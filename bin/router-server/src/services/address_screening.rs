use crate::protocol::ChainId;
use chainalysis_address_screener::{
    ChainalysisAddressScreener, Error as ChainalysisError, RiskLevel,
};
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
                reason_suffix: response
                    .risk_reason
                    .filter(|reason| !reason.trim().is_empty())
                    .map(|reason| format!(", reason={reason}"))
                    .unwrap_or_default(),
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
}

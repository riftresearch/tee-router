use crate::models::DepositVaultFundingObservation;
use alloy::primitives::U256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObservedBitcoinOutpoint {
    pub tx_hash: String,
    pub vout: u32,
    pub amount_sats: u64,
}

pub(crate) fn observed_bitcoin_outpoint(
    observation: Option<&DepositVaultFundingObservation>,
) -> Result<Option<ObservedBitcoinOutpoint>, String> {
    let Some(observation) = observation else {
        return Ok(None);
    };
    let Some(tx_hash) = observation
        .tx_hash
        .as_ref()
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let Some(transfer_index) = observation.transfer_index else {
        return Ok(None);
    };
    let vout = u32::try_from(transfer_index)
        .map_err(|_| format!("bitcoin funding vout {transfer_index} exceeds u32 range"))?;
    let Some(observed_amount) = observation
        .observed_amount
        .as_ref()
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let amount = U256::from_str_radix(observed_amount, 10)
        .map_err(|_| format!("invalid bitcoin funding amount {observed_amount}"))?;
    if amount == U256::ZERO {
        return Err("bitcoin funding amount must be positive".to_string());
    }
    if amount > U256::from(u64::MAX) {
        return Err(format!(
            "bitcoin funding amount {observed_amount} exceeds u64 satoshi range"
        ));
    }

    Ok(Some(ObservedBitcoinOutpoint {
        tx_hash: tx_hash.clone(),
        vout,
        amount_sats: amount.to::<u64>(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    fn observation() -> DepositVaultFundingObservation {
        DepositVaultFundingObservation {
            tx_hash: Some("abc".to_string()),
            sender_address: None,
            sender_addresses: Vec::new(),
            recipient_address: Some("bcrt1qexample".to_string()),
            transfer_index: Some(7),
            observed_amount: Some("12345".to_string()),
            confirmation_state: Some("mempool".to_string()),
            observed_at: Some(Utc::now()),
            evidence: json!({}),
        }
    }

    #[test]
    fn observed_bitcoin_outpoint_extracts_tx_vout_and_amount() {
        let outpoint = observed_bitcoin_outpoint(Some(&observation()))
            .expect("valid")
            .expect("outpoint");

        assert_eq!(
            outpoint,
            ObservedBitcoinOutpoint {
                tx_hash: "abc".to_string(),
                vout: 7,
                amount_sats: 12_345,
            }
        );
    }

    #[test]
    fn observed_bitcoin_outpoint_absent_when_observation_is_incomplete() {
        assert!(observed_bitcoin_outpoint(None).unwrap().is_none());

        let mut missing_tx_hash = observation();
        missing_tx_hash.tx_hash = None;
        assert!(observed_bitcoin_outpoint(Some(&missing_tx_hash))
            .unwrap()
            .is_none());

        let mut missing_vout = observation();
        missing_vout.transfer_index = None;
        assert!(observed_bitcoin_outpoint(Some(&missing_vout))
            .unwrap()
            .is_none());

        let mut missing_amount = observation();
        missing_amount.observed_amount = None;
        assert!(observed_bitcoin_outpoint(Some(&missing_amount))
            .unwrap()
            .is_none());
    }

    #[test]
    fn observed_bitcoin_outpoint_rejects_invalid_amounts() {
        let mut invalid = observation();
        invalid.observed_amount = Some("not-digits".to_string());
        assert!(observed_bitcoin_outpoint(Some(&invalid)).is_err());

        let mut zero = observation();
        zero.observed_amount = Some("0".to_string());
        assert!(observed_bitcoin_outpoint(Some(&zero)).is_err());
    }

    #[test]
    fn observed_bitcoin_outpoint_rejects_vout_overflow() {
        let mut invalid = observation();
        invalid.transfer_index = Some(u64::from(u32::MAX) + 1);

        assert!(observed_bitcoin_outpoint(Some(&invalid)).is_err());
    }
}

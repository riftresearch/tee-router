use crate::models::DepositVaultFundingObservation;
use alloy::primitives::U256;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedBitcoinOutpoint {
    pub tx_hash: String,
    pub vout: u32,
    pub amount_sats: u64,
}

pub fn observed_bitcoin_outpoint(
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

/// All observed spendable outpoints for a deposit vault, parsed from the funding
/// observation's `evidence.utxos` snapshot (the full set sauron observed at the
/// deposit address). Falls back to the single primary outpoint for older hints
/// that predate the UTXO-set field. Skips structurally-incomplete entries; an
/// entry with an invalid/zero/oversized amount or oversized vout is an error
/// (mirrors `observed_bitcoin_outpoint`).
pub fn observed_bitcoin_utxos(
    observation: Option<&DepositVaultFundingObservation>,
) -> Result<Vec<ObservedBitcoinOutpoint>, String> {
    let Some(observation) = observation else {
        return Ok(Vec::new());
    };
    if let Some(entries) = observation
        .evidence
        .get("utxos")
        .and_then(Value::as_array)
    {
        let mut outpoints = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Some(outpoint) = utxo_from_evidence_entry(entry)? {
                outpoints.push(outpoint);
            }
        }
        if !outpoints.is_empty() {
            return Ok(outpoints);
        }
    }
    // Backward compatibility: hints without a utxos snapshot carry only the
    // single primary outpoint in the structured observation fields.
    Ok(observed_bitcoin_outpoint(Some(observation))?
        .into_iter()
        .collect())
}

fn utxo_from_evidence_entry(entry: &Value) -> Result<Option<ObservedBitcoinOutpoint>, String> {
    let Some(tx_hash) = entry
        .get("tx_hash")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let Some(vout_u64) = entry.get("vout").and_then(Value::as_u64) else {
        return Ok(None);
    };
    let vout = u32::try_from(vout_u64)
        .map_err(|_| format!("bitcoin funding vout {vout_u64} exceeds u32 range"))?;
    let Some(amount_str) = entry
        .get("amount")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    else {
        return Ok(None);
    };
    let amount = U256::from_str_radix(amount_str, 10)
        .map_err(|_| format!("invalid bitcoin funding amount {amount_str}"))?;
    if amount == U256::ZERO {
        return Err("bitcoin funding amount must be positive".to_string());
    }
    if amount > U256::from(u64::MAX) {
        return Err(format!(
            "bitcoin funding amount {amount_str} exceeds u64 satoshi range"
        ));
    }
    Ok(Some(ObservedBitcoinOutpoint {
        tx_hash: tx_hash.to_string(),
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

    fn observation_with_utxos(utxos: serde_json::Value) -> DepositVaultFundingObservation {
        let mut obs = observation();
        obs.evidence = json!({ "utxos": utxos });
        obs
    }

    #[test]
    fn observed_bitcoin_utxos_parses_full_set() {
        let obs = observation_with_utxos(json!([
            { "tx_hash": "aa", "vout": 0, "amount": "1000" },
            { "tx_hash": "bb", "vout": 3, "amount": "2500" },
        ]));

        assert_eq!(
            observed_bitcoin_utxos(Some(&obs)).expect("valid"),
            vec![
                ObservedBitcoinOutpoint {
                    tx_hash: "aa".to_string(),
                    vout: 0,
                    amount_sats: 1000,
                },
                ObservedBitcoinOutpoint {
                    tx_hash: "bb".to_string(),
                    vout: 3,
                    amount_sats: 2500,
                },
            ]
        );
    }

    #[test]
    fn observed_bitcoin_utxos_falls_back_to_single_outpoint() {
        // Old hints (no utxos array) fall back to the single primary outpoint.
        assert_eq!(
            observed_bitcoin_utxos(Some(&observation())).expect("valid"),
            vec![ObservedBitcoinOutpoint {
                tx_hash: "abc".to_string(),
                vout: 7,
                amount_sats: 12_345,
            }]
        );
    }

    #[test]
    fn observed_bitcoin_utxos_empty_for_none() {
        assert!(observed_bitcoin_utxos(None).unwrap().is_empty());
    }

    #[test]
    fn observed_bitcoin_utxos_skips_incomplete_but_rejects_bad_amount() {
        // Missing vout -> skipped; the valid sibling still parses.
        let partial = observation_with_utxos(json!([
            { "tx_hash": "aa", "amount": "1000" },
            { "tx_hash": "bb", "vout": 1, "amount": "2000" },
        ]));
        let utxos = observed_bitcoin_utxos(Some(&partial)).expect("valid");
        assert_eq!(utxos.len(), 1);
        assert_eq!(utxos[0].tx_hash, "bb");

        // A present-but-zero amount is an error, not a skip.
        let zero = observation_with_utxos(json!([
            { "tx_hash": "aa", "vout": 0, "amount": "0" },
        ]));
        assert!(observed_bitcoin_utxos(Some(&zero)).is_err());
    }
}

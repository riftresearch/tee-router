use crate::ingest::schema::{HlTransferEvent, HlTransferKind};

#[must_use]
pub fn canonical_key(event: &HlTransferEvent) -> String {
    match &event.kind {
        HlTransferKind::Fill { tid, .. } => format!("fill:{:?}:{tid}", event.user),
        HlTransferKind::Funding { .. } => {
            format!("funding:{:?}:{}:{}", event.user, event.asset, event.time_ms)
        }
        HlTransferKind::Deposit => {
            format!(
                "deposit:{:?}:{}:{}",
                event.user, event.hash, event.amount_delta
            )
        }
        HlTransferKind::Withdraw { nonce } => format!("withdraw:{:?}:{nonce}", event.user),
        HlTransferKind::SpotTransfer { nonce, .. } => {
            format!("spotxfer:{:?}:{nonce}", event.user)
        }
        other => format!(
            "misc:{:?}:{}:{}:{:?}",
            event.user, event.time_ms, event.hash, other
        ),
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;

    use super::*;
    use crate::ingest::schema::{DecimalString, HlMarket};

    fn base_event(kind: HlTransferKind) -> HlTransferEvent {
        HlTransferEvent {
            user: Address::repeat_byte(1),
            time_ms: 100,
            kind,
            asset: "USDC".to_string(),
            market: HlMarket::NotApplicable,
            amount_delta: DecimalString::new("1").expect("valid decimal"),
            fee: None,
            fee_token: None,
            hash: format!("0x{}", "aa".repeat(32)),
            observed_at_ms: 101,
        }
    }

    #[test]
    fn canonical_keys_follow_hl_dedup_rules() {
        assert_eq!(
            canonical_key(&base_event(HlTransferKind::Fill {
                oid: 9,
                tid: 7,
                side: "B".to_string(),
                px: "1".to_string(),
                sz: "1".to_string(),
                crossed: true,
                dir: "Buy".to_string(),
                closed_pnl: "0".to_string(),
                start_position: "0".to_string(),
                builder_fee: None,
            })),
            format!("fill:{:?}:7", Address::repeat_byte(1))
        );
        assert_eq!(
            canonical_key(&base_event(HlTransferKind::Withdraw { nonce: 42 })),
            format!("withdraw:{:?}:42", Address::repeat_byte(1))
        );
        assert_eq!(
            canonical_key(&base_event(HlTransferKind::SpotTransfer {
                destination: format!("{:?}", Address::repeat_byte(2)),
                native_token_fee: "0".to_string(),
                nonce: 4,
                usdc_value: "1".to_string(),
            })),
            format!("spotxfer:{:?}:4", Address::repeat_byte(1))
        );
        assert_eq!(
            canonical_key(&base_event(HlTransferKind::Funding {
                szi: "1".to_string(),
                funding_rate: "0.0001".to_string(),
            })),
            format!("funding:{:?}:USDC:100", Address::repeat_byte(1))
        );
        assert_eq!(
            canonical_key(&base_event(HlTransferKind::Deposit)),
            format!(
                "deposit:{:?}:0x{}:1",
                Address::repeat_byte(1),
                "aa".repeat(32)
            )
        );
        assert!(canonical_key(&base_event(HlTransferKind::InternalTransfer {
            destination: format!("{:?}", Address::repeat_byte(2)),
        }))
        .starts_with("misc:"));
        assert!(
            canonical_key(&base_event(HlTransferKind::AccountClassTransfer {
                to_perp: true
            }))
            .starts_with("misc:")
        );
    }
}

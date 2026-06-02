//! Hyperliquid spot source deposits: sign a spot transfer to the router vault.

use std::time::{SystemTime, UNIX_EPOCH};

use alloy::signers::local::PrivateKeySigner;
use eyre::{eyre, Result};
use hyperliquid_client::{HyperliquidClient, Network};

const DEFAULT_HYPERLIQUID_BASE_URL: &str = "https://api.hyperliquid.xyz";

pub fn address(private_key: &str) -> Result<String> {
    Ok(signer(private_key)?.address().to_string())
}

pub async fn send_deposit(
    private_key: &str,
    destination: &str,
    token_symbol: &str,
    decimals: u8,
    amount_raw: &str,
) -> Result<String> {
    let wallet = signer(private_key)?;
    let mut client =
        HyperliquidClient::new(DEFAULT_HYPERLIQUID_BASE_URL, wallet, None, Network::Mainnet)?;
    client.refresh_spot_meta().await?;
    let token = client.info().spot_token_wire(token_symbol)?;
    let amount = raw_to_decimal(amount_raw, decimals)?;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|err| eyre!("system clock before UNIX epoch: {err}"))?
        .as_millis() as u64;
    let response = client
        .send_asset(
            destination.to_string(),
            "spot".to_string(),
            "spot".to_string(),
            token,
            amount,
            nonce,
        )
        .await?;
    hyperliquid_response_error(&response)?;
    Ok(format!("hl-send-asset:{nonce}"))
}

fn signer(private_key: &str) -> Result<PrivateKeySigner> {
    private_key
        .trim()
        .parse::<PrivateKeySigner>()
        .map_err(|err| eyre!("invalid Hyperliquid/EVM private key: {err}"))
}

fn raw_to_decimal(raw: &str, decimals: u8) -> Result<String> {
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(eyre!("invalid raw Hyperliquid amount `{raw}`"));
    }
    let raw = raw.trim_start_matches('0');
    if raw.is_empty() {
        return Err(eyre!("Hyperliquid amount must be greater than zero"));
    }
    let decimals = decimals as usize;
    if decimals == 0 {
        return Ok(raw.to_string());
    }
    let mut digits = raw.to_string();
    if digits.len() <= decimals {
        let mut padded = String::with_capacity(decimals + 1);
        padded.push('0');
        for _ in 0..(decimals - digits.len()) {
            padded.push('0');
        }
        padded.push_str(&digits);
        digits = padded;
    }
    let split = digits.len() - decimals;
    let whole = &digits[..split];
    let fractional = digits[split..].trim_end_matches('0');
    if fractional.is_empty() {
        Ok(whole.to_string())
    } else {
        Ok(format!("{whole}.{fractional}"))
    }
}

fn hyperliquid_response_error(response: &serde_json::Value) -> Result<()> {
    if response
        .get("status")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("err"))
    {
        return Err(eyre!(
            "Hyperliquid returned transfer error: {}",
            response
                .get("response")
                .map(serde_json::Value::to_string)
                .unwrap_or_else(|| response.to_string())
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::raw_to_decimal;

    #[test]
    fn formats_raw_hyperliquid_amounts() {
        assert_eq!(raw_to_decimal("50000", 8).unwrap(), "0.0005");
        assert_eq!(raw_to_decimal("1234567", 6).unwrap(), "1.234567");
        assert_eq!(raw_to_decimal("1000000000000000", 18).unwrap(), "0.001");
    }
}

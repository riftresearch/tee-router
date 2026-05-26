use std::{env, error::Error, time::Duration};

use alloy::signers::local::PrivateKeySigner;
use clap::{Parser, ValueEnum};
use dotenvy::dotenv;
use hyperliquid_client::{client::Network, HyperliquidClient};
use serde_json::{json, Value};

const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";
const HYPERLIQUID_PRIVATE_KEY: &str = "HYPERLIQUID_LIVE_PRIVATE_KEY";
const HYPE_EVM_SYSTEM_ADDRESS: &str = "0x2222222222222222222222222222222222222222";
const HYPERLIQUID_TRANSFER_SETTLE_POLL_ATTEMPTS: usize = 30;
const HYPERLIQUID_TRANSFER_SETTLE_POLL_INTERVAL: Duration = Duration::from_secs(3);

type CliResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TransferMode {
    SpotSend,
    SendAsset,
}

impl TransferMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::SpotSend => "spot_send",
            Self::SendAsset => "send_asset",
        }
    }
}

#[derive(Parser)]
#[command(about = "Move live HYPE from Hyperliquid spot balance onto HyperEVM")]
struct Args {
    #[arg(
        long,
        env = "HYPERLIQUID_LIVE_BASE_URL",
        default_value = "https://api.hyperliquid.xyz"
    )]
    hyperliquid_base_url: String,

    #[arg(long, env = "HYPERLIQUID_LIVE_NETWORK", default_value = "mainnet")]
    hyperliquid_network: String,

    #[arg(long)]
    private_key: Option<String>,

    #[arg(long, value_enum, default_value = "send-asset")]
    mode: TransferMode,

    #[arg(long, default_value = "HYPE:150")]
    token: String,

    #[arg(long, default_value = "spot")]
    source_dex: String,

    #[arg(long, default_value = "spot")]
    destination_dex: String,

    #[arg(long, default_value = "0.01")]
    amount: String,
}

#[tokio::main]
async fn main() -> CliResult<()> {
    let _ = dotenv();
    let args = Args::parse();
    let private_key = args
        .private_key
        .or_else(|| env::var(LIVE_TEST_PRIVATE_KEY).ok())
        .or_else(|| env::var(HYPERLIQUID_PRIVATE_KEY).ok())
        .ok_or_else(|| {
            format!("missing private key: pass --private-key or set {LIVE_TEST_PRIVATE_KEY}")
        })?;
    let wallet = private_key.parse::<PrivateKeySigner>()?;
    let user = wallet.address();
    let network = match args.hyperliquid_network.to_ascii_lowercase().as_str() {
        "mainnet" => Network::Mainnet,
        "testnet" => Network::Testnet,
        other => return Err(format!("unsupported Hyperliquid network {other:?}").into()),
    };
    let client = HyperliquidClient::new(&args.hyperliquid_base_url, wallet, None, network)?;

    let before_spot = client.spot_clearinghouse_state(user).await?;
    let time_ms: u64 = utc::now().timestamp_millis().try_into()?;
    let response = match args.mode {
        TransferMode::SpotSend => {
            client
                .spot_send(
                    HYPE_EVM_SYSTEM_ADDRESS.to_string(),
                    args.token.clone(),
                    args.amount.clone(),
                    time_ms,
                )
                .await?
        }
        TransferMode::SendAsset => {
            client
                .send_asset(
                    HYPE_EVM_SYSTEM_ADDRESS.to_string(),
                    args.source_dex.clone(),
                    args.destination_dex.clone(),
                    args.token.clone(),
                    args.amount.clone(),
                    time_ms,
                )
                .await?
        }
    };
    if let Some(error) = hyperliquid_response_error(&response) {
        return Err(format!("Hyperliquid returned transfer error: {error}").into());
    }

    let mut after_spot = None;
    for _ in 0..HYPERLIQUID_TRANSFER_SETTLE_POLL_ATTEMPTS {
        let state = client.spot_clearinghouse_state(user).await?;
        let before_hype = before_spot.balance_of("HYPE").parse::<f64>().unwrap_or(0.0);
        let current_hype = state.balance_of("HYPE").parse::<f64>().unwrap_or(0.0);
        if current_hype < before_hype {
            after_spot = Some(state);
            break;
        }
        tokio::time::sleep(HYPERLIQUID_TRANSFER_SETTLE_POLL_INTERVAL).await;
    }
    let after_spot = after_spot.unwrap_or(client.spot_clearinghouse_state(user).await?);

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "user": format!("{user:#x}"),
            "destination": HYPE_EVM_SYSTEM_ADDRESS,
            "mode": args.mode.as_str(),
            "token": args.token,
            "source_dex": args.source_dex,
            "destination_dex": args.destination_dex,
            "amount": args.amount,
            "before_spot": before_spot,
            "exchange_response": response,
            "after_spot": after_spot,
        }))?
    );

    Ok(())
}

fn hyperliquid_response_error(response: &Value) -> Option<String> {
    if response
        .get("status")
        .and_then(Value::as_str)
        .is_some_and(|status| status.eq_ignore_ascii_case("err"))
    {
        return Some(
            response
                .get("response")
                .map(Value::to_string)
                .unwrap_or_else(|| response.to_string()),
        );
    }
    response
        .pointer("/response/data/statuses")
        .and_then(Value::as_array)
        .and_then(|statuses| {
            statuses
                .iter()
                .find_map(|status| status.get("error").map(Value::to_string))
        })
}
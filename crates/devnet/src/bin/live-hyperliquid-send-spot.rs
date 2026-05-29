use std::{
    env,
    error::Error,
    time::{SystemTime, UNIX_EPOCH},
};

use alloy::{primitives::Address, signers::local::PrivateKeySigner};
use clap::Parser;
use dotenvy::dotenv;
use hyperliquid_client::{client::Network, HyperliquidClient, HyperliquidInfoClient};
use serde_json::json;

const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";

#[derive(Parser)]
#[command(
    about = "Send a live Hyperliquid spot token from the live test wallet to another HL account"
)]
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

    #[arg(long)]
    destination: Address,

    #[arg(long, default_value = "HYPE")]
    coin: String,

    #[arg(long)]
    amount: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let _ = dotenv();
    let args = Args::parse();
    let private_key = args
        .private_key
        .or_else(|| env::var(LIVE_TEST_PRIVATE_KEY).ok())
        .ok_or_else(|| {
            format!("missing private key: pass --private-key or set {LIVE_TEST_PRIVATE_KEY}")
        })?;
    let wallet = private_key.parse::<PrivateKeySigner>()?;
    let source = wallet.address();
    let network = match args.hyperliquid_network.to_ascii_lowercase().as_str() {
        "mainnet" => Network::Mainnet,
        "testnet" => Network::Testnet,
        other => return Err(format!("unsupported Hyperliquid network {other:?}").into()),
    };

    let mut info = HyperliquidInfoClient::new(&args.hyperliquid_base_url)?;
    info.refresh_spot_meta().await?;
    let token = info.spot_token_wire(&args.coin)?;
    let before_source = info.spot_clearinghouse_state(source).await?;
    let before_destination = info.spot_clearinghouse_state(args.destination).await?;

    let client = HyperliquidClient::new(&args.hyperliquid_base_url, wallet, None, network)?;
    let response = client
        .send_asset(
            format!("{:#x}", args.destination),
            "spot".to_string(),
            "spot".to_string(),
            token.clone(),
            args.amount.clone(),
            current_time_ms(),
        )
        .await?;

    let after_source = info.spot_clearinghouse_state(source).await?;
    let after_destination = info.spot_clearinghouse_state(args.destination).await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "source": format!("{source:#x}"),
            "destination": format!("{:#x}", args.destination),
            "coin": args.coin,
            "token": token,
            "amount": args.amount,
            "response": response,
            "before_source": before_source,
            "after_source": after_source,
            "before_destination": before_destination,
            "after_destination": after_destination,
        }))?
    );

    Ok(())
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_millis()
        .try_into()
        .expect("timestamp fits u64")
}

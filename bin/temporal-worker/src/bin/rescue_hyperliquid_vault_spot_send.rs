use std::{env, error::Error, fs, path::PathBuf, sync::Arc, time::Duration};

use alloy::primitives::Address;
use chains::{evm::EvmChain, hyperliquid::HyperliquidChain, ChainRegistry};
use clap::Parser;
use router_core::{
    config::Settings,
    db::Database,
    services::custody_action_executor::{
        ChainCall, CustodyAction, CustodyActionExecutor, CustodyActionRequest, HyperliquidCall,
        HyperliquidCallNetwork, HyperliquidCallPayload, HyperliquidRuntimeConfig,
    },
};
use router_primitives::ChainType;
use serde_json::json;
use uuid::Uuid;

#[derive(Parser)]
#[command(about = "Rescue a Hyperliquid spot token from a router custody vault")]
struct Args {
    #[arg(
        long,
        env = "DATABASE_URL",
        default_value = "postgres://router_app:router_app@127.0.0.1:55432/router_db"
    )]
    database_url: String,

    #[arg(long, env = "ROUTER_MASTER_KEY_PATH")]
    master_key_path: Option<PathBuf>,

    #[arg(long, env = "ROUTER_LIVE_LOCAL_MASTER_KEY_HEX", hide = true)]
    master_key_hex: Option<String>,

    #[arg(
        long,
        env = "HYPERLIQUID_API_URL",
        default_value = "https://api.hyperliquid.xyz"
    )]
    hyperliquid_api_url: String,

    #[arg(long, env = "HYPERLIQUID_NETWORK", default_value = "mainnet")]
    hyperliquid_network: String,
    #[arg(long, env = "ARBITRUM_RPC_URL")]
    arbitrum_rpc_url: String,

    #[arg(
        long,
        env = "ARBITRUM_ALLOWED_TOKEN",
        default_value = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
    )]
    arbitrum_reference_token: String,

    #[arg(long)]
    custody_vault_id: Uuid,

    #[arg(long)]
    destination: Address,

    #[arg(long, default_value = "USDC")]
    token: String,

    #[arg(long)]
    amount: String,

    #[arg(long)]
    from_clearinghouse: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();
    let settings = Arc::new(load_settings(&args)?);
    let db = Database::connect(&args.database_url, 8, 1).await?;
    let mut chain_registry = ChainRegistry::new();
    let arbitrum_chain = Arc::new(
        EvmChain::new(
            &args.arbitrum_rpc_url,
            &args.arbitrum_reference_token,
            ChainType::Arbitrum,
            b"router-arbitrum-wallet",
            2,
            Duration::from_secs(2),
        )
        .await?,
    );
    chain_registry.register_evm(ChainType::Arbitrum, arbitrum_chain);
    chain_registry.register(
        ChainType::Hyperliquid,
        Arc::new(HyperliquidChain::new(
            b"router-hyperliquid-wallet",
            1,
            Duration::from_secs(1),
        )),
    );
    let chain_registry = Arc::new(chain_registry);
    let network = match args.hyperliquid_network.to_ascii_lowercase().as_str() {
        "mainnet" => HyperliquidCallNetwork::Mainnet,
        "testnet" => HyperliquidCallNetwork::Testnet,
        other => return Err(format!("unsupported Hyperliquid network {other:?}").into()),
    };
    let executor =
        CustodyActionExecutor::new(db, settings, chain_registry).with_hyperliquid_runtime(Some(
            HyperliquidRuntimeConfig::new(args.hyperliquid_api_url.clone(), network),
        ));

    let transfer_receipt = if args.from_clearinghouse {
        Some(
            executor
                .execute(CustodyActionRequest {
                    custody_vault_id: args.custody_vault_id,
                    action: CustodyAction::Call(ChainCall::Hyperliquid(HyperliquidCall {
                        target_base_url: args.hyperliquid_api_url.clone(),
                        network,
                        vault_address: None,
                        payload: HyperliquidCallPayload::UsdClassTransfer {
                            amount: args.amount.clone(),
                            to_perp: false,
                        },
                    })),
                })
                .await?,
        )
    } else {
        None
    };

    let receipt = executor
        .execute(CustodyActionRequest {
            custody_vault_id: args.custody_vault_id,
            action: CustodyAction::Call(ChainCall::Hyperliquid(HyperliquidCall {
                target_base_url: args.hyperliquid_api_url,
                network,
                vault_address: None,
                payload: HyperliquidCallPayload::SendAsset {
                    destination: format!("{:#x}", args.destination),
                    source_dex: "spot".to_string(),
                    destination_dex: "spot".to_string(),
                    token: args.token.clone(),
                    amount: args.amount.clone(),
                },
            })),
        })
        .await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "custody_vault_id": args.custody_vault_id,
            "destination": format!("{:#x}", args.destination),
            "token": args.token,
            "amount": args.amount,
            "transfer_from_clearinghouse": transfer_receipt.as_ref().map(|receipt| json!({
                "tx_hash": receipt.tx_hash,
                "response": receipt.response,
            })),
            "tx_hash": receipt.tx_hash,
            "response": receipt.response,
        }))?
    );
    Ok(())
}

fn load_settings(args: &Args) -> Result<Settings, Box<dyn Error + Send + Sync>> {
    if let Some(path) = args.master_key_path.as_ref() {
        return Ok(Settings::load(path)?);
    }
    let env_hex;
    let hex = if let Some(hex) = args.master_key_hex.as_deref() {
        hex
    } else {
        env_hex = env::var("ROUTER_LIVE_LOCAL_MASTER_KEY_HEX").map_err(|_| {
            "ROUTER_MASTER_KEY_PATH or ROUTER_LIVE_LOCAL_MASTER_KEY_HEX is required"
        })?;
        env_hex.as_str()
    };
    let mut path = env::temp_dir();
    path.push(format!("tee-router-master-key-{}.hex", std::process::id()));
    fs::write(&path, hex.trim())?;
    let settings = Settings::load(&path)?;
    let _ = fs::remove_file(&path);
    Ok(settings)
}

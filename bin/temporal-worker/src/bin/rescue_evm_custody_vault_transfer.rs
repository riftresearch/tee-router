use std::{env, error::Error, fs, path::PathBuf, sync::Arc, time::Duration};

use chains::evm::{EvmChain, EvmGasSponsorConfig};
use chains::ChainRegistry;
use clap::Parser;
use router_core::{
    config::Settings,
    db::Database,
    services::custody_action_executor::{
        CustodyAction, CustodyActionExecutor, CustodyActionRequest,
    },
};
use router_primitives::ChainType;
use serde_json::json;
use uuid::Uuid;

#[derive(Parser)]
#[command(about = "Rescue an EVM token/native balance from a router custody vault")]
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

    #[arg(long, env = "BASE_RPC_URL")]
    base_rpc_url: String,

    #[arg(long, env = "BASE_PAYMASTER_PRIVATE_KEY")]
    base_paymaster_private_key: Option<String>,

    #[arg(long)]
    custody_vault_id: Uuid,

    #[arg(long)]
    destination: String,

    #[arg(long)]
    amount: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();
    let settings = Arc::new(load_settings(&args)?);
    let db = Database::connect(&args.database_url, 8, 1).await?;

    let mut chain_registry = ChainRegistry::new();
    chain_registry.register_evm(
        ChainType::Base,
        Arc::new(
            EvmChain::new_with_gas_sponsor(
                &args.base_rpc_url,
                ChainType::Base,
                b"router-base-wallet",
                2,
                Duration::from_secs(2),
                args.base_paymaster_private_key
                    .clone()
                    .map(|private_key| EvmGasSponsorConfig { private_key }),
            )
            .await?,
        ),
    );

    let executor = CustodyActionExecutor::new(db, settings, Arc::new(chain_registry));
    let receipt = executor
        .execute(CustodyActionRequest {
            custody_vault_id: args.custody_vault_id,
            action: CustodyAction::Transfer {
                to_address: args.destination.clone(),
                amount: args.amount.clone(),
                bitcoin_fee_budget_sats: None,
            },
        })
        .await?;

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "custody_vault_id": args.custody_vault_id,
            "destination": args.destination,
            "amount": args.amount,
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

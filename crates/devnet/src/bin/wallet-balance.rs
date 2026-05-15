use std::{collections::BTreeSet, env, error::Error, future::Future, str::FromStr, time::Duration};

use alloy::{
    primitives::{Address, U256},
    providers::{Provider, ProviderBuilder},
    signers::local::PrivateKeySigner,
    sol,
};
use clap::Parser;
use devnet::evm_devnet::{
    ARBITRUM_CBBTC_ADDRESS, ARBITRUM_USDC_ADDRESS, ARBITRUM_USDT_ADDRESS, BASE_CBBTC_ADDRESS,
    BASE_USDC_ADDRESS, BASE_USDT_ADDRESS, ETHEREUM_CBBTC_ADDRESS, ETHEREUM_USDC_ADDRESS,
    ETHEREUM_USDT_ADDRESS,
};
use dotenvy::dotenv;
use hyperliquid_client::{HyperliquidInfoClient, SpotClearinghouseState, SpotMeta};
use serde_json::{json, Map, Value};
use url::Url;

type CliResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

const LIVE_TEST_PRIVATE_KEY: &str = "LIVE_TEST_PRIVATE_KEY";
const DEFAULT_ETHEREUM_RPC_URL: &str = "http://localhost:50101";
const DEFAULT_BASE_RPC_URL: &str = "http://localhost:50102";
const DEFAULT_ARBITRUM_RPC_URL: &str = "http://localhost:50103";
const DEFAULT_HYPERLIQUID_BASE_URL: &str = "http://localhost:50107";
const DEFAULT_BITCOIN_ESPLORA_URL: &str = "http://localhost:50110";
const RPC_RETRY_ATTEMPTS: usize = 6;

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }
}

#[derive(Parser)]
#[command(
    name = "wallet-balance",
    about = "Read test-wallet balances across EVM chains, Hyperliquid, and Bitcoin"
)]
struct Args {
    /// Address to inspect. EVM addresses are queried on EVM + Hyperliquid;
    /// non-EVM addresses are treated as Bitcoin addresses.
    #[arg(short = 'a', long = "address")]
    address: Vec<String>,

    /// EVM/Hyperliquid address to inspect.
    #[arg(long = "evm-address")]
    evm_address: Vec<String>,

    /// Bitcoin address to inspect through Esplora.
    #[arg(long = "bitcoin-address")]
    bitcoin_address: Vec<String>,

    /// EVM private key to derive an EVM/Hyperliquid address from.
    #[arg(long = "private-key")]
    private_key: Vec<String>,

    /// Ethereum RPC URL. Defaults to ETH_RPC_URL/ETHEREUM_RPC_URL or local devnet.
    #[arg(long = "ethereum-rpc-url")]
    ethereum_rpc_url: Option<String>,

    /// Base RPC URL. Defaults to BASE_RPC_URL or local devnet.
    #[arg(long = "base-rpc-url")]
    base_rpc_url: Option<String>,

    /// Arbitrum RPC URL. Defaults to ARBITRUM_RPC_URL or local devnet.
    #[arg(long = "arbitrum-rpc-url")]
    arbitrum_rpc_url: Option<String>,

    /// Hyperliquid /info base URL. Defaults to HYPERLIQUID_API_URL,
    /// HYPERLIQUID_LIVE_BASE_URL, or local provider mock.
    #[arg(long = "hyperliquid-base-url")]
    hyperliquid_base_url: Option<String>,

    /// Bitcoin Esplora URL. Defaults to ELECTRUM_HTTP_SERVER_URL,
    /// BITCOIN_ESPLORA_URL, or local devnet Esplora.
    #[arg(long = "bitcoin-esplora-url")]
    bitcoin_esplora_url: Option<String>,

    /// Do not query EVM chains.
    #[arg(long)]
    skip_evm: bool,

    /// Do not query Hyperliquid.
    #[arg(long)]
    skip_hyperliquid: bool,

    /// Do not query Bitcoin/Esplora.
    #[arg(long)]
    skip_bitcoin: bool,
}

struct EvmChain {
    name: &'static str,
    chain_id_hint: u64,
    rpc_url: String,
    tokens: &'static [Erc20Asset],
}

#[derive(Clone, Copy)]
struct Erc20Asset {
    symbol: &'static str,
    address: &'static str,
    decimals: usize,
}

#[tokio::main]
async fn main() -> CliResult<()> {
    let _ = dotenv();
    let args = Args::parse();
    let wallet_set = resolve_wallets(&args)?;
    if wallet_set.evm_addresses.is_empty() && wallet_set.bitcoin_addresses.is_empty() {
        return Err(format!(
            "missing wallet selector: pass --address, --evm-address, --bitcoin-address, --private-key, or set {LIVE_TEST_PRIVATE_KEY}"
        )
        .into());
    }

    let evm_chains = [
        EvmChain {
            name: "ethereum",
            chain_id_hint: 1,
            rpc_url: configured_value(
                args.ethereum_rpc_url,
                &[
                    "ETH_RPC_URL",
                    "ETHEREUM_RPC_URL",
                    "ROUTER_LIVE_ETHEREUM_RPC_URL",
                    "HYPERUNIT_LIVE_ETHEREUM_RPC_URL",
                ],
                DEFAULT_ETHEREUM_RPC_URL,
            ),
            tokens: &[
                Erc20Asset {
                    symbol: "USDC",
                    address: ETHEREUM_USDC_ADDRESS,
                    decimals: 6,
                },
                Erc20Asset {
                    symbol: "USDT",
                    address: ETHEREUM_USDT_ADDRESS,
                    decimals: 6,
                },
                Erc20Asset {
                    symbol: "cbBTC",
                    address: ETHEREUM_CBBTC_ADDRESS,
                    decimals: 8,
                },
            ],
        },
        EvmChain {
            name: "base",
            chain_id_hint: 8453,
            rpc_url: configured_value(
                args.base_rpc_url,
                &[
                    "BASE_RPC_URL",
                    "ROUTER_LIVE_BASE_RPC_URL",
                    "VELORA_LIVE_BASE_RPC_URL",
                    "CCTP_LIVE_BASE_RPC_URL",
                ],
                DEFAULT_BASE_RPC_URL,
            ),
            tokens: &[
                Erc20Asset {
                    symbol: "USDC",
                    address: BASE_USDC_ADDRESS,
                    decimals: 6,
                },
                Erc20Asset {
                    symbol: "USDT",
                    address: BASE_USDT_ADDRESS,
                    decimals: 6,
                },
                Erc20Asset {
                    symbol: "cbBTC",
                    address: BASE_CBBTC_ADDRESS,
                    decimals: 8,
                },
            ],
        },
        EvmChain {
            name: "arbitrum",
            chain_id_hint: 42161,
            rpc_url: configured_value(
                args.arbitrum_rpc_url,
                &[
                    "ARBITRUM_RPC_URL",
                    "ROUTER_LIVE_ARBITRUM_RPC_URL",
                    "CCTP_LIVE_ARBITRUM_RPC_URL",
                    "HYPERLIQUID_LIVE_ARBITRUM_RPC_URL",
                ],
                DEFAULT_ARBITRUM_RPC_URL,
            ),
            tokens: &[
                Erc20Asset {
                    symbol: "USDC",
                    address: ARBITRUM_USDC_ADDRESS,
                    decimals: 6,
                },
                Erc20Asset {
                    symbol: "USDT",
                    address: ARBITRUM_USDT_ADDRESS,
                    decimals: 6,
                },
                Erc20Asset {
                    symbol: "cbBTC",
                    address: ARBITRUM_CBBTC_ADDRESS,
                    decimals: 8,
                },
            ],
        },
    ];
    let hyperliquid_base_url = configured_value(
        args.hyperliquid_base_url,
        &[
            "HYPERLIQUID_API_URL",
            "HYPERLIQUID_LIVE_BASE_URL",
            "ROUTER_LIVE_HYPERLIQUID_API_URL",
        ],
        DEFAULT_HYPERLIQUID_BASE_URL,
    );
    let bitcoin_esplora_url = configured_value(
        args.bitcoin_esplora_url,
        &["ELECTRUM_HTTP_SERVER_URL", "BITCOIN_ESPLORA_URL"],
        DEFAULT_BITCOIN_ESPLORA_URL,
    );

    let mut evm_reports = Vec::new();
    for owner in &wallet_set.evm_addresses {
        evm_reports.push(
            evm_wallet_report(
                *owner,
                (!args.skip_evm).then_some(evm_chains.as_slice()),
                (!args.skip_hyperliquid).then_some(hyperliquid_base_url.as_str()),
            )
            .await,
        );
    }

    let mut bitcoin_reports = Vec::new();
    if !args.skip_bitcoin {
        for address in &wallet_set.bitcoin_addresses {
            bitcoin_reports.push(bitcoin_wallet_report(address, &bitcoin_esplora_url).await);
        }
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&json!({
            "kind": "wallet_balance",
            "evm_wallets": evm_reports,
            "bitcoin_wallets": bitcoin_reports,
        }))?
    );

    Ok(())
}

struct WalletSet {
    evm_addresses: BTreeSet<Address>,
    bitcoin_addresses: BTreeSet<String>,
}

fn resolve_wallets(args: &Args) -> CliResult<WalletSet> {
    let mut evm_addresses = BTreeSet::new();
    let mut bitcoin_addresses = BTreeSet::new();

    for raw in &args.private_key {
        evm_addresses.insert(raw.parse::<PrivateKeySigner>()?.address());
    }
    let has_explicit_selector = !args.address.is_empty()
        || !args.evm_address.is_empty()
        || !args.bitcoin_address.is_empty()
        || !args.private_key.is_empty();
    if !has_explicit_selector {
        if let Some(raw) = env_var_any(&[LIVE_TEST_PRIVATE_KEY]) {
            evm_addresses.insert(raw.parse::<PrivateKeySigner>()?.address());
        }
    }
    for raw in &args.evm_address {
        evm_addresses.insert(Address::from_str(raw)?);
    }
    for raw in &args.bitcoin_address {
        bitcoin_addresses.insert(raw.trim().to_string());
    }
    for raw in &args.address {
        match Address::from_str(raw) {
            Ok(address) => {
                evm_addresses.insert(address);
            }
            Err(_) => {
                bitcoin_addresses.insert(raw.trim().to_string());
            }
        }
    }

    Ok(WalletSet {
        evm_addresses,
        bitcoin_addresses,
    })
}

async fn evm_wallet_report(
    owner: Address,
    evm_chains: Option<&[EvmChain]>,
    hyperliquid_base_url: Option<&str>,
) -> Value {
    let mut chains = Map::new();
    if let Some(evm_chains) = evm_chains {
        for chain in evm_chains {
            chains.insert(chain.name.to_string(), evm_chain_report(chain, owner).await);
        }
    }

    let hyperliquid = match hyperliquid_base_url {
        Some(base_url) => hyperliquid_report(base_url, owner).await,
        None => json!({ "skipped": true }),
    };

    json!({
        "address": format!("{owner:#x}"),
        "evm": chains,
        "hyperliquid": hyperliquid,
    })
}

async fn evm_chain_report(chain: &EvmChain, owner: Address) -> Value {
    let rpc_url = match Url::parse(&chain.rpc_url) {
        Ok(url) => url,
        Err(error) => {
            return json!({
                "chain_id_hint": chain.chain_id_hint,
                "error": format!("invalid RPC URL: {error}"),
            })
        }
    };
    let provider = ProviderBuilder::new().connect_http(rpc_url);
    let chain_id = match retry_rpc(&format!("{} get_chain_id", chain.name), || async {
        provider.get_chain_id().await.map_err(box_error)
    })
    .await
    {
        Ok(chain_id) => chain_id,
        Err(error) => {
            return json!({
                "chain_id_hint": chain.chain_id_hint,
                "error": safe_error(error.as_ref()),
            })
        }
    };

    let native = match retry_rpc(&format!("{} get_balance", chain.name), || async {
        provider.get_balance(owner).await.map_err(box_error)
    })
    .await
    {
        Ok(balance) => amount_json("ETH", None, balance, 18),
        Err(error) => json!({
            "symbol": "ETH",
            "error": safe_error(error.as_ref()),
        }),
    };
    let mut assets = Map::new();
    assets.insert("ETH".to_string(), native);
    for token in chain.tokens {
        let token_address = match Address::from_str(token.address) {
            Ok(address) => address,
            Err(error) => {
                assets.insert(
                    token.symbol.to_string(),
                    json!({
                        "symbol": token.symbol,
                        "token": token.address,
                        "error": format!("invalid token address: {error}"),
                    }),
                );
                continue;
            }
        };
        let balance = read_erc20_balance(&provider, token_address, owner).await;
        assets.insert(
            token.symbol.to_string(),
            match balance {
                Ok(balance) => {
                    amount_json(token.symbol, Some(token_address), balance, token.decimals)
                }
                Err(error) => json!({
                    "symbol": token.symbol,
                    "token": format!("{token_address:#x}"),
                    "error": safe_error(error.as_ref()),
                }),
            },
        );
    }

    json!({
        "chain_id": chain_id,
        "chain_id_hint": chain.chain_id_hint,
        "assets": assets,
    })
}

async fn hyperliquid_report(base_url: &str, owner: Address) -> Value {
    let mut client = match HyperliquidInfoClient::new(base_url) {
        Ok(client) => client,
        Err(error) => return json!({ "error": safe_error(&error) }),
    };
    let meta = match client.refresh_spot_meta().await {
        Ok(meta) => meta,
        Err(error) => return json!({ "error": safe_error(&error) }),
    };
    let spot = match client.spot_clearinghouse_state(owner).await {
        Ok(spot) => spot,
        Err(error) => return json!({ "error": safe_error(&error) }),
    };
    let clearinghouse = match client.clearinghouse_state(owner).await {
        Ok(clearinghouse) => clearinghouse,
        Err(error) => return json!({ "error": safe_error(&error) }),
    };

    let mut assets = Map::new();
    for symbol in ["UETH", "UBTC", "USDC"] {
        assets.insert(
            symbol.to_string(),
            hyperliquid_asset_json(&meta, &spot, symbol),
        );
    }
    json!({
        "user": format!("{owner:#x}"),
        "spot_assets": assets,
        "withdrawable_usdc": clearinghouse.withdrawable,
        "margin_summary": clearinghouse.margin_summary,
        "cross_margin_summary": clearinghouse.cross_margin_summary,
    })
}

fn hyperliquid_asset_json(
    meta: &SpotMeta,
    spot: &SpotClearinghouseState,
    symbol: &'static str,
) -> Value {
    let token = meta
        .tokens
        .iter()
        .find(|token| token.name.eq_ignore_ascii_case(symbol));
    let balance = spot
        .balances
        .iter()
        .find(|balance| balance.coin.eq_ignore_ascii_case(symbol));
    json!({
        "symbol": symbol,
        "token_index": token.map(|token| token.index),
        "token_id": token.and_then(|token| token.token_id.clone()),
        "token_wire": token.and_then(|token| {
            token
                .token_id
                .as_deref()
                .map(|token_id| format!("{}:{token_id}", token.name))
        }),
        "total": balance.map_or("0", |balance| balance.total.as_str()),
        "hold": balance.map_or("0", |balance| balance.hold.as_str()),
    })
}

async fn bitcoin_wallet_report(address: &str, esplora_base_url: &str) -> Value {
    let base_url = match Url::parse(esplora_base_url) {
        Ok(url) => url,
        Err(error) => {
            return json!({
                "address": address,
                "error": format!("invalid Esplora URL: {error}"),
            })
        }
    };
    let mut url = base_url;
    url.set_path(&format!(
        "{}/address/{}",
        url.path().trim_end_matches('/'),
        address.trim()
    ));
    let response = match reqwest::Client::new().get(url.clone()).send().await {
        Ok(response) => response,
        Err(error) => {
            return json!({
                "address": address,
                "error": safe_error(&error),
            })
        }
    };
    let status = response.status();
    let body = match response.text().await {
        Ok(body) => body,
        Err(error) => {
            return json!({
                "address": address,
                "error": safe_error(&error),
            })
        }
    };
    if !status.is_success() {
        return json!({
            "address": address,
            "error": format!("Esplora returned {status}"),
        });
    }
    let body: Value = match serde_json::from_str(&body) {
        Ok(body) => body,
        Err(error) => {
            return json!({
                "address": address,
                "error": format!("invalid Esplora JSON: {error}"),
            })
        }
    };
    let confirmed_sats = stat_balance_sats(&body["chain_stats"]);
    let mempool_sats = stat_balance_sats(&body["mempool_stats"]);
    let total_sats = confirmed_sats + mempool_sats;
    json!({
        "address": address,
        "assets": {
            "BTC": {
                "symbol": "BTC",
                "confirmed_sats": confirmed_sats,
                "mempool_sats": mempool_sats,
                "total_sats": total_sats,
                "formatted": format_signed_units(total_sats, 8),
            }
        }
    })
}

fn stat_balance_sats(stats: &Value) -> i64 {
    let funded = stats
        .get("funded_txo_sum")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let spent = stats
        .get("spent_txo_sum")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    funded - spent
}

fn amount_json(symbol: &str, token: Option<Address>, raw: U256, decimals: usize) -> Value {
    let mut value = json!({
        "symbol": symbol,
        "raw": raw.to_string(),
        "formatted": format_units(raw, decimals),
    });
    if let Some(token) = token {
        value["token"] = json!(format!("{token:#x}"));
    }
    value
}

async fn read_erc20_balance<P>(provider: &P, token: Address, owner: Address) -> CliResult<U256>
where
    P: Provider + Clone,
{
    retry_rpc("erc20 balanceOf", || async {
        IERC20::new(token, provider.clone())
            .balanceOf(owner)
            .call()
            .await
            .map_err(box_error)
    })
    .await
}

fn configured_value(cli_value: Option<String>, env_keys: &[&str], default: &str) -> String {
    cli_value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .or_else(|| env_var_any(env_keys))
        .unwrap_or_else(|| default.to_string())
}

fn env_var_any(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn format_units(value: U256, decimals: usize) -> String {
    format_units_str(&value.to_string(), decimals)
}

fn format_signed_units(value: i64, decimals: usize) -> String {
    if value < 0 {
        format!(
            "-{}",
            format_units_str(&value.saturating_abs().to_string(), decimals)
        )
    } else {
        format_units_str(&value.to_string(), decimals)
    }
}

fn format_units_str(raw: &str, decimals: usize) -> String {
    if decimals == 0 {
        return raw.to_string();
    }

    if raw.len() <= decimals {
        let mut formatted = format!("0.{}{}", "0".repeat(decimals - raw.len()), raw);
        trim_trailing_decimal_zeros(&mut formatted);
        return formatted;
    }

    let split = raw.len() - decimals;
    let mut formatted = format!("{}.{}", &raw[..split], &raw[split..]);
    trim_trailing_decimal_zeros(&mut formatted);
    formatted
}

fn trim_trailing_decimal_zeros(formatted: &mut String) {
    while formatted.ends_with('0') {
        formatted.pop();
    }
    if formatted.ends_with('.') {
        formatted.pop();
    }
}

fn box_error<E>(err: E) -> Box<dyn Error + Send + Sync>
where
    E: Error + Send + Sync + 'static,
{
    Box::new(err)
}

async fn retry_rpc<T, Fut, Op>(label: &str, mut op: Op) -> CliResult<T>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = CliResult<T>>,
{
    let mut delay = Duration::from_millis(500);
    let mut attempt = 1;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(err) if attempt < RPC_RETRY_ATTEMPTS && is_retryable_rpc_error(err.as_ref()) => {
                eprintln!(
                    "retrying RPC {label} after attempt {attempt}/{RPC_RETRY_ATTEMPTS}: {}",
                    safe_error(err.as_ref())
                );
                tokio::time::sleep(delay).await;
                delay = delay.saturating_mul(2);
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

fn safe_error(err: &(dyn Error + 'static)) -> String {
    sanitize_urlish_secrets(&err.to_string())
}

fn sanitize_urlish_secrets(value: &str) -> String {
    value
        .split_whitespace()
        .map(|part| {
            Url::parse(part)
                .map(|mut url| {
                    if !url.username().is_empty() {
                        let _ = url.set_username("redacted");
                    }
                    if url.password().is_some() {
                        let _ = url.set_password(Some("redacted"));
                    }
                    if url.query().is_some() {
                        url.set_query(Some("redacted"));
                    }
                    if url.fragment().is_some() {
                        url.set_fragment(Some("redacted"));
                    }
                    url.to_string()
                })
                .unwrap_or_else(|_| part.to_string())
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn is_retryable_rpc_error(err: &(dyn Error + 'static)) -> bool {
    let mut current = Some(err);
    while let Some(err) = current {
        let text = err.to_string().to_ascii_lowercase();
        if text.contains("429")
            || text.contains("rate limit")
            || text.contains("timeout")
            || text.contains("temporar")
            || text.contains("connection reset")
            || text.contains("connection refused")
            || text.contains("broken pipe")
            || text.contains("eof")
            || text.contains("unavailable")
            || text.contains("overloaded")
            || text.contains("internal error")
            || text.contains("header not found")
        {
            return true;
        }
        current = err.source();
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_unsigned_units() {
        assert_eq!(format_units_str("0", 6), "0");
        assert_eq!(format_units_str("1", 6), "0.000001");
        assert_eq!(format_units_str("1000000", 6), "1");
        assert_eq!(format_units_str("123456789", 6), "123.456789");
    }

    #[test]
    fn classifies_addresses() {
        let args = Args {
            address: vec![
                "0x1111111111111111111111111111111111111111".to_string(),
                "bcrt1qtestaddress".to_string(),
            ],
            evm_address: vec![],
            bitcoin_address: vec![],
            private_key: vec![],
            ethereum_rpc_url: None,
            base_rpc_url: None,
            arbitrum_rpc_url: None,
            hyperliquid_base_url: None,
            bitcoin_esplora_url: None,
            skip_evm: false,
            skip_hyperliquid: false,
            skip_bitcoin: false,
        };
        let wallets = resolve_wallets(&args).unwrap();
        assert_eq!(wallets.evm_addresses.len(), 1);
        assert_eq!(wallets.bitcoin_addresses.len(), 1);
    }
}

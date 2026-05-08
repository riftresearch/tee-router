use std::{
    collections::BTreeMap, env, fmt, future::Future, path::PathBuf, str::FromStr, sync::Arc,
    time::Duration,
};

use alloy::{
    network::{EthereumWallet, TransactionBuilder},
    primitives::{Address, U256},
    providers::{Provider, ProviderBuilder},
    rpc::types::TransactionRequest,
    signers::local::LocalSigner,
    sol,
    sol_types::SolCall,
};
use clap::{Args, Parser, Subcommand, ValueEnum};
use eyre::{eyre, Result, WrapErr};
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, Semaphore},
    task::JoinSet,
    time::Instant,
};
use uuid::Uuid;

const MAX_LOADGEN_RESPONSE_BODY_BYTES: usize = 1024 * 1024;
const LOADGEN_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const LOADGEN_CHAIN_FUNDING_TIMEOUT: Duration = Duration::from_secs(90);

sol! {
    interface IERC20 {
        function transfer(address recipient, uint256 amount) external returns (bool);
    }
}

const RANDOM_ROUTES: &[RandomRoute] = &[
    RandomRoute::new(
        "Ethereum.USDC",
        "Base.USDC",
        RandomAddressKind::Evm,
        RandomAddressKind::Evm,
        LimitAsset::Usdc,
        LimitAsset::Usdc,
        true,
    ),
    RandomRoute::new(
        "Ethereum.USDC",
        "Arbitrum.USDC",
        RandomAddressKind::Evm,
        RandomAddressKind::Evm,
        LimitAsset::Usdc,
        LimitAsset::Usdc,
        true,
    ),
    RandomRoute::new(
        "Base.USDC",
        "Ethereum.USDC",
        RandomAddressKind::Evm,
        RandomAddressKind::Evm,
        LimitAsset::Usdc,
        LimitAsset::Usdc,
        true,
    ),
    RandomRoute::new(
        "Base.USDC",
        "Arbitrum.USDC",
        RandomAddressKind::Evm,
        RandomAddressKind::Evm,
        LimitAsset::Usdc,
        LimitAsset::Usdc,
        true,
    ),
    RandomRoute::new(
        "Arbitrum.USDC",
        "Ethereum.USDC",
        RandomAddressKind::Evm,
        RandomAddressKind::Evm,
        LimitAsset::Usdc,
        LimitAsset::Usdc,
        true,
    ),
    RandomRoute::new(
        "Arbitrum.USDC",
        "Base.USDC",
        RandomAddressKind::Evm,
        RandomAddressKind::Evm,
        LimitAsset::Usdc,
        LimitAsset::Usdc,
        true,
    ),
    RandomRoute::new(
        "Ethereum.ETH",
        "Bitcoin.BTC",
        RandomAddressKind::Evm,
        RandomAddressKind::Bitcoin,
        LimitAsset::Eth,
        LimitAsset::Btc,
        false,
    ),
    RandomRoute::new(
        "Ethereum.USDC",
        "Bitcoin.BTC",
        RandomAddressKind::Evm,
        RandomAddressKind::Bitcoin,
        LimitAsset::Usdc,
        LimitAsset::Btc,
        false,
    ),
    RandomRoute::new(
        "Base.USDC",
        "Bitcoin.BTC",
        RandomAddressKind::Evm,
        RandomAddressKind::Bitcoin,
        LimitAsset::Usdc,
        LimitAsset::Btc,
        false,
    ),
    RandomRoute::new(
        "Arbitrum.USDC",
        "Bitcoin.BTC",
        RandomAddressKind::Evm,
        RandomAddressKind::Bitcoin,
        LimitAsset::Usdc,
        LimitAsset::Btc,
        false,
    ),
    RandomRoute::new(
        "Bitcoin.BTC",
        "Ethereum.ETH",
        RandomAddressKind::Bitcoin,
        RandomAddressKind::Evm,
        LimitAsset::Btc,
        LimitAsset::Eth,
        false,
    ),
    RandomRoute::new(
        "Bitcoin.BTC",
        "Base.USDC",
        RandomAddressKind::Bitcoin,
        RandomAddressKind::Evm,
        LimitAsset::Btc,
        LimitAsset::Usdc,
        false,
    ),
    RandomRoute::new(
        "Bitcoin.BTC",
        "Base.ETH",
        RandomAddressKind::Bitcoin,
        RandomAddressKind::Evm,
        LimitAsset::Btc,
        LimitAsset::Eth,
        false,
    ),
    RandomRoute::new(
        "Base.ETH",
        "Base.USDC",
        RandomAddressKind::Evm,
        RandomAddressKind::Evm,
        LimitAsset::Eth,
        LimitAsset::Usdc,
        true,
    ),
    RandomRoute::new(
        "Base.USDC",
        "Base.ETH",
        RandomAddressKind::Evm,
        RandomAddressKind::Evm,
        LimitAsset::Usdc,
        LimitAsset::Eth,
        true,
    ),
    RandomRoute::new(
        "Base.ETH",
        "Ethereum.USDC",
        RandomAddressKind::Evm,
        RandomAddressKind::Evm,
        LimitAsset::Eth,
        LimitAsset::Usdc,
        false,
    ),
    RandomRoute::new(
        "Base.USDC",
        "Ethereum.ETH",
        RandomAddressKind::Evm,
        RandomAddressKind::Evm,
        LimitAsset::Usdc,
        LimitAsset::Eth,
        false,
    ),
];

const RANDOM_LIMIT_ROUTES: &[RandomLimitRoute] = &[
    RandomLimitRoute::new(
        "Ethereum.USDC",
        "Bitcoin.BTC",
        RandomAddressKind::Evm,
        RandomAddressKind::Bitcoin,
        LimitAsset::Usdc,
        LimitAsset::Btc,
    ),
    RandomLimitRoute::new(
        "Base.USDC",
        "Bitcoin.BTC",
        RandomAddressKind::Evm,
        RandomAddressKind::Bitcoin,
        LimitAsset::Usdc,
        LimitAsset::Btc,
    ),
    RandomLimitRoute::new(
        "Arbitrum.USDC",
        "Bitcoin.BTC",
        RandomAddressKind::Evm,
        RandomAddressKind::Bitcoin,
        LimitAsset::Usdc,
        LimitAsset::Btc,
    ),
    RandomLimitRoute::new(
        "Bitcoin.BTC",
        "Ethereum.USDC",
        RandomAddressKind::Bitcoin,
        RandomAddressKind::Evm,
        LimitAsset::Btc,
        LimitAsset::Usdc,
    ),
    RandomLimitRoute::new(
        "Bitcoin.BTC",
        "Base.USDC",
        RandomAddressKind::Bitcoin,
        RandomAddressKind::Evm,
        LimitAsset::Btc,
        LimitAsset::Usdc,
    ),
    RandomLimitRoute::new(
        "Bitcoin.BTC",
        "Arbitrum.USDC",
        RandomAddressKind::Bitcoin,
        RandomAddressKind::Evm,
        LimitAsset::Btc,
        LimitAsset::Usdc,
    ),
    RandomLimitRoute::new(
        "Ethereum.ETH",
        "Base.USDC",
        RandomAddressKind::Evm,
        RandomAddressKind::Evm,
        LimitAsset::Eth,
        LimitAsset::Usdc,
    ),
    RandomLimitRoute::new(
        "Ethereum.ETH",
        "Arbitrum.USDC",
        RandomAddressKind::Evm,
        RandomAddressKind::Evm,
        LimitAsset::Eth,
        LimitAsset::Usdc,
    ),
    RandomLimitRoute::new(
        "Base.USDC",
        "Ethereum.ETH",
        RandomAddressKind::Evm,
        RandomAddressKind::Evm,
        LimitAsset::Usdc,
        LimitAsset::Eth,
    ),
    RandomLimitRoute::new(
        "Arbitrum.USDC",
        "Ethereum.ETH",
        RandomAddressKind::Evm,
        RandomAddressKind::Evm,
        LimitAsset::Usdc,
        LimitAsset::Eth,
    ),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RandomRoute {
    from: &'static str,
    to: &'static str,
    source_kind: RandomAddressKind,
    destination_kind: RandomAddressKind,
    input_asset: LimitAsset,
    output_asset: LimitAsset,
    allow_exact_out: bool,
}

impl RandomRoute {
    const fn new(
        from: &'static str,
        to: &'static str,
        source_kind: RandomAddressKind,
        destination_kind: RandomAddressKind,
        input_asset: LimitAsset,
        output_asset: LimitAsset,
        allow_exact_out: bool,
    ) -> Self {
        Self {
            from,
            to,
            source_kind,
            destination_kind,
            input_asset,
            output_asset,
            allow_exact_out,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RandomLimitRoute {
    from: &'static str,
    to: &'static str,
    source_kind: RandomAddressKind,
    destination_kind: RandomAddressKind,
    input_asset: LimitAsset,
    output_asset: LimitAsset,
}

impl RandomLimitRoute {
    const fn new(
        from: &'static str,
        to: &'static str,
        source_kind: RandomAddressKind,
        destination_kind: RandomAddressKind,
        input_asset: LimitAsset,
        output_asset: LimitAsset,
    ) -> Self {
        Self {
            from,
            to,
            source_kind,
            destination_kind,
            input_asset,
            output_asset,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RandomAddressKind {
    Bitcoin,
    Evm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LimitAsset {
    Btc,
    Eth,
    Usdc,
}

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Load-test the router gateway quote/order flow"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Create one quote through the router gateway.
    Quote(QuoteCommand),
    /// Create orders through the router gateway and optionally fund them.
    CreateAndFund(CreateAndFundCommand),
}

#[derive(Args, Clone)]
struct QuoteCommand {
    #[command(flatten)]
    quote: QuoteInput,
}

#[derive(Args, Clone)]
struct CreateAndFundCommand {
    #[command(flatten)]
    quote: QuoteInput,

    /// Number of orders to create.
    #[arg(long, default_value_t = 1)]
    count: usize,

    /// Maximum concurrent in-flight quote/order/fund tasks.
    #[arg(long, default_value_t = 1)]
    concurrency: usize,

    /// Request launch rate. Zero disables pacing.
    #[arg(long, default_value_t = 0.0)]
    rps: f64,

    /// Randomize source asset, destination asset, exact-in/exact-out mode, and raw amount.
    #[arg(long)]
    random: bool,

    /// Minimum raw amount when --random is enabled.
    #[arg(long, default_value_t = 100_000_000)]
    random_min_raw_amount: u64,

    /// Maximum raw amount when --random is enabled.
    #[arg(long, default_value_t = 250_000_000)]
    random_max_raw_amount: u64,

    /// Optional RNG seed for repeatable --random runs.
    #[arg(long)]
    random_seed: Option<u64>,

    /// EVM RPC mapping, e.g. evm:8453=http://localhost:50102.
    #[arg(long = "evm-rpc", value_parser = parse_key_value)]
    evm_rpcs: Vec<(String, String)>,

    /// EVM private keys used to fund EVM orders. Can be repeated or comma-separated.
    #[arg(long = "evm-private-key", value_delimiter = ',')]
    evm_private_keys: Vec<String>,

    /// Devnet manifest URL used to discover the funded loadgen EVM key pool.
    #[arg(long, env = "ROUTER_LOADGEN_DEVNET_MANIFEST_URL")]
    devnet_manifest_url: Option<String>,

    /// Bitcoin Core wallet RPC URL used to fund Bitcoin orders.
    #[arg(long, env = "ROUTER_LOADGEN_BITCOIN_RPC_URL")]
    bitcoin_rpc_url: Option<String>,

    /// Bitcoin Core RPC auth: none, cookie file path, or user:password.
    #[arg(long, env = "ROUTER_LOADGEN_BITCOIN_RPC_AUTH", default_value = "none")]
    bitcoin_rpc_auth: String,

    /// Bitcoin address used as random Bitcoin recipient/refund address.
    /// Defaults to the devnet manifest demo Bitcoin address when available.
    #[arg(long, env = "ROUTER_LOADGEN_BITCOIN_ADDRESS")]
    bitcoin_address: Option<String>,

    /// Funding behavior after an order is created.
    #[arg(long, value_enum, default_value_t = FundingMode::Full)]
    funding_mode: FundingMode,
}

#[derive(Args, Clone)]
struct QuoteInput {
    /// Router gateway base URL.
    #[arg(
        long,
        env = "ROUTER_LOADGEN_GATEWAY_URL",
        default_value = "http://localhost:3001"
    )]
    gateway_url: String,

    /// Order type to create.
    #[arg(long, value_enum, default_value_t = OrderType::Market)]
    order_type: OrderType,

    /// Source asset, e.g. Base.USDC or evm:8453.0x....
    #[arg(long)]
    from: Option<String>,

    /// Destination asset, e.g. Ethereum.USDC.
    #[arg(long)]
    to: Option<String>,

    /// Recipient used by the gateway quote API.
    #[arg(long)]
    to_address: String,

    /// Exact-input amount.
    #[arg(long)]
    from_amount: Option<String>,

    /// Exact-output amount.
    #[arg(long)]
    to_amount: Option<String>,

    /// Max slippage. Interpreted as bps when amount-format is raw.
    #[arg(long, default_value = "100")]
    max_slippage: String,

    /// Gateway amount format.
    #[arg(long, value_enum, default_value_t = AmountFormat::Raw)]
    amount_format: AmountFormat,

    /// Optional source/refund address. Defaults to the EVM private key address
    /// for EVM funding.
    #[arg(long)]
    from_address: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedQuoteInput {
    order_type: OrderType,
    from: String,
    to: String,
    to_address: String,
    from_amount: Option<String>,
    to_amount: Option<String>,
    max_slippage: String,
    amount_format: AmountFormat,
    from_address: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum OrderType {
    Market,
    Limit,
}

impl OrderType {
    fn as_gateway_str(self) -> &'static str {
        match self {
            Self::Market => "market_order",
            Self::Limit => "limit_order",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum AmountFormat {
    Raw,
    Readable,
}

impl AmountFormat {
    fn as_gateway_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Readable => "readable",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum FundingMode {
    Full,
    Half,
    Skip,
}

#[derive(Debug, Clone)]
struct RuntimeConfig {
    http: Client,
    gateway_url: Url,
}

#[derive(Clone)]
struct EvmFundingConfig {
    rpcs: BTreeMap<String, String>,
    private_keys: Vec<String>,
    nonce_manager: EvmNonceManager,
}

impl fmt::Debug for EvmFundingConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EvmFundingConfig")
            .field("rpcs", &self.rpcs)
            .field("private_keys", &redacted_vec(self.private_keys.len()))
            .finish()
    }
}

#[derive(Clone, Default)]
struct EvmNonceManager {
    next_by_sender: Arc<Mutex<BTreeMap<EvmNonceKey, u64>>>,
}

impl fmt::Debug for EvmNonceManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EvmNonceManager").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct EvmNonceKey {
    rpc_url: String,
    sender: String,
}

#[derive(Clone)]
struct BitcoinFundingConfig {
    rpc_url: Option<String>,
    rpc_auth: String,
}

impl fmt::Debug for BitcoinFundingConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BitcoinFundingConfig")
            .field("rpc_url", &self.rpc_url)
            .field("rpc_auth", &"redacted")
            .finish()
    }
}

#[derive(Debug, Clone)]
struct FundingConfig {
    evm: EvmFundingConfig,
    bitcoin: BitcoinFundingConfig,
}

#[derive(Debug, Clone)]
struct RandomContext {
    bitcoin_address: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct QuoteRequest<'a> {
    order_type: &'static str,
    from: &'a str,
    to: &'a str,
    to_address: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    from_amount: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    to_amount: Option<&'a str>,
    max_slippage: &'a str,
    amount_format: &'static str,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct QuoteResponse {
    quote_id: String,
    #[serde(default)]
    order_type: Option<String>,
    from: String,
    to: String,
    expiry: String,
    expected_out: String,
    #[serde(default)]
    expected_slippage: Option<String>,
    #[serde(default)]
    min_out: Option<String>,
    #[serde(default)]
    max_in: Option<String>,
    max_slippage: String,
    amount_format: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OrderRequest<'a> {
    quote_id: &'a str,
    from_address: &'a str,
    to_address: &'a str,
    refund_address: &'a str,
    integrator: &'a str,
    idempotency_key: String,
    refund_mode: &'a str,
    refund_authorizer: Option<&'a str>,
    amount_format: &'static str,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
struct OrderResponse {
    order_id: String,
    order_address: String,
    amount_to_send: String,
    quote_id: String,
    #[serde(default)]
    order_type: Option<String>,
    from: String,
    to: String,
    status: String,
    expiry: String,
    expected_out: String,
    #[serde(default)]
    expected_slippage: Option<String>,
    #[serde(default)]
    min_out: Option<String>,
    #[serde(default)]
    max_in: Option<String>,
    max_slippage: String,
    amount_format: String,
    #[serde(default)]
    refund_mode: Option<String>,
    #[serde(default)]
    refund_authorizer: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LoadgenResult {
    index: usize,
    quote: QuoteResponse,
    order: OrderResponse,
    funding_tx_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DevnetManifestResponse {
    accounts: DevnetManifestAccounts,
}

#[derive(Debug, Deserialize)]
struct DevnetManifestAccounts {
    #[serde(default)]
    demo_bitcoin_address: Option<String>,
    #[serde(default)]
    loadgen_evm_accounts: Vec<DevnetManifestEvmAccount>,
}

#[derive(Deserialize)]
struct DevnetManifestEvmAccount {
    private_key: String,
}

impl fmt::Debug for DevnetManifestEvmAccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DevnetManifestEvmAccount")
            .field("private_key", &"redacted")
            .finish()
    }
}

fn redacted_vec(len: usize) -> Vec<&'static str> {
    vec!["redacted"; len]
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Quote(command) => {
            let runtime = runtime_config(&command.quote)?;
            let quote_input = resolve_static_quote_input(&command.quote)?;
            validate_resolved_quote_input(&quote_input)?;
            let quote = create_quote(&runtime, &quote_input).await?;
            println!("{}", serde_json::to_string_pretty(&quote)?);
        }
        Command::CreateAndFund(command) => run_create_and_fund(command).await?,
    }

    Ok(())
}

async fn run_create_and_fund(command: CreateAndFundCommand) -> Result<()> {
    if command.count == 0 {
        return Err(eyre!("--count must be greater than zero"));
    }
    if command.concurrency == 0 {
        return Err(eyre!("--concurrency must be greater than zero"));
    }
    validate_create_and_fund_command(&command)?;

    let runtime = Arc::new(runtime_config(&command.quote)?);
    let devnet_manifest =
        load_devnet_manifest(&runtime.http, command.devnet_manifest_url.as_deref()).await?;
    let evm_private_keys = load_evm_private_keys(&command, devnet_manifest.as_ref());
    let random_context = RandomContext {
        bitcoin_address: command.bitcoin_address.clone().or_else(|| {
            devnet_manifest
                .as_ref()
                .and_then(|manifest| manifest.accounts.demo_bitcoin_address.clone())
        }),
    };
    let funding = Arc::new(FundingConfig {
        evm: EvmFundingConfig {
            rpcs: command.evm_rpcs.iter().cloned().collect(),
            private_keys: evm_private_keys,
            nonce_manager: EvmNonceManager::default(),
        },
        bitcoin: BitcoinFundingConfig {
            rpc_url: command.bitcoin_rpc_url.clone(),
            rpc_auth: command.bitcoin_rpc_auth.clone(),
        },
    });
    let semaphore = Arc::new(Semaphore::new(command.concurrency));
    let mut tasks = JoinSet::new();
    let mut rng = match command.random_seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_entropy(),
    };
    let launch_interval = if command.rps > 0.0 {
        Some(Duration::from_secs_f64(1.0 / command.rps))
    } else {
        None
    };
    let started = Instant::now();

    for index in 0..command.count {
        let permit = semaphore.clone().acquire_owned().await?;
        let runtime = runtime.clone();
        let funding = funding.clone();
        let quote_input = quote_input_for_task(&command, &random_context, &mut rng)?;
        let evm_private_key = evm_private_key_for_task(index, &funding.evm, &quote_input);
        let from_address = resolve_from_address(&quote_input, evm_private_key.as_deref())?;
        let funding_mode = command.funding_mode;
        tasks.spawn(async move {
            let _permit = permit;
            create_order_and_maybe_fund(
                index,
                &runtime,
                &funding,
                &quote_input,
                &from_address,
                evm_private_key.as_deref(),
                funding_mode,
            )
            .await
        });

        if let Some(interval) = launch_interval {
            tokio::time::sleep(interval).await;
        }
    }

    let mut successes = 0usize;
    let mut failures = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(result)) => {
                successes += 1;
                println!("{}", serde_json::to_string(&result)?);
            }
            Ok(Err(error)) => {
                failures.push(format!("{error:?}"));
            }
            Err(error) => {
                failures.push(format!("{error:?}"));
            }
        }
    }

    eprintln!(
        "router-loadgen completed {successes}/{} orders in {:?}",
        command.count,
        started.elapsed()
    );
    if !failures.is_empty() {
        for failure in failures.iter().take(10) {
            eprintln!("router-loadgen task failed: {failure}");
        }
        if failures.len() > 10 {
            eprintln!(
                "router-loadgen suppressed {} additional failures",
                failures.len() - 10
            );
        }
        return Err(eyre!(
            "router-loadgen completed {successes}/{} orders with {} failures",
            command.count,
            failures.len()
        ));
    }
    Ok(())
}

async fn create_order_and_maybe_fund(
    index: usize,
    runtime: &RuntimeConfig,
    funding: &FundingConfig,
    quote_input: &ResolvedQuoteInput,
    from_address: &str,
    evm_private_key: Option<&str>,
    funding_mode: FundingMode,
) -> Result<LoadgenResult> {
    let task_label = format!(
        "task {index} {:?} {} -> {} from_amount={} to_amount={}",
        quote_input.order_type,
        quote_input.from,
        quote_input.to,
        quote_input.from_amount.as_deref().unwrap_or("-"),
        quote_input.to_amount.as_deref().unwrap_or("-")
    );
    let quote = create_quote(runtime, quote_input)
        .await
        .wrap_err_with(|| format!("quote failed for {task_label}"))?;
    let order = create_order(runtime, quote_input, &quote, from_address)
        .await
        .wrap_err_with(|| format!("order creation failed for {task_label}"))?;
    let funding_tx_hash = match funding_mode {
        FundingMode::Skip => None,
        FundingMode::Full | FundingMode::Half => Some(
            fund_order(funding, quote_input, &order, funding_mode, evm_private_key)
                .await
                .wrap_err_with(|| format!("funding failed for {task_label}"))?,
        ),
    };

    Ok(LoadgenResult {
        index,
        quote,
        order,
        funding_tx_hash,
    })
}

async fn create_quote(
    runtime: &RuntimeConfig,
    input: &ResolvedQuoteInput,
) -> Result<QuoteResponse> {
    let url = runtime.gateway_url.join("quote")?;
    let request = QuoteRequest {
        order_type: input.order_type.as_gateway_str(),
        from: &input.from,
        to: &input.to,
        to_address: &input.to_address,
        from_amount: input.from_amount.as_deref(),
        to_amount: input.to_amount.as_deref(),
        max_slippage: &input.max_slippage,
        amount_format: input.amount_format.as_gateway_str(),
    };
    post_json(&runtime.http, url, &request).await
}

async fn create_order(
    runtime: &RuntimeConfig,
    input: &ResolvedQuoteInput,
    quote: &QuoteResponse,
    from_address: &str,
) -> Result<OrderResponse> {
    let url = runtime.gateway_url.join("order/market")?;
    let request = OrderRequest {
        quote_id: &quote.quote_id,
        from_address,
        to_address: &input.to_address,
        refund_address: from_address,
        integrator: "router-loadgen",
        idempotency_key: format!("router-loadgen-{}", Uuid::now_v7()),
        refund_mode: "token",
        refund_authorizer: None,
        amount_format: input.amount_format.as_gateway_str(),
    };
    post_json(&runtime.http, url, &request).await
}

async fn post_json<T, R>(http: &Client, url: Url, body: &T) -> Result<R>
where
    T: Serialize + ?Sized,
    R: for<'de> Deserialize<'de>,
{
    let response = http.post(url.clone()).json(body).send().await?;
    let status = response.status();
    let bytes = read_limited_response_body(response, MAX_LOADGEN_RESPONSE_BODY_BYTES)
        .await
        .wrap_err_with(|| format!("failed to read gateway response from {url}"))?;
    if !status.is_success() {
        let body = String::from_utf8_lossy(&bytes);
        return Err(eyre!(
            "gateway request to {url} failed with {status}: {body}"
        ));
    }
    serde_json::from_slice(&bytes).wrap_err("failed to decode gateway response")
}

async fn get_json<R>(http: &Client, url: Url) -> Result<R>
where
    R: for<'de> Deserialize<'de>,
{
    let response = http.get(url.clone()).send().await?;
    let status = response.status();
    let bytes = read_limited_response_body(response, MAX_LOADGEN_RESPONSE_BODY_BYTES)
        .await
        .wrap_err_with(|| format!("failed to read HTTP response from {url}"))?;
    if !status.is_success() {
        let body = String::from_utf8_lossy(&bytes);
        return Err(eyre!("HTTP request to {url} failed with {status}: {body}"));
    }
    serde_json::from_slice(&bytes).wrap_err("failed to decode HTTP response")
}

async fn read_limited_response_body(
    mut response: reqwest::Response,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if !append_limited_body_chunk(&mut body, chunk.as_ref(), max_bytes) {
            return Err(eyre!("gateway response body exceeded {max_bytes} bytes"));
        }
    }
    Ok(body)
}

fn append_limited_body_chunk(body: &mut Vec<u8>, chunk: &[u8], max_bytes: usize) -> bool {
    if body.len().saturating_add(chunk.len()) > max_bytes {
        return false;
    }
    body.extend_from_slice(chunk);
    true
}

async fn fund_order(
    funding: &FundingConfig,
    input: &ResolvedQuoteInput,
    order: &OrderResponse,
    funding_mode: FundingMode,
    evm_private_key: Option<&str>,
) -> Result<String> {
    let source = parse_asset_identifier(&input.from)?;
    let mut amount = U256::from_str(&order.amount_to_send)
        .wrap_err("order amountToSend must be a raw integer when funding")?;
    if funding_mode == FundingMode::Half {
        amount /= U256::from(2);
    }
    if amount.is_zero() {
        return Err(eyre!("funding amount resolved to zero"));
    }

    if source.chain_id.starts_with("evm:") {
        let rpc_url = funding
            .evm
            .rpcs
            .get(&source.chain_id)
            .ok_or_else(|| eyre!("missing --evm-rpc for {}", source.chain_id))?;
        let private_key = evm_private_key
            .or_else(|| funding.evm.private_keys.first().map(String::as_str))
            .ok_or_else(|| eyre!("missing --evm-private-key for EVM funding"))?;
        return with_funding_timeout(
            format!("EVM funding on {}", source.chain_id),
            fund_evm_asset(
                rpc_url,
                private_key,
                &funding.evm.nonce_manager,
                &source.asset,
                &order.order_address,
                amount,
            ),
        )
        .await;
    }

    if source.chain_id == "bitcoin" {
        if source.asset != "native" {
            return Err(eyre!("Bitcoin funding only supports native BTC"));
        }
        let sats = u64::try_from(amount).map_err(|_| eyre!("Bitcoin amount exceeds u64 sats"))?;
        return with_funding_timeout(
            "Bitcoin funding",
            fund_bitcoin(
                &funding.bitcoin,
                &order.order_address,
                bitcoin::Amount::from_sat(sats),
            ),
        )
        .await;
    }

    Err(eyre!("funding for {} is not supported", source.chain_id))
}

async fn with_funding_timeout<T>(
    operation: impl std::fmt::Display,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout(LOADGEN_CHAIN_FUNDING_TIMEOUT, future)
        .await
        .map_err(|_| eyre!("{operation} timed out after {LOADGEN_CHAIN_FUNDING_TIMEOUT:?}"))?
}

async fn fund_evm_asset(
    rpc_url: &str,
    private_key: &str,
    nonce_manager: &EvmNonceManager,
    asset: &str,
    recipient: &str,
    amount: U256,
) -> Result<String> {
    let signer = LocalSigner::from_str(private_key).wrap_err("invalid EVM private key")?;
    let sender = signer.address();
    let recipient = Address::from_str(recipient).wrap_err("invalid EVM order address")?;
    let rpc_url_key = rpc_url.to_string();
    let rpc_url = rpc_url.parse::<Url>().wrap_err("invalid EVM RPC URL")?;
    let provider = ProviderBuilder::new()
        .wallet(EthereumWallet::new(signer))
        .connect_http(rpc_url);

    let tx = if asset.eq_ignore_ascii_case("native") {
        TransactionRequest::default()
            .with_from(sender)
            .with_to(recipient)
            .with_value(amount)
    } else {
        let token = Address::from_str(asset).wrap_err("invalid EVM token address")?;
        let calldata = IERC20::transferCall { recipient, amount }.abi_encode();
        TransactionRequest::default()
            .with_from(sender)
            .with_to(token)
            .with_input(calldata)
    };

    let pending = {
        let key = EvmNonceKey {
            rpc_url: rpc_url_key,
            sender: format!("{sender:#x}"),
        };
        let mut next_by_sender = nonce_manager.next_by_sender.lock().await;
        let nonce = match next_by_sender.get(&key).copied() {
            Some(nonce) => nonce,
            None => provider
                .get_transaction_count(sender)
                .pending()
                .await
                .wrap_err_with(|| format!("failed to load pending nonce for {sender:#x}"))?,
        };
        let pending = provider.send_transaction(tx.with_nonce(nonce)).await?;
        let next_nonce = nonce
            .checked_add(1)
            .ok_or_else(|| eyre!("EVM nonce overflow for {sender:#x}"))?;
        next_by_sender.insert(key, next_nonce);
        pending
    };
    let tx_hash = *pending.tx_hash();
    let receipt = pending.get_receipt().await?;
    if !receipt.status() {
        return Err(eyre!("funding transaction reverted: {tx_hash:#x}"));
    }
    Ok(format!("{tx_hash:#x}"))
}

async fn fund_bitcoin(
    config: &BitcoinFundingConfig,
    recipient: &str,
    amount: bitcoin::Amount,
) -> Result<String> {
    use bitcoincore_rpc_async::RpcApi;

    let rpc_url = config
        .rpc_url
        .as_deref()
        .ok_or_else(|| eyre!("missing --bitcoin-rpc-url for Bitcoin funding"))?;
    let auth = parse_bitcoin_auth(&config.rpc_auth)?;
    let client = bitcoincore_rpc_async::Client::new(rpc_url.to_string(), auth)
        .await
        .wrap_err("failed to connect to Bitcoin Core RPC")?;
    let recipient = bitcoin::Address::from_str(recipient)
        .wrap_err("invalid Bitcoin order address")?
        .assume_checked();
    let txid = client
        .send_to_address(&recipient, amount)
        .await
        .wrap_err("Bitcoin funding transaction failed")?
        .txid()
        .wrap_err("Bitcoin RPC did not return a txid")?;
    Ok(txid.to_string())
}

fn runtime_config(input: &QuoteInput) -> Result<RuntimeConfig> {
    let gateway_url = normalize_base_url(&input.gateway_url)?;
    Ok(RuntimeConfig {
        http: Client::builder()
            .use_rustls_tls()
            .timeout(LOADGEN_HTTP_TIMEOUT)
            .build()?,
        gateway_url,
    })
}

async fn load_devnet_manifest(
    http: &Client,
    manifest_url: Option<&str>,
) -> Result<Option<DevnetManifestResponse>> {
    let Some(manifest_url) = manifest_url else {
        return Ok(None);
    };

    let manifest_url = Url::parse(manifest_url).wrap_err("invalid devnet manifest URL")?;
    let manifest = get_json(http, manifest_url)
        .await
        .wrap_err("failed to load devnet manifest")?;
    Ok(Some(manifest))
}

fn load_evm_private_keys(
    command: &CreateAndFundCommand,
    devnet_manifest: Option<&DevnetManifestResponse>,
) -> Vec<String> {
    let mut keys = split_private_key_values(command.evm_private_keys.iter().map(String::as_str));

    if keys.is_empty() {
        if let Ok(value) = env::var("ROUTER_LOADGEN_EVM_PRIVATE_KEYS") {
            keys.extend(split_private_key_values(std::iter::once(value.as_str())));
        }
    }
    if keys.is_empty() {
        if let Ok(value) = env::var("ROUTER_LOADGEN_EVM_PRIVATE_KEY") {
            keys.extend(split_private_key_values(std::iter::once(value.as_str())));
        }
    }

    if keys.is_empty() {
        if let Some(manifest) = devnet_manifest {
            keys.extend(
                manifest
                    .accounts
                    .loadgen_evm_accounts
                    .iter()
                    .map(|account| account.private_key.clone()),
            );
        }
    }

    keys
}

fn split_private_key_values<'a>(values: impl IntoIterator<Item = &'a str>) -> Vec<String> {
    values
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn validate_create_and_fund_command(command: &CreateAndFundCommand) -> Result<()> {
    if command.random {
        if command.random_min_raw_amount == 0 {
            return Err(eyre!("--random-min-raw-amount must be greater than zero"));
        }
        if command.random_min_raw_amount > command.random_max_raw_amount {
            return Err(eyre!(
                "--random-min-raw-amount must be less than or equal to --random-max-raw-amount"
            ));
        }
        if command.quote.amount_format != AmountFormat::Raw {
            return Err(eyre!("--random currently requires --amount-format raw"));
        }
        return Ok(());
    }

    let input = resolve_static_quote_input(&command.quote)?;
    validate_resolved_quote_input(&input)
}

fn resolve_static_quote_input(input: &QuoteInput) -> Result<ResolvedQuoteInput> {
    Ok(ResolvedQuoteInput {
        order_type: input.order_type,
        from: input
            .from
            .clone()
            .ok_or_else(|| eyre!("--from is required unless --random is enabled"))?,
        to: input
            .to
            .clone()
            .ok_or_else(|| eyre!("--to is required unless --random is enabled"))?,
        to_address: input.to_address.clone(),
        from_amount: input.from_amount.clone(),
        to_amount: input.to_amount.clone(),
        max_slippage: input.max_slippage.clone(),
        amount_format: input.amount_format,
        from_address: input.from_address.clone(),
    })
}

fn validate_resolved_quote_input(input: &ResolvedQuoteInput) -> Result<()> {
    let amount_count =
        usize::from(input.from_amount.is_some()) + usize::from(input.to_amount.is_some());
    match input.order_type {
        OrderType::Market if amount_count != 1 => {
            return Err(eyre!(
                "exactly one of --from-amount or --to-amount is required for market orders"
            ));
        }
        OrderType::Limit if amount_count != 2 => {
            return Err(eyre!(
                "--from-amount and --to-amount are required for limit orders"
            ));
        }
        _ => {}
    }
    Ok(())
}

fn quote_input_for_task(
    command: &CreateAndFundCommand,
    random_context: &RandomContext,
    rng: &mut StdRng,
) -> Result<ResolvedQuoteInput> {
    if !command.random {
        return resolve_static_quote_input(&command.quote);
    }

    if command.quote.order_type == OrderType::Limit {
        return random_limit_quote_input(command, random_context, rng);
    }

    let route = RANDOM_ROUTES
        .choose(rng)
        .ok_or_else(|| eyre!("random route list is empty"))?
        .to_owned();
    let notional_usdc_raw =
        rng.gen_range(command.random_min_raw_amount..=command.random_max_raw_amount);
    let exact_out = route.allow_exact_out && rng.gen_bool(0.5);
    let amount = market_random_amount(route, notional_usdc_raw, exact_out)?;
    let (to_address, from_address) = random_route_addresses(
        route.source_kind,
        route.destination_kind,
        &command.quote.to_address,
        command.quote.from_address.as_deref(),
        random_context.bitcoin_address.as_deref(),
    )?;

    Ok(ResolvedQuoteInput {
        order_type: OrderType::Market,
        from: route.from.to_string(),
        to: route.to.to_string(),
        to_address,
        from_amount: (!exact_out).then(|| amount.clone()),
        to_amount: exact_out.then_some(amount),
        max_slippage: command.quote.max_slippage.clone(),
        amount_format: AmountFormat::Raw,
        from_address,
    })
}

fn random_limit_quote_input(
    command: &CreateAndFundCommand,
    random_context: &RandomContext,
    rng: &mut StdRng,
) -> Result<ResolvedQuoteInput> {
    let route = RANDOM_LIMIT_ROUTES
        .choose(rng)
        .ok_or_else(|| eyre!("random limit route list is empty"))?
        .to_owned();
    let notional_usdc_raw =
        rng.gen_range(command.random_min_raw_amount..=command.random_max_raw_amount);
    let (from_amount, to_amount) = marketable_limit_amounts(route, notional_usdc_raw)?;
    let (to_address, from_address) = random_route_addresses(
        route.source_kind,
        route.destination_kind,
        &command.quote.to_address,
        command.quote.from_address.as_deref(),
        random_context.bitcoin_address.as_deref(),
    )?;

    Ok(ResolvedQuoteInput {
        order_type: OrderType::Limit,
        from: route.from.to_string(),
        to: route.to.to_string(),
        to_address,
        from_amount: Some(from_amount),
        to_amount: Some(to_amount),
        max_slippage: command.quote.max_slippage.clone(),
        amount_format: AmountFormat::Raw,
        from_address,
    })
}

fn marketable_limit_amounts(
    route: RandomLimitRoute,
    notional_usdc_raw: u64,
) -> Result<(String, String)> {
    let input = asset_amount_for_notional(route.input_asset, notional_usdc_raw)?;
    let fair_output = asset_amount_for_notional(route.output_asset, notional_usdc_raw)?;
    // Keep random limit orders deliberately marketable after the router adds
    // downstream gas-retention amounts to the venue leg. Resting-order tests
    // should use explicit amounts instead of random loadgen mode.
    let output = fair_output
        .checked_div(U256::from(2_u64))
        .ok_or_else(|| eyre!("failed to compute limit output amount"))?;
    if input.is_zero() || output.is_zero() {
        return Err(eyre!("random limit amounts resolved to zero"));
    }
    Ok((input.to_string(), output.to_string()))
}

fn random_route_addresses(
    source_kind: RandomAddressKind,
    destination_kind: RandomAddressKind,
    fallback_to_address: &str,
    fallback_from_address: Option<&str>,
    bitcoin_address: Option<&str>,
) -> Result<(String, Option<String>)> {
    let to_address = if destination_kind == RandomAddressKind::Bitcoin {
        bitcoin_address
            .ok_or_else(|| {
                eyre!(
                    "random Bitcoin destination selected but no --bitcoin-address or devnet manifest demo Bitcoin address is available"
                )
            })?
            .to_string()
    } else {
        fallback_to_address.to_string()
    };
    let from_address = if source_kind == RandomAddressKind::Bitcoin {
        Some(
            bitcoin_address
                .ok_or_else(|| {
                    eyre!(
                        "random Bitcoin source selected but no --bitcoin-address or devnet manifest demo Bitcoin address is available"
                    )
                })?
                .to_string(),
        )
    } else {
        fallback_from_address.map(ToOwned::to_owned)
    };

    Ok((to_address, from_address))
}

fn market_random_amount(
    route: RandomRoute,
    notional_usdc_raw: u64,
    exact_out: bool,
) -> Result<String> {
    let asset = if exact_out {
        route.output_asset
    } else {
        route.input_asset
    };
    let amount = asset_amount_for_notional(asset, notional_usdc_raw)?;
    if amount.is_zero() {
        return Err(eyre!("random market amount resolved to zero"));
    }
    Ok(amount.to_string())
}

fn asset_amount_for_notional(asset: LimitAsset, usdc_raw: u64) -> Result<U256> {
    let amount = match asset {
        LimitAsset::Usdc => U256::from(usdc_raw),
        LimitAsset::Btc => U256::from(usdc_raw)
            .checked_mul(U256::from(100_u64))
            .and_then(|value| value.checked_div(U256::from(60_000_u64)))
            .ok_or_else(|| eyre!("failed to compute BTC notional amount"))?,
        LimitAsset::Eth => U256::from(usdc_raw)
            .checked_mul(U256::from(1_000_000_000_000_u64))
            .and_then(|value| value.checked_div(U256::from(3_000_u64)))
            .ok_or_else(|| eyre!("failed to compute ETH notional amount"))?,
    };
    Ok(amount)
}

fn evm_private_key_for_task(
    index: usize,
    funding: &EvmFundingConfig,
    input: &ResolvedQuoteInput,
) -> Option<String> {
    let source = parse_asset_identifier(&input.from).ok()?;
    if !source.chain_id.starts_with("evm:") || funding.private_keys.is_empty() {
        return None;
    }
    Some(funding.private_keys[index % funding.private_keys.len()].clone())
}

fn resolve_from_address(
    input: &ResolvedQuoteInput,
    evm_private_key: Option<&str>,
) -> Result<String> {
    if let Some(from_address) = &input.from_address {
        return Ok(from_address.clone());
    }

    let source = parse_asset_identifier(&input.from)?;
    if source.chain_id.starts_with("evm:") {
        let private_key = evm_private_key
            .ok_or_else(|| eyre!("--from-address or --evm-private-key is required"))?;
        let signer = LocalSigner::from_str(private_key).wrap_err("invalid EVM private key")?;
        return Ok(format!("{:#x}", signer.address()));
    }

    Err(eyre!(
        "--from-address is required for non-EVM source asset {}",
        input.from
    ))
}

fn parse_bitcoin_auth(raw: &str) -> Result<bitcoincore_rpc_async::Auth> {
    if raw.eq_ignore_ascii_case("none") {
        return Ok(bitcoincore_rpc_async::Auth::None);
    }

    if std::fs::exists(raw).unwrap_or(false) {
        return Ok(bitcoincore_rpc_async::Auth::CookieFile(PathBuf::from(raw)));
    }

    let (user, password) = raw
        .split_once(':')
        .ok_or_else(|| eyre!("Bitcoin RPC auth must be none, a cookie path, or user:password"))?;
    Ok(bitcoincore_rpc_async::Auth::UserPass(
        user.to_string(),
        password.to_string(),
    ))
}

#[derive(Debug)]
struct ParsedAsset {
    chain_id: String,
    asset: String,
}

fn parse_asset_identifier(identifier: &str) -> Result<ParsedAsset> {
    let (chain, asset) = identifier
        .split_once('.')
        .ok_or_else(|| eyre!("asset must use Chain.Asset form: {identifier}"))?;
    let chain_id = match chain.to_ascii_lowercase().as_str() {
        "bitcoin" | "btc" => "bitcoin".to_string(),
        "ethereum" | "eth" | "mainnet" => "evm:1".to_string(),
        "base" => "evm:8453".to_string(),
        "arbitrum" | "arb" => "evm:42161".to_string(),
        "hyperliquid" => "hyperliquid".to_string(),
        raw if raw.starts_with("evm:") => raw.to_string(),
        _ => return Err(eyre!("unsupported asset chain: {chain}")),
    };
    let asset = match (chain_id.as_str(), asset.to_ascii_lowercase().as_str()) {
        ("bitcoin", "btc" | "native") => "native".to_string(),
        ("evm:1", "eth" | "native") => "native".to_string(),
        ("evm:8453", "eth" | "native") => "native".to_string(),
        ("evm:1", "usdc") => "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48".to_string(),
        ("evm:8453", "usdc") => "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913".to_string(),
        ("evm:42161", "usdc") => "0xaf88d065e77c8cc2239327C5EDb3A432268e5831".to_string(),
        (_, value) if value.starts_with("0x") => value.to_string(),
        _ => return Err(eyre!("unsupported source asset: {identifier}")),
    };

    Ok(ParsedAsset { chain_id, asset })
}

fn normalize_base_url(value: &str) -> Result<Url> {
    let mut normalized = value.trim().to_string();
    if !normalized.ends_with('/') {
        normalized.push('/');
    }
    Url::parse(&normalized).wrap_err("invalid gateway URL")
}

fn parse_key_value(raw: &str) -> Result<(String, String), String> {
    let (key, value) = raw
        .split_once('=')
        .ok_or_else(|| "expected KEY=VALUE".to_string())?;
    if key.is_empty() || value.is_empty() {
        return Err("expected non-empty KEY=VALUE".to_string());
    }
    Ok((key.to_string(), value.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_limit_amounts_are_aggressively_marketable() -> Result<()> {
        let route = RandomLimitRoute::new(
            "Bitcoin.BTC",
            "Ethereum.USDC",
            RandomAddressKind::Bitcoin,
            RandomAddressKind::Evm,
            LimitAsset::Btc,
            LimitAsset::Usdc,
        );

        let (from_amount, to_amount) = marketable_limit_amounts(route, 120_000_000)?;

        assert_eq!(from_amount, "200000");
        assert_eq!(to_amount, "60000000");
        Ok(())
    }

    #[test]
    fn random_market_amounts_scale_by_selected_asset() -> Result<()> {
        let route = RandomRoute::new(
            "Ethereum.ETH",
            "Bitcoin.BTC",
            RandomAddressKind::Evm,
            RandomAddressKind::Bitcoin,
            LimitAsset::Eth,
            LimitAsset::Btc,
            false,
        );

        assert_eq!(
            market_random_amount(route, 120_000_000, false)?,
            "40000000000000000"
        );
        assert_eq!(market_random_amount(route, 120_000_000, true)?, "200000");
        Ok(())
    }

    #[test]
    fn random_route_addresses_use_bitcoin_address_for_bitcoin_destinations() -> Result<()> {
        let (to_address, from_address) = random_route_addresses(
            RandomAddressKind::Evm,
            RandomAddressKind::Bitcoin,
            "0x1111111111111111111111111111111111111111",
            Some("0x2222222222222222222222222222222222222222"),
            Some("bcrt1q2pfqp8a574jxyszmk0h5rxf02wwkpaf4hd8009"),
        )?;

        assert_eq!(to_address, "bcrt1q2pfqp8a574jxyszmk0h5rxf02wwkpaf4hd8009");
        assert_eq!(
            from_address.as_deref(),
            Some("0x2222222222222222222222222222222222222222")
        );
        Ok(())
    }

    #[test]
    fn random_route_addresses_use_bitcoin_address_for_bitcoin_sources() -> Result<()> {
        let (to_address, from_address) = random_route_addresses(
            RandomAddressKind::Bitcoin,
            RandomAddressKind::Evm,
            "0x1111111111111111111111111111111111111111",
            None,
            Some("bcrt1q2pfqp8a574jxyszmk0h5rxf02wwkpaf4hd8009"),
        )?;

        assert_eq!(to_address, "0x1111111111111111111111111111111111111111");
        assert_eq!(
            from_address.as_deref(),
            Some("bcrt1q2pfqp8a574jxyszmk0h5rxf02wwkpaf4hd8009")
        );
        Ok(())
    }

    #[test]
    fn random_route_addresses_require_bitcoin_address_for_bitcoin_routes() {
        let destination_error = random_route_addresses(
            RandomAddressKind::Evm,
            RandomAddressKind::Bitcoin,
            "0x1111111111111111111111111111111111111111",
            None,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(destination_error.contains("random Bitcoin destination selected"));

        let source_error = random_route_addresses(
            RandomAddressKind::Bitcoin,
            RandomAddressKind::Evm,
            "0x1111111111111111111111111111111111111111",
            None,
            None,
        )
        .unwrap_err()
        .to_string();
        assert!(source_error.contains("random Bitcoin source selected"));
    }

    #[test]
    fn append_limited_body_chunk_rejects_chunks_past_the_limit_without_mutating() {
        let mut body = b"abcd".to_vec();

        assert!(!append_limited_body_chunk(&mut body, b"ef", 5));
        assert_eq!(body, b"abcd");
    }

    #[test]
    fn append_limited_body_chunk_accepts_chunks_at_the_limit() {
        let mut body = b"abcd".to_vec();

        assert!(append_limited_body_chunk(&mut body, b"ef", 6));
        assert_eq!(body, b"abcdef");
    }

    #[test]
    fn order_response_serialization_redacts_refund_token() -> Result<()> {
        let order: OrderResponse = serde_json::from_value(serde_json::json!({
            "orderId": "019df1c4-8d87-7c20-89a4-e76883e94a0f",
            "orderAddress": "0x1111111111111111111111111111111111111111",
            "amountToSend": "1000",
            "quoteId": "019df1c4-8d87-7c20-89a4-e76883e94a10",
            "orderType": "market_order",
            "from": "Base.USDC",
            "to": "Ethereum.USDC",
            "status": "pending_funding",
            "expiry": "2026-05-05T00:00:00Z",
            "expectedOut": "990",
            "minOut": "980",
            "maxSlippage": "100",
            "amountFormat": "raw",
            "refundMode": "token",
            "refundToken": "secret-cancellation-token"
        }))?;
        let serialized = serde_json::to_value(&order)?;

        assert!(serialized.get("refundToken").is_none());
        Ok(())
    }

    #[test]
    fn funding_config_debug_redacts_loaded_secrets() {
        let config = FundingConfig {
            evm: EvmFundingConfig {
                rpcs: BTreeMap::from([(
                    "evm:8453".to_string(),
                    "http://localhost:50102".to_string(),
                )]),
                private_keys: vec![
                    "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                    "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        .to_string(),
                ],
            },
            bitcoin: BitcoinFundingConfig {
                rpc_url: Some("http://localhost:18443".to_string()),
                rpc_auth: "rpc-user:rpc-password".to_string(),
            },
        };

        let rendered = format!("{config:?}");

        assert!(!rendered.contains("aaaaaaaa"));
        assert!(!rendered.contains("bbbbbbbb"));
        assert!(!rendered.contains("rpc-user"));
        assert!(!rendered.contains("rpc-password"));
        assert!(rendered.contains("redacted"));
    }

    #[test]
    fn devnet_manifest_debug_redacts_private_keys() {
        let manifest = DevnetManifestResponse {
            accounts: DevnetManifestAccounts {
                demo_bitcoin_address: Some(
                    "bcrt1q2pfqp8a574jxyszmk0h5rxf02wwkpaf4hd8009".to_string(),
                ),
                loadgen_evm_accounts: vec![DevnetManifestEvmAccount {
                    private_key:
                        "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                            .to_string(),
                }],
            },
        };

        let rendered = format!("{manifest:?}");

        assert!(!rendered.contains("cccccccc"));
        assert!(rendered.contains("redacted"));
        assert!(rendered.contains("bcrt1q2pfqp8a574jxyszmk0h5rxf02wwkpaf4hd8009"));
    }
}

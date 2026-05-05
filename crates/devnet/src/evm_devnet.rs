use std::{fmt, sync::Arc};

use blockchain_utils::create_websocket_wallet_provider;
use eip3009_erc20_contract::GenericEIP3009ERC20::GenericEIP3009ERC20Instance;
use eip7702_delegator_contract::{
    EIP7702Delegator::EIP7702DelegatorInstance, EIP7702_DELEGATOR_BYTECODE,
    EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS,
};
use eyre::{eyre, Result};
use hyperliquid_client::{
    bridge::bridge_address as hyperliquid_bridge_address, client::Network as HyperliquidNetwork,
};
use tokio::time::Instant;
use tracing::info;

use alloy::{
    node_bindings::{Anvil, AnvilInstance},
    primitives::{Address, Bytes, U256},
    providers::{ext::AnvilApi, DynProvider, Provider},
};

use crate::{
    across_spoke_pool_mock::MockSpokePool::MockSpokePoolInstance,
    cctp_mock::{
        MockCctpMessageTransmitterV2::MockCctpMessageTransmitterV2Instance,
        MockCctpTokenMessengerV2::MockCctpTokenMessengerV2Instance,
    },
    get_new_temp_dir,
    hyperliquid_bridge_mock::MockHyperliquidBridge2::MockHyperliquidBridge2Instance,
    manifest::DEVNET_ETHEREUM_RPC_PORT,
    token_indexerd::TokenIndexerInstance,
    velora_mock::MockVeloraSwap::MockVeloraSwapInstance,
    RiftDevnetCache,
};

pub const MOCK_ERC20_ADDRESS: &str = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf";
pub const MOCK_ACROSS_SPOKE_POOL_ADDRESS: &str = "0xACE055C0C055D0C035E47055D05E7055055BACE0";
pub const ETHEREUM_USDC_ADDRESS: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
pub const ETHEREUM_USDT_ADDRESS: &str = "0xdac17f958d2ee523a2206206994597c13d831ec7";
pub const ETHEREUM_CBBTC_ADDRESS: &str = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf";
pub const ARBITRUM_USDC_ADDRESS: &str = "0xaf88d065e77c8cc2239327C5EDb3A432268e5831";
pub const ARBITRUM_USDT_ADDRESS: &str = "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9";
pub const ARBITRUM_CBBTC_ADDRESS: &str = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf";
pub const BASE_USDC_ADDRESS: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
pub const BASE_USDT_ADDRESS: &str = "0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2";
pub const BASE_CBBTC_ADDRESS: &str = "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf";
pub const MOCK_VELORA_NATIVE_RESERVE_WEI: u128 = 1_000_000_000_000_000_000_000_000;
pub const MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS: &str = "0xcccccccccccccccccccccccccccccccccccc0001";
pub const MOCK_CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS: &str =
    "0xcccccccccccccccccccccccccccccccccccc0002";

/// Holds all Ethereum-related devnet state.
pub struct EthDevnet {
    pub anvil: Arc<AnvilInstance>,
    pub funded_provider: DynProvider,
    pub funded_address: Address,
    pub deploy_mode: Mode,
    pub anvil_datadir: Option<tempfile::TempDir>,
    pub anvil_dump_path: tempfile::TempDir,
    pub anvil_cache_path: tempfile::TempDir,
    pub mock_erc20_contract: GenericEIP3009ERC20Instance<DynProvider>,
    pub token_indexer: Option<TokenIndexerInstance>,
    pub eip7702_delegator_contract: EIP7702DelegatorInstance<DynProvider>,
    pub mock_across_spoke_pool_contract: MockSpokePoolInstance<DynProvider>,
    pub mock_cctp_token_messenger_v2_contract: MockCctpTokenMessengerV2Instance<DynProvider>,
    pub mock_cctp_message_transmitter_v2_contract:
        MockCctpMessageTransmitterV2Instance<DynProvider>,
    pub mock_velora_swap_contract: MockVeloraSwapInstance<DynProvider>,
}

#[derive(Clone, Debug)]
pub enum Mode {
    Fork(ForkConfig),
    Local,
}

impl EthDevnet {
    /// Spawns Anvil, deploys the EVM contracts, returns `(Self, deployment_block_number)`.
    pub async fn setup(
        deploy_mode: Mode,
        devnet_cache: Option<Arc<RiftDevnetCache>>,
        token_indexer_database_url: Option<String>,
        token_indexer_api_key: Option<String>,
        interactive: bool,
        chain_id: u64,
        port: Option<u16>,
        token_indexer_port: Option<u16>,
    ) -> Result<Self> {
        let (anvil, anvil_datadir, anvil_dump_path, anvil_cache_path) = spawn_anvil(
            interactive,
            deploy_mode.clone(),
            devnet_cache.clone(),
            chain_id,
            port,
        )
        .await?;
        info!(
            "Anvil spawned at {}, chain_id={}",
            anvil.endpoint(),
            anvil.chain_id()
        );

        let private_key = anvil.keys()[0].clone().to_bytes().into();
        let funded_address = anvil.addresses()[0];

        let funded_provider = create_websocket_wallet_provider(
            anvil.ws_endpoint_url().to_string().as_str(),
            private_key,
        )
        .await
        .map_err(|e| eyre!(e.to_string()))?
        .erased();

        let (
            mock_erc20_contract,
            eip7702_delegator_contract,
            mock_across_spoke_pool_contract,
            mock_cctp_token_messenger_v2_contract,
            mock_cctp_message_transmitter_v2_contract,
            mock_velora_swap_contract,
        ) = deploy_contracts(funded_provider.clone(), devnet_cache.clone(), chain_id).await?;

        let token_indexer = if let Some(database_url) = token_indexer_database_url {
            let api_key = token_indexer_api_key.clone().ok_or_else(|| {
                eyre!("token indexer API key is required when token indexer is enabled")
            })?;
            Some(
                TokenIndexerInstance::new(
                    interactive,
                    anvil.endpoint_url().to_string().as_str(),
                    anvil.ws_endpoint_url().to_string().as_str(),
                    false,
                    anvil.chain_id(),
                    database_url,
                    api_key,
                    token_indexer_port,
                )
                .await?,
            )
        } else {
            None
        };

        let devnet = EthDevnet {
            anvil: anvil.into(),
            funded_provider,
            funded_address,
            deploy_mode,
            anvil_datadir,
            anvil_dump_path,
            anvil_cache_path,
            mock_erc20_contract,
            token_indexer,
            eip7702_delegator_contract,
            mock_across_spoke_pool_contract,
            mock_cctp_token_messenger_v2_contract,
            mock_cctp_message_transmitter_v2_contract,
            mock_velora_swap_contract,
        };

        Ok(devnet)
    }

    /// Gives `amount_wei` of Ether to `address` (via `anvil_set_balance`).
    pub async fn fund_eth_address(&self, address: Address, amount_wei: U256) -> Result<()> {
        self.funded_provider
            .anvil_set_balance(address, amount_wei)
            .await?;
        Ok(())
    }

    pub async fn mint_mock_erc20(&self, address: Address, amount: U256) -> Result<String> {
        self.mint_erc20(MOCK_ERC20_ADDRESS.parse()?, address, amount)
            .await
    }

    pub async fn mint_erc20(
        &self,
        token: Address,
        address: Address,
        amount: U256,
    ) -> Result<String> {
        let token_contract = GenericEIP3009ERC20Instance::new(token, self.funded_provider.clone());
        let receipt = token_contract
            .mint(address, amount)
            .send()
            .await?
            .get_receipt()
            .await?;
        Ok(receipt.transaction_hash.to_string())
    }

    /*
    /// Mints the mock token for `address`.
    pub async fn mint_token(&self, address: Address, amount: U256) -> Result<()> {
        let impersonate_provider = ProviderBuilder::new()
            .connect_http(format!("http://localhost:{}", self.anvil.port()).parse()?);
        if matches!(self.deploy_mode, Mode::Fork(_)) {
            // 1. Get the master minter address
            let master_minter = self.token_contract.masterMinter().call().await?;

            // 2. Configure master minter with maximum minting allowance
            let max_allowance = U256::MAX;
            let configure_minter_calldata = self
                .token_contract
                .configureMinter(master_minter, max_allowance)
                .calldata()
                .clone();

            let tx = TransactionRequest::default()
                .with_from(master_minter)
                .with_to(*self.token_contract.address())
                .with_input(configure_minter_calldata.clone());

            impersonate_provider
                .anvil_impersonate_account(master_minter)
                .await?;

            impersonate_provider
                .send_transaction(tx)
                .await?
                .get_receipt()
                .await?;

            let mint_calldata = self.token_contract.mint(address, amount).calldata().clone();

            let tx = TransactionRequest::default()
                .with_from(master_minter)
                .with_to(*self.token_contract.address())
                .with_input(mint_calldata.clone());

            // 3. Mint tokens as master minter
            impersonate_provider
                .send_transaction(tx)
                .await?
                .get_receipt()
                .await?;
        } else {
            // For local devnet, directly mint tokens
            self.token_contract
                .mint(address, amount)
                .send()
                .await?
                .get_receipt()
                .await?;
        }
        Ok(())
    }

    */
}

#[derive(Clone)]
pub struct ForkConfig {
    pub url: String,
    pub block_number: Option<u64>,
}

impl fmt::Debug for ForkConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ForkConfig")
            .field("url", &redacted_url_for_debug(&self.url))
            .field("block_number", &self.block_number)
            .finish()
    }
}

fn redacted_url_for_debug(raw_url: &str) -> String {
    let Ok(url) = url::Url::parse(raw_url) else {
        return "<invalid url>".to_string();
    };

    let host = url.host_str().unwrap_or("<missing-host>");
    let mut redacted = format!("{}://{}", url.scheme(), host);
    if let Some(port) = url.port() {
        redacted.push(':');
        redacted.push_str(&port.to_string());
    }
    if url.path() != "/" {
        redacted.push_str("/<redacted-path>");
    }
    if url.query().is_some() {
        redacted.push_str("?<redacted-query>");
    }
    redacted
}

async fn deploy_contracts(
    provider: DynProvider,
    devnet_cache: Option<Arc<RiftDevnetCache>>,
    chain_id: u64,
) -> Result<(
    GenericEIP3009ERC20Instance<DynProvider>,
    EIP7702DelegatorInstance<DynProvider>,
    MockSpokePoolInstance<DynProvider>,
    MockCctpTokenMessengerV2Instance<DynProvider>,
    MockCctpMessageTransmitterV2Instance<DynProvider>,
    MockVeloraSwapInstance<DynProvider>,
)> {
    let mock_across_spoke_pool_address =
        parse_address(MOCK_ACROSS_SPOKE_POOL_ADDRESS, "mock Across spoke pool")?;
    let mock_cctp_token_messenger_address = parse_address(
        MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS,
        "mock CCTP token messenger",
    )?;
    let mock_cctp_message_transmitter_address = parse_address(
        MOCK_CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS,
        "mock CCTP message transmitter",
    )?;
    let mock_erc20_address = parse_address(MOCK_ERC20_ADDRESS, "mock ERC20")?;
    let eip7702_delegator_address =
        parse_address(EIP7702_DELEGATOR_CROSSCHAIN_ADDRESS, "EIP-7702 delegator")?;
    let eip7702_delegator_bytecode =
        parse_bytes(EIP7702_DELEGATOR_BYTECODE, "EIP-7702 delegator bytecode")?;

    let mock_spoke_pool_deployment = MockSpokePoolInstance::deploy(provider.clone()).await?;
    let mock_spoke_pool_deployed_bytecode = provider
        .clone()
        .get_code_at(*mock_spoke_pool_deployment.address())
        .await?;
    provider
        .anvil_set_code(
            mock_across_spoke_pool_address,
            mock_spoke_pool_deployed_bytecode,
        )
        .await?;
    let mock_across_spoke_pool_contract =
        MockSpokePoolInstance::new(mock_across_spoke_pool_address, provider.clone());

    let mock_cctp_token_messenger_deployment =
        MockCctpTokenMessengerV2Instance::deploy(provider.clone()).await?;
    let mock_cctp_token_messenger_deployed_bytecode = provider
        .clone()
        .get_code_at(*mock_cctp_token_messenger_deployment.address())
        .await?;
    provider
        .anvil_set_code(
            mock_cctp_token_messenger_address,
            mock_cctp_token_messenger_deployed_bytecode,
        )
        .await?;
    let mock_cctp_token_messenger_v2_contract =
        MockCctpTokenMessengerV2Instance::new(mock_cctp_token_messenger_address, provider.clone());

    let mock_cctp_message_transmitter_deployment =
        MockCctpMessageTransmitterV2Instance::deploy(provider.clone()).await?;
    let mock_cctp_message_transmitter_deployed_bytecode = provider
        .clone()
        .get_code_at(*mock_cctp_message_transmitter_deployment.address())
        .await?;
    provider
        .anvil_set_code(
            mock_cctp_message_transmitter_address,
            mock_cctp_message_transmitter_deployed_bytecode,
        )
        .await?;
    let mock_cctp_message_transmitter_v2_contract = MockCctpMessageTransmitterV2Instance::new(
        mock_cctp_message_transmitter_address,
        provider.clone(),
    );
    let mock_velora_swap_contract = MockVeloraSwapInstance::deploy(provider.clone()).await?;
    provider
        .anvil_set_balance(
            *mock_velora_swap_contract.address(),
            U256::from(MOCK_VELORA_NATIVE_RESERVE_WEI),
        )
        .await?;

    if devnet_cache.is_some() {
        let mock_erc20_bytecode =
            ensure_mock_erc20_code_at_anchor(provider.clone(), mock_erc20_address).await?;
        install_mock_erc20_code_at_known_assets(provider.clone(), chain_id, mock_erc20_bytecode)
            .await?;
        install_mock_hyperliquid_bridge_code(provider.clone(), chain_id).await?;
        provider
            .anvil_set_code(
                eip7702_delegator_address,
                eip7702_delegator_bytecode.clone(),
            )
            .await?;

        // no need to deploy, just create the instance from the cache
        let mock_erc20_contract =
            GenericEIP3009ERC20Instance::new(mock_erc20_address, provider.clone());
        let delegator_contract = EIP7702DelegatorInstance::new(eip7702_delegator_address, provider);
        return Ok((
            mock_erc20_contract,
            delegator_contract,
            mock_across_spoke_pool_contract,
            mock_cctp_token_messenger_v2_contract,
            mock_cctp_message_transmitter_v2_contract,
            mock_velora_swap_contract,
        ));
    }

    let mock_erc20_deployment = GenericEIP3009ERC20Instance::deploy(provider.clone()).await?;
    let mock_erc20_deployed_bytecode = provider
        .clone()
        .get_code_at(*mock_erc20_deployment.address())
        .await?;

    provider
        .anvil_set_code(mock_erc20_address, mock_erc20_deployed_bytecode.clone())
        .await?;
    install_mock_erc20_code_at_known_assets(
        provider.clone(),
        chain_id,
        mock_erc20_deployed_bytecode,
    )
    .await?;
    install_mock_hyperliquid_bridge_code(provider.clone(), chain_id).await?;

    let mock_erc20_contract =
        GenericEIP3009ERC20Instance::new(mock_erc20_address, provider.clone());

    provider
        .anvil_set_code(eip7702_delegator_address, eip7702_delegator_bytecode)
        .await?;

    let delegator_contract = EIP7702DelegatorInstance::new(eip7702_delegator_address, provider);

    Ok((
        mock_erc20_contract,
        delegator_contract,
        mock_across_spoke_pool_contract,
        mock_cctp_token_messenger_v2_contract,
        mock_cctp_message_transmitter_v2_contract,
        mock_velora_swap_contract,
    ))
}

async fn ensure_mock_erc20_code_at_anchor(provider: DynProvider, anchor: Address) -> Result<Bytes> {
    let cached_bytecode = provider.clone().get_code_at(anchor).await?;
    if !cached_bytecode.is_empty() {
        return Ok(cached_bytecode);
    }

    let deployment = GenericEIP3009ERC20Instance::deploy(provider.clone()).await?;
    let bytecode = provider.clone().get_code_at(*deployment.address()).await?;
    provider.anvil_set_code(anchor, bytecode.clone()).await?;
    Ok(bytecode)
}

async fn install_mock_hyperliquid_bridge_code(provider: DynProvider, chain_id: u64) -> Result<()> {
    if chain_id != 42161 {
        return Ok(());
    }

    let deployment = MockHyperliquidBridge2Instance::deploy(
        provider.clone(),
        parse_address(ARBITRUM_USDC_ADDRESS, "Arbitrum USDC")?,
    )
    .await?;
    let bytecode = provider.clone().get_code_at(*deployment.address()).await?;
    provider
        .anvil_set_code(
            hyperliquid_bridge_address(HyperliquidNetwork::Testnet),
            bytecode,
        )
        .await?;
    Ok(())
}

async fn install_mock_erc20_code_at_known_assets(
    provider: DynProvider,
    chain_id: u64,
    bytecode: Bytes,
) -> Result<()> {
    for address in known_mock_erc20_addresses_for_chain(chain_id) {
        provider
            .anvil_set_code(
                parse_address(address, "known mock ERC20")?,
                bytecode.clone(),
            )
            .await?;
    }
    Ok(())
}

fn parse_address(raw: &str, label: &str) -> Result<Address> {
    raw.parse()
        .map_err(|error| eyre!("invalid {label} address {raw}: {error}"))
}

fn parse_bytes(raw: &str, label: &str) -> Result<Bytes> {
    raw.parse()
        .map_err(|error| eyre!("invalid {label} {raw}: {error}"))
}

fn known_mock_erc20_addresses_for_chain(chain_id: u64) -> &'static [&'static str] {
    match chain_id {
        1 => &[
            ETHEREUM_USDC_ADDRESS,
            ETHEREUM_USDT_ADDRESS,
            ETHEREUM_CBBTC_ADDRESS,
        ],
        8453 => &[BASE_USDC_ADDRESS, BASE_USDT_ADDRESS, BASE_CBBTC_ADDRESS],
        42161 => &[
            ARBITRUM_USDC_ADDRESS,
            ARBITRUM_USDT_ADDRESS,
            ARBITRUM_CBBTC_ADDRESS,
        ],
        _ => &[],
    }
}

/// Spawns Anvil in a blocking task.
async fn spawn_anvil(
    interactive: bool,
    mode: Mode,
    devnet_cache: Option<Arc<RiftDevnetCache>>,
    chain_id: u64,
    port: Option<u16>,
) -> Result<(
    AnvilInstance,
    Option<tempfile::TempDir>,
    tempfile::TempDir,
    tempfile::TempDir,
)> {
    let spawn_start = Instant::now();
    // Create or load anvil datafile
    let anvil_datadir = if let Some(cache) = &devnet_cache {
        let cache_start = Instant::now();
        let datadir = Some(match chain_id {
            8453 => cache.create_anvil_base_datadir().await?,
            42161 => cache.create_anvil_arbitrum_datadir().await?,
            _ => cache.create_anvil_datadir().await?,
        });
        info!(
            "[Anvil] Created anvil datadir from cache in {:?}",
            cache_start.elapsed()
        );
        datadir
    } else {
        None
    };

    let anvil_datadir_pathbuf = anvil_datadir.as_ref().map(|dir| dir.path().to_path_buf());

    // get a directory for the --dump-state flag
    let anvil_dump_path = get_new_temp_dir()?;
    let anvil_dump_pathbuf = anvil_dump_path.path().to_path_buf();

    // Scope Anvil's RPC fork cache to a devnet-owned tempdir instead of ~/.foundry.
    let anvil_cache_path = get_new_temp_dir()?;
    let anvil_cache_pathbuf = anvil_cache_path.path().to_path_buf();

    let anvil_instance = tokio::task::spawn_blocking(move || {
        let mut anvil = Anvil::new()
            .prague()
            .arg("--host")
            .arg("0.0.0.0")
            .chain_id(chain_id)
            // .arg("--steps-tracing")
            .arg("--cache-path")
            .arg(anvil_cache_pathbuf.to_string_lossy().to_string())
            .arg("--dump-state")
            .arg(anvil_dump_pathbuf.to_string_lossy().to_string());

        // Load state if file exists and has content - Anvil can handle the file format directly
        if let Some(state_path) = anvil_datadir_pathbuf {
            info!(
                "[Anvil] Loading state from {}",
                state_path.to_string_lossy()
            );
            anvil = anvil
                .arg("--load-state")
                .arg(state_path.to_string_lossy().to_string());
        }

        match mode {
            Mode::Fork(fork_config) => {
                anvil = anvil.port(port.unwrap_or(DEVNET_ETHEREUM_RPC_PORT));
                anvil = anvil.fork(fork_config.url);
                if let Some(block_number) = fork_config.block_number {
                    anvil = anvil.fork_block_number(block_number);
                }
            }
            Mode::Local => {
                if interactive {
                    anvil = anvil.port(port.unwrap_or(DEVNET_ETHEREUM_RPC_PORT));
                } else if let Some(p) = port {
                    // Use provided port for non-interactive mode if specified
                    anvil = anvil.port(p);
                }
            }
        }
        anvil.try_spawn().map_err(|e| {
            eprintln!("Failed to spawn Anvil: {e:?}");
            eyre!(e)
        })
    })
    .await??;

    info!("[Anvil] Anvil spawned in {:?}", spawn_start.elapsed());

    Ok((
        anvil_instance,
        anvil_datadir,
        anvil_dump_path,
        anvil_cache_path,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_mock_erc20_addresses_include_router_anchor_assets() {
        assert!(known_mock_erc20_addresses_for_chain(1).contains(&ETHEREUM_CBBTC_ADDRESS));
        assert!(known_mock_erc20_addresses_for_chain(8453).contains(&BASE_CBBTC_ADDRESS));
        assert!(known_mock_erc20_addresses_for_chain(42161).contains(&ARBITRUM_CBBTC_ADDRESS));
    }

    #[test]
    fn fork_config_debug_redacts_rpc_url_credentials() {
        let config = ForkConfig {
            url: "https://rpc-user:rpc-pass@mainnet.example/v2/path-secret?api_key=query-secret"
                .to_string(),
            block_number: Some(123),
        };

        let debug = format!("{:?}", Mode::Fork(config));

        assert!(debug.contains("ForkConfig"));
        assert!(debug.contains("block_number: Some(123)"));
        assert!(debug.contains("<redacted-path>"));
        assert!(debug.contains("<redacted-query>"));
        assert!(!debug.contains("rpc-user"));
        assert!(!debug.contains("rpc-pass"));
        assert!(!debug.contains("path-secret"));
        assert!(!debug.contains("query-secret"));
    }

    #[test]
    fn fork_config_debug_does_not_echo_invalid_urls() {
        let config = ForkConfig {
            url: "not a url with secret-token".to_string(),
            block_number: None,
        };

        let debug = format!("{config:?}");

        assert!(debug.contains("<invalid url>"));
        assert!(!debug.contains("secret-token"));
    }
}

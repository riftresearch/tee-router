//! `lib.rs` — central library code.

pub mod across_spoke_pool_mock;
pub mod bitcoin_devnet;
pub mod cctp_mock;
pub mod evm_devnet;
pub mod hyperliquid_bridge_mock;
pub mod manifest;
pub mod mock_integrators;
pub mod token_indexerd;
pub mod velora_mock;

pub use bitcoin_devnet::BitcoinDevnet;
use blockchain_utils::P2WPKHBitcoinWallet;
pub use evm_devnet::EthDevnet;
pub use manifest::{
    deterministic_loadgen_evm_accounts, DevnetEvmAccount, DevnetManifest, DEVNET_ARBITRUM_RPC_PORT,
    DEVNET_ARBITRUM_TOKEN_INDEXER_PORT, DEVNET_BASE_RPC_PORT, DEVNET_BASE_TOKEN_INDEXER_PORT,
    DEVNET_DEFAULT_LOADGEN_EVM_ACCOUNT_COUNT, DEVNET_DEMO_ACCOUNT_SALT, DEVNET_ETHEREUM_RPC_PORT,
    DEVNET_ETHEREUM_TOKEN_INDEXER_PORT, DEVNET_MANIFEST_PORT, DEVNET_MOCK_INTEGRATOR_PORT,
};

use evm_devnet::{
    ForkConfig, ARBITRUM_CBBTC_ADDRESS, ARBITRUM_USDC_ADDRESS, ARBITRUM_USDT_ADDRESS,
    BASE_CBBTC_ADDRESS, BASE_USDC_ADDRESS, BASE_USDT_ADDRESS, ETHEREUM_CBBTC_ADDRESS,
    ETHEREUM_USDC_ADDRESS, ETHEREUM_USDT_ADDRESS, MOCK_ACROSS_SPOKE_POOL_ADDRESS,
    MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS, MOCK_ERC20_ADDRESS,
};
use std::{
    fmt, fs, io,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
};
use tempfile::NamedTempFile;
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant, MissedTickBehavior};
use tracing::{info, warn};
use uuid::Uuid;

use bitcoincore_rpc_async::RpcApi;
use hyperliquid_client::{
    bridge_address as hyperliquid_bridge_address, Network as HyperliquidNetwork,
};

use alloy::{
    network::EthereumWallet,
    primitives::{keccak256, Address},
    providers::{ext::AnvilApi, Provider},
    signers::local::LocalSigner,
};

// ================== Deploy Function ================== //

use crate::evm_devnet::Mode;

const _LOG_CHUNK_SIZE: u64 = 10000;
const EVM_CONFIRMATION_MINING_INTERVAL: Duration = Duration::from_secs(1);

pub struct RiftDevnetCache {
    pub cache_dir: PathBuf,
    populated: bool,
}

const CACHE_DIR_NAME: &str = "rift-devnet";
const BITCOIN_DATADIR_NAME: &str = "bitcoin-datadir";
const ESPLORA_DATADIR_NAME: &str = "esplora-datadir";
const ANVIL_DATADIR_NAME: &str = "anvil-datadir";
const ANVIL_BASE_DATADIR_NAME: &str = "anvil-base-datadir";
const ANVIL_ARBITRUM_DATADIR_NAME: &str = "anvil-arbitrum-datadir";
const TEMP_DIR_NAME: &str = "tmp";
const TOKEN_INDEXER_API_KEY_MIN_LENGTH: usize = 32;
const TEMP_ENTRY_PREFIX: &str = "rift-devnet-p";
const ERROR_MESSAGE: &str = "Cache must be populated before utilizing it,";

pub fn get_new_temp_dir() -> Result<tempfile::TempDir> {
    let temp_root = devnet_temp_root()?;
    let prefix = current_process_temp_entry_prefix();
    tempfile::Builder::new()
        .prefix(&prefix)
        .tempdir_in(temp_root)
        .map_err(|e| eyre::eyre!("Failed to create devnet temp directory: {}", e).into())
}

pub fn get_new_temp_file() -> Result<NamedTempFile> {
    let temp_root = devnet_temp_root()?;
    let prefix = current_process_temp_entry_prefix();
    tempfile::Builder::new()
        .prefix(&prefix)
        .tempfile_in(temp_root)
        .map_err(|e| eyre::eyre!("Failed to create devnet temp file: {}", e).into())
}

fn devnet_cache_root() -> Result<PathBuf> {
    dirs::cache_dir()
        .map(|cache_dir| cache_dir.join(CACHE_DIR_NAME))
        .ok_or_else(|| eyre::eyre!("Failed to find user cache directory").into())
}

fn devnet_temp_root() -> Result<PathBuf> {
    let temp_root = devnet_temp_root_path()?;
    std::fs::create_dir_all(&temp_root)
        .map_err(|e| eyre::eyre!("Failed to create devnet temp root: {}", e))?;
    Ok(temp_root)
}

fn devnet_temp_root_path() -> Result<PathBuf> {
    Ok(devnet_cache_root()?.join(TEMP_DIR_NAME))
}

fn current_process_temp_entry_prefix() -> String {
    format!("{TEMP_ENTRY_PREFIX}{}-", std::process::id())
}

fn token_indexer_api_key() -> Result<String> {
    if let Ok(value) = std::env::var("EVM_TOKEN_INDEXER_API_KEY") {
        let trimmed = value.trim();
        if trimmed.len() < TOKEN_INDEXER_API_KEY_MIN_LENGTH {
            return Err(eyre::eyre!(
                "EVM_TOKEN_INDEXER_API_KEY must be at least {TOKEN_INDEXER_API_KEY_MIN_LENGTH} characters"
            )
            .into());
        }
        return Ok(trimmed.to_string());
    }

    Ok(format!(
        "devnet-token-indexer-{}",
        Uuid::now_v7().as_simple()
    ))
}

pub fn cleanup_current_process_temp_dirs() -> Result<usize> {
    let temp_root = devnet_temp_root_path()?;
    if !temp_root.exists() {
        return Ok(0);
    }

    cleanup_temp_entries_for_pid(&temp_root, std::process::id())
        .map_err(|e| eyre::eyre!("Failed to cleanup current devnet temp dirs: {}", e).into())
}

pub fn cleanup_stale_devnet_temp_dirs() -> Result<usize> {
    let temp_root = devnet_temp_root_path()?;
    if !temp_root.exists() {
        return Ok(0);
    }

    cleanup_stale_temp_entries(&temp_root)
        .map_err(|e| eyre::eyre!("Failed to cleanup stale devnet temp dirs: {}", e).into())
}

fn cleanup_temp_entries_for_pid(temp_root: &Path, pid: u32) -> io::Result<usize> {
    let mut removed = 0;
    for entry in fs::read_dir(temp_root)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };

        if temp_entry_pid(file_name) == Some(pid) {
            remove_temp_entry(&entry.path())?;
            removed += 1;
        }
    }

    Ok(removed)
}

fn cleanup_stale_temp_entries(temp_root: &Path) -> io::Result<usize> {
    let current_pid = std::process::id();
    let mut removed = 0;
    for entry in fs::read_dir(temp_root)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Some(pid) = temp_entry_pid(file_name) else {
            continue;
        };

        if pid != current_pid && !process_is_running(pid) {
            remove_temp_entry(&entry.path())?;
            removed += 1;
        }
    }

    Ok(removed)
}

fn temp_entry_pid(file_name: &str) -> Option<u32> {
    let rest = file_name.strip_prefix(TEMP_ENTRY_PREFIX)?;
    let (pid, _) = rest.split_once('-')?;
    pid.parse().ok()
}

fn remove_temp_entry(path: &Path) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };

    if metadata.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

#[cfg(unix)]
fn process_is_running(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    // SAFETY: `kill(pid, 0)` performs permission/existence checks only. It
    // does not send a signal, dereference pointers, or mutate Rust memory.
    let status = unsafe { libc::kill(pid, 0) };
    status == 0 || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn process_is_running(_pid: u32) -> bool {
    true
}

impl RiftDevnetCache {
    pub fn new() -> Result<Self> {
        let cache_dir = devnet_cache_root()?;
        let populated = Self::cache_is_populated(&cache_dir);
        Ok(Self {
            cache_dir,
            populated,
        })
    }

    fn cache_is_populated(cache_dir: &Path) -> bool {
        [
            BITCOIN_DATADIR_NAME,
            ESPLORA_DATADIR_NAME,
            ANVIL_DATADIR_NAME,
            ANVIL_BASE_DATADIR_NAME,
            ANVIL_ARBITRUM_DATADIR_NAME,
        ]
        .iter()
        .all(|dir_name| cache_dir.join(dir_name).is_dir())
    }

    /// Generic helper to copy a cached directory to a new temporary directory
    async fn copy_cached_dir(
        &self,
        dir_name: &str,
        operation_name: &str,
    ) -> Result<tempfile::TempDir> {
        if !self.populated {
            return Err(eyre::eyre!("{} {}", ERROR_MESSAGE, operation_name).into());
        }

        let cache_dir = self.cache_dir.join(dir_name);
        let temp_dir = get_new_temp_dir()?;

        // We need to copy the directory contents, not the directory itself
        let output = tokio::process::Command::new("cp")
            .arg("-R")
            .arg(format!("{}/.", cache_dir.to_string_lossy()))
            .arg(temp_dir.path())
            .output()
            .await
            .map_err(|e| eyre::eyre!("Failed to copy {}: {}", operation_name, e))?;

        if !output.status.success() {
            return Err(eyre::eyre!(
                "Failed to copy {}: {}",
                operation_name,
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(temp_dir)
    }

    pub async fn create_bitcoin_datadir(&self) -> Result<tempfile::TempDir> {
        let temp_dir = self
            .copy_cached_dir(BITCOIN_DATADIR_NAME, "bitcoin datadir")
            .await?;

        // Remove the cached .cookie file as bitcoind will generate a new one
        let cookie_path = temp_dir.path().join("regtest").join(".cookie");
        if cookie_path.exists() {
            tokio::fs::remove_file(&cookie_path)
                .await
                .map_err(|e| eyre::eyre!("Failed to remove cookie file: {}", e))?;
            tracing::info!("Removed cached .cookie file to allow bitcoind to generate a new one");
        }

        Ok(temp_dir)
    }

    pub async fn create_electrsd_datadir(&self) -> Result<tempfile::TempDir> {
        self.copy_cached_dir(ESPLORA_DATADIR_NAME, "electrsd datadir")
            .await
    }

    pub async fn create_anvil_datadir(&self) -> Result<tempfile::TempDir> {
        self.copy_cached_dir(ANVIL_DATADIR_NAME, "anvil datadir")
            .await
    }

    pub async fn create_anvil_base_datadir(&self) -> Result<tempfile::TempDir> {
        self.copy_cached_dir(ANVIL_BASE_DATADIR_NAME, "anvil base datadir")
            .await
    }

    pub async fn create_anvil_arbitrum_datadir(&self) -> Result<tempfile::TempDir> {
        self.copy_cached_dir(ANVIL_ARBITRUM_DATADIR_NAME, "anvil arbitrum datadir")
            .await
    }

    pub async fn save_devnet(&self, mut devnet: RiftDevnet) -> Result<()> {
        use fs2::FileExt;
        use std::fs;
        let save_start = Instant::now();

        // Create cache directory if it doesn't exist
        fs::create_dir_all(&self.cache_dir)
            .map_err(|e| eyre::eyre!("Failed to create cache directory: {}", e))?;

        // Get a file lock to prevent concurrent saves
        let lock_file_path = self.cache_dir.join(".lock");
        let lock_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&lock_file_path)
            .map_err(|e| eyre::eyre!("Failed to open lock file: {}", e))?;

        // Try to get exclusive lock
        lock_file
            .try_lock_exclusive()
            .map_err(|_| eyre::eyre!("Another process is already saving the cache"))?;

        // Check if cache was populated while waiting for lock
        if self.cache_dir.join(BITCOIN_DATADIR_NAME).exists() {
            tracing::info!("Cache already populated by another process");
            return Ok(());
        }

        info!("[Cache] Starting devnet save to cache...");

        // stop all tasks in the join set so the services dont complain about bitcoin + evm shutting down
        devnet.join_set.abort_all();

        // 1. Gracefully shut down Bitcoin Core to ensure all blocks are flushed to disk
        let bitcoin_shutdown_start = Instant::now();
        info!("[Cache] Shutting down Bitcoin Core to flush all data to disk...");
        match devnet.bitcoin.rpc_client.stop().await {
            Ok(_) => {
                info!("[Cache] Bitcoin Core shutdown initiated successfully");
                // Wait a bit for shutdown to complete
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
                info!(
                    "[Cache] Bitcoin shutdown took {:?}",
                    bitcoin_shutdown_start.elapsed()
                );
            }
            Err(e) => {
                // If stop fails, it might already be shutting down or have other issues
                tracing::warn!(
                    "Failed to stop Bitcoin Core gracefully: {}. Proceeding anyway.",
                    e
                );
            }
        }

        // 2. Gracefully shut down the Ethereum anvil instance
        let anvil_shutdown_start = Instant::now();
        info!("[Cache] Shutting down Ethereum Anvil to flush all data to disk...");
        let anvil_pid = devnet.ethereum.anvil.child().id();
        tokio::process::Command::new("kill")
            .arg("-SIGTERM")
            .arg(anvil_pid.to_string())
            .output()
            .await
            .map_err(|e| eyre::eyre!("Failed to shutdown Ethereum Anvil: {}", e))?;
        info!(
            "[Cache] Ethereum Anvil shutdown took {:?}",
            anvil_shutdown_start.elapsed()
        );

        // 3. Gracefully shut down the Base anvil instance
        let base_anvil_shutdown_start = Instant::now();
        info!("[Cache] Shutting down Base Anvil to flush all data to disk...");
        let base_anvil_pid = devnet.base.anvil.child().id();
        tokio::process::Command::new("kill")
            .arg("-SIGTERM")
            .arg(base_anvil_pid.to_string())
            .output()
            .await
            .map_err(|e| eyre::eyre!("Failed to shutdown Base Anvil: {}", e))?;
        info!(
            "[Cache] Base Anvil shutdown took {:?}",
            base_anvil_shutdown_start.elapsed()
        );

        let arbitrum_anvil_shutdown_start = Instant::now();
        info!("[Cache] Shutting down Arbitrum Anvil to flush all data to disk...");
        let arbitrum_anvil_pid = devnet.arbitrum.anvil.child().id();
        tokio::process::Command::new("kill")
            .arg("-SIGTERM")
            .arg(arbitrum_anvil_pid.to_string())
            .output()
            .await
            .map_err(|e| eyre::eyre!("Failed to shutdown Arbitrum Anvil: {}", e))?;
        info!(
            "[Cache] Arbitrum Anvil shutdown took {:?}",
            arbitrum_anvil_shutdown_start.elapsed()
        );

        // 2. Save Bitcoin datadir (now with all blocks properly flushed)
        let _copy_start = Instant::now();
        info!("[Cache] Starting to copy directories to cache...");
        let bitcoin_datadir_src = devnet.bitcoin.bitcoin_datadir.path();
        let bitcoin_datadir_dst = self.cache_dir.join(BITCOIN_DATADIR_NAME);
        let bitcoin_copy_start = Instant::now();
        Self::copy_dir_recursive(bitcoin_datadir_src, &bitcoin_datadir_dst)
            .await
            .map_err(|e| eyre::eyre!("Failed to copy Bitcoin datadir: {}", e))?;
        info!(
            "[Cache] Bitcoin datadir copied in {:?}",
            bitcoin_copy_start.elapsed()
        );

        // Remove the .cookie file from cache as it will be regenerated on startup
        let cached_cookie = bitcoin_datadir_dst.join("regtest").join(".cookie");
        if cached_cookie.exists() {
            tokio::fs::remove_file(&cached_cookie)
                .await
                .map_err(|e| eyre::eyre!("Failed to remove .cookie file: {}", e))?;
            info!("[Cache] Removed .cookie file from cache");
        }

        // 4. Save Electrsd datadir
        let electrsd_datadir_src = devnet.bitcoin.electrsd_datadir.path();
        let electrsd_datadir_dst = self.cache_dir.join(ESPLORA_DATADIR_NAME);
        let electrsd_copy_start = Instant::now();
        Self::copy_dir_recursive(electrsd_datadir_src, &electrsd_datadir_dst).await?;
        info!(
            "[Cache] Electrsd datadir copied in {:?}",
            electrsd_copy_start.elapsed()
        );

        // 7. Save Ethereum Anvil state file
        // Anvil automatically dumps state on exit to the anvil_datafile when --dump-state is used
        // We just need to copy it to our cache directory
        let anvil_dump_path = devnet.ethereum.anvil_dump_path.path();
        info!(
            "[Cache] Saving Ethereum anvil state from {}",
            anvil_dump_path.to_string_lossy()
        );

        let anvil_dst = self.cache_dir.join(ANVIL_DATADIR_NAME);
        let anvil_copy_start = Instant::now();
        Self::copy_dir_recursive(anvil_dump_path, &anvil_dst).await?;
        info!(
            "[Cache] Ethereum Anvil state copied in {:?}",
            anvil_copy_start.elapsed()
        );

        // 8. Save Base Anvil state file
        let base_anvil_dump_path = devnet.base.anvil_dump_path.path();
        info!(
            "[Cache] Saving Base anvil state from {}",
            base_anvil_dump_path.to_string_lossy()
        );

        let base_anvil_dst = self.cache_dir.join(ANVIL_BASE_DATADIR_NAME);
        let base_anvil_copy_start = Instant::now();
        Self::copy_dir_recursive(base_anvil_dump_path, &base_anvil_dst).await?;
        info!(
            "[Cache] Base Anvil state copied in {:?}",
            base_anvil_copy_start.elapsed()
        );

        let arbitrum_anvil_dump_path = devnet.arbitrum.anvil_dump_path.path();
        info!(
            "[Cache] Saving Arbitrum anvil state from {}",
            arbitrum_anvil_dump_path.to_string_lossy()
        );

        let arbitrum_anvil_dst = self.cache_dir.join(ANVIL_ARBITRUM_DATADIR_NAME);
        let arbitrum_anvil_copy_start = Instant::now();
        Self::copy_dir_recursive(arbitrum_anvil_dump_path, &arbitrum_anvil_dst).await?;
        info!(
            "[Cache] Arbitrum Anvil state copied in {:?}",
            arbitrum_anvil_copy_start.elapsed()
        );

        // Release lock by dropping it
        drop(lock_file);

        info!(
            "[Cache] Devnet saved to cache successfully! Total time: {:?}",
            save_start.elapsed()
        );
        Ok(())
    }

    async fn copy_dir_recursive(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
        tokio::fs::create_dir_all(dst)
            .await
            .map_err(|e| eyre::eyre!("Failed to create directory: {}", e))?;

        // Copy contents of src to dst
        let output = tokio::process::Command::new("cp")
            .arg("-R")
            .arg(format!("{}/.", src.to_string_lossy()))
            .arg(dst)
            .output()
            .await
            .map_err(|e| eyre::eyre!("Failed to copy directory: {}", e))?;

        if !output.status.success() {
            return Err(eyre::eyre!(
                "Failed to copy directory: {}",
                String::from_utf8_lossy(&output.stderr)
            )
            .into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_is_populated_requires_all_persistent_directories() {
        let cache_dir = tempfile::tempdir().expect("create temp cache dir");

        std::fs::create_dir(cache_dir.path().join(TEMP_DIR_NAME)).expect("create temp dir");
        assert!(!RiftDevnetCache::cache_is_populated(cache_dir.path()));

        for dir_name in [
            BITCOIN_DATADIR_NAME,
            ESPLORA_DATADIR_NAME,
            ANVIL_DATADIR_NAME,
            ANVIL_BASE_DATADIR_NAME,
            ANVIL_ARBITRUM_DATADIR_NAME,
        ] {
            std::fs::create_dir(cache_dir.path().join(dir_name))
                .expect("create persistent cache dir");
        }

        assert!(RiftDevnetCache::cache_is_populated(cache_dir.path()));
    }

    #[test]
    fn temp_entry_pid_only_parses_pid_scoped_devnet_names() {
        assert_eq!(temp_entry_pid("rift-devnet-p123-abc"), Some(123));
        assert_eq!(temp_entry_pid("rift-devnet-p123-abc.tmp"), Some(123));
        assert_eq!(temp_entry_pid("rift-devnet-legacy"), None);
        assert_eq!(temp_entry_pid("other-p123-abc"), None);
    }

    #[test]
    fn cleanup_temp_entries_for_pid_removes_only_matching_entries() {
        let temp_root = tempfile::tempdir().expect("create temp root");
        let matching_dir = temp_root.path().join("rift-devnet-p123-alpha");
        let matching_file = temp_root.path().join("rift-devnet-p123-beta");
        let other_pid_dir = temp_root.path().join("rift-devnet-p456-alpha");
        let legacy_dir = temp_root.path().join("rift-devnet-legacy");

        std::fs::create_dir(&matching_dir).expect("create matching dir");
        std::fs::write(matching_dir.join("state.json"), "{}").expect("write nested file");
        std::fs::write(&matching_file, "{}").expect("write matching file");
        std::fs::create_dir(&other_pid_dir).expect("create other pid dir");
        std::fs::create_dir(&legacy_dir).expect("create legacy dir");

        let removed =
            cleanup_temp_entries_for_pid(temp_root.path(), 123).expect("cleanup pid entries");

        assert_eq!(removed, 2);
        assert!(!matching_dir.exists());
        assert!(!matching_file.exists());
        assert!(other_pid_dir.exists());
        assert!(legacy_dir.exists());
    }

    #[test]
    fn multichain_account_debug_redacts_private_material() {
        let account = MultichainAccount::new(123).expect("multichain account");

        let debug = format!("{account:?}");

        assert!(debug.contains("MultichainAccount"));
        assert!(debug.contains("secret_bytes: \"<redacted>\""));
        assert!(debug.contains("bitcoin_mnemonic: \"<redacted>\""));
        assert!(debug.contains(&format!("{:#x}", account.ethereum_address)));
        assert!(debug.contains(&account.bitcoin_wallet.address.to_string()));
        assert!(!debug.contains(&alloy::hex::encode(account.secret_bytes)));
        assert!(!debug.contains(&account.bitcoin_mnemonic.to_string()));
        assert!(!debug.contains(&account.bitcoin_wallet.private_key.to_wif()));
        assert!(!debug.contains(
            &account
                .bitcoin_wallet
                .secret_key
                .display_secret()
                .to_string()
        ));
    }
}

#[derive(Debug, snafu::Snafu)]
pub enum DevnetError {
    #[snafu(display("Failed to build devnet: {}", source))]
    Build { source: eyre::Report },

    #[snafu(display("Timeout waiting for esplora to sync after {timeout:?}"))]
    EsploraSyncTimeout { timeout: std::time::Duration },
}

impl From<eyre::Report> for DevnetError {
    fn from(report: eyre::Report) -> Self {
        DevnetError::Build { source: report }
    }
}

pub type Result<T, E = DevnetError> = std::result::Result<T, E>;

// ================== RiftDevnet ================== //

/// The "combined" Devnet which holds:
/// - a `BitcoinDevnet`
/// - an `EthDevnet` for Ethereum (chain ID 1)
/// - an `EthDevnet` for Base (chain ID 8453, port 50102)
/// - an `EthDevnet` for Arbitrum (chain ID 42161, port 50103)
/// - provider mocks when running the interactive CLI server
pub struct RiftDevnet {
    pub bitcoin: Arc<BitcoinDevnet>,
    pub ethereum: Arc<EthDevnet>,
    pub base: Arc<EthDevnet>,
    pub arbitrum: Arc<EthDevnet>,
    pub loadgen_evm_accounts: Vec<DevnetEvmAccount>,
    pub mock_integrators: Option<mock_integrators::MockIntegratorServer>,
    pub join_set: JoinSet<Result<()>>,
}

impl RiftDevnet {
    #[must_use]
    pub fn builder() -> RiftDevnetBuilder {
        RiftDevnetBuilder::new()
    }

    #[must_use]
    pub fn builder_for_cached() -> RiftDevnetBuilder {
        RiftDevnetBuilder::for_cached()
    }
}

/// A builder for configuring a `RiftDevnet` instantiation.
#[derive(Default)]
pub struct RiftDevnetBuilder {
    interactive: bool,
    funded_evm_addresses: Vec<String>,
    funded_bitcoin_addreses: Vec<String>,
    fork_config: Option<ForkConfig>,
    using_esplora: bool,
    using_mock_integrators: bool,
    token_indexer_database_url: Option<String>,
    bitcoin_mining_mode: crate::bitcoin_devnet::MiningMode,
    loadgen_evm_account_count: usize,
}

impl RiftDevnetBuilder {
    /// Create a new builder with all default values.
    #[must_use]
    pub fn new() -> Self {
        Default::default()
    }

    /// Create a builder with settings for a cached devnet.
    #[must_use]
    pub fn for_cached() -> Self {
        RiftDevnetBuilder {
            interactive: false,
            funded_evm_addresses: vec![],
            funded_bitcoin_addreses: vec![],
            fork_config: None,
            using_esplora: true,
            using_mock_integrators: false,
            token_indexer_database_url: None,
            bitcoin_mining_mode: crate::bitcoin_devnet::MiningMode::default(),
            loadgen_evm_account_count: 0,
        }
    }

    /// Toggle whether the devnet runs in "interactive" mode:
    /// - If true, binds Anvil on a stable port and starts a local `RiftIndexerServer`.
    /// - If false, does minimal ephemeral setup.
    #[must_use]
    pub fn interactive(mut self, value: bool) -> Self {
        self.interactive = value;
        self
    }

    pub fn using_token_indexer(mut self, database_url: String) -> Self {
        self.token_indexer_database_url = Some(database_url);
        self
    }

    /// Start local provider mocks alongside the interactive devnet.
    pub fn using_mock_integrators(mut self, value: bool) -> Self {
        self.using_mock_integrators = value;
        self
    }

    /// Optionally fund a given EVM address with Ether and tokens.
    pub fn funded_evm_address<T: Into<String>>(mut self, address: T) -> Self {
        self.funded_evm_addresses.push(address.into());
        self
    }

    /// Optionally fund a given Bitcoin address.
    pub fn funded_bitcoin_address<T: Into<String>>(mut self, address: T) -> Self {
        self.funded_bitcoin_addreses.push(address.into());
        self
    }

    pub fn bitcoin_mining_mode(mut self, mining_mode: crate::bitcoin_devnet::MiningMode) -> Self {
        self.bitcoin_mining_mode = mining_mode;
        self
    }

    /// Allocate a deterministic pool of EVM keys for load generation and fund
    /// them on every local EVM chain.
    pub fn loadgen_evm_account_count(mut self, count: usize) -> Self {
        self.loadgen_evm_account_count = count;
        self
    }

    /// Provide a fork configuration (RPC URL/block) if you want to fork a public chain.
    #[must_use]
    pub fn fork_config(mut self, config: ForkConfig) -> Self {
        self.fork_config = Some(config);
        self
    }

    /// Start a blockstream/electrs esplora REST API server for bitcoin data indexing.
    #[must_use]
    pub fn using_esplora(mut self, value: bool) -> Self {
        self.using_esplora = value;
        self
    }

    pub async fn build(self) -> Result<(crate::RiftDevnet, u64)> {
        match cleanup_stale_devnet_temp_dirs() {
            Ok(removed) if removed > 0 => {
                tracing::info!("Removed {removed} stale devnet temp entries");
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!("Failed to cleanup stale devnet temp entries: {error}");
            }
        }

        // dont bother with the cache if we're in interactive mode for now
        // could help startup time a little bit if we care to enable it later
        if self.interactive {
            Ok(self.build_internal(None).await?)
        } else {
            let cache = Arc::new(RiftDevnetCache::new()?);

            if cache.populated {
                tracing::info!("Cache directory exists, loading devnet from cache...");
                let (devnet, funded_sats) = self.build_internal(Some(cache.clone())).await?;
                Ok((devnet, funded_sats))
            } else {
                tracing::info!("Cache directory does not exist, building fresh devnet...");
                let (devnet, funded_sats) = self.build_internal(None).await?;
                Ok((devnet, funded_sats))
            }
        }
    }

    /// Actually build the `RiftDevnet`, consuming this builder.
    ///
    /// Returns a tuple of:
    ///   - The devnet instance
    ///   - The number of satoshis funded to `funded_bitcoin_address` (if any)
    async fn build_internal(
        self,
        devnet_cache: Option<Arc<RiftDevnetCache>>,
    ) -> Result<(crate::RiftDevnet, u64)> {
        let build_start = Instant::now();
        info!("[Devnet Builder] Starting devnet build...");
        let mut join_set = JoinSet::new();

        // 1) Bitcoin side
        let bitcoin_start = Instant::now();
        let bitcoin_setup = crate::bitcoin_devnet::BitcoinDevnet::setup(
            self.funded_bitcoin_addreses.clone(),
            self.using_esplora,
            self.interactive,
            self.bitcoin_mining_mode,
            &mut join_set,
            devnet_cache.clone(),
        )
        .await;
        let (bitcoin_devnet, current_mined_height) = match bitcoin_setup {
            Ok(result) => result,
            Err(error) if devnet_cache.is_some() => {
                warn!(
                    "Cached Bitcoin devnet setup failed: {}. Retrying with a fresh bitcoin datadir.",
                    error
                );
                crate::bitcoin_devnet::BitcoinDevnet::setup(
                    self.funded_bitcoin_addreses.clone(),
                    self.using_esplora,
                    self.interactive,
                    self.bitcoin_mining_mode,
                    &mut join_set,
                    None,
                )
                .await
                .map_err(|retry_error| {
                    eyre::eyre!(
                        "[devnet builder] Failed to setup Bitcoin devnet from cache ({}) and fresh ({})",
                        error,
                        retry_error
                    )
                })?
            }
            Err(error) => {
                return Err(eyre::eyre!(
                    "[devnet builder] Failed to setup Bitcoin devnet: {}",
                    error
                )
                .into())
            }
        };
        info!(
            "[Devnet Builder] Bitcoin devnet setup took {:?}",
            bitcoin_start.elapsed()
        );

        // Drop build lock here, only really necessary for bitcoin devnet setup
        let funding_sats = bitcoin_devnet.funded_sats;

        // 2) Collect Bitcoin checkpoint leaves
        info!(
            "[Devnet Builder] Processing checkpoint leaves from block range 0..{current_mined_height}"
        );

        let deploy_mode = if let Some(fork_config) = self.fork_config.clone() {
            Mode::Fork(fork_config)
        } else {
            Mode::Local
        };
        let token_indexer_api_key = if self.token_indexer_database_url.is_some() {
            Some(token_indexer_api_key()?)
        } else {
            None
        };

        // 5) Ethereum side (chain ID 31337, default port)
        let ethereum_start = Instant::now();

        let ethereum_devnet =
            crate::evm_devnet::EthDevnet::setup(crate::evm_devnet::EthDevnetSetup {
                deploy_mode: deploy_mode.clone(),
                devnet_cache: devnet_cache.clone(),
                token_indexer_database_url: self.token_indexer_database_url.clone(),
                token_indexer_api_key: token_indexer_api_key.clone(),
                interactive: self.interactive,
                chain_id: 1, // Ethereum chain ID
                port: if self.interactive {
                    Some(DEVNET_ETHEREUM_RPC_PORT)
                } else {
                    None
                },
                token_indexer_port: if self.interactive {
                    Some(DEVNET_ETHEREUM_TOKEN_INDEXER_PORT)
                } else {
                    None
                },
            })
            .await
            .map_err(|e| eyre::eyre!("[devnet builder] Failed to setup Ethereum devnet: {}", e))?;

        info!(
            "[Devnet Builder] Ethereum devnet setup took {:?}",
            ethereum_start.elapsed()
        );

        // 6) Base side (chain ID 8453, port 50102 in interactive mode)
        let base_start = Instant::now();

        let base_devnet = crate::evm_devnet::EthDevnet::setup(crate::evm_devnet::EthDevnetSetup {
            deploy_mode: deploy_mode.clone(),
            devnet_cache: devnet_cache.clone(), // Use cache for Base
            token_indexer_database_url: self.token_indexer_database_url.clone(),
            token_indexer_api_key: token_indexer_api_key.clone(),
            interactive: self.interactive,
            chain_id: 8453, // Base chain ID
            port: if self.interactive {
                Some(DEVNET_BASE_RPC_PORT)
            } else {
                None
            },
            token_indexer_port: if self.interactive {
                Some(DEVNET_BASE_TOKEN_INDEXER_PORT)
            } else {
                None
            },
        })
        .await
        .map_err(|e| eyre::eyre!("[devnet builder] Failed to setup Base devnet: {}", e))?;

        info!(
            "[Devnet Builder] Base devnet setup took {:?}",
            base_start.elapsed()
        );

        // 7) Arbitrum side (chain ID 42161, port 50103 in interactive mode)
        let arbitrum_start = Instant::now();

        let arbitrum_devnet =
            crate::evm_devnet::EthDevnet::setup(crate::evm_devnet::EthDevnetSetup {
                deploy_mode,
                devnet_cache: devnet_cache.clone(),
                token_indexer_database_url: self.token_indexer_database_url.clone(),
                token_indexer_api_key,
                interactive: self.interactive,
                chain_id: 42161, // Arbitrum chain ID
                port: if self.interactive {
                    Some(DEVNET_ARBITRUM_RPC_PORT)
                } else {
                    None
                },
                token_indexer_port: if self.interactive {
                    Some(DEVNET_ARBITRUM_TOKEN_INDEXER_PORT)
                } else {
                    None
                },
            })
            .await
            .map_err(|e| eyre::eyre!("[devnet builder] Failed to setup Arbitrum devnet: {}", e))?;

        info!(
            "[Devnet Builder] Arbitrum devnet setup took {:?}",
            arbitrum_start.elapsed()
        );

        let loadgen_evm_accounts =
            deterministic_loadgen_evm_accounts(self.loadgen_evm_account_count).map_err(
                |error| eyre::eyre!("[devnet builder] Failed to derive loadgen accounts: {error}"),
            )?;
        let mut funded_evm_addresses = self.funded_evm_addresses.clone();
        funded_evm_addresses.extend(
            loadgen_evm_accounts
                .iter()
                .map(|account| account.address.clone()),
        );

        // 9) Fund optional EVM addresses with Ether and tokens on all EVM chains.
        let funding_start = if funded_evm_addresses.is_empty() {
            None
        } else {
            info!(
                "[Devnet Builder] Funding {} EVM addresses on Ethereum, Base, and Arbitrum...",
                funded_evm_addresses.len()
            );
            Some(Instant::now())
        };
        for addr_str in funded_evm_addresses {
            use alloy::primitives::Address;
            use std::str::FromStr;
            let address = Address::from_str(&addr_str)
                .map_err(|e| eyre::eyre!("Failed to parse EVM address: {}", e))?;
            let token_amount = alloy::primitives::U256::from_str("100000000000000")
                .map_err(|e| eyre::eyre!("Failed to parse U256: {}", e))?;

            // ~10 ETH on Ethereum
            ethereum_devnet
                .fund_eth_address(
                    address,
                    alloy::primitives::U256::from_str("10000000000000000000")
                        .map_err(|e| eyre::eyre!("Failed to parse U256: {}", e))?,
                )
                .await
                .map_err(|e| {
                    eyre::eyre!(
                        "[devnet builder] Failed to fund ETH address on Ethereum: {}",
                        e
                    )
                })?;

            // ~10 ETH on Base
            base_devnet
                .fund_eth_address(
                    address,
                    alloy::primitives::U256::from_str("10000000000000000000")
                        .map_err(|e| eyre::eyre!("Failed to parse U256: {}", e))?,
                )
                .await
                .map_err(|e| {
                    eyre::eyre!("[devnet builder] Failed to fund ETH address on Base: {}", e)
                })?;

            // ~10 ETH on Arbitrum
            arbitrum_devnet
                .fund_eth_address(
                    address,
                    alloy::primitives::U256::from_str("10000000000000000000")
                        .map_err(|e| eyre::eyre!("Failed to parse U256: {}", e))?,
                )
                .await
                .map_err(|e| {
                    eyre::eyre!(
                        "[devnet builder] Failed to fund ETH address on Arbitrum: {}",
                        e
                    )
                })?;

            // Debugging: check funded balances
            let eth_balance = ethereum_devnet
                .funded_provider
                .get_balance(address)
                .await
                .map_err(|e| {
                    eyre::eyre!(
                        "[devnet builder] Failed to get ETH balance on Ethereum: {}",
                        e
                    )
                })?;
            info!("[Devnet Builder] Ethereum Balance of {addr_str} => {eth_balance:?}");

            let base_balance = base_devnet
                .funded_provider
                .get_balance(address)
                .await
                .map_err(|e| {
                    eyre::eyre!("[devnet builder] Failed to get ETH balance on Base: {}", e)
                })?;
            info!("[Devnet Builder] Base Balance of {addr_str} => {base_balance:?}");

            let arbitrum_balance = arbitrum_devnet
                .funded_provider
                .get_balance(address)
                .await
                .map_err(|e| {
                    eyre::eyre!(
                        "[devnet builder] Failed to get ETH balance on Arbitrum: {}",
                        e
                    )
                })?;
            info!("[Devnet Builder] Arbitrum Balance of {addr_str} => {arbitrum_balance:?}");

            mint_default_local_tokens(
                &ethereum_devnet,
                address,
                &[
                    MOCK_ERC20_ADDRESS,
                    ETHEREUM_USDC_ADDRESS,
                    ETHEREUM_USDT_ADDRESS,
                    ETHEREUM_CBBTC_ADDRESS,
                ],
                token_amount,
            )
            .await?;
            mint_default_local_tokens(
                &base_devnet,
                address,
                &[
                    MOCK_ERC20_ADDRESS,
                    BASE_USDC_ADDRESS,
                    BASE_USDT_ADDRESS,
                    BASE_CBBTC_ADDRESS,
                ],
                token_amount,
            )
            .await?;
            mint_default_local_tokens(
                &arbitrum_devnet,
                address,
                &[
                    MOCK_ERC20_ADDRESS,
                    ARBITRUM_USDC_ADDRESS,
                    ARBITRUM_USDT_ADDRESS,
                    ARBITRUM_CBBTC_ADDRESS,
                ],
                token_amount,
            )
            .await?;
        }
        if let Some(start) = funding_start {
            info!("[Devnet Builder] Funded addresses in {:?}", start.elapsed());
        }

        if self.interactive {
            self.setup_interactive_mode(
                &bitcoin_devnet,
                &ethereum_devnet,
                &base_devnet,
                &arbitrum_devnet,
                self.using_esplora,
            )
            .await?;
        }

        let mock_integrators = if self.using_mock_integrators {
            Some(
                self.setup_mock_integrators(
                    &bitcoin_devnet,
                    &ethereum_devnet,
                    &base_devnet,
                    &arbitrum_devnet,
                )
                .await?,
            )
        } else {
            None
        };

        let ethereum_devnet = Arc::new(ethereum_devnet);
        let base_devnet = Arc::new(base_devnet);
        let arbitrum_devnet = Arc::new(arbitrum_devnet);

        if self.interactive {
            spawn_evm_confirmation_miner("Ethereum", ethereum_devnet.clone(), &mut join_set);
            spawn_evm_confirmation_miner("Base", base_devnet.clone(), &mut join_set);
            spawn_evm_confirmation_miner("Arbitrum", arbitrum_devnet.clone(), &mut join_set);
        }

        // 11) Create the devnet
        let devnet = crate::RiftDevnet {
            bitcoin: Arc::new(bitcoin_devnet),
            ethereum: ethereum_devnet,
            base: base_devnet,
            arbitrum: arbitrum_devnet,
            loadgen_evm_accounts,
            mock_integrators,
            join_set,
        };
        info!(
            "[Devnet Builder] Devnet setup took {:?}",
            build_start.elapsed()
        );

        Ok((devnet, funding_sats))
    }

    async fn setup_mock_integrators(
        &self,
        bitcoin_devnet: &BitcoinDevnet,
        ethereum_devnet: &EthDevnet,
        base_devnet: &EthDevnet,
        arbitrum_devnet: &EthDevnet,
    ) -> Result<mock_integrators::MockIntegratorServer> {
        let bind_addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            DEVNET_MOCK_INTEGRATOR_PORT,
        );
        let config = mock_integrators::MockIntegratorConfig::default()
            .with_bind_addr(bind_addr)
            .with_advertised_base_url(format!("http://127.0.0.1:{DEVNET_MOCK_INTEGRATOR_PORT}"))
            .with_across_spoke_pool_address(MOCK_ACROSS_SPOKE_POOL_ADDRESS)
            .with_across_evm_rpc_url(base_devnet.anvil.endpoint())
            .with_across_chain(
                ethereum_devnet.anvil.chain_id(),
                MOCK_ACROSS_SPOKE_POOL_ADDRESS,
                ethereum_devnet.anvil.endpoint(),
            )
            .with_across_chain(
                base_devnet.anvil.chain_id(),
                MOCK_ACROSS_SPOKE_POOL_ADDRESS,
                base_devnet.anvil.endpoint(),
            )
            .with_across_chain(
                arbitrum_devnet.anvil.chain_id(),
                MOCK_ACROSS_SPOKE_POOL_ADDRESS,
                arbitrum_devnet.anvil.endpoint(),
            )
            .with_across_auth("devnet-across", "rift-devnet")
            .with_cctp_chain(
                ethereum_devnet.anvil.chain_id(),
                MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS,
                ethereum_devnet.anvil.endpoint(),
            )
            .with_cctp_chain(
                base_devnet.anvil.chain_id(),
                MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS,
                base_devnet.anvil.endpoint(),
            )
            .with_cctp_chain(
                arbitrum_devnet.anvil.chain_id(),
                MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS,
                arbitrum_devnet.anvil.endpoint(),
            )
            .with_cctp_destination_token(ethereum_devnet.anvil.chain_id(), ETHEREUM_USDC_ADDRESS)
            .with_cctp_destination_token(base_devnet.anvil.chain_id(), BASE_USDC_ADDRESS)
            .with_cctp_destination_token(arbitrum_devnet.anvil.chain_id(), ARBITRUM_USDC_ADDRESS)
            .with_velora_swap_contract_address(
                ethereum_devnet.anvil.chain_id(),
                format!("{:#x}", ethereum_devnet.mock_velora_swap_contract.address()),
            )
            .with_velora_swap_contract_address(
                base_devnet.anvil.chain_id(),
                format!("{:#x}", base_devnet.mock_velora_swap_contract.address()),
            )
            .with_velora_swap_contract_address(
                arbitrum_devnet.anvil.chain_id(),
                format!("{:#x}", arbitrum_devnet.mock_velora_swap_contract.address()),
            )
            .with_unit_evm_rpc_url(
                hyperunit_client::UnitChain::Ethereum,
                ethereum_devnet.anvil.endpoint(),
            )
            .with_unit_evm_rpc_url(
                hyperunit_client::UnitChain::Base,
                base_devnet.anvil.endpoint(),
            )
            .with_unit_bitcoin_rpc(
                bitcoin_devnet.rpc_url.clone(),
                bitcoin_devnet.cookie.clone(),
            )
            .with_hyperliquid_bridge_address(format!(
                "{:#x}",
                hyperliquid_bridge_address(HyperliquidNetwork::Testnet)
            ))
            .with_hyperliquid_evm_rpc_url(arbitrum_devnet.anvil.endpoint())
            .with_hyperliquid_usdc_token_address(ARBITRUM_USDC_ADDRESS);

        mock_integrators::MockIntegratorServer::spawn_with_config(config)
            .await
            .map_err(|error| {
                eyre::eyre!("[devnet builder] Failed to start mock integrators: {error}")
            })
            .map_err(Into::into)
    }

    /// Setup interactive mode with demo funding, auto-mining, and logging
    async fn setup_interactive_mode(
        &self,
        bitcoin_devnet: &BitcoinDevnet,
        ethereum_devnet: &EthDevnet,
        base_devnet: &EthDevnet,
        arbitrum_devnet: &EthDevnet,
        using_esplora: bool,
    ) -> Result<()> {
        let setup_start = Instant::now();

        let demo_account = MultichainAccount::new(DEVNET_DEMO_ACCOUNT_SALT)?;

        let funding_start = Instant::now();
        info!("[Interactive Setup] Funding demo_account with ETH...");
        ethereum_devnet
            .fund_eth_address(
                demo_account.ethereum_address,
                alloy::primitives::U256::from_str_radix("1000000000000000000000000", 10)
                    .map_err(|e| eyre::eyre!("Conversion error: {}", e))?,
            )
            .await
            .map_err(|e| {
                eyre::eyre!(
                    "[devnet builder-demo_account] Failed to fund ETH address: {}",
                    e
                )
            })?;

        let funding_amount = 1000_u64 * 100_000_000;

        // Fund demo_account with the mock ERC-20 on Ethereum
        let _tx_hash = ethereum_devnet
            .mint_mock_erc20(
                demo_account.ethereum_address,
                alloy::primitives::U256::from(funding_amount),
            )
            .await?;
        mint_default_local_tokens(
            ethereum_devnet,
            demo_account.ethereum_address,
            &[
                ETHEREUM_USDC_ADDRESS,
                ETHEREUM_USDT_ADDRESS,
                ETHEREUM_CBBTC_ADDRESS,
            ],
            alloy::primitives::U256::from(funding_amount),
        )
        .await?;

        // Fund demo_account with ETH on Base
        info!("[Interactive Setup] Funding demo_account with ETH on Base...");
        base_devnet
            .fund_eth_address(
                demo_account.ethereum_address,
                alloy::primitives::U256::from_str_radix("1000000000000000000000000", 10)
                    .map_err(|e| eyre::eyre!("Conversion error: {}", e))?,
            )
            .await
            .map_err(|e| {
                eyre::eyre!(
                    "[devnet builder-demo_account] Failed to fund ETH address on Base: {}",
                    e
                )
            })?;

        // Fund demo_account with the mock ERC-20 on Base
        let _tx_hash = base_devnet
            .mint_mock_erc20(
                demo_account.ethereum_address,
                alloy::primitives::U256::from(funding_amount),
            )
            .await?;
        mint_default_local_tokens(
            base_devnet,
            demo_account.ethereum_address,
            &[BASE_USDC_ADDRESS, BASE_USDT_ADDRESS, BASE_CBBTC_ADDRESS],
            alloy::primitives::U256::from(funding_amount),
        )
        .await?;

        // Fund demo_account with ETH on Arbitrum
        info!("[Interactive Setup] Funding demo_account with ETH on Arbitrum...");
        arbitrum_devnet
            .fund_eth_address(
                demo_account.ethereum_address,
                alloy::primitives::U256::from_str_radix("1000000000000000000000000", 10)
                    .map_err(|e| eyre::eyre!("Conversion error: {}", e))?,
            )
            .await
            .map_err(|e| {
                eyre::eyre!(
                    "[devnet builder-demo_account] Failed to fund ETH address on Arbitrum: {}",
                    e
                )
            })?;

        // Fund demo_account with the mock ERC-20 on Arbitrum
        let _tx_hash = arbitrum_devnet
            .mint_mock_erc20(
                demo_account.ethereum_address,
                alloy::primitives::U256::from(funding_amount),
            )
            .await?;
        mint_default_local_tokens(
            arbitrum_devnet,
            demo_account.ethereum_address,
            &[
                ARBITRUM_USDC_ADDRESS,
                ARBITRUM_USDT_ADDRESS,
                ARBITRUM_CBBTC_ADDRESS,
            ],
            alloy::primitives::U256::from(funding_amount),
        )
        .await?;

        // Fund demo_account with Bitcoin
        info!("[Interactive Setup] Funding demo_account with Bitcoin...");
        bitcoin_devnet
            .deal_bitcoin(
                &demo_account.bitcoin_wallet.address,
                &bitcoin::Amount::from_sat(funding_amount),
            )
            .await
            .map_err(|e| {
                eyre::eyre!(
                    "[devnet builder-demo_account] Failed to deal bitcoin: {}",
                    e
                )
            })?;

        info!(
            "[Interactive Setup] Account funding took {:?}",
            funding_start.elapsed()
        );

        // Log interactive info
        info!(
            "[Interactive Setup] Interactive mode setup complete in {:?}",
            setup_start.elapsed()
        );
        println!("---RIFT DEVNET---");
        println!(
            "Demo EVM Address:           {}",
            demo_account.ethereum_address
        );
        println!(
            "Demo BTC Address:           {}",
            demo_account.bitcoin_wallet.address
        );
        println!(
            "Demo EVM Private Key:       {}",
            alloy::hex::encode(demo_account.secret_bytes)
        );
        println!(
            "Demo BTC Descriptor:        {}",
            demo_account.bitcoin_wallet.descriptor()
        );
        println!(
            "Ethereum Anvil HTTP Url:    http://0.0.0.0:{}",
            ethereum_devnet.anvil.port()
        );
        println!(
            "Ethereum Anvil WS Url:      ws://0.0.0.0:{}",
            ethereum_devnet.anvil.port()
        );
        println!(
            "Ethereum Chain ID:          {}",
            ethereum_devnet.anvil.chain_id()
        );
        println!(
            "Base Anvil HTTP Url:        http://0.0.0.0:{}",
            base_devnet.anvil.port()
        );
        println!(
            "Base Anvil WS Url:          ws://0.0.0.0:{}",
            base_devnet.anvil.port()
        );
        println!(
            "Base Chain ID:              {}",
            base_devnet.anvil.chain_id()
        );
        println!(
            "Arbitrum Anvil HTTP Url:    http://0.0.0.0:{}",
            arbitrum_devnet.anvil.port()
        );
        println!(
            "Arbitrum Anvil WS Url:      ws://0.0.0.0:{}",
            arbitrum_devnet.anvil.port()
        );
        println!(
            "Arbitrum Chain ID:          {}",
            arbitrum_devnet.anvil.chain_id()
        );
        println!("Bitcoin RPC URL:            {}", bitcoin_devnet.rpc_url);
        println!(
            "Bitcoin RPC Cookie File:    {}",
            bitcoin_devnet.cookie.display()
        );

        if using_esplora {
            if let Some(esplora_url) = bitcoin_devnet.esplora_url.as_ref() {
                println!("Esplora API URL:            {esplora_url}");
            } else {
                warn!("Esplora was requested, but no Esplora URL is available");
            }
        }

        if let Some(eth_indexer) = &ethereum_devnet.token_indexer {
            println!("Ethereum Token Indexer:     {}", eth_indexer.api_server_url);
        }

        if let Some(base_indexer) = &base_devnet.token_indexer {
            println!(
                "Base Token Indexer:         {}",
                base_indexer.api_server_url
            );
        }

        if let Some(arbitrum_indexer) = &arbitrum_devnet.token_indexer {
            println!(
                "Arbitrum Token Indexer:     {}",
                arbitrum_indexer.api_server_url
            );
        }

        match self.bitcoin_mining_mode {
            crate::bitcoin_devnet::MiningMode::Interval(interval) => {
                println!("Bitcoin Auto-mining:        Every {} seconds", interval);
            }
            crate::bitcoin_devnet::MiningMode::Manual => {
                println!("Bitcoin Auto-mining:        Disabled");
            }
        }

        println!("Anvil Auto-mining:          On demand");
        println!(
            "Anvil Confirmation Mining:  Every {} second",
            EVM_CONFIRMATION_MINING_INTERVAL.as_secs()
        );
        println!("---RIFT DEVNET---");

        Ok(())
    }
}

fn spawn_evm_confirmation_miner(
    chain_name: &'static str,
    devnet: Arc<EthDevnet>,
    join_set: &mut JoinSet<Result<()>>,
) {
    let provider = devnet.funded_provider.clone();
    join_set.spawn(async move {
        let mut interval = tokio::time::interval(EVM_CONFIRMATION_MINING_INTERVAL);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            interval.tick().await;
            if let Err(error) = provider.anvil_mine(Some(1), None).await {
                warn!(chain = chain_name, %error, "EVM confirmation miner failed to mine block");
            }
        }
    });
}

async fn mint_default_local_tokens(
    devnet: &EthDevnet,
    recipient: Address,
    token_addresses: &[&str],
    amount: alloy::primitives::U256,
) -> Result<()> {
    let mut minted = std::collections::BTreeSet::new();
    for token in token_addresses {
        let token_address = token
            .parse()
            .map_err(|error| eyre::eyre!("invalid local token address {token}: {error}"))?;
        if !minted.insert(token_address) {
            continue;
        }
        devnet.mint_erc20(token_address, recipient, amount).await?;
    }

    Ok(())
}

/// Holds the components of a multichain account including secret bytes and wallets.
pub struct MultichainAccount {
    /// The raw secret bytes used to derive wallets
    pub secret_bytes: [u8; 32],
    /// The BIP-39 mnemonic phrase for the Bitcoin wallet (seeded from the secret bytes)
    pub bitcoin_mnemonic: bip39::Mnemonic,
    /// The Ethereum wallet derived from the secret
    pub ethereum_wallet: EthereumWallet,
    /// The Ethereum address associated with the wallet
    pub ethereum_address: Address,
    /// The Bitcoin wallet derived from the secret
    pub bitcoin_wallet: P2WPKHBitcoinWallet,
}

impl fmt::Debug for MultichainAccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultichainAccount")
            .field("secret_bytes", &"<redacted>")
            .field("bitcoin_mnemonic", &"<redacted>")
            .field("ethereum_address", &self.ethereum_address)
            .field("bitcoin_address", &self.bitcoin_wallet.address)
            .finish()
    }
}

impl MultichainAccount {
    /// Creates a new multichain account from the given derivation salt
    pub fn new(derivation_salt: u32) -> Result<Self> {
        Self::with_network(derivation_salt, ::bitcoin::Network::Regtest)
    }

    /// Creates a new multichain account with the Bitcoin network explicitly specified
    pub fn with_network(derivation_salt: u32, network: ::bitcoin::Network) -> Result<Self> {
        let secret_bytes: [u8; 32] = keccak256(derivation_salt.to_le_bytes()).into();

        let signer = LocalSigner::from_bytes(&secret_bytes.into()).map_err(|error| {
            eyre::eyre!(
                "failed to derive deterministic EVM signer for salt {derivation_salt}: {error}"
            )
        })?;
        let ethereum_wallet = EthereumWallet::new(signer);

        let ethereum_address = ethereum_wallet.default_signer().address();

        let bitcoin_mnemonic = bip39::Mnemonic::from_entropy(&secret_bytes).map_err(|error| {
            eyre::eyre!(
                "failed to derive deterministic Bitcoin mnemonic for salt {derivation_salt}: {error}"
            )
        })?;

        let bitcoin_wallet =
            P2WPKHBitcoinWallet::from_mnemonic(&bitcoin_mnemonic.to_string(), None, network, None)
                .map_err(|error| {
                    eyre::eyre!(
                "failed to derive deterministic Bitcoin wallet for salt {derivation_salt}: {error}"
            )
                })?;

        Ok(Self {
            secret_bytes,
            bitcoin_mnemonic,
            ethereum_wallet,
            ethereum_address,
            bitcoin_wallet,
        })
    }
}

use serde::Serialize;
use std::fmt;

use crate::{
    evm_devnet::{
        ARBITRUM_CBBTC_ADDRESS, ARBITRUM_USDC_ADDRESS, ARBITRUM_USDT_ADDRESS, BASE_CBBTC_ADDRESS,
        BASE_USDC_ADDRESS, BASE_USDT_ADDRESS, ETHEREUM_CBBTC_ADDRESS, ETHEREUM_USDC_ADDRESS,
        ETHEREUM_USDT_ADDRESS, MOCK_ACROSS_SPOKE_POOL_ADDRESS,
        MOCK_CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS, MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS,
        MOCK_ERC20_ADDRESS,
    },
    MultichainAccount, Result, RiftDevnet,
};

pub const DEVNET_DEMO_ACCOUNT_SALT: u32 = 110_101;
pub const DEVNET_BITCOIN_RPC_PORT: u16 = 50_100;
pub const DEVNET_ETHEREUM_RPC_PORT: u16 = 50_101;
pub const DEVNET_BASE_RPC_PORT: u16 = 50_102;
pub const DEVNET_ARBITRUM_RPC_PORT: u16 = 50_103;
pub const DEVNET_ETHEREUM_TOKEN_INDEXER_PORT: u16 = 50_104;
pub const DEVNET_BASE_TOKEN_INDEXER_PORT: u16 = 50_105;
pub const DEVNET_ARBITRUM_TOKEN_INDEXER_PORT: u16 = 50_106;
pub const DEVNET_MOCK_INTEGRATOR_PORT: u16 = 50_107;
pub const DEVNET_MANIFEST_PORT: u16 = 50_108;
pub const DEVNET_ESPLORA_PORT: u16 = 50_110;
pub const DEVNET_BITCOIN_ZMQ_RAWTX_PORT: u16 = 50_111;
pub const DEVNET_BITCOIN_ZMQ_SEQUENCE_PORT: u16 = 50_112;
pub const DEVNET_BITCOIN_ZMQ_RAWBLOCK_PORT: u16 = 50_118;
pub const DEVNET_BITCOIN_RPC_USER: &str = "devnet";
pub const DEVNET_BITCOIN_RPC_PASSWORD: &str = "devnet";
pub const DEVNET_LOADGEN_ACCOUNT_SALT_START: u32 = 220_000;
pub const DEVNET_DEFAULT_LOADGEN_EVM_ACCOUNT_COUNT: usize = 16;

#[derive(Debug, Clone, Serialize)]
pub struct DevnetManifest {
    pub version: u32,
    pub deterministic: bool,
    pub accounts: DevnetAccounts,
    pub chains: Vec<DevnetChain>,
    pub services: DevnetServices,
    pub contracts: DevnetContracts,
}

#[derive(Clone, Serialize)]
pub struct DevnetAccounts {
    pub demo_evm_address: String,
    pub demo_evm_private_key: String,
    pub demo_bitcoin_address: String,
    pub demo_bitcoin_descriptor: String,
    pub anvil_default_private_key: String,
    pub anvil_default_address: String,
    pub loadgen_evm_accounts: Vec<DevnetEvmAccount>,
}

impl fmt::Debug for DevnetAccounts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DevnetAccounts")
            .field("demo_evm_address", &self.demo_evm_address)
            .field("demo_evm_private_key", &"redacted")
            .field("demo_bitcoin_address", &self.demo_bitcoin_address)
            .field("demo_bitcoin_descriptor", &"redacted")
            .field("anvil_default_private_key", &"redacted")
            .field("anvil_default_address", &self.anvil_default_address)
            .field("loadgen_evm_accounts", &self.loadgen_evm_accounts)
            .finish()
    }
}

#[derive(Clone, Serialize)]
pub struct DevnetEvmAccount {
    pub address: String,
    pub private_key: String,
}

impl fmt::Debug for DevnetEvmAccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DevnetEvmAccount")
            .field("address", &self.address)
            .field("private_key", &"redacted")
            .finish()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DevnetChain {
    pub id: String,
    pub name: String,
    pub chain_id: Option<u64>,
    pub host_rpc_url: String,
    pub compose_rpc_url: String,
    pub host_ws_url: Option<String>,
    pub compose_ws_url: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DevnetServices {
    pub provider_mocks_host_url: String,
    pub provider_mocks_compose_url: String,
    pub manifest_host_url: String,
    pub manifest_compose_url: String,
    pub bitcoin_esplora_host_url: String,
    pub bitcoin_esplora_compose_url: String,
    pub bitcoin_zmq_rawblock_compose_endpoint: String,
    pub bitcoin_zmq_rawtx_compose_endpoint: String,
    pub bitcoin_zmq_sequence_compose_endpoint: String,
    pub ethereum_token_indexer_compose_url: Option<String>,
    pub base_token_indexer_compose_url: Option<String>,
    pub arbitrum_token_indexer_compose_url: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DevnetContracts {
    pub mock_erc20: String,
    pub ethereum_usdc: String,
    pub ethereum_usdt: String,
    pub ethereum_cbbtc: String,
    pub base_usdc: String,
    pub base_usdt: String,
    pub base_cbbtc: String,
    pub arbitrum_usdc: String,
    pub arbitrum_usdt: String,
    pub arbitrum_cbbtc: String,
    pub mock_across_spoke_pool: String,
    pub mock_cctp_token_messenger_v2: String,
    pub mock_cctp_message_transmitter_v2: String,
}

impl DevnetManifest {
    pub fn from_devnet(devnet: &RiftDevnet) -> Result<Self> {
        let demo = MultichainAccount::new(DEVNET_DEMO_ACCOUNT_SALT)?;
        Ok(Self {
            version: 1,
            deterministic: true,
            accounts: DevnetAccounts {
                demo_evm_address: format!("{:#x}", demo.ethereum_address),
                demo_evm_private_key: format!("0x{}", alloy::hex::encode(demo.secret_bytes)),
                demo_bitcoin_address: demo.bitcoin_wallet.address.to_string(),
                demo_bitcoin_descriptor: demo.bitcoin_wallet.descriptor(),
                anvil_default_private_key:
                    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".to_string(),
                anvil_default_address: "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266".to_string(),
                loadgen_evm_accounts: devnet.loadgen_evm_accounts.clone(),
            },
            chains: vec![
                evm_chain(
                    "evm:1",
                    "ethereum",
                    devnet.ethereum.anvil.chain_id(),
                    DEVNET_ETHEREUM_RPC_PORT,
                ),
                evm_chain(
                    "evm:8453",
                    "base",
                    devnet.base.anvil.chain_id(),
                    DEVNET_BASE_RPC_PORT,
                ),
                evm_chain(
                    "evm:42161",
                    "arbitrum",
                    devnet.arbitrum.anvil.chain_id(),
                    DEVNET_ARBITRUM_RPC_PORT,
                ),
                DevnetChain {
                    id: "bitcoin".to_string(),
                    name: "bitcoin-regtest".to_string(),
                    chain_id: None,
                    host_rpc_url: format!("http://127.0.0.1:{DEVNET_BITCOIN_RPC_PORT}"),
                    compose_rpc_url: format!("http://devnet:{DEVNET_BITCOIN_RPC_PORT}"),
                    host_ws_url: None,
                    compose_ws_url: None,
                },
            ],
            services: DevnetServices {
                provider_mocks_host_url: format!("http://127.0.0.1:{DEVNET_MOCK_INTEGRATOR_PORT}"),
                provider_mocks_compose_url: format!("http://devnet:{DEVNET_MOCK_INTEGRATOR_PORT}"),
                manifest_host_url: format!("http://127.0.0.1:{DEVNET_MANIFEST_PORT}"),
                manifest_compose_url: format!("http://devnet:{DEVNET_MANIFEST_PORT}"),
                bitcoin_esplora_host_url: format!("http://127.0.0.1:{DEVNET_ESPLORA_PORT}"),
                bitcoin_esplora_compose_url: format!("http://devnet:{DEVNET_ESPLORA_PORT}"),
                bitcoin_zmq_rawblock_compose_endpoint: format!(
                    "tcp://devnet:{DEVNET_BITCOIN_ZMQ_RAWBLOCK_PORT}"
                ),
                bitcoin_zmq_rawtx_compose_endpoint: format!(
                    "tcp://devnet:{DEVNET_BITCOIN_ZMQ_RAWTX_PORT}"
                ),
                bitcoin_zmq_sequence_compose_endpoint: format!(
                    "tcp://devnet:{DEVNET_BITCOIN_ZMQ_SEQUENCE_PORT}"
                ),
                ethereum_token_indexer_compose_url: token_indexer_url(
                    devnet.ethereum.token_indexer.is_some(),
                    DEVNET_ETHEREUM_TOKEN_INDEXER_PORT,
                ),
                base_token_indexer_compose_url: token_indexer_url(
                    devnet.base.token_indexer.is_some(),
                    DEVNET_BASE_TOKEN_INDEXER_PORT,
                ),
                arbitrum_token_indexer_compose_url: token_indexer_url(
                    devnet.arbitrum.token_indexer.is_some(),
                    DEVNET_ARBITRUM_TOKEN_INDEXER_PORT,
                ),
            },
            contracts: DevnetContracts {
                mock_erc20: MOCK_ERC20_ADDRESS.to_string(),
                ethereum_usdc: ETHEREUM_USDC_ADDRESS.to_string(),
                ethereum_usdt: ETHEREUM_USDT_ADDRESS.to_string(),
                ethereum_cbbtc: ETHEREUM_CBBTC_ADDRESS.to_string(),
                base_usdc: BASE_USDC_ADDRESS.to_string(),
                base_usdt: BASE_USDT_ADDRESS.to_string(),
                base_cbbtc: BASE_CBBTC_ADDRESS.to_string(),
                arbitrum_usdc: ARBITRUM_USDC_ADDRESS.to_string(),
                arbitrum_usdt: ARBITRUM_USDT_ADDRESS.to_string(),
                arbitrum_cbbtc: ARBITRUM_CBBTC_ADDRESS.to_string(),
                mock_across_spoke_pool: MOCK_ACROSS_SPOKE_POOL_ADDRESS.to_string(),
                mock_cctp_token_messenger_v2: MOCK_CCTP_TOKEN_MESSENGER_V2_ADDRESS.to_string(),
                mock_cctp_message_transmitter_v2: MOCK_CCTP_MESSAGE_TRANSMITTER_V2_ADDRESS
                    .to_string(),
            },
        })
    }
}

pub fn deterministic_loadgen_evm_accounts(count: usize) -> Result<Vec<DevnetEvmAccount>> {
    (0..count)
        .map(|index| {
            let index = u32::try_from(index).map_err(|error| {
                eyre::eyre!("loadgen account index {index} exceeds u32 salt space: {error}")
            })?;
            let account = MultichainAccount::new(DEVNET_LOADGEN_ACCOUNT_SALT_START + index)?;
            Ok(DevnetEvmAccount {
                address: format!("{:#x}", account.ethereum_address),
                private_key: format!("0x{}", alloy::hex::encode(account.secret_bytes)),
            })
        })
        .collect()
}

fn evm_chain(id: &str, name: &str, chain_id: u64, port: u16) -> DevnetChain {
    DevnetChain {
        id: id.to_string(),
        name: name.to_string(),
        chain_id: Some(chain_id),
        host_rpc_url: format!("http://127.0.0.1:{port}"),
        compose_rpc_url: format!("http://devnet:{port}"),
        host_ws_url: Some(format!("ws://127.0.0.1:{port}")),
        compose_ws_url: Some(format!("ws://devnet:{port}")),
    }
}

fn token_indexer_url(enabled: bool, port: u16) -> Option<String> {
    enabled.then(|| format!("http://devnet:{port}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn devnet_accounts_debug_redacts_private_material() {
        let accounts = DevnetAccounts {
            demo_evm_address: "0x1111111111111111111111111111111111111111".to_string(),
            demo_evm_private_key:
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            demo_bitcoin_address: "bcrt1q2pfqp8a574jxyszmk0h5rxf02wwkpaf4hd8009".to_string(),
            demo_bitcoin_descriptor: "wpkh(secret-descriptor)".to_string(),
            anvil_default_private_key:
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            anvil_default_address: "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266".to_string(),
            loadgen_evm_accounts: vec![DevnetEvmAccount {
                address: "0x2222222222222222222222222222222222222222".to_string(),
                private_key: "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .to_string(),
            }],
        };

        let rendered = format!("{accounts:?}");

        assert!(!rendered.contains("aaaaaaaa"));
        assert!(!rendered.contains("secret-descriptor"));
        assert!(!rendered.contains("bbbbbbbb"));
        assert!(!rendered.contains("cccccccc"));
        assert!(rendered.contains("redacted"));
        assert!(rendered.contains("1111111111111111111111111111111111111111"));
        assert!(rendered.contains("2222222222222222222222222222222222222222"));
    }

    #[test]
    fn devnet_accounts_serialization_still_exports_loadgen_keys() {
        let accounts = DevnetAccounts {
            demo_evm_address: "0x1111111111111111111111111111111111111111".to_string(),
            demo_evm_private_key:
                "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            demo_bitcoin_address: "bcrt1q2pfqp8a574jxyszmk0h5rxf02wwkpaf4hd8009".to_string(),
            demo_bitcoin_descriptor: "wpkh(local-devnet-descriptor)".to_string(),
            anvil_default_private_key:
                "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string(),
            anvil_default_address: "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266".to_string(),
            loadgen_evm_accounts: vec![DevnetEvmAccount {
                address: "0x2222222222222222222222222222222222222222".to_string(),
                private_key: "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                    .to_string(),
            }],
        };

        let serialized = serde_json::to_value(&accounts).expect("serialize");

        assert_eq!(
            serialized["demo_evm_private_key"],
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        assert_eq!(
            serialized["loadgen_evm_accounts"][0]["private_key"],
            "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        );
    }
}

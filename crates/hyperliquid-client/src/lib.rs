//! Thin Hyperliquid REST client for spot trading: action types, EIP-712
//! signing, HTTP plumbing. Perps are intentionally out of scope — every
//! code path here assumes the spot (HyperCore) universe.
//!
//! Target base URL is configurable — mainnet, testnet, or a devnet mock can
//! all slot in.

pub mod actions;
pub mod bridge;
pub mod client;
pub mod eip712;
pub mod error;
pub mod http;
pub mod info;
pub mod meta;
pub mod signature;
pub mod wire;

pub use actions::{
    Actions, BulkCancel, BulkOrder, CancelRequest, Limit, Order, OrderRequest, ScheduleCancel,
    SendAsset, SpotSend, Tif, UsdClassTransfer, Withdraw3,
};
pub use bridge::{
    bridge_address, build_bridge_deposit_tx, build_bridge_deposit_tx_to, minimum_bridge_deposit,
    BridgeDepositTx, MAINNET_BRIDGE2_ADDRESS, MINIMUM_BRIDGE_DEPOSIT_USDC, TESTNET_BRIDGE2_ADDRESS,
};
pub use client::{
    HyperliquidBridgeClient, HyperliquidClient, HyperliquidExchangeClient, HyperliquidInfoClient,
    Network,
};
pub use eip712::Eip712;
pub use error::{Error, Result};
pub use http::HttpClient;
pub use info::{
    BasicOrderInfo, ClearinghouseState, L2BookSnapshot, L2Level, MarginSummary, OpenOrder,
    OrderInfoEnvelope, OrderStatusResponse, SpotBalance, SpotClearinghouseState, UserFill,
};
pub use meta::{
    spot_wire_asset_index, SpotAssetMeta, SpotMeta, TokenInfo, SPOT_ASSET_INDEX_OFFSET,
};
pub use signature::{recover_l1_signer, recover_typed_signer, sign_l1_action, sign_typed_data};
pub use wire::float_to_wire;

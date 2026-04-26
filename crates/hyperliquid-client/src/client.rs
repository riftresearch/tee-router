//! Hyperliquid client surfaces split by responsibility:
//! - [`HyperliquidInfoClient`] for read-only `/info` queries
//! - [`HyperliquidExchangeClient`] for signed `/exchange` actions
//! - [`HyperliquidBridgeClient`] for bridge-address lookup and deposit tx
//!   construction
//!
//! [`HyperliquidClient`] remains as a compatibility wrapper that composes the
//! narrower clients and delegates to them.

use std::collections::HashMap;

use alloy::{
    primitives::{Address, Signature},
    signers::local::PrivateKeySigner,
};
use reqwest::Client;
use serde::{ser::SerializeStruct, Deserialize, Serialize, Serializer};
use url::Url;

use crate::{
    actions::{
        Actions, BulkCancel, BulkOrder, CancelRequest, OrderRequest, ScheduleCancel, SpotSend,
        UsdClassTransfer, Withdraw3,
    },
    bridge::{self, BridgeDepositTx},
    error::Error,
    http::HttpClient,
    info::{
        ClearinghouseState, L2BookSnapshot, OpenOrder, OrderStatusResponse, SpotClearinghouseState,
        UserFill,
    },
    meta::SpotMeta,
    signature::{sign_l1_action, sign_typed_data},
};

/// Mainnet / testnet routing affects both the L1 signing "source" byte AND
/// which `hyperliquidChain` string user actions stamp. Bundled here so
/// callers pick once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Network {
    Mainnet,
    Testnet,
}

impl Network {
    #[must_use]
    pub fn is_mainnet(self) -> bool {
        matches!(self, Network::Mainnet)
    }

    #[must_use]
    pub fn hyperliquid_chain(self) -> &'static str {
        match self {
            Network::Mainnet => "Mainnet",
            Network::Testnet => "Testnet",
        }
    }

    /// `signature_chain_id` stamped into user actions (Withdraw3, UsdSend,
    /// ...). HL uses Arbitrum for both environments: 42161 on mainnet, 421614
    /// on testnet.
    #[must_use]
    pub fn signature_chain_id(self) -> u64 {
        match self {
            Network::Mainnet => 42_161,
            Network::Testnet => 421_614,
        }
    }
}

/// Read-only Hyperliquid `/info` client.
#[derive(Debug)]
pub struct HyperliquidInfoClient {
    http: HttpClient,
    spot_meta: Option<SpotMeta>,
    coin_to_asset: HashMap<String, u32>,
}

impl HyperliquidInfoClient {
    pub fn new(base_url: &str) -> Result<Self, Error> {
        Ok(Self {
            http: build_http(base_url)?,
            spot_meta: None,
            coin_to_asset: HashMap::new(),
        })
    }

    #[must_use]
    pub fn http(&self) -> &HttpClient {
        &self.http
    }

    #[must_use]
    pub fn spot_meta(&self) -> Option<&SpotMeta> {
        self.spot_meta.as_ref()
    }

    /// Fetch spot meta without mutating any local cache. Useful for callers
    /// that want the metadata snapshot but keep their own caching strategy.
    pub async fn fetch_spot_meta(&self) -> Result<SpotMeta, Error> {
        let req = serde_json::json!({ "type": "spotMeta" });
        self.http.post_json("/info", &req).await
    }

    /// Fetch spot meta and rebuild the coin-name -> asset-index map. Must be
    /// called at least once before [`Self::asset_index`].
    pub async fn refresh_spot_meta(&mut self) -> Result<SpotMeta, Error> {
        let meta = self.fetch_spot_meta().await?;
        self.coin_to_asset = meta.coin_to_asset_map();
        self.spot_meta = Some(meta.clone());
        Ok(meta)
    }

    /// Resolve a spot pair name (`"UBTC/USDC"` or `"@0"`) to its wire asset
    /// id. Fails if [`Self::refresh_spot_meta`] hasn't been called or the pair
    /// is unknown.
    pub fn asset_index(&self, coin: &str) -> Result<u32, Error> {
        self.coin_to_asset
            .get(coin)
            .copied()
            .ok_or_else(|| Error::AssetNotFound {
                asset: coin.to_string(),
            })
    }

    /// Resolve a token symbol (for example `UETH`) to the `tokenName:tokenId`
    /// wire form required by Hyperliquid `spotSend`.
    pub fn spot_token_wire(&self, symbol: &str) -> Result<String, Error> {
        let Some(meta) = self.spot_meta.as_ref() else {
            return Err(Error::AssetNotFound {
                asset: symbol.to_string(),
            });
        };
        meta.spot_token_wire(symbol)
            .ok_or_else(|| Error::AssetNotFound {
                asset: symbol.to_string(),
            })
    }

    /// L2 orderbook snapshot for a spot pair. Used at quote time to estimate
    /// fill size against resting liquidity; HL does not expose a first-class
    /// "quote" endpoint, so callers walk the book.
    pub async fn l2_book(&self, coin: &str) -> Result<L2BookSnapshot, Error> {
        let req = serde_json::json!({ "type": "l2Book", "coin": coin });
        self.http.post_json("/info", &req).await
    }

    /// Poll the status of a single resting / historical order by `oid`.
    pub async fn order_status(
        &self,
        user: Address,
        oid: u64,
    ) -> Result<OrderStatusResponse, Error> {
        let req = serde_json::json!({
            "type": "orderStatus",
            "user": format!("{user:?}"),
            "oid": oid,
        });
        self.http.post_json("/info", &req).await
    }

    /// Fetch the spot clearinghouse state for `user`: per-token balances
    /// (`total`) with any portion locked by resting orders (`hold`).
    pub async fn spot_clearinghouse_state(
        &self,
        user: Address,
    ) -> Result<SpotClearinghouseState, Error> {
        let req = serde_json::json!({
            "type": "spotClearinghouseState",
            "user": format!("{user:?}"),
        });
        self.http.post_json("/info", &req).await
    }

    /// Fetch the perp/cross-margin clearinghouse state for `user`. Bridge
    /// deposits from Arbitrum show up here as withdrawable USDC collateral.
    pub async fn clearinghouse_state(&self, user: Address) -> Result<ClearinghouseState, Error> {
        let req = serde_json::json!({
            "type": "clearinghouseState",
            "user": format!("{user:?}"),
        });
        self.http.post_json("/info", &req).await
    }

    /// List `user`'s resting (not-yet-filled, not-cancelled) orders. Returns
    /// an empty vector when none are open.
    pub async fn open_orders(&self, user: Address) -> Result<Vec<OpenOrder>, Error> {
        let req = serde_json::json!({
            "type": "openOrders",
            "user": format!("{user:?}"),
        });
        self.http.post_json("/info", &req).await
    }

    /// Fetch `user`'s historical fills, newest first.
    pub async fn user_fills(&self, user: Address) -> Result<Vec<UserFill>, Error> {
        let req = serde_json::json!({
            "type": "userFills",
            "user": format!("{user:?}"),
        });
        self.http.post_json("/info", &req).await
    }
}

/// Wallet-bound Hyperliquid `/exchange` client for provider-native actions.
#[derive(Debug)]
pub struct HyperliquidExchangeClient {
    http: HttpClient,
    wallet: PrivateKeySigner,
    vault_address: Option<Address>,
    network: Network,
}

impl HyperliquidExchangeClient {
    pub fn new(
        base_url: &str,
        wallet: PrivateKeySigner,
        vault_address: Option<Address>,
        network: Network,
    ) -> Result<Self, Error> {
        Ok(Self {
            http: build_http(base_url)?,
            wallet,
            vault_address,
            network,
        })
    }

    #[must_use]
    pub fn wallet(&self) -> &PrivateKeySigner {
        &self.wallet
    }

    #[must_use]
    pub fn http(&self) -> &HttpClient {
        &self.http
    }

    #[must_use]
    pub fn network(&self) -> Network {
        self.network
    }

    #[must_use]
    pub fn vault_address(&self) -> Option<Address> {
        self.vault_address
    }

    /// Submit an L1 Order action. Caller is responsible for having populated
    /// `OrderRequest::asset`.
    pub async fn place_orders(
        &self,
        orders: Vec<OrderRequest>,
        grouping: &str,
    ) -> Result<serde_json::Value, Error> {
        let action = Actions::Order(BulkOrder {
            orders,
            grouping: grouping.to_string(),
        });
        self.post_l1_action(&action).await
    }

    /// Cancel one or more resting orders by (asset, oid).
    pub async fn cancel_orders(
        &self,
        cancels: Vec<CancelRequest>,
    ) -> Result<serde_json::Value, Error> {
        let action = Actions::Cancel(BulkCancel { cancels });
        self.post_l1_action(&action).await
    }

    /// Arm or clear Hyperliquid's dead-man switch. When `time` is set, HL
    /// cancels the account's open orders once that deadline is reached.
    pub async fn schedule_cancel(&self, time: Option<u64>) -> Result<serde_json::Value, Error> {
        let action = Actions::ScheduleCancel(ScheduleCancel { time });
        self.post_l1_action(&action).await
    }

    /// USDC withdrawal to Arbitrum. `amount` is a plain decimal string
    /// ("1.23"); `destination` is a 0x-prefixed EVM address.
    pub async fn withdraw_to_bridge(
        &self,
        destination: String,
        amount: String,
        time_ms: u64,
    ) -> Result<serde_json::Value, Error> {
        let payload = Withdraw3 {
            signature_chain_id: self.network.signature_chain_id(),
            hyperliquid_chain: self.network.hyperliquid_chain().to_string(),
            destination,
            amount,
            time: time_ms,
        };
        let signature = sign_typed_data(&payload, &self.wallet)?;
        self.post_user_action(&payload, "withdraw3", signature, time_ms)
            .await
    }

    /// Spot-token transfer on HL Core. Used as the on-HL leg of a HyperUnit
    /// withdrawal: signs the transfer to the Unit bridge address, which
    /// guardians observe and settle on the destination chain. `token` is the
    /// HL spot wire form (e.g., `"UBTC:0x..."`); `destination` is a
    /// 0x-prefixed address; `amount` is a decimal string.
    pub async fn spot_send(
        &self,
        destination: String,
        token: String,
        amount: String,
        time_ms: u64,
    ) -> Result<serde_json::Value, Error> {
        let payload = SpotSend {
            signature_chain_id: self.network.signature_chain_id(),
            hyperliquid_chain: self.network.hyperliquid_chain().to_string(),
            destination,
            token,
            amount,
            time: time_ms,
        };
        let signature = sign_typed_data(&payload, &self.wallet)?;
        self.post_user_action(&payload, "spotSend", signature, time_ms)
            .await
    }

    /// Move USDC between Hyperliquid's perp clearinghouse and spot wallet.
    /// `to_perp=false` moves withdrawable margin into spot; `to_perp=true`
    /// moves spot USDC back into the perp clearinghouse.
    pub async fn usd_class_transfer(
        &self,
        amount: String,
        to_perp: bool,
        time_ms: u64,
    ) -> Result<serde_json::Value, Error> {
        let wire_amount = match self.vault_address {
            Some(vault_address) => format!("{amount} subaccount:{vault_address:#x}"),
            None => amount,
        };
        let payload = UsdClassTransfer {
            signature_chain_id: self.network.signature_chain_id(),
            hyperliquid_chain: self.network.hyperliquid_chain().to_string(),
            amount: wire_amount.clone(),
            to_perp,
            nonce: time_ms,
        };
        let signature = sign_typed_data(&payload, &self.wallet)?;
        let envelope = ExchangePayload {
            action: serde_json::json!({
                "type": "usdClassTransfer",
                "amount": wire_amount,
                "toPerp": to_perp,
                "nonce": time_ms,
                "signatureChainId": format!("0x{:x}", self.network.signature_chain_id()),
                "hyperliquidChain": self.network.hyperliquid_chain(),
            }),
            signature,
            nonce: time_ms,
            vault_address: None,
        };
        self.post_exchange(&envelope).await
    }

    /// Post an EIP-712 user action. `/exchange` expects the action JSON to
    /// carry a top-level `type` discriminator, which user-action payloads
    /// don't serialize on their own, so it's injected here.
    async fn post_user_action<T: Serialize>(
        &self,
        payload: &T,
        action_type: &'static str,
        signature: Signature,
        time_ms: u64,
    ) -> Result<serde_json::Value, Error> {
        let mut action = serde_json::to_value(payload).map_err(|source| Error::Json { source })?;
        if let Some(map) = action.as_object_mut() {
            map.insert(
                "type".to_string(),
                serde_json::Value::String(action_type.to_string()),
            );
        }
        let envelope = ExchangePayload {
            action,
            signature,
            nonce: time_ms,
            vault_address: self.vault_address,
        };
        self.post_exchange(&envelope).await
    }

    async fn post_l1_action(&self, action: &Actions) -> Result<serde_json::Value, Error> {
        let timestamp = nonce_ms();
        let connection_id = action.hash(timestamp, self.vault_address)?;
        let signature = sign_l1_action(&self.wallet, connection_id, self.network.is_mainnet())?;
        let envelope = ExchangePayload {
            action: serde_json::to_value(action).map_err(|source| Error::Json { source })?,
            signature,
            nonce: timestamp,
            vault_address: self.vault_address,
        };
        self.post_exchange(&envelope).await
    }

    async fn post_exchange(&self, payload: &ExchangePayload) -> Result<serde_json::Value, Error> {
        let body = serde_json::to_string(payload).map_err(|source| Error::Json { source })?;
        let text = self.http.post_raw("/exchange", &body).await?;
        serde_json::from_str(&text).map_err(|source| Error::Json { source })
    }
}

/// Small helper for Hyperliquid Bridge2 deposit construction. No wallet is
/// stored; callers pass or derive the source address separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HyperliquidBridgeClient {
    from: Address,
    network: Network,
}

impl HyperliquidBridgeClient {
    #[must_use]
    pub fn new(from: Address, network: Network) -> Self {
        Self { from, network }
    }

    #[must_use]
    pub fn from(&self) -> Address {
        self.from
    }

    #[must_use]
    pub fn network(&self) -> Network {
        self.network
    }

    #[must_use]
    pub fn bridge_address(&self) -> Address {
        bridge::bridge_address(self.network)
    }

    pub fn build_usdc_bridge_deposit(
        &self,
        usdc_token: Address,
        amount: alloy::primitives::U256,
    ) -> Result<BridgeDepositTx, Error> {
        bridge::build_bridge_deposit_tx(self.network, self.from, usdc_token, amount)
    }

    pub fn build_usdc_bridge_deposit_to(
        &self,
        bridge_address: Address,
        usdc_token: Address,
        amount: alloy::primitives::U256,
    ) -> Result<BridgeDepositTx, Error> {
        bridge::build_bridge_deposit_tx_to(self.from, usdc_token, bridge_address, amount)
    }
}

/// Backward-compatible wrapper that composes the split clients.
#[derive(Debug)]
pub struct HyperliquidClient {
    info: HyperliquidInfoClient,
    exchange: HyperliquidExchangeClient,
    bridge: HyperliquidBridgeClient,
}

impl HyperliquidClient {
    /// Build a compatibility client without pre-fetched metadata. Asset
    /// lookups will fail until [`Self::refresh_spot_meta`] has been called.
    pub fn new(
        base_url: &str,
        wallet: PrivateKeySigner,
        vault_address: Option<Address>,
        network: Network,
    ) -> Result<Self, Error> {
        let from = wallet.address();
        Ok(Self {
            info: HyperliquidInfoClient::new(base_url)?,
            exchange: HyperliquidExchangeClient::new(base_url, wallet, vault_address, network)?,
            bridge: HyperliquidBridgeClient::new(from, network),
        })
    }

    #[must_use]
    pub fn info(&self) -> &HyperliquidInfoClient {
        &self.info
    }

    #[must_use]
    pub fn info_mut(&mut self) -> &mut HyperliquidInfoClient {
        &mut self.info
    }

    #[must_use]
    pub fn exchange(&self) -> &HyperliquidExchangeClient {
        &self.exchange
    }

    #[must_use]
    pub fn bridge(&self) -> &HyperliquidBridgeClient {
        &self.bridge
    }

    #[must_use]
    pub fn wallet(&self) -> &PrivateKeySigner {
        self.exchange.wallet()
    }

    #[must_use]
    pub fn http(&self) -> &HttpClient {
        self.exchange.http()
    }

    #[must_use]
    pub fn network(&self) -> Network {
        self.exchange.network()
    }

    #[must_use]
    pub fn bridge_address(&self) -> Address {
        self.bridge.bridge_address()
    }

    pub fn build_usdc_bridge_deposit(
        &self,
        usdc_token: Address,
        amount: alloy::primitives::U256,
    ) -> Result<BridgeDepositTx, Error> {
        self.bridge.build_usdc_bridge_deposit(usdc_token, amount)
    }

    pub fn build_usdc_bridge_deposit_to(
        &self,
        bridge_address: Address,
        usdc_token: Address,
        amount: alloy::primitives::U256,
    ) -> Result<BridgeDepositTx, Error> {
        self.bridge
            .build_usdc_bridge_deposit_to(bridge_address, usdc_token, amount)
    }

    #[must_use]
    pub fn vault_address(&self) -> Option<Address> {
        self.exchange.vault_address()
    }

    pub async fn fetch_spot_meta(&self) -> Result<SpotMeta, Error> {
        self.info.fetch_spot_meta().await
    }

    pub async fn refresh_spot_meta(&mut self) -> Result<SpotMeta, Error> {
        self.info.refresh_spot_meta().await
    }

    pub fn asset_index(&self, coin: &str) -> Result<u32, Error> {
        self.info.asset_index(coin)
    }

    pub async fn l2_book(&self, coin: &str) -> Result<L2BookSnapshot, Error> {
        self.info.l2_book(coin).await
    }

    pub async fn order_status(
        &self,
        user: Address,
        oid: u64,
    ) -> Result<OrderStatusResponse, Error> {
        self.info.order_status(user, oid).await
    }

    pub async fn spot_clearinghouse_state(
        &self,
        user: Address,
    ) -> Result<SpotClearinghouseState, Error> {
        self.info.spot_clearinghouse_state(user).await
    }

    pub async fn clearinghouse_state(&self, user: Address) -> Result<ClearinghouseState, Error> {
        self.info.clearinghouse_state(user).await
    }

    pub async fn open_orders(&self, user: Address) -> Result<Vec<OpenOrder>, Error> {
        self.info.open_orders(user).await
    }

    pub async fn user_fills(&self, user: Address) -> Result<Vec<UserFill>, Error> {
        self.info.user_fills(user).await
    }

    pub async fn place_orders(
        &self,
        orders: Vec<OrderRequest>,
        grouping: &str,
    ) -> Result<serde_json::Value, Error> {
        self.exchange.place_orders(orders, grouping).await
    }

    pub async fn cancel_orders(
        &self,
        cancels: Vec<CancelRequest>,
    ) -> Result<serde_json::Value, Error> {
        self.exchange.cancel_orders(cancels).await
    }

    pub async fn schedule_cancel(&self, time: Option<u64>) -> Result<serde_json::Value, Error> {
        self.exchange.schedule_cancel(time).await
    }

    pub async fn withdraw_to_bridge(
        &self,
        destination: String,
        amount: String,
        time_ms: u64,
    ) -> Result<serde_json::Value, Error> {
        self.exchange
            .withdraw_to_bridge(destination, amount, time_ms)
            .await
    }

    pub async fn spot_send(
        &self,
        destination: String,
        token: String,
        amount: String,
        time_ms: u64,
    ) -> Result<serde_json::Value, Error> {
        self.exchange
            .spot_send(destination, token, amount, time_ms)
            .await
    }

    pub async fn usd_class_transfer(
        &self,
        amount: String,
        to_perp: bool,
        time_ms: u64,
    ) -> Result<serde_json::Value, Error> {
        self.exchange
            .usd_class_transfer(amount, to_perp, time_ms)
            .await
    }
}

/// Wire envelope HL's `/exchange` expects: action body + `{r,s,v}` sig +
/// nonce + optional vault address. `v` is serialized as `27 + parity` to
/// match HL's Ethereum-style convention.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExchangePayload {
    action: serde_json::Value,
    #[serde(serialize_with = "serialize_signature")]
    signature: Signature,
    nonce: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    vault_address: Option<Address>,
}

fn serialize_signature<S>(sig: &Signature, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut state = s.serialize_struct("Signature", 3)?;
    state.serialize_field("r", &sig.r())?;
    state.serialize_field("s", &sig.s())?;
    state.serialize_field("v", &(27_u64 + u64::from(sig.v())))?;
    state.end()
}

fn build_http(base_url: &str) -> Result<HttpClient, Error> {
    let parsed = Url::parse(base_url).map_err(|source| Error::InvalidBaseUrl { source })?;
    let normalized = parsed.as_str().trim_end_matches('/').to_string();
    let client = Client::builder()
        .use_rustls_tls()
        .build()
        .map_err(|source| Error::HttpRequest { source })?;
    Ok(HttpClient::new(client, normalized))
}

fn nonce_ms() -> u64 {
    utc::now().timestamp_millis().max(0) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_mapping_matches_hl_conventions() {
        assert!(Network::Mainnet.is_mainnet());
        assert_eq!(Network::Mainnet.hyperliquid_chain(), "Mainnet");
        assert_eq!(Network::Mainnet.signature_chain_id(), 42_161);
        assert!(!Network::Testnet.is_mainnet());
        assert_eq!(Network::Testnet.hyperliquid_chain(), "Testnet");
        assert_eq!(Network::Testnet.signature_chain_id(), 421_614);
    }

    #[test]
    fn info_construction_rejects_bad_urls() {
        let err = HyperliquidInfoClient::new("not a url").unwrap_err();
        assert!(matches!(err, Error::InvalidBaseUrl { .. }));
    }

    #[test]
    fn exchange_construction_rejects_bad_urls() {
        let wallet = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e"
            .parse::<PrivateKeySigner>()
            .unwrap();
        let err = HyperliquidExchangeClient::new("not a url", wallet, None, Network::Testnet)
            .unwrap_err();
        assert!(matches!(err, Error::InvalidBaseUrl { .. }));
    }

    #[test]
    fn construction_trims_trailing_slashes() {
        let wallet = "e908f86dbb4d55ac876378565aafeabc187f6690f046459397b17d9b9a19688e"
            .parse::<PrivateKeySigner>()
            .unwrap();
        let client =
            HyperliquidClient::new("https://example.test/", wallet, None, Network::Testnet)
                .unwrap();
        assert_eq!(client.http().base_url(), "https://example.test");
        assert_eq!(client.info().http().base_url(), "https://example.test");
    }

    #[test]
    fn bridge_client_exposes_bridge_address_for_network() {
        let from = Address::repeat_byte(0x11);
        let client = HyperliquidBridgeClient::new(from, Network::Testnet);
        assert_eq!(client.from(), from);
        assert_eq!(client.bridge_address(), bridge::TESTNET_BRIDGE2_ADDRESS);
    }
}

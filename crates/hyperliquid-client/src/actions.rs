//! Action payloads posted to Hyperliquid's `/exchange` endpoint.
//!
//! Two categories, with different signing pipelines:
//!   * L1 actions (`Order`, `Cancel`, ...) — wrapped in [`Actions`] and
//!     hashed via [`Actions::hash`], then passed to
//!     [`crate::sign_l1_action`].
//!   * User actions (`Withdraw3`, ...) — standalone structs implementing
//!     [`crate::Eip712`], signed directly via [`crate::sign_typed_data`].

use alloy::{
    dyn_abi::Eip712Domain,
    primitives::{keccak256, Address, B256},
    sol_types::{eip712_domain, SolValue},
};
use serde::{Deserialize, Serialize};

use crate::{eip712::Eip712, error::Error};

/// Time-in-force. Hyperliquid accepts `Gtc` (good-till-cancel), `Ioc`
/// (immediate-or-cancel), `Alo` (add-liquidity-only).
#[derive(Debug, Clone, Copy)]
pub enum Tif {
    Gtc,
    Ioc,
    Alo,
}

impl Tif {
    #[must_use]
    pub fn as_wire(self) -> &'static str {
        match self {
            Tif::Gtc => "Gtc",
            Tif::Ioc => "Ioc",
            Tif::Alo => "Alo",
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
pub struct Limit {
    pub tif: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum Order {
    Limit(Limit),
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct OrderRequest {
    #[serde(rename = "a", alias = "asset")]
    pub asset: u32,
    #[serde(rename = "b", alias = "isBuy")]
    pub is_buy: bool,
    #[serde(rename = "p", alias = "limitPx")]
    pub limit_px: String,
    #[serde(rename = "s", alias = "sz")]
    pub sz: String,
    #[serde(rename = "r", alias = "reduceOnly", default)]
    pub reduce_only: bool,
    #[serde(rename = "t", alias = "orderType")]
    pub order_type: Order,
    #[serde(rename = "c", alias = "cloid", skip_serializing_if = "Option::is_none")]
    pub cloid: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BulkOrder {
    pub orders: Vec<OrderRequest>,
    pub grouping: String,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CancelRequest {
    #[serde(rename = "a")]
    pub asset: u32,
    #[serde(rename = "o")]
    pub oid: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct BulkCancel {
    pub cancels: Vec<CancelRequest>,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleCancel {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<u64>,
}

/// Bridge-withdrawal action (USDC -> Arbitrum). Signed as EIP-712 typed data,
/// not as an L1 action.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Withdraw3 {
    #[serde(
        serialize_with = "serialize_chain_id_hex",
        deserialize_with = "deserialize_chain_id"
    )]
    pub signature_chain_id: u64,
    pub hyperliquid_chain: String,
    pub destination: String,
    pub amount: String,
    pub time: u64,
}

impl Eip712 for Withdraw3 {
    fn domain(&self) -> Eip712Domain {
        eip712_domain! {
            name: "HyperliquidSignTransaction",
            version: "1",
            chain_id: self.signature_chain_id,
            verifying_contract: Address::ZERO,
        }
    }

    fn struct_hash(&self) -> B256 {
        let items = (
            keccak256(
                "HyperliquidTransaction:Withdraw(string hyperliquidChain,string destination,string amount,uint64 time)",
            ),
            keccak256(&self.hyperliquid_chain),
            keccak256(&self.destination),
            keccak256(&self.amount),
            self.time,
        );
        keccak256(items.abi_encode())
    }
}

/// Spot-token transfer on Hyperliquid Core. Signed as EIP-712 typed data.
/// The HyperUnit bridge observes these on-HL transfers and releases the
/// native asset on the destination chain.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SpotSend {
    #[serde(
        serialize_with = "serialize_chain_id_hex",
        deserialize_with = "deserialize_chain_id"
    )]
    pub signature_chain_id: u64,
    pub hyperliquid_chain: String,
    pub destination: String,
    pub token: String,
    pub amount: String,
    pub time: u64,
}

impl Eip712 for SpotSend {
    fn domain(&self) -> Eip712Domain {
        eip712_domain! {
            name: "HyperliquidSignTransaction",
            version: "1",
            chain_id: self.signature_chain_id,
            verifying_contract: Address::ZERO,
        }
    }

    fn struct_hash(&self) -> B256 {
        let items = (
            keccak256(
                "HyperliquidTransaction:SpotSend(string hyperliquidChain,string destination,string token,string amount,uint64 time)",
            ),
            keccak256(&self.hyperliquid_chain),
            keccak256(&self.destination),
            keccak256(&self.token),
            keccak256(&self.amount),
            self.time,
        );
        keccak256(items.abi_encode())
    }
}

/// Transfer USDC between the perp clearinghouse and spot wallet. Bridge
/// deposits land in the perp clearinghouse first; spot trading requires
/// moving funds into the spot class when the account is not unified.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UsdClassTransfer {
    #[serde(
        serialize_with = "serialize_chain_id_hex",
        deserialize_with = "deserialize_chain_id"
    )]
    pub signature_chain_id: u64,
    pub hyperliquid_chain: String,
    pub amount: String,
    pub to_perp: bool,
    pub nonce: u64,
}

impl Eip712 for UsdClassTransfer {
    fn domain(&self) -> Eip712Domain {
        eip712_domain! {
            name: "HyperliquidSignTransaction",
            version: "1",
            chain_id: self.signature_chain_id,
            verifying_contract: Address::ZERO,
        }
    }

    fn struct_hash(&self) -> B256 {
        let items = (
            keccak256(
                "HyperliquidTransaction:UsdClassTransfer(string hyperliquidChain,string amount,bool toPerp,uint64 nonce)",
            ),
            keccak256(&self.hyperliquid_chain),
            keccak256(&self.amount),
            self.to_perp,
            self.nonce,
        );
        keccak256(items.abi_encode())
    }
}

fn serialize_chain_id_hex<S>(value: &u64, s: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    s.serialize_str(&format!("0x{value:x}"))
}

/// Accept both plain u64 and 0x-prefixed hex string forms of
/// `signature_chain_id`. The wire canonical form is hex (matches HL) but
/// round-tripping in tests through the Rust struct should still parse.
fn deserialize_chain_id<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error as _;
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Either {
        Number(u64),
        Str(String),
    }
    match Either::deserialize(deserializer)? {
        Either::Number(n) => Ok(n),
        Either::Str(s) => {
            let trimmed = s.trim_start_matches("0x").trim_start_matches("0X");
            u64::from_str_radix(trimmed, 16)
                .map_err(|e| D::Error::custom(format!("invalid chain_id hex {s:?}: {e}")))
        }
    }
}

/// L1 action envelope — the `type` discriminator is what HL switches on.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum Actions {
    Order(BulkOrder),
    Cancel(BulkCancel),
    ScheduleCancel(ScheduleCancel),
}

impl Actions {
    /// Canonical L1 connection hash: msgpack(action) || timestamp (u64 BE) ||
    /// vault_address_marker (0 or 1 + 20 bytes) -> keccak256.
    ///
    /// Matches the SDK's `Actions::hash` byte-for-byte. Deviations here
    /// invalidate signatures, so keep this stable.
    pub fn hash(&self, timestamp: u64, vault_address: Option<Address>) -> Result<B256, Error> {
        let mut bytes = rmp_serde::to_vec_named(self).map_err(|e| Error::Msgpack {
            message: e.to_string(),
        })?;
        bytes.extend(timestamp.to_be_bytes());
        if let Some(addr) = vault_address {
            bytes.push(1);
            bytes.extend(addr.as_slice());
        } else {
            bytes.push(0);
        }
        Ok(keccak256(bytes))
    }
}

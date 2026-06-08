//! `hyperliquid_core.rs` — a devnet "chain" component modelling Hyperliquid
//! account state, peer to [`bitcoin_devnet`](crate::bitcoin_devnet) and
//! [`evm_devnet`](crate::evm_devnet).
//!
//! [`HyperliquidCore`] owns the two USDC balance pools (spot + clearinghouse),
//! the spot order book, fundings, and meta behind a single mutex. The three
//! venues that settle on Hyperliquid — the spot/exchange `/info`+`/exchange`
//! handlers, the Bridge2 deposit indexer, and HyperUnit deposit/withdrawal
//! completion — all go THROUGH this component's deliberate `pub(crate)`
//! interface rather than reaching into each other's state.
//!
//! The state is shared by `Arc<HyperliquidCore>`: a single instance is
//! constructed once and handed to every settlement path. `HyperliquidCore`
//! wraps the state in a `tokio::sync::Mutex`, exposing
//! [`HyperliquidCore::lock`] so callers can perform a sequence of operations
//! atomically under one lock hold — exactly the locking discipline the venue
//! handlers used before this extraction.

use alloy::primitives::Address;
use chrono::Utc;
use hyperliquid_client::{
    spot_wire_asset_index, PerpAssetMeta, PerpMeta, SpotAssetMeta, SpotMeta, TokenInfo, UserFill,
    UserFunding, UserNonFundingLedgerUpdate, UserRateLimit, SPOT_ASSET_INDEX_OFFSET,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use tokio::sync::{Mutex, MutexGuard};

/// The Hyperliquid devnet "chain": the account state behind a single mutex,
/// shared across every settlement path by `Arc<HyperliquidCore>`.
#[derive(Default)]
pub(crate) struct HyperliquidCore {
    state: Mutex<HyperliquidCoreState>,
}

impl HyperliquidCore {
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(HyperliquidCoreState::new()),
        }
    }

    /// Acquire the chain state for a sequence of operations under one lock
    /// hold. This is the entry point every venue uses to settle on
    /// Hyperliquid; the returned guard exposes the deliberate `pub(crate)`
    /// interface on [`HyperliquidCoreState`].
    pub(crate) async fn lock(&self) -> MutexGuard<'_, HyperliquidCoreState> {
        self.state.lock().await
    }
}

/// Mock's internal Hyperliquid state: per-user spot balances, per-user
/// clearinghouse balances, a hardcoded exchange-rate table per pair, and
/// historical / resting orders.
///
/// The mock still uses a synthetic external book at the configured rate, but
/// it now tracks production-relevant order lifecycle semantics:
/// - IoC orders fill immediately when marketable.
/// - Non-marketable Gtc / Alo orders rest on the book.
/// - Resting orders reserve `hold` balances and appear in `openOrders`.
/// - Cancels remove resting orders and move them into terminal history.
///
/// That is enough for the router and client integration tests to exercise the
/// same shapes production uses without implementing a full matching engine.
#[derive(Default)]
pub(crate) struct HyperliquidCoreState {
    next_oid: u64,
    next_tid: u64,
    /// `user -> coin -> total spot balance (natural units, e.g. 1.0 UBTC)`.
    spot_balances: BTreeMap<Address, BTreeMap<String, f64>>,
    /// `user -> coin -> clearinghouse balance (currently only USDC matters
    /// for the mock). Bridge deposits land here as withdrawable collateral.
    clearinghouse_balances: BTreeMap<Address, BTreeMap<String, f64>>,
    /// `(base, quote) -> rate`. Rate semantics: `1 base = rate * quote`. A
    /// buy of `sz` base costs `sz * rate` quote; a sell of `sz` base yields
    /// `sz * rate` quote. Tests install pairs via
    /// [`MockIntegratorServer::set_hyperliquid_rate`].
    rates: BTreeMap<(String, String), f64>,
    /// `(base, quote) -> top-of-book depth` advertised on the synthesized
    /// bid/ask levels. Immediate marketable orders can only consume this much
    /// liquidity before either resting (Gtc) or dropping the remainder (Ioc).
    book_depths: BTreeMap<(String, String), HyperliquidBookDepth>,
    /// Orders that have left the open-order set and reached a terminal state.
    /// Queryable via `/info { type: "orderStatus" }`.
    terminal_orders: BTreeMap<u64, TerminalOrder>,
    /// `user -> fills`, newest first (real API ordering).
    fills: BTreeMap<Address, Vec<HyperliquidFillRecord>>,
    /// `user -> externally seeded fills`, used by backfill endpoints that
    /// tests need to control independently of the synthetic exchange engine.
    recorded_fills: BTreeMap<Address, Vec<UserFill>>,
    /// `user -> non-funding ledger updates`, newest first.
    pub(crate) ledger_updates: BTreeMap<Address, Vec<UserNonFundingLedgerUpdate>>,
    /// `user -> funding payments`, newest first.
    fundings: BTreeMap<Address, Vec<UserFunding>>,
    /// Currently resting open orders, keyed by oid.
    open_orders: BTreeMap<u64, HyperliquidSubmittedOrder>,
    /// Per-user dead-man switch deadline, in unix milliseconds. Once the
    /// scheduled time is reached the mock cancels all currently open orders
    /// for that user and clears the schedule.
    scheduled_cancels: BTreeMap<Address, u64>,
    /// Spot meta served from `/info { type: "spotMeta" }` and also used to
    /// resolve asset ids → pair names for rate lookup.
    spot_meta: Option<SpotMeta>,
    /// Perp meta served from `/info { type: "meta" }`.
    perp_meta: PerpMeta,
}

#[derive(Debug, Clone, Copy)]
struct HyperliquidBookDepth {
    bid: f64,
    ask: f64,
}

#[derive(Debug, Clone)]
struct HyperliquidSubmittedOrder {
    oid: u64,
    user: Address,
    asset: u32,
    coin: String,
    is_buy: bool,
    limit_px: f64,
    sz: f64,
    orig_sz: f64,
    tif: String,
    cloid: Option<String>,
    timestamp: u64,
}

#[derive(Debug, Clone)]
struct TerminalOrder {
    order: HyperliquidSubmittedOrder,
    /// Final lifecycle status string returned by `/info { type: "orderStatus" }`
    /// — one of `"filled"`, `"rejected"`, `"canceled"`, `"scheduledCancel"`.
    status: String,
    status_timestamp: u64,
}

#[derive(Debug, Clone)]
struct HyperliquidFillRecord {
    oid: u64,
    tid: u64,
    coin: String,
    is_buy: bool,
    px: f64,
    sz: f64,
    time: u64,
    fee: f64,
    fee_token: String,
}

/// Size difference below this threshold is treated as "zero remaining" —
/// guards against f64 drift when partial fills consume a level exactly.
const HYPERLIQUID_AMOUNT_EPSILON: f64 = 1e-6;
const HYPERLIQUID_RELATIVE_AMOUNT_EPSILON: f64 = 1e-12;
const HYPERLIQUID_PRICE_TOLERANCE_BPS: f64 = 1.0;

fn hyperliquid_amount_epsilon(amount: f64) -> f64 {
    HYPERLIQUID_AMOUNT_EPSILON.max(amount.abs() * HYPERLIQUID_RELATIVE_AMOUNT_EPSILON)
}

pub(crate) fn hyperliquid_has_sufficient_amount(available: f64, required: f64) -> bool {
    available + hyperliquid_amount_epsilon(required) >= required
}

fn hyperliquid_is_effectively_zero(amount: f64) -> bool {
    amount <= hyperliquid_amount_epsilon(amount)
}

fn hyperliquid_price_epsilon(price: f64) -> f64 {
    HYPERLIQUID_AMOUNT_EPSILON.max(price.abs() * HYPERLIQUID_PRICE_TOLERANCE_BPS / 10_000.0)
}

fn hyperliquid_order_crosses(is_buy: bool, limit_px: f64, rate: f64) -> bool {
    let tolerance = hyperliquid_price_epsilon(rate);
    if is_buy {
        limit_px + tolerance >= rate
    } else {
        limit_px <= rate + tolerance
    }
}

fn hyperliquid_execution_px(is_buy: bool, limit_px: f64, rate: f64) -> f64 {
    if is_buy {
        limit_px.min(rate)
    } else {
        limit_px.max(rate)
    }
}

const HYPERLIQUID_INFO_RESULT_LIMIT: usize = 2_000;

pub(crate) fn format_hl_amount(value: f64) -> String {
    // Canonical HL wire form trims trailing zeros from an 8-decimal string
    // ("1.5" not "1.50000000"). `float_to_wire` does exactly this.
    hyperliquid_client::float_to_wire(value)
}

fn format_hl_token_amount(value: f64, decimals: u8) -> String {
    if decimals <= hyperliquid_client::wire::WIRE_DECIMALS {
        return format_hl_amount(value);
    }
    let scale = 10_f64.powi(i32::from(decimals));
    let tolerance = (value.abs() * 1e-15).max(32.0 / scale);
    for precision in 0..=usize::from(hyperliquid_client::wire::WIRE_DECIMALS) {
        let precision_scale = 10_f64.powi(i32::try_from(precision).unwrap_or_default());
        let rounded = (value * precision_scale).round() / precision_scale;
        if (rounded - value).abs() <= tolerance {
            return trim_decimal_string(&format!("{rounded:.precision$}"));
        }
    }

    let scaled = value * scale;
    let rounded = scaled.round();
    let raw_tolerance = (scaled.abs() * f64::EPSILON * 8.0).max(1e-6);
    let raw = if (scaled - rounded).abs() <= raw_tolerance {
        rounded
    } else {
        scaled.ceil()
    }
    .max(0.0) as u128;
    format_raw_decimal(raw, decimals)
}

fn hyperliquid_token_amount_is_visible(value: f64, decimals: u8) -> bool {
    if value <= 0.0 {
        return false;
    }
    if decimals <= hyperliquid_client::wire::WIRE_DECIMALS {
        return !hyperliquid_is_effectively_zero(value);
    }
    let raw_units = value * 10_f64.powi(i32::from(decimals));
    raw_units >= 0.5
}

fn format_raw_decimal(raw: u128, decimals: u8) -> String {
    let decimals = usize::from(decimals);
    if decimals == 0 {
        return raw.to_string();
    }
    let digits = raw.to_string();
    let padded = if digits.len() <= decimals {
        format!("{:0>width$}", digits, width = decimals + 1)
    } else {
        digits
    };
    let split_at = padded.len() - decimals;
    let (whole, frac) = padded.split_at(split_at);
    trim_decimal_string(&format!("{whole}.{frac}"))
}

fn trim_decimal_string(value: &str) -> String {
    if value.contains('.') {
        let trimmed = value.trim_end_matches('0').trim_end_matches('.');
        if trimmed.is_empty() {
            "0".to_string()
        } else {
            trimmed.to_string()
        }
    } else {
        value.to_string()
    }
}

/// Default spot meta served by the mock's `/info { type: "spotMeta" }`. Same
/// token universe the live devnet uses: USDC + UBTC + UETH, with the two
/// canonical pairs at indices 140 / 141.
fn default_hyperliquid_spot_meta() -> SpotMeta {
    SpotMeta {
        tokens: vec![
            TokenInfo {
                name: "USDC".to_string(),
                sz_decimals: 8,
                wei_decimals: 8,
                index: 0,
                token_id: Some("0x6d1e7cde53ba9467b783cb7c530ce054".to_string()),
                is_canonical: true,
            },
            TokenInfo {
                name: "UBTC".to_string(),
                sz_decimals: 5,
                wei_decimals: 8,
                index: 1,
                token_id: Some("0x11111111111111111111111111111111".to_string()),
                is_canonical: true,
            },
            TokenInfo {
                name: "UETH".to_string(),
                sz_decimals: 4,
                wei_decimals: 18,
                index: 2,
                token_id: Some("0x22222222222222222222222222222222".to_string()),
                is_canonical: true,
            },
        ],
        universe: vec![
            SpotAssetMeta {
                tokens: [1, 0],
                name: "@140".to_string(),
                index: 140,
                is_canonical: true,
            },
            SpotAssetMeta {
                tokens: [2, 0],
                name: "@141".to_string(),
                index: 141,
                is_canonical: true,
            },
        ],
    }
}

fn default_hyperliquid_perp_meta() -> PerpMeta {
    PerpMeta {
        universe: vec![
            PerpAssetMeta {
                name: "BTC".to_string(),
                sz_decimals: 5,
                max_leverage: 50,
                only_isolated: None,
            },
            PerpAssetMeta {
                name: "ETH".to_string(),
                sz_decimals: 4,
                max_leverage: 50,
                only_isolated: None,
            },
            PerpAssetMeta {
                name: "HYPE".to_string(),
                sz_decimals: 2,
                max_leverage: 3,
                only_isolated: None,
            },
        ],
    }
}

struct AssetResolution {
    pair_name: String,
    base_symbol: String,
    quote_symbol: String,
}

/// Default rates seeded in `HyperliquidCoreState::new`. Picks for devnet so
/// the "typical" UBTC / UETH round-trip works out of the box; tests needing
/// a specific fill price install their own rate via `set_hyperliquid_rate`.
const DEFAULT_UBTC_USDC_RATE: f64 = 60_000.0;
const DEFAULT_UETH_USDC_RATE: f64 = 3_000.0;

/// Default size advertised on each synthesized book level. Tests that care
/// about partial fills can override this per pair.
const SYNTHESIZED_BOOK_DEPTH: f64 = 1_000_000.0;

impl HyperliquidCoreState {
    pub(crate) fn new() -> Self {
        let mut state = Self {
            next_oid: 1000,
            next_tid: 1,
            spot_meta: Some(default_hyperliquid_spot_meta()),
            perp_meta: default_hyperliquid_perp_meta(),
            ..Default::default()
        };
        state.set_rate("UBTC", "USDC", DEFAULT_UBTC_USDC_RATE);
        state.set_rate("UETH", "USDC", DEFAULT_UETH_USDC_RATE);
        state
    }

    pub(crate) fn spot_meta_value(&self) -> Result<Value, String> {
        let Some(spot_meta) = self.spot_meta.as_ref() else {
            return Err("mock Hyperliquid spot meta is not initialized".to_string());
        };
        serde_json::to_value(spot_meta)
            .map_err(|error| format!("mock Hyperliquid spot meta failed to serialize: {error}"))
    }

    pub(crate) fn perp_meta(&self) -> PerpMeta {
        self.perp_meta.clone()
    }

    pub(crate) fn record_fill(&mut self, user: Address, fill: UserFill) {
        let fills = self.recorded_fills.entry(user).or_default();
        fills.push(fill);
        fills.sort_by(|left, right| right.time.cmp(&left.time));
    }

    pub(crate) fn record_ledger_update(
        &mut self,
        user: Address,
        update: UserNonFundingLedgerUpdate,
    ) {
        let updates = self.ledger_updates.entry(user).or_default();
        updates.push(update);
        updates.sort_by(|left, right| right.time.cmp(&left.time));
    }

    pub(crate) fn record_funding(&mut self, user: Address, funding: UserFunding) {
        let fundings = self.fundings.entry(user).or_default();
        fundings.push(funding);
        fundings.sort_by(|left, right| right.time.cmp(&left.time));
    }

    fn allocate_oid(&mut self) -> u64 {
        let oid = self.next_oid;
        self.next_oid += 1;
        oid
    }

    fn allocate_tid(&mut self) -> u64 {
        let tid = self.next_tid;
        self.next_tid += 1;
        tid
    }

    fn apply_spot_fill(
        &mut self,
        user: Address,
        debit_coin: &str,
        debit_amount: f64,
        credit_coin: &str,
        credit_amount: f64,
    ) {
        self.debit_spot_total(user, debit_coin, debit_amount);
        self.credit_spot(user, credit_coin, credit_amount);
    }

    fn reject_order(&mut self, order: HyperliquidSubmittedOrder, timestamp: u64) {
        self.terminal_orders.insert(
            order.oid,
            TerminalOrder {
                order,
                status: "rejected".to_string(),
                status_timestamp: timestamp,
            },
        );
    }

    pub(crate) fn credit_spot(&mut self, user: Address, coin: &str, amount: f64) {
        *self
            .spot_balances
            .entry(user)
            .or_default()
            .entry(coin.to_string())
            .or_insert(0.0) += amount;
    }

    pub(crate) fn credit_clearinghouse(&mut self, user: Address, coin: &str, amount: f64) {
        *self
            .clearinghouse_balances
            .entry(user)
            .or_default()
            .entry(coin.to_string())
            .or_insert(0.0) += amount;
    }

    pub(crate) fn debit_spot_total(&mut self, user: Address, coin: &str, amount: f64) {
        let entry = self
            .spot_balances
            .entry(user)
            .or_default()
            .entry(coin.to_string())
            .or_insert(0.0);
        *entry -= amount;
        if hyperliquid_is_effectively_zero(*entry) {
            *entry = 0.0;
        }
    }

    pub(crate) fn debit_clearinghouse_total(&mut self, user: Address, coin: &str, amount: f64) {
        let entry = self
            .clearinghouse_balances
            .entry(user)
            .or_default()
            .entry(coin.to_string())
            .or_insert(0.0);
        *entry -= amount;
        if hyperliquid_is_effectively_zero(*entry) {
            *entry = 0.0;
        }
    }

    pub(crate) fn spot_total(&self, user: Address, coin: &str) -> f64 {
        self.spot_balances
            .get(&user)
            .and_then(|m| m.get(coin))
            .copied()
            .unwrap_or(0.0)
    }

    pub(crate) fn clearinghouse_total(&self, user: Address, coin: &str) -> f64 {
        self.clearinghouse_balances
            .get(&user)
            .and_then(|m| m.get(coin))
            .copied()
            .unwrap_or(0.0)
    }

    pub(crate) fn available_spot(&self, user: Address, coin: &str) -> f64 {
        (self.spot_total(user, coin) - self.spot_hold_total(user, coin)).max(0.0)
    }

    pub(crate) fn set_book_depth(&mut self, base: &str, quote: &str, depth: f64) {
        let depth = depth.max(0.0);
        self.book_depths.insert(
            (base.to_string(), quote.to_string()),
            HyperliquidBookDepth {
                bid: depth,
                ask: depth,
            },
        );
    }

    fn book_depth_for(&self, base: &str, quote: &str) -> HyperliquidBookDepth {
        self.book_depths
            .get(&(base.to_string(), quote.to_string()))
            .copied()
            .unwrap_or(HyperliquidBookDepth {
                bid: SYNTHESIZED_BOOK_DEPTH,
                ask: SYNTHESIZED_BOOK_DEPTH,
            })
    }

    fn spot_hold_total(&self, user: Address, coin: &str) -> f64 {
        self.spot_holds_for(user)
            .get(coin)
            .copied()
            .unwrap_or_default()
    }

    fn spot_holds_for(&self, user: Address) -> BTreeMap<String, f64> {
        let mut holds = BTreeMap::new();
        for order in self.open_orders.values().filter(|order| order.user == user) {
            let Some(resolution) = self.resolve_asset(order.asset) else {
                continue;
            };
            let (coin, amount) = if order.is_buy {
                (resolution.quote_symbol, order.limit_px * order.sz)
            } else {
                (resolution.base_symbol, order.sz)
            };
            *holds.entry(coin).or_default() += amount;
        }
        holds
    }

    pub(crate) fn set_rate(&mut self, base: &str, quote: &str, rate: f64) {
        self.run_due_scheduled_cancels();
        self.rates
            .insert((base.to_string(), quote.to_string()), rate);
        self.book_depths
            .entry((base.to_string(), quote.to_string()))
            .or_insert(HyperliquidBookDepth {
                bid: SYNTHESIZED_BOOK_DEPTH,
                ask: SYNTHESIZED_BOOK_DEPTH,
            });
        self.fill_crossed_resting_orders(base, quote, rate);
    }

    pub(crate) fn rate_for(&self, base: &str, quote: &str) -> Option<f64> {
        self.rates
            .get(&(base.to_string(), quote.to_string()))
            .copied()
    }

    fn resolve_asset(&self, asset: u32) -> Option<AssetResolution> {
        let meta = self.spot_meta.as_ref()?;
        asset.checked_sub(SPOT_ASSET_INDEX_OFFSET)?;
        let pair = meta
            .universe
            .iter()
            .find(|p| spot_wire_asset_index(p.index) == Some(asset))?;
        let base = meta.tokens.iter().find(|t| t.index == pair.tokens[0])?;
        let quote = meta.tokens.iter().find(|t| t.index == pair.tokens[1])?;
        Some(AssetResolution {
            pair_name: pair.name.clone(),
            base_symbol: base.name.clone(),
            quote_symbol: quote.name.clone(),
        })
    }

    /// Walk the spot-meta universe to find the base/quote token pair for a
    /// wire coin string (`"UBTC/USDC"` or `"@140"`). Used by the synthesized
    /// L2 book so clients that query either alias see the same levels.
    fn pair_tokens(&self, coin: &str) -> Option<(String, String)> {
        let meta = self.spot_meta.as_ref()?;
        let pair = meta.universe.iter().find(|p| p.name == coin).or_else(|| {
            let (base_name, quote_name) = coin.split_once('/')?;
            let base = meta.tokens.iter().find(|t| t.name == base_name)?;
            let quote = meta.tokens.iter().find(|t| t.name == quote_name)?;
            meta.universe
                .iter()
                .find(|p| p.tokens[0] == base.index && p.tokens[1] == quote.index)
        })?;
        let base = meta.tokens.iter().find(|t| t.index == pair.tokens[0])?;
        let quote = meta.tokens.iter().find(|t| t.index == pair.tokens[1])?;
        Some((base.name.clone(), quote.name.clone()))
    }

    /// Synthesize a one-level L2 book from the configured rate: bid == ask ==
    /// rate. Depth is configurable per pair so tests can exercise partial-fill
    /// behavior without a full matching engine.
    pub(crate) fn l2_book_snapshot(&self, coin: &str) -> Value {
        let levels = self
            .pair_tokens(coin)
            .and_then(|(base, quote)| {
                self.rate_for(&base, &quote).map(|rate| {
                    let depth = self.book_depth_for(&base, &quote);
                    let bid = json!({
                        "n": 1,
                        "px": format_hl_amount(rate),
                        "sz": format_hl_amount(depth.bid),
                    });
                    let ask = json!({
                        "n": 1,
                        "px": format_hl_amount(rate),
                        "sz": format_hl_amount(depth.ask),
                    });
                    (bid, ask)
                })
            })
            .map(|(bid, ask)| {
                let level = json!({
                    "n": 1,
                    "px": bid["px"],
                    "sz": bid["sz"],
                });
                let ask_level = json!({
                    "n": 1,
                    "px": ask["px"],
                    "sz": ask["sz"],
                });
                (level, ask_level)
            });
        let (bids, asks) = match levels {
            Some((bid, ask)) => (vec![bid], vec![ask]),
            None => (vec![], vec![]),
        };
        json!({
            "coin": coin,
            "time": Utc::now().timestamp_millis(),
            "levels": [bids, asks]
        })
    }

    pub(crate) fn order_status_snapshot(&self, user: Address, oid: u64) -> Value {
        if let Some(open) = self
            .open_orders
            .get(&oid)
            .filter(|order| order.user == user)
        {
            json!({
                "status": "order",
                "order": self.order_info_envelope(open, "open", open.timestamp),
            })
        } else if let Some(terminal) = self
            .terminal_orders
            .get(&oid)
            .filter(|terminal| terminal.order.user == user)
        {
            json!({
                "status": "order",
                "order": self.order_info_envelope(
                    &terminal.order,
                    &terminal.status,
                    terminal.status_timestamp,
                ),
            })
        } else {
            json!({ "status": "unknownOid" })
        }
    }

    fn order_info_envelope(
        &self,
        order: &HyperliquidSubmittedOrder,
        status: &str,
        status_timestamp: u64,
    ) -> Value {
        json!({
            "order": {
                "coin": order.coin,
                "side": if order.is_buy { "B" } else { "A" },
                "limitPx": format_hl_amount(order.limit_px),
                "sz": format_hl_amount(order.sz),
                "oid": order.oid,
                "timestamp": order.timestamp,
                "origSz": format_hl_amount(order.orig_sz),
                "cloid": order.cloid,
                "tif": order.tif,
            },
            "status": status,
            "statusTimestamp": status_timestamp,
        })
    }

    pub(crate) fn spot_clearinghouse_snapshot(&self, user: Address) -> Value {
        let default = BTreeMap::new();
        let totals = self.spot_balances.get(&user).unwrap_or(&default);
        let Some(meta) = self.spot_meta.as_ref() else {
            return json!({ "balances": [] });
        };
        let holds = self.spot_holds_for(user);
        let balances: Vec<Value> = totals
            .iter()
            .filter_map(|(coin, total)| {
                let token = meta.tokens.iter().find(|t| &t.name == coin);
                let token_index = token.map_or(0u64, |t| t.index as u64);
                let token_decimals =
                    token.map_or(hyperliquid_client::wire::WIRE_DECIMALS, |t| t.wei_decimals);
                let hold = holds.get(coin).copied().unwrap_or_default();
                if !hyperliquid_token_amount_is_visible(*total, token_decimals)
                    && !hyperliquid_token_amount_is_visible(hold, token_decimals)
                {
                    return None;
                }
                Some(json!({
                    "coin": coin,
                    "token": token_index,
                    "hold": format_hl_token_amount(hold, token_decimals),
                    "total": format_hl_token_amount(*total, token_decimals),
                }))
            })
            .collect();
        json!({ "balances": balances })
    }

    pub(crate) fn clearinghouse_snapshot(&self, user: Address) -> Value {
        let withdrawable = self.clearinghouse_total(user, "USDC");
        let withdrawable = format_hl_amount(withdrawable);
        json!({
            "marginSummary": {
                "accountValue": withdrawable,
                "totalNtlPos": "0",
                "totalRawUsd": withdrawable,
                "totalMarginUsed": "0",
            },
            "crossMarginSummary": {
                "accountValue": withdrawable,
                "totalNtlPos": "0",
                "totalRawUsd": withdrawable,
                "totalMarginUsed": "0",
            },
            "crossMaintenanceMarginUsed": "0",
            "withdrawable": withdrawable,
            "assetPositions": [],
            "time": Utc::now().timestamp_millis(),
        })
    }

    pub(crate) fn open_orders_for(&self, user: Address) -> Value {
        Value::Array(
            self.open_orders
                .values()
                .filter(|order| order.user == user)
                .map(|order| {
                    json!({
                        "coin": order.coin,
                        "side": if order.is_buy { "B" } else { "A" },
                        "limitPx": format_hl_amount(order.limit_px),
                        "sz": format_hl_amount(order.sz),
                        "oid": order.oid,
                        "timestamp": order.timestamp,
                        "origSz": format_hl_amount(order.orig_sz),
                        "cloid": order.cloid,
                    })
                })
                .collect(),
        )
    }

    pub(crate) fn fills_for(&self, user: Address) -> Vec<UserFill> {
        let mut rows = self.generated_fills_for(user);
        rows.extend(self.recorded_fills.get(&user).cloned().unwrap_or_default());
        rows.sort_by(|left, right| right.time.cmp(&left.time));
        rows.truncate(HYPERLIQUID_INFO_RESULT_LIMIT);
        rows
    }

    pub(crate) fn fills_by_time(
        &self,
        user: Address,
        start_time: u64,
        end_time: Option<u64>,
        _aggregate_by_time: bool,
    ) -> Vec<UserFill> {
        let mut rows = self.fills_for(user);
        rows.retain(|fill| fill.time >= start_time && end_time.is_none_or(|end| fill.time <= end));
        rows.sort_by(|left, right| left.time.cmp(&right.time));
        rows.truncate(HYPERLIQUID_INFO_RESULT_LIMIT);
        rows
    }

    pub(crate) fn ledger_updates_by_time(
        &self,
        user: Address,
        start_time: u64,
        end_time: Option<u64>,
    ) -> Vec<UserNonFundingLedgerUpdate> {
        let mut rows = self.ledger_updates.get(&user).cloned().unwrap_or_default();
        rows.retain(|update| {
            update.time >= start_time && end_time.is_none_or(|end| update.time <= end)
        });
        rows.sort_by(|left, right| left.time.cmp(&right.time));
        rows.truncate(HYPERLIQUID_INFO_RESULT_LIMIT);
        rows
    }

    pub(crate) fn fundings_by_time(
        &self,
        user: Address,
        start_time: u64,
        end_time: Option<u64>,
    ) -> Vec<UserFunding> {
        let mut rows = self.fundings.get(&user).cloned().unwrap_or_default();
        rows.retain(|funding| {
            funding.time >= start_time && end_time.is_none_or(|end| funding.time <= end)
        });
        rows.sort_by(|left, right| left.time.cmp(&right.time));
        rows.truncate(HYPERLIQUID_INFO_RESULT_LIMIT);
        rows
    }

    pub(crate) fn user_rate_limit(&self, _user: Address) -> UserRateLimit {
        UserRateLimit {
            cum_vlm: "0".to_string(),
            n_requests_used: 0,
            n_requests_cap: 1200,
            n_requests_surplus: 1200,
        }
    }

    fn generated_fills_for(&self, user: Address) -> Vec<UserFill> {
        self.fills
            .get(&user)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(Self::fill_record_to_user_fill)
            .collect()
    }

    fn fill_record_to_user_fill(fill: HyperliquidFillRecord) -> UserFill {
        UserFill {
            coin: fill.coin,
            px: format_hl_amount(fill.px),
            sz: format_hl_amount(fill.sz),
            side: if fill.is_buy { "B" } else { "A" }.to_string(),
            time: fill.time,
            start_position: "0".to_string(),
            dir: if fill.is_buy { "Buy" } else { "Sell" }.to_string(),
            closed_pnl: "0".to_string(),
            hash: format!("0x{}", "ab".repeat(32)),
            oid: fill.oid,
            crossed: true,
            fee: format_hl_amount(fill.fee),
            tid: fill.tid,
            fee_token: fill.fee_token,
        }
    }

    fn fill_crossed_resting_orders(&mut self, base: &str, quote: &str, rate: f64) {
        let depth = self.book_depth_for(base, quote);
        let mut remaining_bid_depth = depth.bid.max(0.0);
        let mut remaining_ask_depth = depth.ask.max(0.0);
        let matching_oids: Vec<u64> = self
            .open_orders
            .iter()
            .filter_map(|(oid, order)| {
                let resolution = self.resolve_asset(order.asset)?;
                if resolution.base_symbol != base || resolution.quote_symbol != quote {
                    return None;
                }
                let crosses = hyperliquid_order_crosses(order.is_buy, order.limit_px, rate);
                crosses.then_some(*oid)
            })
            .collect();

        for oid in matching_oids {
            let Some(mut order) = self.open_orders.remove(&oid) else {
                continue;
            };
            let Some(resolution) = self.resolve_asset(order.asset) else {
                continue;
            };
            let side_depth = if order.is_buy {
                &mut remaining_ask_depth
            } else {
                &mut remaining_bid_depth
            };
            let fill_sz = order.sz.min((*side_depth).max(0.0));
            if hyperliquid_is_effectively_zero(fill_sz) {
                self.open_orders.insert(order.oid, order);
                continue;
            }

            let execution_px = hyperliquid_execution_px(order.is_buy, order.limit_px, rate);
            let (debit_coin, debit_amount, credit_coin, credit_amount) = if order.is_buy {
                (
                    resolution.quote_symbol.clone(),
                    fill_sz * execution_px,
                    resolution.base_symbol.clone(),
                    fill_sz,
                )
            } else {
                (
                    resolution.base_symbol.clone(),
                    fill_sz,
                    resolution.quote_symbol.clone(),
                    fill_sz * execution_px,
                )
            };
            if !hyperliquid_has_sufficient_amount(
                self.spot_total(order.user, &debit_coin),
                debit_amount,
            ) {
                self.reject_order(order, Utc::now().timestamp_millis().max(0) as u64);
                continue;
            }

            self.apply_spot_fill(
                order.user,
                &debit_coin,
                debit_amount,
                &credit_coin,
                credit_amount,
            );

            let timestamp = Utc::now().timestamp_millis().max(0) as u64;
            let tid = self.allocate_tid();
            *side_depth = (*side_depth - fill_sz).max(0.0);
            self.fills.entry(order.user).or_default().insert(
                0,
                HyperliquidFillRecord {
                    oid: order.oid,
                    tid,
                    coin: resolution.pair_name.clone(),
                    is_buy: order.is_buy,
                    px: execution_px,
                    sz: fill_sz,
                    time: timestamp,
                    fee: 0.0,
                    fee_token: if order.is_buy {
                        resolution.base_symbol
                    } else {
                        resolution.quote_symbol
                    },
                },
            );
            order.sz = (order.sz - fill_sz).max(0.0);
            if hyperliquid_is_effectively_zero(order.sz) {
                self.terminal_orders.insert(
                    order.oid,
                    TerminalOrder {
                        order,
                        status: "filled".to_string(),
                        status_timestamp: timestamp,
                    },
                );
            } else {
                self.open_orders.insert(order.oid, order);
            }
        }
    }

    pub(crate) fn set_schedule_cancel(&mut self, user: Address, time: Option<u64>) {
        match time {
            Some(time) => {
                self.scheduled_cancels.insert(user, time);
            }
            None => {
                self.scheduled_cancels.remove(&user);
            }
        }
    }

    pub(crate) fn run_due_scheduled_cancels(&mut self) {
        let now = Utc::now().timestamp_millis().max(0) as u64;
        let due_users: Vec<Address> = self
            .scheduled_cancels
            .iter()
            .filter_map(|(user, time)| (*time <= now).then_some(*user))
            .collect();
        for user in due_users {
            self.cancel_all_open_orders_for_user(user, "scheduledCancel");
            self.scheduled_cancels.remove(&user);
        }
    }

    fn cancel_all_open_orders_for_user(&mut self, user: Address, status: &str) {
        let matching_oids: Vec<u64> = self
            .open_orders
            .iter()
            .filter_map(|(oid, order)| (order.user == user).then_some(*oid))
            .collect();
        let timestamp = Utc::now().timestamp_millis().max(0) as u64;
        for oid in matching_oids {
            let Some(order) = self.open_orders.remove(&oid) else {
                continue;
            };
            self.terminal_orders.insert(
                order.oid,
                TerminalOrder {
                    order,
                    status: status.to_string(),
                    status_timestamp: timestamp,
                },
            );
        }
    }

    /// Apply each order in the bulk against the hardcoded rate table.
    ///
    /// The mock exposes just enough lifecycle behavior for production-equivalent
    /// client integration:
    /// - `Ioc` fills immediately when marketable and errors otherwise.
    /// - `Gtc` fills immediately when marketable, otherwise rests on the book.
    /// - `Alo` rests when non-marketable and errors if it would cross.
    ///
    /// All fills execute at the configured synthetic rate. Resting orders reserve
    /// `hold` balance but do not otherwise match until they are cancelled.
    pub(crate) fn place_spot_orders(
        &mut self,
        user: Address,
        bulk: &hyperliquid_client::BulkOrder,
    ) -> Vec<Value> {
        bulk.orders
            .iter()
            .map(|order| {
                let Some(resolution) = self.resolve_asset(order.asset) else {
                    return json!({ "error": format!("unknown asset {}", order.asset) });
                };
                let limit_px = match order.limit_px.parse::<f64>() {
                    Ok(v) if v > 0.0 => v,
                    _ => {
                        return json!({
                            "error": format!("invalid limit_px {}", order.limit_px)
                        });
                    }
                };
                let sz = match order.sz.parse::<f64>() {
                    Ok(v) if v > 0.0 => v,
                    _ => return json!({ "error": format!("invalid sz {}", order.sz) }),
                };
                let tif = match &order.order_type {
                    hyperliquid_client::Order::Limit(limit) => limit.tif.clone(),
                };

                let base_coin = resolution.base_symbol.clone();
                let quote_coin = resolution.quote_symbol.clone();
                let Some(rate) = self.rate_for(&base_coin, &quote_coin) else {
                    return json!({
                        "error": format!(
                            "no rate configured for {base_coin}/{quote_coin}; call \
                             set_hyperliquid_rate before placing orders"
                        )
                    });
                };

                let crosses = hyperliquid_order_crosses(order.is_buy, limit_px, rate);

                if tif == "Alo" && crosses {
                    return json!({
                        "error": format!(
                            "add-liquidity-only order would cross rate {rate} for \
                             {base_coin}/{quote_coin}"
                        )
                    });
                }

                if !crosses {
                    if tif == "Ioc" {
                        return json!({
                            "error": format!(
                                "limit price {limit_px} does not cross rate {rate} for \
                                 {base_coin}/{quote_coin} ({})",
                                if order.is_buy { "buy needs limit_px ≥ rate" } else { "sell needs limit_px ≤ rate" }
                            )
                        });
                    }

                    if tif != "Gtc" && tif != "Alo" {
                        return json!({
                            "error": format!("unsupported time-in-force {tif}")
                        });
                    }

                    let reserve_coin = if order.is_buy {
                        quote_coin.clone()
                    } else {
                        base_coin.clone()
                    };
                    let reserve_amount = if order.is_buy { sz * limit_px } else { sz };
                    let available = self.available_spot(user, &reserve_coin);
                    if !hyperliquid_has_sufficient_amount(available, reserve_amount) {
                        return json!({
                            "error": format!(
                                "insufficient {reserve_coin} balance: have {available}, need {reserve_amount}",
                            )
                        });
                    }

                    let oid = self.allocate_oid();
                    let timestamp = Utc::now().timestamp_millis().max(0) as u64;
                    let submitted = HyperliquidSubmittedOrder {
                        oid,
                        user,
                        asset: order.asset,
                        coin: resolution.pair_name.clone(),
                        is_buy: order.is_buy,
                        limit_px,
                        sz,
                        orig_sz: sz,
                        tif: tif.clone(),
                        cloid: order.cloid.clone(),
                        timestamp,
                    };
                    self.open_orders.insert(oid, submitted);
                    return json!({
                        "resting": {
                            "oid": oid,
                        }
                    });
                }

                if tif != "Ioc" && tif != "Gtc" {
                    return json!({
                        "error": format!("unsupported time-in-force {tif}")
                    });
                }

                let oid = self.allocate_oid();
                let timestamp = Utc::now().timestamp_millis().max(0) as u64;
                let submitted = HyperliquidSubmittedOrder {
                    oid,
                    user,
                    asset: order.asset,
                    coin: resolution.pair_name.clone(),
                    is_buy: order.is_buy,
                    limit_px,
                    sz,
                    orig_sz: sz,
                    tif: tif.clone(),
                    cloid: order.cloid.clone(),
                    timestamp,
                };
                let top_of_book_depth = self.book_depth_for(&base_coin, &quote_coin);
                let fill_sz = if order.is_buy {
                    sz.min(top_of_book_depth.ask.max(0.0))
                } else {
                    sz.min(top_of_book_depth.bid.max(0.0))
                };
                let remaining_sz = (sz - fill_sz).max(0.0);

                if hyperliquid_is_effectively_zero(fill_sz) {
                    if tif == "Ioc" {
                        return json!({
                            "error": format!(
                                "no liquidity available at or better than limit_px {limit_px} for \
                                 {base_coin}/{quote_coin}"
                            )
                        });
                    }
                    self.open_orders.insert(oid, submitted);
                    return json!({
                        "resting": {
                            "oid": oid,
                        }
                    });
                }

                let reserve_coin = if order.is_buy {
                    quote_coin.clone()
                } else {
                    base_coin.clone()
                };
                let execution_px = hyperliquid_execution_px(order.is_buy, limit_px, rate);
                let reserve_amount = if order.is_buy {
                    if tif == "Gtc" {
                        (fill_sz * execution_px) + (remaining_sz * limit_px)
                    } else {
                        fill_sz * execution_px
                    }
                } else {
                    sz
                };
                let available = self.available_spot(user, &reserve_coin);
                if !hyperliquid_has_sufficient_amount(available, reserve_amount) {
                    self.reject_order(submitted, timestamp);
                    return json!({
                        "error": format!(
                            "insufficient {reserve_coin} balance: have {available}, need {reserve_amount}",
                        )
                    });
                }

                let (debit_coin, debit_amount, credit_coin, credit_amount) = if order.is_buy {
                    (
                        quote_coin.clone(),
                        fill_sz * execution_px,
                        base_coin.clone(),
                        fill_sz,
                    )
                } else {
                    (
                        base_coin.clone(),
                        fill_sz,
                        quote_coin.clone(),
                        fill_sz * execution_px,
                    )
                };

                self.apply_spot_fill(
                    user,
                    &debit_coin,
                    debit_amount,
                    &credit_coin,
                    credit_amount,
                );

                let tid = self.allocate_tid();
                let fill = HyperliquidFillRecord {
                    oid,
                    tid,
                    coin: resolution.pair_name.clone(),
                    is_buy: order.is_buy,
                    px: execution_px,
                    sz: fill_sz,
                    time: timestamp,
                    fee: 0.0,
                    fee_token: if order.is_buy {
                        base_coin.clone()
                    } else {
                        quote_coin.clone()
                    },
                };
                self.fills.entry(user).or_default().insert(0, fill);

                if tif == "Gtc" && !hyperliquid_is_effectively_zero(remaining_sz) {
                    let mut resting = submitted;
                    resting.sz = remaining_sz;
                    self.open_orders.insert(oid, resting);
                    return json!({
                        "resting": {
                            "oid": oid,
                        }
                    });
                }

                let mut terminal_order = submitted;
                if !hyperliquid_is_effectively_zero(remaining_sz) {
                    terminal_order.sz = remaining_sz;
                }
                self.terminal_orders.insert(
                    oid,
                    TerminalOrder {
                        order: terminal_order,
                        status: "filled".to_string(),
                        status_timestamp: timestamp,
                    },
                );

                json!({
                    "filled": {
                        "totalSz": format_hl_amount(fill_sz),
                        "avgPx": format_hl_amount(execution_px),
                        "oid": oid,
                    }
                })
            })
            .collect()
    }

    pub(crate) fn cancel_spot_orders(
        &mut self,
        user: Address,
        bulk: &hyperliquid_client::BulkCancel,
    ) -> Vec<Value> {
        bulk.cancels
            .iter()
            .map(|cancel| match self.open_orders.remove(&cancel.oid) {
                Some(order) if order.user == user && order.asset == cancel.asset => {
                    self.terminal_orders.insert(
                        order.oid,
                        TerminalOrder {
                            order,
                            status: "canceled".to_string(),
                            status_timestamp: Utc::now().timestamp_millis().max(0) as u64,
                        },
                    );
                    json!("success")
                }
                Some(order) => {
                    self.open_orders.insert(order.oid, order);
                    json!({
                        "error": format!(
                            "oid {} is not an open order for this user/asset",
                            cancel.oid
                        )
                    })
                }
                None => json!({
                    "error": format!(
                        "devnet HL mock has no resting order for oid {}",
                        cancel.oid
                    )
                }),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spot_clearinghouse_snapshot_preserves_token_wei_decimals() {
        let mut state = HyperliquidCoreState::new();
        let user = Address::repeat_byte(0x66);
        state.credit_spot(user, "UETH", 0.044810844333333334);

        let snapshot = state.spot_clearinghouse_snapshot(user);
        let total = snapshot["balances"][0]["total"]
            .as_str()
            .expect("ueth total");

        assert_ne!(total, "0.04481084");
        assert!(
            total.starts_with("0.0448108443333333"),
            "unexpected UETH balance precision: {total}"
        );
    }

    #[test]
    fn spot_clearinghouse_snapshot_exposes_high_precision_token_dust() {
        let mut state = HyperliquidCoreState::new();
        let user = Address::repeat_byte(0x66);
        state.credit_spot(user, "UETH", 1e-15);

        let snapshot = state.spot_clearinghouse_snapshot(user);
        let total = snapshot["balances"][0]["total"]
            .as_str()
            .expect("ueth total");

        assert_eq!(total, "0.000000000000001");
    }

    #[test]
    fn spot_balance_formatting_rounds_float_dust_up_to_token_precision() {
        assert_eq!(format_hl_token_amount(0.068_2, 18), "0.0682");
    }

    #[test]
    fn hyperliquid_mock_resolve_asset_rejects_wrapped_pair_indexes() {
        let Ok(overflow_index) = usize::try_from(u64::from(u32::MAX) + 1 + 140) else {
            return;
        };
        let mut state = HyperliquidCoreState::new();
        state.spot_meta.as_mut().expect("spot meta").universe = vec![SpotAssetMeta {
            tokens: [1, 0],
            name: "@overflow".to_string(),
            index: overflow_index,
            is_canonical: false,
        }];

        assert!(state.resolve_asset(10_140).is_none());
    }

    #[test]
    fn hyperliquid_amount_tolerance_accepts_large_decimal_dust() {
        assert!(hyperliquid_has_sufficient_amount(
            121_891.2,
            121_891.20000000001
        ));
        assert!(hyperliquid_has_sufficient_amount(0.29999999998835847, 0.3));
        assert!(hyperliquid_has_sufficient_amount(
            214.799999,
            214.79999999999998
        ));
        assert!(!hyperliquid_has_sufficient_amount(121_891.2, 121_891.3));
    }
}

//! Dedicated router / graph-traversal test suite.
//!
//! Exercises the routing-graph BFS in [`AssetRegistry`] plus the post-BFS
//! pipeline used by the production order manager:
//!
//!   1. BFS enumerate candidate transition paths (`select_transition_paths`)
//!   2. Filter to executable terminal kinds (mirror of
//!      `bin/router-server/src/services/order_manager.rs::is_executable_transition_path`)
//!   3. Prefer same-chain EVM paths when source/destination share an EVM chain
//!      (mirror of `prefer_same_chain_evm_paths`)
//!   4. Rank by structural cost (`rank_transition_paths_structurally`)
//!
//! Provider-configured / health / unit-compatibility filters live in
//! `router-server` and are out of scope here -- they need a live
//! `ActionProviderRegistry` and belong in a router-server-level test.
//!
//! Run with:
//!     cargo nextest run -p router-core --test graph_traversal
//!     cargo test     -p router-core --test graph_traversal -- --nocapture

use router_core::{
    protocol::{AssetId, ChainId, DepositAsset},
    services::{
        asset_registry::{
            AssetRegistry, CanonicalAsset, MarketOrderNode, MarketOrderTransitionKind,
            TransitionPath,
        },
        pricing::PricingSnapshot,
        route_costs::{
            amount_aware_path_score, confidence_band, rank_transition_paths_structurally,
            structural_path_score, RankedTransitionPath, DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
        },
    },
};
use std::collections::HashMap;

const USDC_BASE: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const USDC_ARBITRUM: &str = "0xaf88d065e77c8cc2239327c5edb3a432268e5831";

fn asset(chain: &str, asset: AssetId) -> DepositAsset {
    DepositAsset {
        chain: ChainId::parse(chain).expect("valid chain id"),
        asset,
    }
}

fn evm_token(chain: &str, address: &str) -> DepositAsset {
    asset(chain, AssetId::reference(address))
}

/// Local copy of `is_executable_transition_path` from
/// `bin/router-server/src/services/order_manager.rs`. Kept in sync manually.
fn is_executable_transition_path(registry: &AssetRegistry, path: &TransitionPath) -> bool {
    if path.transitions.is_empty() {
        return false;
    }
    let last_kind = path.transitions.last().map(|t| t.kind);
    matches!(last_kind, Some(MarketOrderTransitionKind::UnitWithdrawal))
        || matches!(
            last_kind,
            Some(MarketOrderTransitionKind::UniversalRouterSwap)
        )
        || (matches!(
            last_kind,
            Some(
                MarketOrderTransitionKind::AcrossBridge | MarketOrderTransitionKind::CctpBridge
            )
        ) && path_contains_runtime_asset(registry, path))
}

fn path_contains_runtime_asset(registry: &AssetRegistry, path: &TransitionPath) -> bool {
    path.transitions.iter().any(|t| {
        registry.chain_asset(&t.input.asset).is_none()
            || registry.chain_asset(&t.output.asset).is_none()
    })
}

/// Local copy of `prefer_same_chain_evm_paths` from the order manager.
fn prefer_same_chain_evm_paths(
    source: &DepositAsset,
    destination: &DepositAsset,
    paths: &mut Vec<TransitionPath>,
) {
    if source.chain != destination.chain || !source.chain.as_str().starts_with("evm:") {
        return;
    }
    let chain = &source.chain;
    let same_chain: Vec<_> = paths
        .iter()
        .filter(|p| {
            p.transitions
                .iter()
                .all(|t| t.input.asset.chain == *chain && t.output.asset.chain == *chain)
        })
        .cloned()
        .collect();
    if !same_chain.is_empty() {
        *paths = same_chain;
    }
}

// -- Tests -----------------------------------------------------------------

#[test]
fn edge_list_is_nonempty_and_well_formed() {
    let registry = AssetRegistry::default();
    let edges = registry.market_order_transitions();

    assert!(
        !edges.is_empty(),
        "static asset registry should produce a non-empty transition edge list"
    );
    for edge in &edges {
        assert_ne!(
            edge.from, edge.to,
            "transition edge must not be a self-loop: {edge:?}"
        );
    }
}

#[test]
fn bfs_finds_base_usdc_to_arbitrum_usdc_via_cctp() {
    let registry = AssetRegistry::default();
    let source = evm_token("evm:8453", USDC_BASE);
    let destination = evm_token("evm:42161", USDC_ARBITRUM);

    let paths = registry.select_transition_paths(&source, &destination, 2);

    assert!(
        paths.iter().any(|p| {
            p.transitions.len() == 1
                && p.transitions[0].kind == MarketOrderTransitionKind::CctpBridge
        }),
        "expected a direct CCTP path Base.USDC -> Arbitrum.USDC; got: {paths:#?}"
    );
}

#[test]
fn bfs_finds_eth_to_btc_via_hyperliquid() {
    let registry = AssetRegistry::default();
    let source = asset("evm:1", AssetId::Native);
    let destination = asset("bitcoin", AssetId::Native);

    let paths = registry.select_transition_paths(&source, &destination, 5);

    assert!(!paths.is_empty(), "expected at least one ETH -> BTC path");

    // Hyperliquid spot only trades against USDC, so ETH -> BTC routes through
    // two `HyperliquidTrade` hops (UETH -> USDC -> UBTC).
    let unit_hyperliquid_unit_path = paths.iter().any(|p| {
        let kinds: Vec<_> = p.transitions.iter().map(|t| t.kind).collect();
        kinds
            == vec![
                MarketOrderTransitionKind::UnitDeposit,
                MarketOrderTransitionKind::HyperliquidTrade,
                MarketOrderTransitionKind::HyperliquidTrade,
                MarketOrderTransitionKind::UnitWithdrawal,
            ]
    });

    assert!(
        unit_hyperliquid_unit_path,
        "expected a [UnitDeposit, HyperliquidTrade, HyperliquidTrade, UnitWithdrawal] path \
         for ETH -> BTC; got: {:#?}",
        paths
            .iter()
            .map(|p| p.transitions.iter().map(|t| t.kind).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    );
}

#[test]
fn bfs_respects_max_depth() {
    let registry = AssetRegistry::default();
    let source = asset("evm:1", AssetId::Native);
    let destination = asset("bitcoin", AssetId::Native);

    let shallow = registry.select_transition_paths(&source, &destination, 1);
    for path in &shallow {
        assert!(
            path.transitions.len() <= 1,
            "depth=1 must not yield multi-hop paths: {path:#?}"
        );
    }

    let deep = registry.select_transition_paths(&source, &destination, 5);
    assert!(
        deep.iter().any(|p| p.transitions.len() > 1),
        "depth=5 must yield at least one multi-hop path for ETH -> BTC"
    );
}

#[test]
fn bfs_returns_empty_for_unknown_endpoints() {
    let registry = AssetRegistry::default();
    // "solana" is a syntactically valid ChainId but is not in the registry,
    // and no provider declares capabilities on it -- so no transition edges
    // can ever originate from or terminate on it.
    let source = asset("solana", AssetId::reference("sol-fake-mint-a"));
    let destination = asset("solana", AssetId::reference("sol-fake-mint-b"));

    let paths = registry.select_transition_paths(&source, &destination, 5);

    assert!(
        paths.is_empty(),
        "expected no paths for endpoints on an unsupported chain; got: {paths:#?}"
    );
}

#[test]
fn pipeline_terminates_with_executable_kind_and_same_chain_preference() {
    let registry = AssetRegistry::default();

    // Two arbitrary, unknown ERC20 addresses on Base. These are not in the
    // static registry, so they exercise the Velora runtime anchor path.
    let source = evm_token("evm:8453", "0x3333333333333333333333333333333333333333");
    let destination = evm_token("evm:8453", "0x4444444444444444444444444444444444444444");

    let bfs = registry.select_transition_paths(&source, &destination, 5);
    assert!(
        !bfs.is_empty(),
        "expected BFS to discover at least one Base.<rand> -> Base.<rand> path"
    );

    let mut executable: Vec<_> = bfs
        .into_iter()
        .filter(|p| is_executable_transition_path(&registry, p))
        .collect();
    assert!(
        !executable.is_empty(),
        "expected at least one executable path after terminal-kind filter"
    );

    prefer_same_chain_evm_paths(&source, &destination, &mut executable);
    assert!(
        executable
            .iter()
            .all(|p| p.transitions.iter().all(|t| {
                t.input.asset.chain.as_str() == "evm:8453"
                    && t.output.asset.chain.as_str() == "evm:8453"
            })),
        "same-chain preference should restrict all hops to evm:8453"
    );

    rank_transition_paths_structurally(&mut executable, DEFAULT_SAMPLE_AMOUNT_USD_MICROS);
    let pricing = PricingSnapshot::static_bootstrap(chrono::Utc::now());
    let mut prev_key: Option<(usize, u64, u64, usize)> = None;
    for path in &executable {
        let score = structural_path_score(path, &pricing, DEFAULT_SAMPLE_AMOUNT_USD_MICROS);
        let key = (
            score.missing_edges,
            score.total_effective_cost_usd_micros,
            score.total_latency_ms,
            path.transitions.len(),
        );
        if let Some(prev) = prev_key {
            assert!(
                prev <= key,
                "structural ranking violated: previous {prev:?} should be <= current {key:?}"
            );
        }
        prev_key = Some(key);
    }

    let last_kinds: Vec<_> = executable
        .iter()
        .filter_map(|p| p.transitions.last().map(|t| t.kind))
        .collect();
    let allowed = [
        MarketOrderTransitionKind::UniversalRouterSwap,
        MarketOrderTransitionKind::UnitWithdrawal,
        MarketOrderTransitionKind::AcrossBridge,
        MarketOrderTransitionKind::CctpBridge,
    ];
    for kind in &last_kinds {
        assert!(
            allowed.contains(kind),
            "terminal hop kind {kind:?} not in executable allow-list"
        );
    }
}

// -- Regression tests for audit items S2.1 and S1.5 ------------------------

/// Audit S2.1: previously `venue_canonicals` was collected through a
/// `HashSet`, so the order of `HyperliquidTrade` edges varied process-to-
/// process. After the fix, those edges enumerate in lexicographic canonical
/// order. This test asserts both the structural sort invariant (which would
/// fail under `HashSet`'s randomised order on most processes) and within-call
/// stability.
#[test]
fn hyperliquid_trade_edges_are_deterministic_and_sorted_by_canonical() {
    fn trade_pairs(registry: &AssetRegistry) -> Vec<(CanonicalAsset, CanonicalAsset)> {
        registry
            .market_order_transitions()
            .into_iter()
            .filter(|t| t.kind == MarketOrderTransitionKind::HyperliquidTrade)
            .filter_map(|t| match (t.from, t.to) {
                (
                    MarketOrderNode::Venue {
                        canonical: from, ..
                    },
                    MarketOrderNode::Venue { canonical: to, .. },
                ) => Some((from, to)),
                _ => None,
            })
            .collect()
    }

    let registry = AssetRegistry::default();
    let first = trade_pairs(&registry);

    assert!(
        !first.is_empty(),
        "expected at least one HyperliquidTrade edge"
    );

    let mut sorted = first.clone();
    sorted.sort_by_key(|(from, to)| (from.as_str(), to.as_str()));
    assert_eq!(
        first, sorted,
        "HyperliquidTrade edges must enumerate in canonical-name order, not HashSet order"
    );

    let second = trade_pairs(&registry);
    assert_eq!(
        first, second,
        "market_order_transitions() must be deterministic across calls"
    );
}

/// Audit S1.5: BFS used to record a path on goal-reach but then keep
/// expanding from that state, producing "reach destination, leave, return"
/// detours that inflated the candidate set. After the fix, no returned path
/// may have the destination asset as an intermediate input or non-final
/// output.
#[test]
fn bfs_does_not_expand_past_goal() {
    let registry = AssetRegistry::default();
    let source = asset("evm:1", AssetId::Native);
    let destination = asset("bitcoin", AssetId::Native);

    let paths = registry.select_transition_paths(&source, &destination, 5);
    assert!(
        !paths.is_empty(),
        "expected at least one ETH -> BTC path to exercise the no-detour invariant"
    );

    for path in &paths {
        let n = path.transitions.len();
        for (i, transition) in path.transitions.iter().enumerate() {
            assert_ne!(
                transition.input.asset, destination,
                "path {} has destination as input on hop {i} (kinds={:?})",
                path.id,
                path.transitions.iter().map(|t| t.kind).collect::<Vec<_>>(),
            );
            if i + 1 != n {
                assert_ne!(
                    transition.output.asset, destination,
                    "path {} has destination as intermediate output on hop {i} (kinds={:?})",
                    path.id,
                    path.transitions.iter().map(|t| t.kind).collect::<Vec<_>>(),
                );
            }
        }
    }
}

// -- S1.2 integration tests: amount-aware ranking and confidence band ------

const USD_MICRO_HERE: u64 = 1_000_000;

/// Audit S1.2 - confidence-band round trip via the public API: scoring +
/// `confidence_band` together must return a non-zero band whose first
/// element is the structural leader, and must always include at least one
/// path when there is anything to score.
#[test]
fn confidence_band_round_trip_via_public_api() {
    let source = asset("evm:1", AssetId::Native);
    let destination = asset("bitcoin", AssetId::Native);
    let registry = AssetRegistry::default();
    let pricing = PricingSnapshot::static_bootstrap(chrono::Utc::now());
    let mut paths = registry.select_transition_paths(&source, &destination, 5);
    assert!(!paths.is_empty(), "expected at least one candidate path");
    let empty_cache: HashMap<String, _> = HashMap::new();
    // Sort by amount-aware score in place, then build the ranked list.
    let request_usd_micros = 50_000 * USD_MICRO_HERE;
    paths.sort_by(|a, b| {
        let sa = amount_aware_path_score(a, &empty_cache, &pricing, request_usd_micros);
        let sb = amount_aware_path_score(b, &empty_cache, &pricing, request_usd_micros);
        (sa.total_effective_cost_usd_micros, a.transitions.len(), a.id.clone())
            .cmp(&(sb.total_effective_cost_usd_micros, b.transitions.len(), b.id.clone()))
    });
    let ranked: Vec<RankedTransitionPath> = paths
        .iter()
        .map(|path| RankedTransitionPath {
            path: path.clone(),
            score: amount_aware_path_score(path, &empty_cache, &pricing, request_usd_micros),
        })
        .collect();
    let band = confidence_band(&ranked, request_usd_micros, 8);
    assert!(band >= 1, "band must always contain at least the leader");
    assert!(band <= ranked.len().min(8), "band must respect top_k");
}

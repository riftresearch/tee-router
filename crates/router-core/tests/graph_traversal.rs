//! Dedicated router / graph-traversal test suite.
//!
//! Exercises the routing-graph BFS in [`AssetRegistry`] plus the post-BFS
//! pipeline used by the production order manager:
//!
//!   1. BFS enumerate candidate transition paths (`select_transition_paths`)
//!   2. Drop empty paths (mirror of
//!      `bin/router-server/src/services/order_manager.rs::is_executable_transition_path`)
//!   3. Rank by structural cost (`rank_transition_paths_structurally`)
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
            rank_transition_paths_structurally, structural_path_score,
            DEFAULT_SAMPLE_AMOUNT_USD_MICROS,
        },
    },
};

const USDC_BASE: &str = "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913";
const USDC_ARBITRUM: &str = "0xaf88d065e77c8cc2239327c5edb3a432268e5831";
const USDC_ETHEREUM: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
/// A niche ERC20 not in the static registry, so it must be routed via the
/// runtime Velora wrap into/out of a primary anchor (USDC/USDT/ETH).
const PEPE_ETHEREUM: &str = "0x6982508145454ce325ddbe47a25d4ec3d2311933";

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
/// `bin/router-server/src/services/order_manager.rs`. Kept in sync manually:
/// production only drops empty paths here (a bridge exit to a registered
/// destination asset is just as executable as a Velora or Unit exit); the
/// real filtering happens in the provider/quote stages downstream.
fn is_executable_transition_path(path: &TransitionPath) -> bool {
    !path.transitions.is_empty()
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
fn bfs_routes_same_canonical_across_chains_as_distinct_corridor() {
    // Same canonical (USDC) on two different chains is a legitimate cross-chain
    // route, not a no-op: BFS must find a direct CCTP corridor.
    let registry = AssetRegistry::default();
    let source = evm_token("evm:8453", USDC_BASE);
    let destination = evm_token("evm:1", USDC_ETHEREUM);

    let paths = registry.select_transition_paths(&source, &destination, 5);
    assert!(
        paths.iter().any(|p| {
            p.transitions.len() == 1
                && p.transitions[0].kind == MarketOrderTransitionKind::CctpBridge
        }),
        "expected a direct CCTP corridor Base.USDC -> Ethereum.USDC; got: {paths:#?}"
    );
}

#[test]
fn bfs_wraps_arbitrary_erc20_into_anchor_for_pepe_to_btc() {
    // A niche ERC20 source (PEPE) must begin every route with a Velora wrap
    // (UniversalRouterSwap) into a primary anchor, then route the anchor through
    // the cached graph to BTC.
    let registry = AssetRegistry::default();
    let source = evm_token("evm:1", PEPE_ETHEREUM);
    let destination = asset("bitcoin", AssetId::Native);

    let paths = registry.select_transition_paths(&source, &destination, 5);
    assert!(!paths.is_empty(), "expected at least one PEPE -> BTC path");

    let executable: Vec<_> = paths
        .into_iter()
        .filter(is_executable_transition_path)
        .collect();
    assert!(
        !executable.is_empty(),
        "expected at least one executable PEPE -> BTC path"
    );

    for path in &executable {
        assert!(
            path.transitions.len() <= 5,
            "PEPE route exceeds max depth 5: {path:#?}"
        );
        let first = &path.transitions[0];
        assert_eq!(
            first.kind,
            MarketOrderTransitionKind::UniversalRouterSwap,
            "PEPE route must start with a Velora wrap; got {:?}",
            path.transitions.iter().map(|t| t.kind).collect::<Vec<_>>()
        );
        assert_eq!(first.input.asset, source, "wrap leg input must be PEPE");
        let anchor = registry
            .chain_asset(&first.output.asset)
            .expect("Velora wrap must land on a registry anchor asset");
        assert!(
            matches!(
                anchor.canonical,
                CanonicalAsset::Usdc | CanonicalAsset::Usdt | CanonicalAsset::Eth
            ),
            "anchor must be USDC/USDT/ETH; got {:?}",
            anchor.canonical
        );
    }

    assert!(
        executable.iter().any(|p| {
            p.transitions
                .last()
                .map(|t| t.output.asset == destination)
                .unwrap_or(false)
        }),
        "expected a PEPE -> ... -> BTC route terminating at bitcoin native"
    );
}

#[test]
fn bfs_wraps_arbitrary_erc20_at_tail_for_btc_to_pepe() {
    // The reverse direction: a niche ERC20 destination must end every route with
    // a Velora wrap out of a primary anchor into PEPE.
    let registry = AssetRegistry::default();
    let source = asset("bitcoin", AssetId::Native);
    let destination = evm_token("evm:1", PEPE_ETHEREUM);

    let paths = registry.select_transition_paths(&source, &destination, 5);
    let executable: Vec<_> = paths
        .into_iter()
        .filter(is_executable_transition_path)
        .collect();
    assert!(
        !executable.is_empty(),
        "expected at least one executable BTC -> PEPE path"
    );

    for path in &executable {
        let last = path
            .transitions
            .last()
            .expect("executable path has at least one transition");
        assert_eq!(
            last.kind,
            MarketOrderTransitionKind::UniversalRouterSwap,
            "BTC -> PEPE route must end with a Velora wrap into PEPE; got {:?}",
            path.transitions.iter().map(|t| t.kind).collect::<Vec<_>>()
        );
        assert_eq!(
            last.output.asset, destination,
            "final wrap output must be PEPE"
        );
        let anchor = registry
            .chain_asset(&last.input.asset)
            .expect("Velora wrap input must be a registry anchor asset");
        assert!(
            matches!(
                anchor.canonical,
                CanonicalAsset::Usdc | CanonicalAsset::Usdt | CanonicalAsset::Eth
            ),
            "anchor must be USDC/USDT/ETH; got {:?}",
            anchor.canonical
        );
    }
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
fn pipeline_ranks_same_chain_candidates_for_arbitrary_erc20_pair() {
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
        .filter(is_executable_transition_path)
        .collect();
    assert!(
        !executable.is_empty(),
        "expected at least one executable path after the empty-path filter"
    );

    // No same-chain preference filter: cross-chain candidates stay in the set
    // and must win on ranked cost. The direct same-chain Velora route must
    // still be among the candidates.
    assert!(
        executable.iter().any(|p| p.transitions.iter().all(|t| {
            t.input.asset.chain.as_str() == "evm:8453"
                && t.output.asset.chain.as_str() == "evm:8453"
        })),
        "expected at least one all-same-chain candidate for a same-chain pair"
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


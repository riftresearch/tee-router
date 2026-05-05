use crate::protocol::{AssetId, ChainId, DepositAsset};
use serde::{Deserialize, Serialize};
use std::collections::{HashSet, VecDeque};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalAsset {
    Btc,
    Cbbtc,
    Eth,
    Usdc,
    Usdt,
    Hype,
}

impl CanonicalAsset {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Btc => "btc",
            Self::Cbbtc => "cbbtc",
            Self::Eth => "eth",
            Self::Usdc => "usdc",
            Self::Usdt => "usdt",
            Self::Hype => "hype",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderId {
    Across,
    Cctp,
    Unit,
    HyperliquidBridge,
    Hyperliquid,
    Velora,
}

impl ProviderId {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "across" => Some(Self::Across),
            "cctp" => Some(Self::Cctp),
            "unit" => Some(Self::Unit),
            "hyperliquid_bridge" => Some(Self::HyperliquidBridge),
            "hyperliquid" => Some(Self::Hyperliquid),
            "velora" => Some(Self::Velora),
            _ => None,
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Across => "across",
            Self::Cctp => "cctp",
            Self::Unit => "unit",
            Self::HyperliquidBridge => "hyperliquid_bridge",
            Self::Hyperliquid => "hyperliquid",
            Self::Velora => "velora",
        }
    }

    #[must_use]
    pub fn venue_kind(self) -> ProviderVenueKind {
        match self {
            Self::Across | Self::Cctp | Self::Unit | Self::HyperliquidBridge => {
                ProviderVenueKind::CrossChain
            }
            Self::Hyperliquid => ProviderVenueKind::MonoChain {
                mono_chain_kind: MonoChainVenueKind::FixedPairExchange,
            },
            Self::Velora => ProviderVenueKind::MonoChain {
                mono_chain_kind: MonoChainVenueKind::UniversalRouter,
            },
        }
    }

    #[must_use]
    pub fn asset_support_model(self) -> AssetSupportModel {
        match self {
            Self::Across => AssetSupportModel::RuntimeEnumerated,
            Self::Cctp | Self::Unit | Self::HyperliquidBridge | Self::Hyperliquid => {
                AssetSupportModel::StaticDeclared
            }
            Self::Velora => AssetSupportModel::OpenAddressQuote,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderVenueKind {
    CrossChain,
    MonoChain { mono_chain_kind: MonoChainVenueKind },
}

impl ProviderVenueKind {
    #[must_use]
    pub fn is_cross_chain(self) -> bool {
        matches!(self, Self::CrossChain)
    }

    #[must_use]
    pub fn is_mono_chain(self) -> bool {
        matches!(self, Self::MonoChain { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MonoChainVenueKind {
    /// A finite venue with explicit supported pairs, like Hyperliquid spot.
    FixedPairExchange,
    /// A per-chain router that can quote dynamic token paths, like Uniswap.
    UniversalRouter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetSupportModel {
    /// The router owns the supported asset rows in this registry.
    StaticDeclared,
    /// The provider exposes a supported asset set that can change at runtime.
    RuntimeEnumerated,
    /// The provider accepts arbitrary asset addresses and quote success is the
    /// support check.
    OpenAddressQuote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProviderAssetCapability {
    BridgeInput,
    BridgeOutput,
    UnitDeposit,
    UnitWithdrawal,
    ExchangeInput,
    ExchangeOutput,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainAsset {
    pub canonical: CanonicalAsset,
    pub chain: ChainId,
    pub asset: AssetId,
    pub decimals: u8,
}

impl ChainAsset {
    #[must_use]
    pub fn deposit_asset(&self) -> DepositAsset {
        DepositAsset {
            chain: self.chain.clone(),
            asset: self.asset.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderAsset {
    pub provider: ProviderId,
    pub canonical: CanonicalAsset,
    pub chain: ChainId,
    pub provider_chain: String,
    pub provider_asset: String,
    pub decimals: u8,
    pub capabilities: Vec<ProviderAssetCapability>,
}

impl ProviderAsset {
    #[must_use]
    pub fn supports(&self, capability: ProviderAssetCapability) -> bool {
        self.capabilities.contains(&capability)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketOrderNode {
    External(DepositAsset),
    Venue {
        provider: ProviderId,
        canonical: CanonicalAsset,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketOrderTransitionKind {
    AcrossBridge,
    CctpBridge,
    UnitDeposit,
    HyperliquidBridgeDeposit,
    HyperliquidBridgeWithdrawal,
    HyperliquidTrade,
    UniversalRouterSwap,
    UnitWithdrawal,
}

impl MarketOrderTransitionKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AcrossBridge => "across_bridge",
            Self::CctpBridge => "cctp_bridge",
            Self::UnitDeposit => "unit_deposit",
            Self::HyperliquidBridgeDeposit => "hyperliquid_bridge_deposit",
            Self::HyperliquidBridgeWithdrawal => "hyperliquid_bridge_withdrawal",
            Self::HyperliquidTrade => "hyperliquid_trade",
            Self::UniversalRouterSwap => "universal_router_swap",
            Self::UnitWithdrawal => "unit_withdrawal",
        }
    }

    #[must_use]
    pub fn route_edge_kind(self) -> RouteEdgeKind {
        match self {
            Self::AcrossBridge
            | Self::CctpBridge
            | Self::UnitDeposit
            | Self::HyperliquidBridgeDeposit
            | Self::HyperliquidBridgeWithdrawal
            | Self::UnitWithdrawal => RouteEdgeKind::CrossChainTransfer,
            Self::HyperliquidTrade => RouteEdgeKind::FixedPairSwap,
            Self::UniversalRouterSwap => RouteEdgeKind::UniversalRouterSwap,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteEdgeKind {
    CrossChainTransfer,
    FixedPairSwap,
    UniversalRouterSwap,
}

impl RouteEdgeKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CrossChainTransfer => "cross_chain_transfer",
            Self::FixedPairSwap => "fixed_pair_swap",
            Self::UniversalRouterSwap => "universal_router_swap",
        }
    }

    #[must_use]
    pub fn is_cross_chain(self) -> bool {
        matches!(self, Self::CrossChainTransfer)
    }

    #[must_use]
    pub fn is_mono_chain(self) -> bool {
        matches!(self, Self::FixedPairSwap | Self::UniversalRouterSwap)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequiredCustodyRole {
    SourceOrIntermediate,
    IntermediateExecution,
    HyperliquidSpot,
    DestinationPayout,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssetSlot {
    pub asset: DepositAsset,
    pub required_custody_role: RequiredCustodyRole,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionDecl {
    pub id: String,
    pub kind: MarketOrderTransitionKind,
    pub provider: ProviderId,
    pub input: AssetSlot,
    pub output: AssetSlot,
    pub from: MarketOrderNode,
    pub to: MarketOrderNode,
}

impl TransitionDecl {
    #[must_use]
    pub fn route_edge_kind(&self) -> RouteEdgeKind {
        self.kind.route_edge_kind()
    }

    #[must_use]
    pub fn provider_venue_kind(&self) -> ProviderVenueKind {
        self.provider.venue_kind()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransitionPath {
    pub id: String,
    pub transitions: Vec<TransitionDecl>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MarketOrderTransition {
    pub kind: MarketOrderTransitionKind,
    pub provider: ProviderId,
    pub from: MarketOrderNode,
    pub to: MarketOrderNode,
}

impl MarketOrderTransition {
    #[must_use]
    pub fn route_edge_kind(&self) -> RouteEdgeKind {
        self.kind.route_edge_kind()
    }

    #[must_use]
    pub fn provider_venue_kind(&self) -> ProviderVenueKind {
        self.provider.venue_kind()
    }
}

/// Static router capability registry.
///
/// This is the source of truth for:
/// - canonical asset identities across chains/venues
/// - which providers support which assets on which chains
/// - the transition graph used by quoting, route-minimum discovery, planning,
///   and refund path search
///
/// Adding a new asset or venue starts here. If the new venue introduces a new
/// transition kind, the rest of the quote/execution/refund pipeline must also
/// be taught how to quote and materialize that transition.
///
/// Venue taxonomy is explicit but intentionally separate from serialized route
/// declarations: providers expose their venue kind through [`ProviderId`], and
/// transition kinds expose their execution semantics through
/// [`MarketOrderTransitionKind::route_edge_kind`].
#[derive(Debug, Clone)]
pub struct AssetRegistry {
    chain_assets: Vec<ChainAsset>,
    provider_assets: Vec<ProviderAsset>,
}

impl Default for AssetRegistry {
    fn default() -> Self {
        Self::with_builtin_assets()
    }
}

impl AssetRegistry {
    #[must_use]
    pub fn with_builtin_assets() -> Self {
        Self {
            chain_assets: builtin_chain_assets(),
            provider_assets: builtin_provider_assets(),
        }
    }

    #[must_use]
    pub fn chain_assets(&self) -> &[ChainAsset] {
        &self.chain_assets
    }

    #[must_use]
    pub fn provider_assets(&self) -> &[ProviderAsset] {
        &self.provider_assets
    }

    #[must_use]
    pub fn chain_asset(&self, asset: &DepositAsset) -> Option<&ChainAsset> {
        self.chain_assets
            .iter()
            .find(|entry| entry.chain == asset.chain && entry.asset == asset.asset)
    }

    #[must_use]
    pub fn canonical_for(&self, asset: &DepositAsset) -> Option<CanonicalAsset> {
        self.chain_asset(asset).map(|entry| entry.canonical)
    }

    #[must_use]
    pub fn canonical_assets_match(&self, asset_a: &DepositAsset, asset_b: &DepositAsset) -> bool {
        match (self.canonical_for(asset_a), self.canonical_for(asset_b)) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }

    #[must_use]
    pub fn anchor_assets(&self) -> Vec<DepositAsset> {
        self.chain_assets
            .iter()
            .filter(|entry| {
                entry.chain.evm_chain_id().is_some() && is_anchor_canonical(entry.canonical)
            })
            .map(ChainAsset::deposit_asset)
            .collect()
    }

    #[must_use]
    pub fn provider_asset(
        &self,
        provider: ProviderId,
        asset: &DepositAsset,
        capability: ProviderAssetCapability,
    ) -> Option<&ProviderAsset> {
        let canonical = self.canonical_for(asset)?;
        self.provider_assets.iter().find(|entry| {
            entry.provider == provider
                && entry.canonical == canonical
                && entry.chain == asset.chain
                && entry.supports(capability)
        })
    }

    #[must_use]
    pub fn supports_provider_capability(
        &self,
        provider: ProviderId,
        asset: &DepositAsset,
        capability: ProviderAssetCapability,
    ) -> bool {
        self.provider_asset(provider, asset, capability).is_some()
    }

    #[must_use]
    pub fn market_order_transitions(&self) -> Vec<MarketOrderTransition> {
        let mut transitions = Vec::new();

        for source in &self.chain_assets {
            let source_asset = source.deposit_asset();

            if self.supports_provider_capability(
                ProviderId::Cctp,
                &source_asset,
                ProviderAssetCapability::BridgeInput,
            ) {
                for destination in self.chain_assets.iter().filter(|candidate| {
                    candidate.canonical == source.canonical
                        && candidate.deposit_asset() != source_asset
                        && self.supports_provider_capability(
                            ProviderId::Cctp,
                            &candidate.deposit_asset(),
                            ProviderAssetCapability::BridgeOutput,
                        )
                }) {
                    transitions.push(MarketOrderTransition {
                        kind: MarketOrderTransitionKind::CctpBridge,
                        provider: ProviderId::Cctp,
                        from: MarketOrderNode::External(source_asset.clone()),
                        to: MarketOrderNode::External(destination.deposit_asset()),
                    });
                }
            }

            if self.supports_provider_capability(
                ProviderId::Across,
                &source_asset,
                ProviderAssetCapability::BridgeInput,
            ) {
                for destination in self.chain_assets.iter().filter(|candidate| {
                    candidate.canonical == source.canonical
                        && candidate.deposit_asset() != source_asset
                        && self.supports_provider_capability(
                            ProviderId::Across,
                            &candidate.deposit_asset(),
                            ProviderAssetCapability::BridgeOutput,
                        )
                }) {
                    transitions.push(MarketOrderTransition {
                        kind: MarketOrderTransitionKind::AcrossBridge,
                        provider: ProviderId::Across,
                        from: MarketOrderNode::External(source_asset.clone()),
                        to: MarketOrderNode::External(destination.deposit_asset()),
                    });
                }
            }

            if self.supports_provider_capability(
                ProviderId::Unit,
                &source_asset,
                ProviderAssetCapability::UnitDeposit,
            ) && self.has_hyperliquid_venue(source.canonical)
            {
                transitions.push(MarketOrderTransition {
                    kind: MarketOrderTransitionKind::UnitDeposit,
                    provider: ProviderId::Unit,
                    from: MarketOrderNode::External(source_asset.clone()),
                    to: hyperliquid_venue(source.canonical),
                });
            }

            if self.supports_provider_capability(
                ProviderId::HyperliquidBridge,
                &source_asset,
                ProviderAssetCapability::BridgeInput,
            ) && self.has_hyperliquid_venue(source.canonical)
            {
                transitions.push(MarketOrderTransition {
                    kind: MarketOrderTransitionKind::HyperliquidBridgeDeposit,
                    provider: ProviderId::HyperliquidBridge,
                    from: MarketOrderNode::External(source_asset.clone()),
                    to: hyperliquid_venue(source.canonical),
                });
            }

            if self.supports_provider_capability(
                ProviderId::HyperliquidBridge,
                &source_asset,
                ProviderAssetCapability::BridgeOutput,
            ) && self.has_hyperliquid_venue(source.canonical)
            {
                transitions.push(MarketOrderTransition {
                    kind: MarketOrderTransitionKind::HyperliquidBridgeWithdrawal,
                    provider: ProviderId::HyperliquidBridge,
                    from: hyperliquid_venue(source.canonical),
                    to: MarketOrderNode::External(source_asset.clone()),
                });
            }

            if self.supports_provider_capability(
                ProviderId::Unit,
                &source_asset,
                ProviderAssetCapability::UnitWithdrawal,
            ) && self.has_hyperliquid_venue(source.canonical)
            {
                transitions.push(MarketOrderTransition {
                    kind: MarketOrderTransitionKind::UnitWithdrawal,
                    provider: ProviderId::Unit,
                    from: hyperliquid_venue(source.canonical),
                    to: MarketOrderNode::External(source_asset.clone()),
                });
            }
        }

        let venue_canonicals: Vec<_> = self
            .provider_assets
            .iter()
            .filter(|entry| {
                entry.provider == ProviderId::Hyperliquid
                    && entry.supports(ProviderAssetCapability::ExchangeInput)
                    && entry.supports(ProviderAssetCapability::ExchangeOutput)
            })
            .map(|entry| entry.canonical)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        for input in &venue_canonicals {
            for output in &venue_canonicals {
                if input == output
                    || (*input != CanonicalAsset::Usdc && *output != CanonicalAsset::Usdc)
                {
                    continue;
                }
                transitions.push(MarketOrderTransition {
                    kind: MarketOrderTransitionKind::HyperliquidTrade,
                    provider: ProviderId::Hyperliquid,
                    from: hyperliquid_venue(*input),
                    to: hyperliquid_venue(*output),
                });
            }
        }

        transitions
    }

    #[must_use]
    pub fn transition_declarations(&self) -> Vec<TransitionDecl> {
        self.market_order_transitions()
            .into_iter()
            .filter_map(|edge| self.transition_decl_from_edge(edge))
            .collect()
    }

    #[must_use]
    pub fn select_transition_paths(
        &self,
        source_asset: &DepositAsset,
        destination_asset: &DepositAsset,
        max_depth: usize,
    ) -> Vec<TransitionPath> {
        self.select_transition_paths_between(
            MarketOrderNode::External(source_asset.clone()),
            MarketOrderNode::External(destination_asset.clone()),
            max_depth,
        )
    }

    #[must_use]
    pub fn select_transition_paths_between(
        &self,
        start: MarketOrderNode,
        goal: MarketOrderNode,
        max_depth: usize,
    ) -> Vec<TransitionPath> {
        if self.asset_for_node(&start).is_none() || self.asset_for_node(&goal).is_none() {
            return vec![];
        }

        #[derive(Debug, Clone)]
        struct DeclSearchState {
            node: MarketOrderNode,
            path: Vec<TransitionDecl>,
            visited: HashSet<MarketOrderNode>,
        }

        let transitions = self.transition_declarations_for_endpoints(&start, &goal);
        let mut queue = VecDeque::from([DeclSearchState {
            node: start.clone(),
            path: vec![],
            visited: HashSet::from([start]),
        }]);
        let mut paths = Vec::new();
        let max_depth = max_depth.max(1);

        while let Some(state) = queue.pop_front() {
            if state.node == goal && !state.path.is_empty() {
                let id = transition_path_id(&state.path);
                paths.push(TransitionPath {
                    id,
                    transitions: state.path.clone(),
                });
            }

            if state.path.len() >= max_depth {
                continue;
            }

            for transition in transitions.iter().filter(|edge| edge.from == state.node) {
                if state.visited.contains(&transition.to) && transition.to != goal {
                    continue;
                }
                let mut visited = state.visited.clone();
                visited.insert(transition.to.clone());
                let mut path = state.path.clone();
                path.push(transition.clone());
                queue.push_back(DeclSearchState {
                    node: transition.to.clone(),
                    path,
                    visited,
                });
            }
        }

        paths.sort_by_key(|path| path.transitions.len());
        let mut unique = Vec::new();
        let mut seen = HashSet::new();
        for path in paths {
            if seen.insert(path.id.clone()) {
                unique.push(path);
            }
        }
        unique
    }

    fn transition_declarations_for_endpoints(
        &self,
        start: &MarketOrderNode,
        goal: &MarketOrderNode,
    ) -> Vec<TransitionDecl> {
        let mut transitions = self.transition_declarations();
        transitions.extend(self.runtime_velora_transition_declarations(start, goal));
        dedupe_transition_declarations(transitions)
    }

    fn runtime_velora_transition_declarations(
        &self,
        start: &MarketOrderNode,
        goal: &MarketOrderNode,
    ) -> Vec<TransitionDecl> {
        let start_external = external_node_asset(start);
        let goal_external = external_node_asset(goal);
        let mut transitions = Vec::new();

        if let (Some(source), Some(destination)) = (start_external, goal_external) {
            if self.can_runtime_velora_edge(source, destination) {
                if let Some(transition) = self.velora_runtime_transition(source, destination) {
                    transitions.push(transition);
                }
            }
        }

        if let Some(source) = start_external {
            if is_evm_external_asset(source) {
                for anchor in self.velora_runtime_anchor_assets_on_chain(&source.chain) {
                    if self.can_runtime_velora_edge(source, &anchor) {
                        if let Some(transition) = self.velora_runtime_transition(source, &anchor) {
                            transitions.push(transition);
                        }
                    }
                }
            }
        }

        if let Some(destination) = goal_external {
            if is_evm_external_asset(destination) {
                for anchor in self.velora_runtime_anchor_assets_on_chain(&destination.chain) {
                    if self.can_runtime_velora_edge(&anchor, destination) {
                        if let Some(transition) =
                            self.velora_runtime_transition(&anchor, destination)
                        {
                            transitions.push(transition);
                        }
                    }
                }
            }
        }

        transitions
    }

    fn velora_runtime_anchor_assets_on_chain(&self, chain: &ChainId) -> Vec<DepositAsset> {
        self.chain_assets
            .iter()
            .filter(|entry| {
                &entry.chain == chain
                    && is_anchor_canonical(entry.canonical)
                    && entry.chain.evm_chain_id().is_some()
            })
            .map(ChainAsset::deposit_asset)
            .collect()
    }

    fn can_runtime_velora_edge(&self, source: &DepositAsset, destination: &DepositAsset) -> bool {
        is_evm_external_asset(source)
            && is_evm_external_asset(destination)
            && source.chain == destination.chain
            && source != destination
    }

    fn velora_runtime_transition(
        &self,
        source: &DepositAsset,
        destination: &DepositAsset,
    ) -> Option<TransitionDecl> {
        self.transition_decl_from_edge(MarketOrderTransition {
            kind: MarketOrderTransitionKind::UniversalRouterSwap,
            provider: ProviderId::Velora,
            from: MarketOrderNode::External(source.clone()),
            to: MarketOrderNode::External(destination.clone()),
        })
    }

    #[must_use]
    pub fn select_transition_path(
        &self,
        start: MarketOrderNode,
        goal: MarketOrderNode,
    ) -> Option<Vec<MarketOrderTransition>> {
        let transitions = self.market_order_transitions();
        let mut queue = VecDeque::from([SearchState {
            node: start.clone(),
            path: vec![],
            visited: HashSet::from([start]),
        }]);

        while let Some(state) = queue.pop_front() {
            if state.node == goal {
                return Some(state.path);
            }

            if state.path.len() >= 4 {
                continue;
            }

            for transition in transitions.iter().filter(|edge| edge.from == state.node) {
                if state.visited.contains(&transition.to) && transition.to != goal {
                    continue;
                }
                let mut visited = state.visited.clone();
                visited.insert(transition.to.clone());
                let mut path = state.path.clone();
                path.push(transition.clone());
                queue.push_back(SearchState {
                    node: transition.to.clone(),
                    path,
                    visited,
                });
            }
        }

        None
    }

    #[must_use]
    pub fn external_asset_for_canonical(
        &self,
        canonical: CanonicalAsset,
        preferred_chain: Option<&ChainId>,
    ) -> Option<DepositAsset> {
        if let Some(preferred_chain) = preferred_chain {
            if let Some(asset) = self
                .chain_assets
                .iter()
                .find(|entry| entry.canonical == canonical && &entry.chain == preferred_chain)
            {
                return Some(asset.deposit_asset());
            }
        }

        self.chain_assets
            .iter()
            .find(|entry| entry.canonical == canonical && entry.chain.as_str() != "hyperliquid")
            .map(ChainAsset::deposit_asset)
    }

    #[must_use]
    pub fn hyperliquid_coin_asset(
        &self,
        coin: &str,
        preferred_chain: Option<&ChainId>,
    ) -> Option<(CanonicalAsset, DepositAsset)> {
        let canonical = self.provider_assets.iter().find_map(|entry| {
            (entry.provider == ProviderId::Hyperliquid
                && entry.provider_asset.eq_ignore_ascii_case(coin))
            .then_some(entry.canonical)
        })?;
        let asset = self.external_asset_for_canonical(canonical, preferred_chain)?;
        Some((canonical, asset))
    }

    fn has_hyperliquid_venue(&self, canonical: CanonicalAsset) -> bool {
        self.provider_assets.iter().any(|entry| {
            entry.provider == ProviderId::Hyperliquid
                && entry.canonical == canonical
                && entry.supports(ProviderAssetCapability::ExchangeInput)
                && entry.supports(ProviderAssetCapability::ExchangeOutput)
        })
    }

    fn transition_decl_from_edge(&self, edge: MarketOrderTransition) -> Option<TransitionDecl> {
        let input_asset = self.asset_for_node(&edge.from)?;
        let output_asset = self.asset_for_node(&edge.to)?;
        let (input_role, output_role) = required_roles_for_transition_kind(edge.kind);
        let id = format!(
            "{}:{}:{}:{}->{}:{}",
            edge.kind.as_str(),
            edge.provider.as_str(),
            input_asset.chain.as_str(),
            input_asset.asset.as_str(),
            output_asset.chain.as_str(),
            output_asset.asset.as_str()
        );
        Some(TransitionDecl {
            id,
            kind: edge.kind,
            provider: edge.provider,
            input: AssetSlot {
                asset: input_asset,
                required_custody_role: input_role,
            },
            output: AssetSlot {
                asset: output_asset,
                required_custody_role: output_role,
            },
            from: edge.from,
            to: edge.to,
        })
    }

    fn asset_for_node(&self, node: &MarketOrderNode) -> Option<DepositAsset> {
        match node {
            MarketOrderNode::External(asset) => Some(asset.clone()),
            MarketOrderNode::Venue {
                provider: ProviderId::Hyperliquid,
                canonical,
            } => self
                .chain_assets
                .iter()
                .find(|entry| {
                    entry.canonical == *canonical && entry.chain.as_str() == "hyperliquid"
                })
                .map(ChainAsset::deposit_asset)
                .or_else(|| self.external_asset_for_canonical(*canonical, None)),
            MarketOrderNode::Venue { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
struct SearchState {
    node: MarketOrderNode,
    path: Vec<MarketOrderTransition>,
    visited: HashSet<MarketOrderNode>,
}

fn external_node_asset(node: &MarketOrderNode) -> Option<&DepositAsset> {
    match node {
        MarketOrderNode::External(asset) => Some(asset),
        MarketOrderNode::Venue { .. } => None,
    }
}

fn is_evm_external_asset(asset: &DepositAsset) -> bool {
    asset.chain.evm_chain_id().is_some()
}

fn dedupe_transition_declarations(transitions: Vec<TransitionDecl>) -> Vec<TransitionDecl> {
    let mut deduped = Vec::new();
    let mut seen = HashSet::new();
    for transition in transitions {
        if seen.insert(transition.id.clone()) {
            deduped.push(transition);
        }
    }
    deduped
}

fn hyperliquid_venue(canonical: CanonicalAsset) -> MarketOrderNode {
    MarketOrderNode::Venue {
        provider: ProviderId::Hyperliquid,
        canonical,
    }
}

fn required_roles_for_transition_kind(
    kind: MarketOrderTransitionKind,
) -> (RequiredCustodyRole, RequiredCustodyRole) {
    match kind {
        MarketOrderTransitionKind::AcrossBridge | MarketOrderTransitionKind::CctpBridge => (
            RequiredCustodyRole::SourceOrIntermediate,
            RequiredCustodyRole::IntermediateExecution,
        ),
        MarketOrderTransitionKind::UnitDeposit => (
            RequiredCustodyRole::SourceOrIntermediate,
            RequiredCustodyRole::HyperliquidSpot,
        ),
        MarketOrderTransitionKind::HyperliquidBridgeDeposit => (
            RequiredCustodyRole::IntermediateExecution,
            RequiredCustodyRole::IntermediateExecution,
        ),
        MarketOrderTransitionKind::HyperliquidBridgeWithdrawal => (
            RequiredCustodyRole::HyperliquidSpot,
            RequiredCustodyRole::IntermediateExecution,
        ),
        MarketOrderTransitionKind::HyperliquidTrade => (
            RequiredCustodyRole::IntermediateExecution,
            RequiredCustodyRole::IntermediateExecution,
        ),
        MarketOrderTransitionKind::UniversalRouterSwap => (
            RequiredCustodyRole::SourceOrIntermediate,
            RequiredCustodyRole::IntermediateExecution,
        ),
        MarketOrderTransitionKind::UnitWithdrawal => (
            RequiredCustodyRole::IntermediateExecution,
            RequiredCustodyRole::DestinationPayout,
        ),
    }
}

fn transition_path_id(transitions: &[TransitionDecl]) -> String {
    transitions
        .iter()
        .map(|transition| transition.id.as_str())
        .collect::<Vec<_>>()
        .join(">")
}

fn is_anchor_canonical(canonical: CanonicalAsset) -> bool {
    matches!(
        canonical,
        CanonicalAsset::Eth | CanonicalAsset::Usdc | CanonicalAsset::Usdt | CanonicalAsset::Cbbtc
    )
}

fn builtin_chain_assets() -> Vec<ChainAsset> {
    // Router-known external/venue assets. Adding a new asset to routing begins
    // by registering its canonical identity and chain-local representation
    // here.
    vec![
        chain_asset(CanonicalAsset::Btc, "bitcoin", AssetId::Native, 8),
        chain_asset(CanonicalAsset::Eth, "evm:1", AssetId::Native, 18),
        chain_asset(
            CanonicalAsset::Usdc,
            "evm:1",
            AssetId::reference("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            6,
        ),
        chain_asset(
            CanonicalAsset::Usdt,
            "evm:1",
            AssetId::reference("0xdac17f958d2ee523a2206206994597c13d831ec7"),
            6,
        ),
        chain_asset(
            CanonicalAsset::Cbbtc,
            "evm:1",
            AssetId::reference("0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"),
            8,
        ),
        chain_asset(
            CanonicalAsset::Usdc,
            "evm:42161",
            AssetId::reference("0xaf88d065e77c8cc2239327c5edb3a432268e5831"),
            6,
        ),
        chain_asset(
            CanonicalAsset::Usdt,
            "evm:42161",
            AssetId::reference("0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"),
            6,
        ),
        chain_asset(
            CanonicalAsset::Cbbtc,
            "evm:42161",
            AssetId::reference("0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"),
            8,
        ),
        chain_asset(CanonicalAsset::Eth, "evm:8453", AssetId::Native, 18),
        chain_asset(
            CanonicalAsset::Usdc,
            "evm:8453",
            AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
            6,
        ),
        chain_asset(
            CanonicalAsset::Usdt,
            "evm:8453",
            AssetId::reference("0xfde4c96c8593536e31f229ea8f37b2ada2699bb2"),
            6,
        ),
        chain_asset(
            CanonicalAsset::Cbbtc,
            "evm:8453",
            AssetId::reference("0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"),
            8,
        ),
        // Hyperliquid spot assets. These are venue balances, not public
        // user-facing start/end assets; Unit deposits/withdrawals bridge
        // between these venue assets and their external chain representations.
        chain_asset(
            CanonicalAsset::Btc,
            "hyperliquid",
            AssetId::reference("UBTC"),
            8,
        ),
        chain_asset(
            CanonicalAsset::Eth,
            "hyperliquid",
            AssetId::reference("UETH"),
            18,
        ),
        chain_asset(CanonicalAsset::Usdc, "hyperliquid", AssetId::Native, 6),
    ]
}

fn builtin_provider_assets() -> Vec<ProviderAsset> {
    // Provider capability declarations drive transition discovery. A new asset
    // does not become reachable until the relevant provider capability rows are
    // added here.
    vec![
        provider_asset(
            ProviderId::Unit,
            CanonicalAsset::Btc,
            "bitcoin",
            "bitcoin",
            "btc",
            8,
            &[
                ProviderAssetCapability::UnitDeposit,
                ProviderAssetCapability::UnitWithdrawal,
            ],
        ),
        provider_asset(
            ProviderId::Unit,
            CanonicalAsset::Eth,
            "evm:1",
            "ethereum",
            "eth",
            18,
            &[
                ProviderAssetCapability::UnitDeposit,
                ProviderAssetCapability::UnitWithdrawal,
            ],
        ),
        provider_asset(
            ProviderId::Unit,
            CanonicalAsset::Eth,
            "evm:8453",
            "base",
            "eth",
            18,
            &[ProviderAssetCapability::UnitWithdrawal],
        ),
        provider_asset(
            ProviderId::Cctp,
            CanonicalAsset::Usdc,
            "evm:1",
            "0",
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            6,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Cctp,
            CanonicalAsset::Usdc,
            "evm:42161",
            "3",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            6,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Cctp,
            CanonicalAsset::Usdc,
            "evm:8453",
            "6",
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            6,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Across,
            CanonicalAsset::Eth,
            "evm:1",
            "1",
            "native",
            18,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Across,
            CanonicalAsset::Usdc,
            "evm:1",
            "1",
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            6,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Across,
            CanonicalAsset::Usdt,
            "evm:1",
            "1",
            "0xdac17f958d2ee523a2206206994597c13d831ec7",
            6,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Across,
            CanonicalAsset::Cbbtc,
            "evm:1",
            "1",
            "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
            8,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Across,
            CanonicalAsset::Usdc,
            "evm:42161",
            "42161",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            6,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Across,
            CanonicalAsset::Usdt,
            "evm:42161",
            "42161",
            "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
            6,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Across,
            CanonicalAsset::Cbbtc,
            "evm:42161",
            "42161",
            "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
            8,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Across,
            CanonicalAsset::Eth,
            "evm:8453",
            "8453",
            "native",
            18,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Across,
            CanonicalAsset::Usdt,
            "evm:8453",
            "8453",
            "0xfde4c96c8593536e31f229ea8f37b2ada2699bb2",
            6,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Across,
            CanonicalAsset::Cbbtc,
            "evm:8453",
            "8453",
            "0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf",
            8,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Across,
            CanonicalAsset::Usdc,
            "evm:8453",
            "8453",
            "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
            6,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::HyperliquidBridge,
            CanonicalAsset::Usdc,
            "evm:42161",
            "42161",
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            6,
            &[
                ProviderAssetCapability::BridgeInput,
                ProviderAssetCapability::BridgeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Hyperliquid,
            CanonicalAsset::Btc,
            "hyperliquid",
            "hypercore",
            "UBTC",
            8,
            &[
                ProviderAssetCapability::ExchangeInput,
                ProviderAssetCapability::ExchangeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Hyperliquid,
            CanonicalAsset::Eth,
            "hyperliquid",
            "hypercore",
            "UETH",
            18,
            &[
                ProviderAssetCapability::ExchangeInput,
                ProviderAssetCapability::ExchangeOutput,
            ],
        ),
        provider_asset(
            ProviderId::Hyperliquid,
            CanonicalAsset::Usdc,
            "hyperliquid",
            "hypercore",
            "USDC",
            6,
            &[
                ProviderAssetCapability::ExchangeInput,
                ProviderAssetCapability::ExchangeOutput,
            ],
        ),
    ]
}

fn chain_asset(
    canonical: CanonicalAsset,
    chain: &'static str,
    asset: AssetId,
    decimals: u8,
) -> ChainAsset {
    ChainAsset {
        canonical,
        chain: ChainId::from_trusted_static(chain),
        asset,
        decimals,
    }
}

fn provider_asset(
    provider: ProviderId,
    canonical: CanonicalAsset,
    chain: &'static str,
    provider_chain: &'static str,
    provider_asset: &'static str,
    decimals: u8,
    capabilities: &[ProviderAssetCapability],
) -> ProviderAsset {
    ProviderAsset {
        provider,
        canonical,
        chain: ChainId::from_trusted_static(chain),
        provider_chain: provider_chain.to_string(),
        provider_asset: provider_asset.to_string(),
        decimals,
        capabilities: capabilities.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asset(chain: &str, asset: AssetId) -> DepositAsset {
        DepositAsset {
            chain: ChainId::parse(chain).unwrap(),
            asset,
        }
    }

    #[test]
    fn maps_chain_assets_to_canonical_assets() {
        let registry = AssetRegistry::default();

        assert_eq!(
            registry.canonical_for(&asset("evm:1", AssetId::Native)),
            Some(CanonicalAsset::Eth)
        );
        assert_eq!(
            registry.canonical_for(&asset("evm:8453", AssetId::Native)),
            Some(CanonicalAsset::Eth)
        );
        assert_eq!(
            registry.canonical_for(&asset(
                "evm:1",
                AssetId::reference("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
            )),
            Some(CanonicalAsset::Usdc)
        );
        assert_eq!(
            registry.canonical_for(&asset("bitcoin", AssetId::Native)),
            Some(CanonicalAsset::Btc)
        );
        assert_eq!(
            registry.canonical_for(&asset("hyperliquid", AssetId::reference("UBTC"))),
            Some(CanonicalAsset::Btc)
        );
        assert_eq!(
            registry.canonical_for(&asset("hyperliquid", AssetId::reference("UETH"))),
            Some(CanonicalAsset::Eth)
        );
        assert_eq!(
            registry.canonical_for(&asset(
                "evm:8453",
                AssetId::reference("0xfde4c96c8593536e31f229ea8f37b2ada2699bb2"),
            )),
            Some(CanonicalAsset::Usdt)
        );
        assert_eq!(
            registry.canonical_for(&asset(
                "evm:42161",
                AssetId::reference("0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"),
            )),
            Some(CanonicalAsset::Cbbtc)
        );
    }

    #[test]
    fn exposes_provider_specific_representations_without_changing_router_asset_type() {
        let registry = AssetRegistry::default();

        let unit_eth = registry
            .provider_asset(
                ProviderId::Unit,
                &asset("evm:1", AssetId::Native),
                ProviderAssetCapability::UnitDeposit,
            )
            .unwrap();
        assert_eq!(unit_eth.provider_chain, "ethereum");
        assert_eq!(unit_eth.provider_asset, "eth");

        let hyperliquid_btc = registry
            .provider_asset(
                ProviderId::Hyperliquid,
                &asset("hyperliquid", AssetId::reference("UBTC")),
                ProviderAssetCapability::ExchangeOutput,
            )
            .unwrap();
        assert_eq!(hyperliquid_btc.provider_chain, "hypercore");
        assert_eq!(hyperliquid_btc.provider_asset, "UBTC");
    }

    #[test]
    fn provider_venue_kinds_classify_current_providers() {
        assert_eq!(
            ProviderId::Across.venue_kind(),
            ProviderVenueKind::CrossChain
        );
        assert_eq!(ProviderId::Cctp.venue_kind(), ProviderVenueKind::CrossChain);
        assert_eq!(ProviderId::Unit.venue_kind(), ProviderVenueKind::CrossChain);
        assert_eq!(
            ProviderId::HyperliquidBridge.venue_kind(),
            ProviderVenueKind::CrossChain
        );
        assert_eq!(
            ProviderId::Hyperliquid.venue_kind(),
            ProviderVenueKind::MonoChain {
                mono_chain_kind: MonoChainVenueKind::FixedPairExchange,
            }
        );
        assert_eq!(
            ProviderId::Velora.venue_kind(),
            ProviderVenueKind::MonoChain {
                mono_chain_kind: MonoChainVenueKind::UniversalRouter,
            }
        );
    }

    #[test]
    fn provider_asset_support_models_classify_current_providers() {
        assert_eq!(
            ProviderId::Across.asset_support_model(),
            AssetSupportModel::RuntimeEnumerated
        );
        assert_eq!(
            ProviderId::Cctp.asset_support_model(),
            AssetSupportModel::StaticDeclared
        );
        assert_eq!(
            ProviderId::Unit.asset_support_model(),
            AssetSupportModel::StaticDeclared
        );
        assert_eq!(
            ProviderId::HyperliquidBridge.asset_support_model(),
            AssetSupportModel::StaticDeclared
        );
        assert_eq!(
            ProviderId::Hyperliquid.asset_support_model(),
            AssetSupportModel::StaticDeclared
        );
        assert_eq!(
            ProviderId::Velora.asset_support_model(),
            AssetSupportModel::OpenAddressQuote
        );
    }

    #[test]
    fn transition_kinds_expose_route_edge_semantics() {
        assert_eq!(
            MarketOrderTransitionKind::AcrossBridge.route_edge_kind(),
            RouteEdgeKind::CrossChainTransfer
        );
        assert_eq!(
            MarketOrderTransitionKind::CctpBridge.route_edge_kind(),
            RouteEdgeKind::CrossChainTransfer
        );
        assert_eq!(
            MarketOrderTransitionKind::UnitDeposit.route_edge_kind(),
            RouteEdgeKind::CrossChainTransfer
        );
        assert_eq!(
            MarketOrderTransitionKind::HyperliquidBridgeDeposit.route_edge_kind(),
            RouteEdgeKind::CrossChainTransfer
        );
        assert_eq!(
            MarketOrderTransitionKind::HyperliquidBridgeWithdrawal.route_edge_kind(),
            RouteEdgeKind::CrossChainTransfer
        );
        assert_eq!(
            MarketOrderTransitionKind::UnitWithdrawal.route_edge_kind(),
            RouteEdgeKind::CrossChainTransfer
        );
        assert_eq!(
            MarketOrderTransitionKind::HyperliquidTrade.route_edge_kind(),
            RouteEdgeKind::FixedPairSwap
        );
        assert_eq!(
            MarketOrderTransitionKind::UniversalRouterSwap.route_edge_kind(),
            RouteEdgeKind::UniversalRouterSwap
        );
    }

    #[test]
    fn generated_transitions_keep_provider_and_edge_semantics_aligned() {
        let registry = AssetRegistry::default();

        for transition in registry.market_order_transitions() {
            match transition.kind {
                MarketOrderTransitionKind::HyperliquidTrade => {
                    assert_eq!(transition.route_edge_kind(), RouteEdgeKind::FixedPairSwap);
                    assert_eq!(
                        transition.provider_venue_kind(),
                        ProviderVenueKind::MonoChain {
                            mono_chain_kind: MonoChainVenueKind::FixedPairExchange,
                        }
                    );
                    assert!(transition.route_edge_kind().is_mono_chain());
                    assert!(transition.provider_venue_kind().is_mono_chain());
                }
                MarketOrderTransitionKind::UniversalRouterSwap => {
                    assert_eq!(
                        transition.route_edge_kind(),
                        RouteEdgeKind::UniversalRouterSwap
                    );
                    assert_eq!(
                        transition.provider_venue_kind(),
                        ProviderVenueKind::MonoChain {
                            mono_chain_kind: MonoChainVenueKind::UniversalRouter,
                        }
                    );
                    assert!(transition.route_edge_kind().is_mono_chain());
                    assert!(transition.provider_venue_kind().is_mono_chain());
                }
                MarketOrderTransitionKind::AcrossBridge
                | MarketOrderTransitionKind::CctpBridge
                | MarketOrderTransitionKind::UnitDeposit
                | MarketOrderTransitionKind::HyperliquidBridgeDeposit
                | MarketOrderTransitionKind::HyperliquidBridgeWithdrawal
                | MarketOrderTransitionKind::UnitWithdrawal => {
                    assert_eq!(
                        transition.route_edge_kind(),
                        RouteEdgeKind::CrossChainTransfer
                    );
                    assert_eq!(
                        transition.provider_venue_kind(),
                        ProviderVenueKind::CrossChain
                    );
                    assert!(transition.route_edge_kind().is_cross_chain());
                    assert!(transition.provider_venue_kind().is_cross_chain());
                }
            }
        }
    }

    #[test]
    fn market_order_transitions_encode_provider_directions() {
        let registry = AssetRegistry::default();
        let transitions = registry.market_order_transitions();

        assert!(transitions.iter().any(|transition| {
            transition.kind == MarketOrderTransitionKind::AcrossBridge
                && transition.from == MarketOrderNode::External(asset("evm:8453", AssetId::Native))
                && transition.to == MarketOrderNode::External(asset("evm:1", AssetId::Native))
        }));
        assert!(transitions.iter().any(|transition| {
            transition.kind == MarketOrderTransitionKind::AcrossBridge
                && transition.from == MarketOrderNode::External(asset("evm:1", AssetId::Native))
                && transition.to == MarketOrderNode::External(asset("evm:8453", AssetId::Native))
        }));
        assert!(transitions.iter().any(|transition| {
            transition.kind == MarketOrderTransitionKind::CctpBridge
                && transition.from
                    == MarketOrderNode::External(asset(
                        "evm:8453",
                        AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
                    ))
                && transition.to
                    == MarketOrderNode::External(asset(
                        "evm:42161",
                        AssetId::reference("0xaf88d065e77c8cc2239327c5edb3a432268e5831"),
                    ))
        }));
        assert!(transitions.iter().any(|transition| {
            transition.kind == MarketOrderTransitionKind::HyperliquidBridgeDeposit
                && transition.from
                    == MarketOrderNode::External(asset(
                        "evm:42161",
                        AssetId::reference("0xaf88d065e77c8cc2239327c5edb3a432268e5831"),
                    ))
                && transition.to == hyperliquid_venue(CanonicalAsset::Usdc)
        }));
        assert!(transitions.iter().any(|transition| {
            transition.kind == MarketOrderTransitionKind::UnitDeposit
                && transition.from == MarketOrderNode::External(asset("bitcoin", AssetId::Native))
        }));
        assert!(transitions.iter().any(|transition| {
            transition.kind == MarketOrderTransitionKind::UnitWithdrawal
                && transition.to == MarketOrderNode::External(asset("evm:8453", AssetId::Native))
        }));
    }

    #[test]
    fn hyperliquid_spot_transitions_only_trade_directly_against_usdc() {
        let registry = AssetRegistry::default();
        let transitions = registry.market_order_transitions();

        assert!(!transitions.iter().any(|transition| {
            transition.kind == MarketOrderTransitionKind::HyperliquidTrade
                && transition.from == hyperliquid_venue(CanonicalAsset::Eth)
                && transition.to == hyperliquid_venue(CanonicalAsset::Btc)
        }));
        assert!(transitions.iter().any(|transition| {
            transition.kind == MarketOrderTransitionKind::HyperliquidTrade
                && transition.from == hyperliquid_venue(CanonicalAsset::Eth)
                && transition.to == hyperliquid_venue(CanonicalAsset::Usdc)
        }));
        assert!(transitions.iter().any(|transition| {
            transition.kind == MarketOrderTransitionKind::HyperliquidTrade
                && transition.from == hyperliquid_venue(CanonicalAsset::Usdc)
                && transition.to == hyperliquid_venue(CanonicalAsset::Btc)
        }));
    }

    #[test]
    fn transition_declarations_use_hyperliquid_venue_assets_inside_hyperliquid() {
        let registry = AssetRegistry::default();
        let declarations = registry.transition_declarations();
        let hl_usdc = asset("hyperliquid", AssetId::Native);
        let hl_eth = asset("hyperliquid", AssetId::reference("UETH"));
        let base_eth = asset("evm:8453", AssetId::Native);

        assert!(declarations.iter().any(|transition| {
            transition.kind == MarketOrderTransitionKind::HyperliquidTrade
                && transition.input.asset == hl_usdc
                && transition.output.asset == hl_eth
        }));
        assert!(declarations.iter().any(|transition| {
            transition.kind == MarketOrderTransitionKind::UnitWithdrawal
                && transition.input.asset == hl_eth
                && transition.output.asset == base_eth
        }));
    }

    #[test]
    fn anchor_assets_include_usdt_and_cbbtc() {
        let registry = AssetRegistry::default();
        let anchors = registry.anchor_assets();

        assert!(anchors.contains(&asset(
            "evm:8453",
            AssetId::reference("0xfde4c96c8593536e31f229ea8f37b2ada2699bb2"),
        )));
        assert!(anchors.contains(&asset(
            "evm:42161",
            AssetId::reference("0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"),
        )));
    }

    #[test]
    fn velora_runtime_anchor_assets_include_usdt_and_cbbtc() {
        let registry = AssetRegistry::default();
        let base = ChainId::parse("evm:8453").unwrap();
        let arbitrum = ChainId::parse("evm:42161").unwrap();
        let mut anchors = registry.velora_runtime_anchor_assets_on_chain(&base);
        anchors.extend(registry.velora_runtime_anchor_assets_on_chain(&arbitrum));

        assert!(anchors.contains(&asset(
            "evm:8453",
            AssetId::reference("0xfde4c96c8593536e31f229ea8f37b2ada2699bb2"),
        )));
        assert!(anchors.contains(&asset(
            "evm:42161",
            AssetId::reference("0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf"),
        )));
    }

    #[test]
    fn arbitrary_evm_source_can_route_to_bitcoin_through_velora_anchor() {
        let registry = AssetRegistry::default();
        let random_base_token = asset(
            "evm:8453",
            AssetId::reference("0x1111111111111111111111111111111111111111"),
        );
        let btc = asset("bitcoin", AssetId::Native);

        let paths = registry.select_transition_paths(&random_base_token, &btc, 5);

        assert!(paths.iter().any(|path| {
            path.transitions.first().is_some_and(|transition| {
                transition.kind == MarketOrderTransitionKind::UniversalRouterSwap
                    && transition.input.asset == random_base_token
                    && transition.output.asset.chain == random_base_token.chain
                    && registry.chain_asset(&transition.output.asset).is_some()
            }) && path.transitions.last().is_some_and(|transition| {
                transition.kind == MarketOrderTransitionKind::UnitWithdrawal
            })
        }));
    }

    #[test]
    fn base_usdc_can_route_to_bitcoin_through_cctp_and_hyperliquid() {
        let registry = AssetRegistry::default();
        let base_usdc = asset(
            "evm:8453",
            AssetId::reference("0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"),
        );
        let btc = asset("bitcoin", AssetId::Native);

        let paths = registry.select_transition_paths(&base_usdc, &btc, 5);

        assert!(paths.iter().any(|path| {
            path.transitions
                .iter()
                .map(|transition| transition.kind)
                .collect::<Vec<_>>()
                == vec![
                    MarketOrderTransitionKind::CctpBridge,
                    MarketOrderTransitionKind::HyperliquidBridgeDeposit,
                    MarketOrderTransitionKind::HyperliquidTrade,
                    MarketOrderTransitionKind::UnitWithdrawal,
                ]
        }));
    }

    #[test]
    fn bitcoin_can_route_to_arbitrary_evm_destination_through_velora_exit() {
        let registry = AssetRegistry::default();
        let btc = asset("bitcoin", AssetId::Native);
        let random_base_token = asset(
            "evm:8453",
            AssetId::reference("0x2222222222222222222222222222222222222222"),
        );

        let paths = registry.select_transition_paths(&btc, &random_base_token, 5);

        assert!(paths.iter().any(|path| {
            path.transitions
                .first()
                .is_some_and(|transition| transition.kind == MarketOrderTransitionKind::UnitDeposit)
                && path.transitions.last().is_some_and(|transition| {
                    transition.kind == MarketOrderTransitionKind::UniversalRouterSwap
                        && transition.output.asset == random_base_token
                        && transition.input.asset.chain == random_base_token.chain
                        && registry.chain_asset(&transition.input.asset).is_some()
                })
        }));
    }

    #[test]
    fn arbitrary_evm_source_can_route_to_arbitrary_same_chain_destination_via_anchor() {
        let registry = AssetRegistry::default();
        let random_base_input = asset(
            "evm:8453",
            AssetId::reference("0x3333333333333333333333333333333333333333"),
        );
        let random_base_output = asset(
            "evm:8453",
            AssetId::reference("0x4444444444444444444444444444444444444444"),
        );

        let paths = registry.select_transition_paths(&random_base_input, &random_base_output, 3);

        assert!(paths.iter().any(|path| {
            path.transitions.len() == 2
                && path.transitions.iter().all(|transition| {
                    transition.kind == MarketOrderTransitionKind::UniversalRouterSwap
                })
                && path.transitions[0].input.asset == random_base_input
                && path.transitions[0].output.asset.chain == random_base_input.chain
                && registry
                    .chain_asset(&path.transitions[0].output.asset)
                    .is_some()
                && path.transitions[1].input.asset == path.transitions[0].output.asset
                && path.transitions[1].output.asset == random_base_output
        }));
    }

    #[test]
    fn transition_paths_model_eth_to_btc_as_two_hyperliquid_trade_hops() {
        let registry = AssetRegistry::default();
        let paths = registry.select_transition_paths(
            &asset("evm:1", AssetId::Native),
            &asset("bitcoin", AssetId::Native),
            5,
        );

        assert!(paths.iter().any(|path| {
            path.transitions
                .iter()
                .map(|transition| transition.kind)
                .collect::<Vec<_>>()
                == vec![
                    MarketOrderTransitionKind::UnitDeposit,
                    MarketOrderTransitionKind::HyperliquidTrade,
                    MarketOrderTransitionKind::HyperliquidTrade,
                    MarketOrderTransitionKind::UnitWithdrawal,
                ]
        }));
    }

    #[test]
    fn transition_paths_between_nodes_allow_refund_style_hl_to_source_routes() {
        let registry = AssetRegistry::default();
        let paths = registry.select_transition_paths_between(
            hyperliquid_venue(CanonicalAsset::Usdc),
            MarketOrderNode::External(asset("evm:8453", AssetId::Native)),
            5,
        );

        assert!(paths.iter().any(|path| {
            path.transitions
                .iter()
                .map(|transition| transition.kind)
                .collect::<Vec<_>>()
                == vec![
                    MarketOrderTransitionKind::HyperliquidTrade,
                    MarketOrderTransitionKind::UnitWithdrawal,
                    MarketOrderTransitionKind::AcrossBridge,
                ]
        }));
    }

    #[test]
    fn unit_support_distinguishes_deposit_from_withdrawal() {
        let registry = AssetRegistry::default();
        let base_eth = asset("evm:8453", AssetId::Native);

        assert!(!registry.supports_provider_capability(
            ProviderId::Unit,
            &base_eth,
            ProviderAssetCapability::UnitDeposit,
        ));
        assert!(registry.supports_provider_capability(
            ProviderId::Unit,
            &base_eth,
            ProviderAssetCapability::UnitWithdrawal,
        ));
    }

    #[test]
    fn unit_deposit_ingress_is_limited_to_bitcoin_and_ethereum_eth() {
        let registry = AssetRegistry::default();

        assert!(registry.supports_provider_capability(
            ProviderId::Unit,
            &asset("bitcoin", AssetId::Native),
            ProviderAssetCapability::UnitDeposit,
        ));
        assert!(registry.supports_provider_capability(
            ProviderId::Unit,
            &asset("evm:1", AssetId::Native),
            ProviderAssetCapability::UnitDeposit,
        ));
        assert!(!registry.supports_provider_capability(
            ProviderId::Unit,
            &asset("evm:8453", AssetId::Native),
            ProviderAssetCapability::UnitDeposit,
        ));
    }
}

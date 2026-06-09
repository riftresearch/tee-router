import { useCallback, useEffect, useMemo, useState } from 'react'
import {
  ReactFlow,
  Background,
  Controls,
  MarkerType,
  Position,
  type Edge,
  type Node
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { Play, RefreshCw, Zap } from 'lucide-react'

import { fetchRouteCosts, fetchRouteGraph, postRouteExplain } from './api'
import type {
  RouteExplain,
  RouteExplainGraph,
  RouteExplainGraphNode,
  RouteGraphEdge,
  RouteGraphNode,
  RouteGraphResponse
} from './types'

// USD tier ladder mirrored from `route_costs::ROUTE_COST_TIERS`. The dropdown
// drives both the bps edge labels (matched against the snapshot `amountBucket`)
// and a representative `amount_in` filled into the editable amount field.
const ROUTE_TIERS: Array<{ label: string; usd: number }> = [
  { label: 'usd_100', usd: 100 },
  { label: 'usd_1000', usd: 1_000 },
  { label: 'usd_10000', usd: 10_000 },
  { label: 'usd_25000', usd: 25_000 },
  { label: 'usd_50000', usd: 50_000 },
  { label: 'usd_75000', usd: 75_000 },
  { label: 'usd_100000', usd: 100_000 },
  { label: 'usd_200000', usd: 200_000 },
  { label: 'usd_500000', usd: 500_000 },
  { label: 'usd_1000000', usd: 1_000_000 },
  { label: 'usd_5000000', usd: 5_000_000 },
  { label: 'usd_10000000', usd: 10_000_000 }
]

// Rough unit prices (USD) per canonical asset, only used to translate a USD
// tier preset into a plausible base-unit `amount_in`. The backend recomputes
// the true tier from live pricing and echoes it back, so these need only be
// in the right ballpark.
const CANONICAL_PRICE_USD: Record<string, number> = {
  btc: 100_000,
  eth: 3_500,
  usdc: 1,
  usdt: 1
}

// Example arbitrary ERC20s (not part of the curated graph) offered in the
// From/To pickers so the visualizer can demonstrate the runtime Velora wrap:
// the engine swaps the random token <-> an anchor canonical (USDC/USDT/ETH) on
// its chain via Velora, then routes the rest through the cached graph. The
// backend accepts any valid EVM token address here; these are just convenient
// presets. `canonical` doubles as the display symbol.
const EXAMPLE_ERC20S: RouteGraphNode[] = [
  {
    key: 'erc20:evm:1:pepe',
    kind: 'external',
    chain: 'evm:1',
    asset: '0x6982508145454ce325ddbe47a25d4ec3d2311933',
    canonical: 'pepe',
    decimals: 18
  },
  {
    key: 'erc20:evm:1:link',
    kind: 'external',
    chain: 'evm:1',
    asset: '0x514910771af9ca656af840dff83e8264ecf986ca',
    canonical: 'link',
    decimals: 18
  },
  {
    key: 'erc20:evm:1:shib',
    kind: 'external',
    chain: 'evm:1',
    asset: '0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce',
    canonical: 'shib',
    decimals: 18
  },
  {
    key: 'erc20:evm:8453:degen',
    kind: 'external',
    chain: 'evm:8453',
    asset: '0x4ed4e862860bed51a9570b96d89af5e1b0efefed',
    canonical: 'degen',
    decimals: 18
  },
  {
    key: 'erc20:evm:42161:arb',
    kind: 'external',
    chain: 'evm:42161',
    asset: '0x912ce59144191c1204e64559fe8253a0e49e6548',
    canonical: 'arb',
    decimals: 18
  }
]

const EXAMPLE_ERC20_BY_REF = new Map(
  EXAMPLE_ERC20S.map((token) => [`${token.chain}:${token.asset}`, token])
)

// Left-to-right column ordering for the layered layout.
const CHAIN_COLUMN_ORDER = [
  'bitcoin',
  'evm:1',
  'evm:8453',
  'evm:42161',
  'evm:999',
  'venue:hyperliquid'
]

const CHAIN_LABELS: Record<string, string> = {
  bitcoin: 'Bitcoin',
  'evm:1': 'Ethereum',
  'evm:8453': 'Base',
  'evm:42161': 'Arbitrum',
  'evm:999': 'HyperEVM',
  'venue:hyperliquid': 'Hyperliquid',
  'venue:unit': 'Unit'
}

const PROVIDER_COLOR: Record<string, string> = {
  across: '#f59e0b',
  cctp: '#3b82f6',
  unit: '#ec4899',
  velora: '#a855f7',
  hyperliquid: '#14b8a6',
  hyperliquid_bridge: '#14b8a6',
  hypercore_bridge: '#0ea5e9'
}

// Friendly venue names shown on edge labels and in the ranked-path detail.
const PROVIDER_LABEL: Record<string, string> = {
  across: 'Across',
  cctp: 'CCTP',
  unit: 'Unit',
  velora: 'Velora',
  hyperliquid: 'HyperCore',
  hyperliquid_bridge: 'HL Bridge',
  hypercore_bridge: 'HC Bridge'
}

const COLUMN_GAP = 280
const ROW_GAP = 78
const COLUMN_TOP = 70

const EDGE_DEFAULT = '#3a423d'
const EDGE_FAINT = '#52525b'
// Top-3 ranking colors: #1 green, #2 amber, #3 orange. Lower ranks fall back to
// a muted purple so they read as "also viable, but not in the top three".
// Best-route accent used on the graph. Only the winner is colored now (2nd/3rd
// are intentionally not highlighted on the canvas); index 0 is the green accent.
const RANK_COLORS = ['#34d399']

type HighlightKind = 'winner' | 'top' | 'ranked' | 'none'

// Per-edge ranking info derived from the explain result: the best (lowest) rank
// of any path traversing the edge, and whether the single "best" path uses it.
type EdgeRank = { rank: number; isBest: boolean }

export function RouteGraphView() {
  const [graph, setGraph] = useState<RouteGraphResponse | null>(null)
  const [bpsByTransition, setBpsByTransition] = useState<Map<string, number>>(
    new Map()
  )
  const [loadError, setLoadError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)

  const [tierLabel, setTierLabel] = useState('usd_1000')
  const [fromKey, setFromKey] = useState('')
  const [toKey, setToKey] = useState('')
  const [amountIn, setAmountIn] = useState('')
  const [liveQuote, setLiveQuote] = useState(false)

  const [explain, setExplain] = useState<RouteExplain | null>(null)
  const [explainError, setExplainError] = useState<string | null>(null)
  const [ranking, setRanking] = useState(false)

  const loadGraph = useCallback(async () => {
    setLoading(true)
    setLoadError(null)
    try {
      const [graphResponse, costs] = await Promise.all([
        fetchRouteGraph(),
        fetchRouteCosts().catch(() => null)
      ])
      setGraph(graphResponse)
      if (costs) {
        rebuildBpsMap(costs.snapshots, setBpsByTransition)
      }
    } catch (error) {
      setLoadError(error instanceof Error ? error.message : 'Failed to load graph')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void loadGraph()
  }, [loadGraph])

  // Refresh bps labels when the tier changes (cheap; reuses cached snapshots).
  useEffect(() => {
    let cancelled = false
    void fetchRouteCosts()
      .then((costs) => {
        if (cancelled) return
        rebuildBpsMapForTier(costs.snapshots, tierLabel, setBpsByTransition)
      })
      .catch(() => undefined)
    return () => {
      cancelled = true
    }
  }, [tierLabel])

  const coreNodes = useMemo(
    () =>
      (graph?.nodes ?? [])
        .filter((node) => node.kind === 'external')
        .sort((a, b) => a.key.localeCompare(b.key)),
    [graph]
  )

  // Core graph assets plus the example ERC20 presets; this is the full set of
  // selectable From/To assets and the lookup table for node resolution/labels.
  const selectableAssets = useMemo(
    () => [...coreNodes, ...EXAMPLE_ERC20S],
    [coreNodes]
  )

  // Nodes used for label resolution in the explain panel: the live graph nodes
  // plus the example ERC20s (which the backend reports with an empty canonical).
  const labelNodes = useMemo(
    () => [...(graph?.nodes ?? []), ...EXAMPLE_ERC20S],
    [graph]
  )

  // Seed the from/to selects once the graph arrives.
  useEffect(() => {
    if (coreNodes.length === 0) return
    setFromKey((current) => current || pickDefault(coreNodes, 'eth'))
    setToKey((current) => current || pickDefault(coreNodes, 'btc'))
  }, [coreNodes])

  const selectedTierUsd = useMemo(
    () => ROUTE_TIERS.find((tier) => tier.label === tierLabel)?.usd ?? 1_000,
    [tierLabel]
  )

  // Auto-fill the amount field from the tier + source asset whenever either
  // changes (the field stays editable for free-typing).
  const fromNode = useMemo(
    () => selectableAssets.find((node) => node.key === fromKey) ?? null,
    [selectableAssets, fromKey]
  )
  useEffect(() => {
    if (!fromNode) return
    setAmountIn(rawAmountForTier(fromNode, selectedTierUsd))
  }, [fromNode, selectedTierUsd])

  const edgeRanks = useMemo(() => buildEdgeRanks(explain), [explain])

  // The canvas now reflects the selected source -> destination: when an explain
  // result is present we render its candidate subgraph (source leftmost,
  // destination rightmost, laid out by hop depth). The static full graph is
  // only a brief fallback before the first dry run completes.
  const flowNodes = useMemo<Node[]>(
    () =>
      explain
        ? layoutExplainNodes(explain.graph)
        : graph
          ? layoutNodes(graph.nodes)
          : [],
    [explain, graph]
  )

  const flowEdges = useMemo<Edge[]>(
    () =>
      explain
        ? buildExplainEdges(explain.graph, bpsByTransition, edgeRanks)
        : graph
          ? buildEdges(graph.edges, bpsByTransition, new Map())
          : [],
    [explain, graph, bpsByTransition, edgeRanks]
  )

  // Remount ReactFlow (and thus re-run fitView) whenever the subgraph shape
  // changes, so a new source/destination always frames itself.
  const canvasKey = explain
    ? `${explain.from_asset.chain}:${explain.from_asset.asset}->${explain.to_asset.chain}:${explain.to_asset.asset}#${explain.graph.nodes.length}`
    : 'static'

  const runExplain = useCallback(
    async (live: boolean) => {
      if (!fromNode) return
      const toNode = selectableAssets.find((node) => node.key === toKey)
      if (!toNode) return
      setRanking(true)
      setExplainError(null)
      try {
        const result = await postRouteExplain({
          from_asset: { chain: fromNode.chain, asset: fromNode.asset },
          to_asset: { chain: toNode.chain, asset: toNode.asset },
          amount_in: amountIn.trim() || '0',
          live_quote: live
        })
        setExplain(result)
      } catch (error) {
        setExplain(null)
        setExplainError(
          error instanceof Error ? error.message : 'Failed to rank route'
        )
      } finally {
        setRanking(false)
      }
    },
    [fromNode, toKey, selectableAssets, amountIn]
  )

  const onRank = useCallback(() => runExplain(liveQuote), [runExplain, liveQuote])

  // Keep the graph in sync with the source/destination/amount selection by
  // auto-running a cache-only dry run (debounced). Live quoting stays manual
  // behind the Rank button so we never hit provider APIs unprompted.
  useEffect(() => {
    if (!fromNode || !toKey) return
    const handle = setTimeout(() => void runExplain(false), 300)
    return () => clearTimeout(handle)
  }, [fromNode, toKey, amountIn, runExplain])

  if (loading) {
    return (
      <main className="dashboard route-graph">
        <div className="route-graph-empty">Loading routing graph…</div>
      </main>
    )
  }

  if (loadError) {
    return (
      <main className="dashboard route-graph">
        <div className="route-graph-empty route-graph-error">
          {loadError}
          <button type="button" className="rg-btn" onClick={() => void loadGraph()}>
            <RefreshCw size={14} /> Retry
          </button>
        </div>
      </main>
    )
  }

  return (
    <main className="dashboard route-graph">
      <div className="route-graph-controls">
        <label className="rg-field">
          <span>Tier</span>
          <select
            value={tierLabel}
            onChange={(event) => setTierLabel(event.target.value)}
          >
            {ROUTE_TIERS.map((tier) => (
              <option key={tier.label} value={tier.label}>
                {formatUsd(tier.usd)}
              </option>
            ))}
          </select>
        </label>
        <label className="rg-field rg-field-amount">
          <span>Amount in (base units)</span>
          <input
            value={amountIn}
            onChange={(event) => setAmountIn(event.target.value)}
            spellCheck={false}
          />
        </label>
        <label className="rg-field">
          <span>From</span>
          <select value={fromKey} onChange={(event) => setFromKey(event.target.value)}>
            <AssetOptions coreNodes={coreNodes} />
          </select>
        </label>
        <label className="rg-field">
          <span>To</span>
          <select value={toKey} onChange={(event) => setToKey(event.target.value)}>
            <AssetOptions coreNodes={coreNodes} />
          </select>
        </label>
        <label className="rg-toggle">
          <input
            type="checkbox"
            checked={liveQuote}
            onChange={(event) => setLiveQuote(event.target.checked)}
          />
          <Zap size={13} />
          <span>Live quote</span>
        </label>
        <button
          type="button"
          className="rg-btn rg-btn-primary"
          onClick={() => void onRank()}
          disabled={ranking || !fromKey || !toKey}
        >
          <Play size={14} />
          {ranking ? 'Ranking…' : 'Rank'}
        </button>
      </div>

      <div className="route-graph-canvas">
        <ReactFlow
          key={canvasKey}
          nodes={flowNodes}
          edges={flowEdges}
          fitView
          nodesDraggable={false}
          nodesConnectable={false}
          elementsSelectable={false}
          proOptions={{ hideAttribution: true }}
          minZoom={0.2}
          maxZoom={1.8}
        >
          <Background color="#222824" gap={24} />
          <Controls showInteractive={false} />
        </ReactFlow>
        <ColumnLegend />
      </div>

      <section className="route-graph-routes">
        {explainError ? (
          <div className="route-graph-panel-error">{explainError}</div>
        ) : null}
        {explain ? (
          <ExplainPanel
            explain={explain}
            nodes={labelNodes}
            bps={bpsByTransition}
          />
        ) : (
          <div className="route-graph-panel-hint">
            Pick a start and end asset and a tier — the graph redraws for that
            source → destination automatically. Try an{' '}
            <strong>Example ERC20</strong> (PEPE, LINK, DEGEN…) as the source or
            destination to see the runtime <strong>Velora</strong> wrap: the
            token swaps to/from an anchor (USDC/USDT/ETH) on its chain, then
            routes through the cached graph. Press <strong>Rank</strong> with{' '}
            <strong>Live quote</strong> on to fetch real per-path outputs and the
            best route. Edge labels show the venue and cached bps; Velora legs are
            live-priced and drawn dashed.
          </div>
        )}
      </section>
    </main>
  )
}

function ExplainPanel({
  explain,
  nodes,
  bps
}: {
  explain: RouteExplain
  nodes: RouteGraphNode[]
  bps: Map<string, number>
}) {
  const { counts, timings } = explain
  const fromLabel = assetDisplayLabel(explain.from_asset, nodes)
  const toLabel = assetDisplayLabel(explain.to_asset, nodes)
  const bestPathId = bestPathIdFor(explain)
  return (
    <div className="route-graph-explain">
      <header className="rg-explain-header">
        <div className="rg-explain-route">
          {fromLabel} → {toLabel}
        </div>
        <div className="rg-explain-tier">
          {formatTierLabel(explain.tier_label)} · {formatUsdMicros(explain.request_usd_micros)}
        </div>
      </header>

      <div className="rg-stats">
        <Stat label="Enumerated" value={counts.paths_enumerated} />
        <Stat label="Executable" value={counts.paths_after_executable} />
        <Stat label="Providers" value={counts.paths_after_provider} />
        <Stat label="HyperEVM" value={counts.paths_after_hyperevm} />
        <Stat label="Same-chain" value={counts.paths_after_same_chain} />
        <Stat label="Ranked" value={counts.ranked_count} />
      </div>

      <div className="rg-timings">
        <span>bfs {timings.bfs_ms}ms</span>
        <span>rank {timings.rank_ms}ms</span>
        {explain.live_quote ? <span>live {timings.live_quote_ms}ms</span> : null}
        <span>total {timings.total_ms}ms</span>
      </div>

      <ol className="rg-ranked">
        {explain.ranked.map((path) => {
          const rankClass =
            path.rank <= 3 ? ` rg-rank-${path.rank}` : ''
          const isBest = path.path_id === bestPathId
          const summary = pathBpsSummary(path.transitions, bps)
          return (
            <li
              key={path.path_id}
              className={`rg-ranked-item${rankClass}${isBest ? ' rg-best' : ''}`}
            >
              <div className="rg-ranked-head">
                <span className="rg-ranked-rank">#{path.rank}</span>
                <span className="rg-ranked-venues">
                  {path.transitions.map((transition, index) => (
                    <span key={transition.id}>
                      {index > 0 ? ' › ' : ''}
                      <span style={{ color: providerColor(transition.provider) }}>
                        {providerLabel(transition.provider)}
                      </span>
                    </span>
                  ))}
                </span>
                {isBest ? <span className="rg-badge rg-badge-best">best</span> : null}
                <span className="rg-ranked-summary">
                  <span
                    className={`rg-sum-bps${summary.allKnown ? '' : ' rg-sum-partial'}`}
                    title={
                      summary.allKnown
                        ? 'Total cached cost summed across every leg'
                        : 'Sum of cached legs only; live/uncached legs are not included, so the true total is higher'
                    }
                  >
                    {formatPathBpsTotal(summary)}
                  </span>
                  <span className="rg-sum-hops">{path.hop_count} hops</span>
                  {path.missing_edges > 0 ? (
                    <span className="rg-missing">{path.missing_edges} uncached</span>
                  ) : null}
                  {path.total_latency_ms > 0 ? (
                    <span>{path.total_latency_ms}ms</span>
                  ) : null}
                  {path.estimated_amount_out ? (
                    <span className="rg-out">out {path.estimated_amount_out}</span>
                  ) : null}
                </span>
              </div>

              <div className="rg-path-chain">
                <span className="rg-chain-asset">
                  {assetDisplayLabel(
                    {
                      chain: path.transitions[0]?.from_chain ?? '',
                      asset: path.transitions[0]?.from_asset ?? ''
                    },
                    nodes
                  )}
                </span>
                {path.transitions.map((transition) => {
                  const bpsValue = bps.get(transition.id)
                  const isVelora = transition.provider === 'velora'
                  const stepBps = isVelora
                    ? 'live'
                    : bpsValue !== undefined
                      ? `${formatBps(bpsValue)} bps`
                      : '—'
                  const bpsClass = isVelora
                    ? ' rg-hop-live'
                    : bpsValue === undefined
                      ? ' rg-hop-missing'
                      : ''
                  return (
                    <span className="rg-chain-step" key={`${transition.id}-step`}>
                      <span
                        className="rg-chain-hop"
                        style={{ color: providerColor(transition.provider) }}
                      >
                        <span className="rg-hop-name">
                          {providerLabel(transition.provider)}
                        </span>
                        <span className={`rg-hop-bps${bpsClass}`}>{stepBps}</span>
                      </span>
                      <span className="rg-chain-asset">
                        {assetDisplayLabel(
                          { chain: transition.to_chain, asset: transition.to_asset },
                          nodes
                        )}
                      </span>
                    </span>
                  )
                })}
              </div>
            </li>
          )
        })}
      </ol>
    </div>
  )
}

// From/To picker contents: core graph assets, then the example ERC20 presets
// in their own group so they read as a separate "arbitrary token" demo.
function AssetOptions({ coreNodes }: { coreNodes: RouteGraphNode[] }) {
  return (
    <>
      <optgroup label="Core assets">
        {coreNodes.map((node) => (
          <option key={node.key} value={node.key}>
            {nodeOptionLabel(node)}
          </option>
        ))}
      </optgroup>
      <optgroup label="Example ERC20s (Velora wrap)">
        {EXAMPLE_ERC20S.map((node) => (
          <option key={node.key} value={node.key}>
            {nodeOptionLabel(node)}
          </option>
        ))}
      </optgroup>
    </>
  )
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <div className="rg-stat">
      <strong>{value}</strong>
      <span>{label}</span>
    </div>
  )
}

function ColumnLegend() {
  return (
    <div className="route-graph-legend">
      <span className="rg-legend-item">
        <i style={{ background: RANK_COLORS[0] }} />
        best route
      </span>
      <span className="rg-legend-sep" />
      {Object.entries(PROVIDER_LABEL).map(([provider, label]) => (
        <span key={provider} className="rg-legend-item">
          <i style={{ background: PROVIDER_COLOR[provider] ?? '#cbd5d1' }} />
          {label}
        </span>
      ))}
      <span className="rg-legend-item">
        <i className="rg-legend-dash" />
        velora (live)
      </span>
    </div>
  )
}

// -- helpers ---------------------------------------------------------------

function layoutNodes(nodes: RouteGraphNode[]): Node[] {
  const columns = new Map<string, RouteGraphNode[]>()
  for (const node of nodes) {
    const column = columns.get(node.chain) ?? []
    column.push(node)
    columns.set(node.chain, column)
  }
  const orderedChains = [...columns.keys()].sort(
    (a, b) => columnIndex(a) - columnIndex(b)
  )
  const flow: Node[] = []
  orderedChains.forEach((chain, colIdx) => {
    const column = (columns.get(chain) ?? []).sort((a, b) =>
      a.canonical.localeCompare(b.canonical)
    )
    column.forEach((node, rowIdx) => {
      flow.push({
        id: node.key,
        position: { x: colIdx * COLUMN_GAP, y: COLUMN_TOP + rowIdx * ROW_GAP },
        data: { label: nodeFlowLabel(node, chain) },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
        style: {
          width: 168,
          fontSize: 11,
          borderRadius: 8,
          border: `1px solid ${node.kind === 'venue' ? '#14b8a6' : '#3a423d'}`,
          background: node.kind === 'venue' ? 'rgba(20,184,166,0.12)' : '#161b18',
          color: '#e6ebe8',
          padding: 6
        }
      })
    })
  })
  return flow
}

// Static full-graph fallback (rendered only before the first dry run). No
// ranking highlight — just provider-colored edges with a bps label.
function buildEdges(
  edges: RouteGraphEdge[],
  bps: Map<string, number>,
  _highlight: Map<string, HighlightKind>
): Edge[] {
  return edges.map((edge) => {
    const isVelora = edge.provider === 'velora'
    const color = PROVIDER_COLOR[edge.provider] ?? EDGE_DEFAULT
    const bpsValue = bps.get(edge.id)
    const label =
      isVelora || bpsValue === undefined ? undefined : `${formatBps(bpsValue)} bps`
    return {
      id: edge.id,
      source: edge.from,
      target: edge.to,
      label,
      style: {
        stroke: color,
        strokeWidth: 1.5,
        strokeDasharray: isVelora ? '5 4' : undefined,
        opacity: 0.8
      },
      labelStyle: { fill: '#cbd5d1', fontSize: 10 },
      labelBgStyle: { fill: '#0f1311' },
      labelBgPadding: [3, 2] as [number, number],
      markerEnd: { type: MarkerType.ArrowClosed, color, width: 14, height: 14 }
    }
  })
}

// Lay out the source -> destination subgraph in depth columns: source pinned
// left (depth 0), destination pinned right (max_depth), everything else by its
// BFS hop distance. Columns are vertically centered for readability.
function layoutExplainNodes(graph: RouteExplainGraph): Node[] {
  const byDepth = new Map<number, RouteExplainGraphNode[]>()
  for (const node of graph.nodes) {
    const column = byDepth.get(node.depth) ?? []
    column.push(node)
    byDepth.set(node.depth, column)
  }
  const tallest = Math.max(1, ...[...byDepth.values()].map((c) => c.length))
  const flow: Node[] = []
  for (let depth = 0; depth <= graph.max_depth; depth += 1) {
    const column = (byDepth.get(depth) ?? []).sort((a, b) =>
      a.canonical.localeCompare(b.canonical)
    )
    const offset = ((tallest - column.length) * ROW_GAP) / 2
    column.forEach((node, rowIdx) => {
      const accent =
        node.role === 'source'
          ? '#38bdf8'
          : node.role === 'destination'
            ? '#34d399'
            : node.kind === 'venue'
              ? '#14b8a6'
              : '#3a423d'
      flow.push({
        id: node.key,
        position: {
          x: depth * COLUMN_GAP,
          y: COLUMN_TOP + offset + rowIdx * ROW_GAP
        },
        data: { label: explainNodeLabel(node) },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
        style: {
          width: 168,
          fontSize: 11,
          borderRadius: 8,
          border: `${node.role === 'source' || node.role === 'destination' ? 2 : 1}px solid ${accent}`,
          background:
            node.role === 'source'
              ? 'rgba(56,189,248,0.12)'
              : node.role === 'destination'
                ? 'rgba(52,211,153,0.12)'
                : node.kind === 'venue'
                  ? 'rgba(20,184,166,0.12)'
                  : '#161b18',
          color: '#e6ebe8',
          padding: 6
        }
      })
    })
  }
  return flow
}

// Edges for the candidate subgraph: labeled with the venue (+ cached bps when
// available). Only the single best route is emphasized (green, thick,
// animated); every other edge - including the 2nd/3rd ranked routes - is drawn
// as a faint corridor edge so the winner is not lost in a tangle of overlays.
function buildExplainEdges(
  graph: RouteExplainGraph,
  bps: Map<string, number>,
  ranks: Map<string, EdgeRank>
): Edge[] {
  return graph.edges.map((edge) => {
    const isVelora = edge.provider === 'velora'
    const info = ranks.get(edge.id)
    const isBest = info?.isBest ?? false
    const color = isBest ? RANK_COLORS[0] : EDGE_FAINT
    const venue = providerLabel(edge.provider)
    const bpsValue = bps.get(edge.id)
    const label = isVelora
      ? `${venue} · live`
      : bpsValue !== undefined
        ? `${venue} · ${formatBps(bpsValue)} bps`
        : venue
    return {
      id: edge.id,
      source: edge.from,
      target: edge.to,
      label,
      animated: isBest,
      style: {
        stroke: color,
        strokeWidth: isBest ? 3.5 : 1.5,
        strokeDasharray: isVelora ? '6 4' : undefined,
        opacity: isBest ? 1 : 0.35
      },
      labelStyle: { fill: '#dfe6e2', fontSize: 10, fontWeight: isBest ? 600 : 400 },
      labelBgStyle: { fill: '#0f1311' },
      labelBgPadding: [3, 2] as [number, number],
      markerEnd: { type: MarkerType.ArrowClosed, color, width: 14, height: 14 }
    }
  })
}

// For each subgraph edge, record the lowest rank of any path using it and
// whether the single "best" path traverses it.
function buildEdgeRanks(explain: RouteExplain | null): Map<string, EdgeRank> {
  const map = new Map<string, EdgeRank>()
  if (!explain) return map
  const bestPathId = bestPathIdFor(explain)
  for (const path of explain.ranked) {
    const isBest = path.path_id === bestPathId
    for (const transition of path.transitions) {
      const existing = map.get(transition.id)
      map.set(transition.id, {
        rank: existing ? Math.min(existing.rank, path.rank) : path.rank,
        isBest: (existing?.isBest ?? false) || isBest
      })
    }
  }
  return map
}

// The single best route: the live-quote winner when available, otherwise the
// top structural rank.
function bestPathIdFor(explain: RouteExplain): string | null {
  if (explain.winner_path_id) return explain.winner_path_id
  return explain.ranked.find((path) => path.rank === 1)?.path_id ?? null
}

function providerColor(provider: string): string {
  return PROVIDER_COLOR[provider] ?? '#cbd5d1'
}

function providerLabel(provider: string): string {
  return PROVIDER_LABEL[provider] ?? provider
}

function columnIndex(chain: string): number {
  const index = CHAIN_COLUMN_ORDER.indexOf(chain)
  return index === -1 ? CHAIN_COLUMN_ORDER.length : index
}

function nodeFlowLabel(node: RouteGraphNode, chain: string): string {
  const chainLabel = CHAIN_LABELS[chain] ?? chain
  return `${node.canonical.toUpperCase()}\n${chainLabel}`
}

function explainNodeLabel(node: RouteExplainGraphNode): string {
  if (node.kind === 'venue') {
    const venue = PROVIDER_LABEL[node.chain.replace('venue:', '')] ?? node.chain
    return `${node.canonical.toUpperCase()}\n${venue}`
  }
  const chainLabel = CHAIN_LABELS[node.chain] ?? node.chain
  const symbol = node.canonical
    ? node.canonical.toUpperCase()
    : erc20DisplaySymbol(node.chain, node.asset)
  return `${symbol}\n${chainLabel}`
}

// Display symbol for a non-canonical token: a known example ERC20's symbol, or
// a truncated address fallback for anything else the user might enter.
function erc20DisplaySymbol(chain: string, asset: string): string {
  const known = EXAMPLE_ERC20_BY_REF.get(`${chain}:${asset}`)
  if (known) return known.canonical.toUpperCase()
  if (asset.startsWith('0x') && asset.length > 10) {
    return `${asset.slice(0, 6)}…${asset.slice(-4)}`
  }
  return asset.toUpperCase()
}

function nodeOptionLabel(node: RouteGraphNode): string {
  const chainLabel = CHAIN_LABELS[node.chain] ?? node.chain
  return `${node.canonical.toUpperCase()} · ${chainLabel}`
}

function assetDisplayLabel(
  asset: { chain: string; asset: string },
  nodes: RouteGraphNode[]
): string {
  const match = nodes.find(
    (node) => node.chain === asset.chain && node.asset === asset.asset
  )
  const canonical =
    match && match.canonical
      ? match.canonical.toUpperCase()
      : erc20DisplaySymbol(asset.chain, asset.asset)
  const chainLabel = CHAIN_LABELS[asset.chain] ?? asset.chain
  return `${canonical} · ${chainLabel}`
}

function pickDefault(nodes: RouteGraphNode[], canonical: string): string {
  return (
    nodes.find((node) => node.canonical === canonical)?.key ?? nodes[0]?.key ?? ''
  )
}

function rawAmountForTier(node: RouteGraphNode, tierUsd: number): string {
  const price = CANONICAL_PRICE_USD[node.canonical] ?? 1
  const decimals = Number.isFinite(node.decimals) ? node.decimals : 0
  try {
    const tierMicros = BigInt(Math.round(tierUsd * 1_000_000))
    const priceMicros = BigInt(Math.round(price * 1_000_000))
    const scale = 10n ** BigInt(decimals)
    if (priceMicros === 0n) return '0'
    return ((tierMicros * scale) / priceMicros).toString()
  } catch (_error) {
    return '0'
  }
}

function rebuildBpsMap(
  snapshots: { transitionId: string; amountBucket: string; estimatedFeeBps: number }[],
  set: (map: Map<string, number>) => void
) {
  // Default to the usd_1000 tier on first load.
  rebuildBpsMapForTier(snapshots, 'usd_1000', set)
}

function rebuildBpsMapForTier(
  snapshots: { transitionId: string; amountBucket: string; estimatedFeeBps: number }[],
  tierLabel: string,
  set: (map: Map<string, number>) => void
) {
  const map = new Map<string, number>()
  for (const snapshot of snapshots) {
    if (snapshot.amountBucket === tierLabel) {
      map.set(snapshot.transitionId, snapshot.estimatedFeeBps)
    }
  }
  set(map)
}

function formatBps(bps: number): string {
  if (!Number.isFinite(bps)) return '—'
  return bps >= 100 ? bps.toFixed(0) : bps.toFixed(1)
}

// Sum the per-leg cached bps for a ranked path. `velora` legs are live-priced
// and uncached legs (e.g. HyperCore trades, HL bridge) have no measured cost,
// so the total only reflects legs with a cached snapshot. When some legs are
// unknown the sum is a lower bound, surfaced as `>=` in the label.
function pathBpsSummary(
  transitions: { id: string; provider: string }[],
  bps: Map<string, number>
): { total: number; allKnown: boolean; anyKnown: boolean } {
  let total = 0
  let known = 0
  for (const transition of transitions) {
    const value = bps.get(transition.id)
    if (value !== undefined) {
      total += value
      known += 1
    }
  }
  return {
    total,
    allKnown: transitions.length > 0 && known === transitions.length,
    anyKnown: known > 0
  }
}

function formatPathBpsTotal(summary: {
  total: number
  allKnown: boolean
  anyKnown: boolean
}): string {
  if (!summary.anyKnown) return 'Σ — bps'
  return `Σ ${summary.allKnown ? '' : '≥'}${formatBps(summary.total)} bps`
}

function formatUsd(usd: number): string {
  if (usd >= 1_000_000) return `$${usd / 1_000_000}M`
  if (usd >= 1_000) return `$${usd / 1_000}k`
  return `$${usd}`
}

function formatUsdMicros(micros: number): string {
  const usd = micros / 1_000_000
  if (usd >= 1_000_000) return `$${(usd / 1_000_000).toFixed(2)}M`
  if (usd >= 1_000) return `$${(usd / 1_000).toFixed(1)}k`
  if (usd >= 1) return `$${usd.toFixed(2)}`
  return `$${usd.toFixed(4)}`
}

function formatTierLabel(label: string): string {
  const tier = ROUTE_TIERS.find((entry) => entry.label === label)
  return tier ? formatUsd(tier.usd) : label
}

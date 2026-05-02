import {
  Activity,
  ChevronDown,
  ChevronRight,
  Check,
  CircleAlert,
  Clock3,
  Copy,
  LogIn,
  LogOut,
  RefreshCw,
  ShieldCheck,
  Wifi,
  WifiOff
} from 'lucide-react'
import { useCallback, useEffect, useRef, useState } from 'react'

import { fetchMe, fetchOrders, parseSnapshotEvent, parseUpsertEvent } from './api'
import { authClient } from './auth-client'
import type {
  AssetRef,
  MeResponse,
  OrderExecutionLeg,
  OrderExecutionStep,
  OrderFirehoseRow,
  OrderMetrics,
  ProviderOperation,
  UsdAmountValuation,
  UsdValuation
} from './types'

type StreamState = 'idle' | 'connecting' | 'live' | 'closed' | 'error'

const ORDER_LIMIT = 100
const SHOW_USD_VALUES = true
const WAIT_FOR_DEPOSIT_STEP_TYPE = 'wait_for_deposit'
const CHAIN_DISPLAY_NAMES: Record<string, string> = {
  bitcoin: 'Bitcoin',
  'evm:1': 'Ethereum',
  'evm:42161': 'Arbitrum',
  'evm:8453': 'Base',
  hyperliquid: 'Hyperliquid'
}

const EVM_EXPLORER_BASE_URLS: Record<string, string> = {
  'evm:1': 'https://etherscan.io',
  'evm:10': 'https://optimistic.etherscan.io',
  'evm:56': 'https://bscscan.com',
  'evm:137': 'https://polygonscan.com',
  'evm:8453': 'https://basescan.org',
  'evm:42161': 'https://arbiscan.io',
  'evm:43114': 'https://snowtrace.io',
  'evm:11155111': 'https://sepolia.etherscan.io',
  'evm:84532': 'https://sepolia.basescan.org',
  'evm:421614': 'https://sepolia.arbiscan.io'
}

// Frontend-only mirror of the gateway's known asset aliases for readable labels.
const ASSET_DISPLAY_NAMES: Record<string, string> = {
  'bitcoin|native': 'BTC',
  'evm:1|native': 'ETH',
  'evm:1|0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 'USDC',
  'evm:1|0xdac17f958d2ee523a2206206994597c13d831ec7': 'USDT',
  'evm:1|0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf': 'cbBTC',
  'evm:42161|native': 'ETH',
  'evm:42161|0xaf88d065e77c8cc2239327c5edb3a432268e5831': 'USDC',
  'evm:42161|0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9': 'USDT',
  'evm:42161|0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf': 'cbBTC',
  'evm:8453|native': 'ETH',
  'evm:8453|0x833589fcd6edb6e08f4c7c32d4f71b54bda02913': 'USDC',
  'evm:8453|0xfde4c96c8593536e31f229ea8f37b2ada2699bb2': 'USDT',
  'evm:8453|0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf': 'cbBTC',
  'hyperliquid|native': 'USDC'
}

const ASSET_DECIMALS: Record<string, number> = {
  'bitcoin|native': 8,
  'evm:1|native': 18,
  'evm:1|0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48': 6,
  'evm:1|0xdac17f958d2ee523a2206206994597c13d831ec7': 6,
  'evm:1|0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf': 8,
  'evm:42161|native': 18,
  'evm:42161|0xaf88d065e77c8cc2239327c5edb3a432268e5831': 6,
  'evm:42161|0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9': 6,
  'evm:42161|0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf': 8,
  'evm:8453|native': 18,
  'evm:8453|0x833589fcd6edb6e08f4c7c32d4f71b54bda02913': 6,
  'evm:8453|0xfde4c96c8593536e31f229ea8f37b2ada2699bb2': 6,
  'evm:8453|0xcbb7c0000ab88b473b1f5afd9ef808440eed33bf': 8,
  'hyperliquid|native': 6
}

export function App() {
  const [me, setMe] = useState<MeResponse | null>(null)
  const [orders, setOrders] = useState<OrderFirehoseRow[]>([])
  const [nextCursor, setNextCursor] = useState<string | undefined>()
  const [metrics, setMetrics] = useState<OrderMetrics | null>(null)
  const [expandedOrderId, setExpandedOrderId] = useState<string | null>(null)
  const [copiedOrderId, setCopiedOrderId] = useState<string | null>(null)
  const [flashingOrderIds, setFlashingOrderIds] = useState<Set<string>>(
    () => new Set()
  )
  const [streamState, setStreamState] = useState<StreamState>('idle')
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const loadMoreRef = useRef<HTMLDivElement | null>(null)
  const copiedOrderTimeoutRef = useRef<number | undefined>(undefined)
  const rowFlashTimeoutsRef = useRef<Map<string, number>>(new Map())

  const loadSession = useCallback(async () => {
    setError(null)
    const session = await fetchMe()
    setMe(session)
    return session
  }, [])

  const loadOrders = useCallback(async () => {
    const response = await fetchOrders(ORDER_LIMIT)
    setOrders(sortOrders(response.orders))
    setNextCursor(response.nextCursor)
    setMetrics(response.metrics)
  }, [])

  const loadMoreOrders = useCallback(async () => {
    if (!nextCursor || loadingMore) return

    setLoadingMore(true)
    setError(null)
    try {
      const response = await fetchOrders(ORDER_LIMIT, nextCursor)
      setOrders((current) => mergeOrders(current, response.orders))
      setNextCursor(response.nextCursor)
      setMetrics(response.metrics)
    } catch (loadError) {
      setError(errorMessage(loadError))
    } finally {
      setLoadingMore(false)
    }
  }, [loadingMore, nextCursor])

  const copyOrderId = useCallback(async (orderId: string) => {
    try {
      await navigator.clipboard.writeText(orderId)
      setCopiedOrderId(orderId)
      if (copiedOrderTimeoutRef.current) {
        window.clearTimeout(copiedOrderTimeoutRef.current)
      }
      copiedOrderTimeoutRef.current = window.setTimeout(() => {
        setCopiedOrderId((current) => (current === orderId ? null : current))
      }, 1300)
    } catch (copyError) {
      setError(errorMessage(copyError))
    }
  }, [])

  const markOrderFlashed = useCallback((orderId: string) => {
    const existingTimeout = rowFlashTimeoutsRef.current.get(orderId)
    if (existingTimeout) window.clearTimeout(existingTimeout)

    setFlashingOrderIds((current) => {
      const next = new Set(current)
      next.add(orderId)
      return next
    })

    const timeout = window.setTimeout(() => {
      rowFlashTimeoutsRef.current.delete(orderId)
      setFlashingOrderIds((current) => {
        const next = new Set(current)
        next.delete(orderId)
        return next
      })
    }, 1100)
    rowFlashTimeoutsRef.current.set(orderId, timeout)
  }, [])

  useEffect(() => {
    let cancelled = false

    const load = async () => {
      try {
        const session = await loadSession()
        if (!cancelled && session.authorized) {
          await loadOrders()
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(errorMessage(loadError))
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    }

    void load()

    return () => {
      cancelled = true
    }
  }, [loadOrders, loadSession])

  useEffect(() => {
    if (!me?.authorized) return

    setStreamState('connecting')
    const source = new EventSource(`/api/orders/events?limit=${ORDER_LIMIT}`, {
      withCredentials: true
    })

    source.addEventListener('open', () => setStreamState('live'))
    source.addEventListener('snapshot', (event) => {
      const snapshot = parseSnapshotEvent(event as MessageEvent<string>)
      setOrders((current) => mergeOrders(current, snapshot.orders))
      if (snapshot.metrics) setMetrics(snapshot.metrics)
      setStreamState('live')
    })
    source.addEventListener('upsert', (event) => {
      const { order, metrics: nextMetrics } = parseUpsertEvent(
        event as MessageEvent<string>
      )
      setOrders((current) => upsertOrder(current, order))
      if (nextMetrics) setMetrics(nextMetrics)
      markOrderFlashed(order.id)
    })
    source.addEventListener('error', () => {
      setStreamState(source.readyState === EventSource.CLOSED ? 'closed' : 'error')
    })

    return () => {
      source.close()
      setStreamState('closed')
    }
  }, [markOrderFlashed, me?.authorized])

  useEffect(() => {
    return () => {
      if (copiedOrderTimeoutRef.current) {
        window.clearTimeout(copiedOrderTimeoutRef.current)
      }
      for (const timeout of rowFlashTimeoutsRef.current.values()) {
        window.clearTimeout(timeout)
      }
      rowFlashTimeoutsRef.current.clear()
    }
  }, [])

  useEffect(() => {
    if (!me?.authorized || !nextCursor) return
    const node = loadMoreRef.current
    if (!node) return

    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) {
          void loadMoreOrders()
        }
      },
      { rootMargin: '700px 0px' }
    )

    observer.observe(node)
    return () => observer.disconnect()
  }, [loadMoreOrders, me?.authorized, nextCursor])

  if (loading) {
    return (
      <Shell>
        <LoadingPanel />
      </Shell>
    )
  }

  if (me?.missingAuthConfig?.length) {
    return (
      <Shell>
        <ConfigPanel missing={me.missingAuthConfig} />
      </Shell>
    )
  }

  if (!me?.authenticated) {
    return (
      <Shell>
        <LoginPanel />
      </Shell>
    )
  }

  if (!me.authorized) {
    return (
      <Shell>
        <DeniedPanel me={me} />
      </Shell>
    )
  }

  return (
    <Shell
      right={
        <UserMenu
          me={me}
          streamState={streamState}
          onRefresh={() => void loadOrders()}
        />
      }
    >
      <main className="dashboard">
        <section className="metrics" aria-label="Order metrics">
          <Metric
            icon={<Activity size={18} />}
            label="Orders"
            value={metrics?.total ?? 0}
          />
          <Metric
            icon={<Clock3 size={18} />}
            label="Active"
            value={metrics?.active ?? 0}
          />
          <Metric
            icon={<CircleAlert size={18} />}
            label="Needs Attention"
            value={metrics?.needsAttention ?? 0}
            tone={metrics?.needsAttention ? 'danger' : 'neutral'}
          />
          <Metric
            icon={<ShieldCheck size={18} />}
            label="Admin Key"
            value={Boolean(me.routerAdminKeyConfigured) ? 'Set' : 'Missing'}
            tone={Boolean(me.routerAdminKeyConfigured) ? 'success' : 'warning'}
          />
        </section>

        {error ? <div className="notice error">{error}</div> : null}

        <section className="orders-surface" aria-label="Orders">
          <div className="table-toolbar">
            <div>
              <h2>Orders</h2>
              <p>created_at DESC</p>
            </div>
            <StreamBadge state={streamState} />
          </div>

          <div className="table-scroll">
            <table className="orders-table">
              <thead>
                <tr>
                  <th aria-label="Expand"></th>
                  <th>Created</th>
                  <th>Order</th>
                  <th>Route</th>
                  <th>Kind</th>
                  <th>Quote</th>
                  <th>Executed</th>
                  <th>Status</th>
                  <th>Venue Progress</th>
                </tr>
              </thead>
              <tbody>
                {orders.length === 0 ? (
                  <tr>
                    <td className="empty" colSpan={9}>
                      No orders found
                    </td>
                  </tr>
                ) : (
                  orders.map((order) => (
                    <OrderRows
                      key={order.id}
                      order={order}
                      expanded={expandedOrderId === order.id}
                      flashing={flashingOrderIds.has(order.id)}
                      copied={copiedOrderId === order.id}
                      onToggle={() =>
                        setExpandedOrderId((current) =>
                          current === order.id ? null : order.id
                        )
                      }
                      onCopyOrderId={() => void copyOrderId(order.id)}
                    />
                  ))
                )}
              </tbody>
            </table>
          </div>
          <div ref={loadMoreRef} className="pagination-sentinel">
            {loadingMore ? (
              <>
                <RefreshCw size={15} className="spin" />
                <span>Loading older orders</span>
              </>
            ) : nextCursor ? (
              <span>Older orders available</span>
            ) : orders.length > 0 ? (
              <span>End of orders</span>
            ) : null}
          </div>
        </section>
      </main>
    </Shell>
  )
}

function Shell({
  children,
  right
}: {
  children: React.ReactNode
  right?: React.ReactNode
}) {
  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand">
          <span className="brand-mark">R</span>
          <div>
            <h1>Rift Admin</h1>
            <p>TEE Router</p>
          </div>
        </div>
        {right}
      </header>
      {children}
    </div>
  )
}

function LoginPanel() {
  const [signingIn, setSigningIn] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const signIn = async () => {
    setSigningIn(true)
    setError(null)
    const result = await authClient.signIn.social({
      provider: 'google',
      callbackURL: window.location.origin
    })

    if (result.error) {
      setError(result.error.message ?? 'Google sign-in failed')
      setSigningIn(false)
    }
  }

  return (
    <main className="center-stage">
      <section className="auth-panel">
        <div className="auth-icon">
          <ShieldCheck size={30} />
        </div>
        <h2>Rift Admin</h2>
        <p>cliff@rift.trade, samee@rift.trade, tristan@rift.trade</p>
        <button className="primary-button" onClick={() => void signIn()}>
          {signingIn ? <RefreshCw size={18} className="spin" /> : <LogIn size={18} />}
          Google Sign-In
        </button>
        {error ? <div className="notice error">{error}</div> : null}
      </section>
    </main>
  )
}

function DeniedPanel({ me }: { me: MeResponse }) {
  return (
    <main className="center-stage">
      <section className="auth-panel">
        <div className="auth-icon danger">
          <CircleAlert size={30} />
        </div>
        <h2>Access Denied</h2>
        <p>{me.user?.email ?? 'Unknown account'}</p>
        <button className="secondary-button" onClick={() => void signOut()}>
          <LogOut size={18} />
          Sign Out
        </button>
      </section>
    </main>
  )
}

function ConfigPanel({ missing }: { missing: string[] }) {
  return (
    <main className="center-stage">
      <section className="auth-panel wide">
        <div className="auth-icon warning">
          <CircleAlert size={30} />
        </div>
        <h2>Auth Config Missing</h2>
        <div className="missing-list">
          {missing.map((item) => (
            <code key={item}>{item}</code>
          ))}
        </div>
      </section>
    </main>
  )
}

function LoadingPanel() {
  return (
    <main className="center-stage">
      <div className="loading-mark">
        <RefreshCw size={28} className="spin" />
      </div>
    </main>
  )
}

function UserMenu({
  me,
  streamState,
  onRefresh
}: {
  me: MeResponse
  streamState: StreamState
  onRefresh: () => void
}) {
  return (
    <div className="user-menu">
      <StreamBadge state={streamState} compact />
      <button className="icon-button" onClick={onRefresh} aria-label="Refresh orders">
        <RefreshCw size={17} />
      </button>
      <div className="user-chip">
        <span>{me.user?.email}</span>
      </div>
      <button
        className="icon-button"
        onClick={() => void signOut()}
        aria-label="Sign out"
      >
        <LogOut size={17} />
      </button>
    </div>
  )
}

function Metric({
  icon,
  label,
  value,
  detail,
  tone = 'neutral'
}: {
  icon: React.ReactNode
  label: string
  value: string | number
  detail?: string
  tone?: 'neutral' | 'success' | 'warning' | 'danger'
}) {
  return (
    <div className={`metric ${tone}`}>
        <div className="metric-icon">{icon}</div>
      <div>
        <span>{label}</span>
        <strong>{typeof value === 'number' ? formatCount(value) : value}</strong>
        {detail ? <em>{detail}</em> : null}
      </div>
    </div>
  )
}

function OrderRows({
  order,
  expanded,
  flashing,
  copied,
  onToggle,
  onCopyOrderId
}: {
  order: OrderFirehoseRow
  expanded: boolean
  flashing: boolean
  copied: boolean
  onToggle: () => void
  onCopyOrderId: () => void
}) {
  return (
    <>
      <tr className={`order-row ${flashing ? 'flash' : ''}`} onClick={onToggle}>
        <td>
          <button
            className="expand-button"
            aria-label={expanded ? 'Collapse order' : 'Expand order'}
            onClick={(event) => {
              event.stopPropagation()
              onToggle()
            }}
          >
            {expanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
          </button>
        </td>
        <td>
          <time dateTime={order.createdAt}>{formatDate(order.createdAt)}</time>
        </td>
        <td>
          <CopyableOrderId
            orderId={order.id}
            copied={copied}
            onCopy={onCopyOrderId}
          />
        </td>
        <td>
          <RouteCell order={order} />
        </td>
        <td>
          <span className="kind-pill">{formatKind(order.orderKind)}</span>
        </td>
        <td>
          <QuoteCell order={order} />
        </td>
        <td>
          <ExecutedCell order={order} />
        </td>
        <td>
          <StatusPill status={order.status} />
        </td>
        <td>
          <ProgressCell order={order} />
        </td>
      </tr>
      {expanded ? (
        <tr className="details-row">
          <td colSpan={9}>
            <OrderDetails order={order} />
          </td>
        </tr>
      ) : null}
    </>
  )
}

function RouteCell({ order }: { order: OrderFirehoseRow }) {
  return (
    <div className="route-cell">
      <AssetLabel asset={order.source} />
      <span className="route-arrow">to</span>
      <AssetLabel asset={order.destination} />
    </div>
  )
}

function AssetLabel({ asset }: { asset: AssetRef }) {
  const assetDisplay = assetDisplayName(asset)
  const isKnownAsset = assetDisplay !== asset.assetId
  const addressUrl = isKnownAsset ? undefined : explorerAddressUrl(asset.chainId, asset.assetId)
  const assetCode = (
    <code className={isKnownAsset ? 'known-asset' : undefined}>{assetDisplay}</code>
  )

  return (
    <span className="asset-label" title={`${asset.chainId} / ${asset.assetId}`}>
      <span>{chainDisplayName(asset.chainId)}</span>
      {addressUrl ? (
        <a
          href={addressUrl}
          target="_blank"
          rel="noreferrer"
          onClick={(event) => event.stopPropagation()}
        >
          {assetCode}
        </a>
      ) : (
        assetCode
      )}
    </span>
  )
}

function CopyableOrderId({
  orderId,
  copied,
  onCopy
}: {
  orderId: string
  copied: boolean
  onCopy: () => void
}) {
  const Icon = copied ? Check : Copy
  return (
    <button
      type="button"
      className={`copy-id-button ${copied ? 'copied' : ''}`}
      title={copied ? 'Copied' : `Copy ${orderId}`}
      onClick={(event) => {
        event.stopPropagation()
        onCopy()
      }}
    >
      <code className="mono compact-id">{shortId(orderId)}</code>
      <Icon size={14} />
    </button>
  )
}

function ExplorerValue({
  chainId,
  value,
  kind,
  full = false
}: {
  chainId?: string
  value: string
  kind: 'address' | 'tx' | 'auto'
  full?: boolean
}) {
  const url = explorerUrl(chainId, value, kind)
  const label = full ? value : shortId(value)
  if (!url) return <code title={value}>{label}</code>

  return (
    <a
      className="explorer-link"
      href={url}
      title={value}
      target="_blank"
      rel="noreferrer"
      onClick={(event) => event.stopPropagation()}
    >
      <code>{label}</code>
    </a>
  )
}

type AmountPairSide = {
  label: string
  amount?: string
  asset?: AssetRef
  usdValuation?: UsdAmountValuation
}

function AmountPairCell({
  input,
  output,
  placeholder
}: {
  input?: AmountPairSide
  output?: AmountPairSide
  placeholder?: string
}) {
  if (placeholder) {
    return <div className="quote-cell placeholder">{placeholder}</div>
  }

  return (
    <div className="quote-cell">
      {input ? (
        <span>
          {input.label}{' '}
          <code title={rawAmountTitle(input.amount, input.asset)}>
            {formatAmount(input.amount, input.asset)}
          </code>
          <UsdValue valuation={input.usdValuation} />
        </span>
      ) : null}
      {output ? (
        <span>
          {output.label}{' '}
          <code title={rawAmountTitle(output.amount, output.asset)}>
            {formatAmount(output.amount, output.asset)}
          </code>
          <UsdValue valuation={output.usdValuation} />
        </span>
      ) : null}
    </div>
  )
}

function QuoteCell({ order }: { order: OrderFirehoseRow }) {
  return (
    <AmountPairCell
      input={{
        label: 'in',
        amount: order.quotedInputAmount,
        asset: order.source,
        usdValuation: order.quoteUsdValuation?.amounts?.input
      }}
      output={{
        label: 'out',
        amount: order.quotedOutputAmount,
        asset: order.destination,
        usdValuation: order.quoteUsdValuation?.amounts?.output
      }}
    />
  )
}

function ExecutedCell({ order }: { order: OrderFirehoseRow }) {
  const executed = executedAmounts(order)
  if ('placeholder' in executed) {
    return <AmountPairCell placeholder={executed.placeholder} />
  }

  return (
    <AmountPairCell
      input={{
        label: 'in',
        amount: executed.input.amount,
        asset: executed.input.asset,
        usdValuation: executed.input.usdValuation
      }}
      output={{
        label: 'out',
        amount: executed.output.amount,
        asset: executed.output.asset,
        usdValuation: executed.output.usdValuation
      }}
    />
  )
}

function ProgressCell({ order }: { order: OrderFirehoseRow }) {
  const progress = order.progress
  const tone =
    progress.failedStages > 0
      ? 'danger'
      : progress.totalStages > 0 && progress.completedStages === progress.totalStages
        ? 'success'
        : 'warning'

  return (
    <div className="progress-cell">
      <div className={`progress-bar ${tone}`}>
        <span
          style={{
            width:
              progress.totalStages === 0
                ? '0%'
                : `${Math.round((progress.completedStages / progress.totalStages) * 100)}%`
          }}
        />
      </div>
      <span>
        {progress.totalStages === 0
          ? 'No stages'
          : `${progress.completedStages}/${progress.totalStages}`}
      </span>
      {progress.activeStage ? <em>{progress.activeStage}</em> : null}
    </div>
  )
}

function OrderDetails({ order }: { order: OrderFirehoseRow }) {
  return (
    <div className="details-grid">
      <QuoteFlowPanel order={order} />
      <ExecutionStatusPanel order={order} />
      <OrderMetadataPanel order={order} />
    </div>
  )
}

function QuoteFlowPanel({ order }: { order: OrderFirehoseRow }) {
  const legs = quoteFlowLegs(order)

  return (
    <div className="detail-block">
      <h3>Quoted Flow</h3>
      <div className="flow-list">
        <FundingFlowCard order={order} amountLabel="required" />
        {legs.length === 0 ? (
          <div className="empty-detail">No quote legs</div>
        ) : (
          legs.map((leg) => (
            <FlowCard
              key={leg.key}
              index={leg.index}
              title={humanize(leg.provider)}
              subtitle={humanize(leg.kind)}
              inputAsset={leg.inputAsset}
              outputAsset={leg.outputAsset}
              inputAmount={leg.amountIn}
              outputAmount={leg.amountOut}
              inputUsdValuation={leg.inputUsdValuation}
              outputUsdValuation={leg.outputUsdValuation}
              outputLabel="quoted out"
              footer={
                <>
                  {leg.minAmountOut ? (
                    <span>min {formatAmount(leg.minAmountOut, leg.outputAsset)}</span>
                  ) : null}
                  {leg.maxAmountIn ? (
                    <span>max {formatAmount(leg.maxAmountIn, leg.inputAsset)}</span>
                  ) : null}
                </>
              }
            />
          ))
        )}
      </div>
    </div>
  )
}

function ExecutionStatusPanel({ order }: { order: OrderFirehoseRow }) {
  const executionLegs = order.executionLegs ?? []
  return (
    <div className="detail-block">
      <h3>Execution Status</h3>
      <div className="flow-list">
        <FundingFlowCard order={order} amountLabel="deposit" showRuntimeState />
        {executionLegs.length === 0 ? (
          <div className="empty-detail">No execution legs</div>
        ) : (
          executionLegs.map((leg) => {
            const completed = leg.status === 'completed' || Boolean(leg.completedAt)
            return (
              <FlowCard
                key={leg.id}
                index={leg.legIndex}
                title={humanize(leg.provider)}
                subtitle={humanize(leg.legType)}
                status={leg.status}
                inputAsset={leg.input}
                outputAsset={leg.output}
                inputAmount={completed ? leg.actualAmountIn ?? leg.amountIn : leg.amountIn}
                outputAmount={completed ? leg.actualAmountOut : leg.expectedAmountOut}
                inputUsdValuation={
                  leg.usdValuation?.amounts?.actualInput ??
                  leg.usdValuation?.amounts?.plannedInput
                }
                outputUsdValuation={
                  completed
                    ? leg.usdValuation?.amounts?.actualOutput ??
                      leg.usdValuation?.amounts?.plannedMinOutput
                    : leg.usdValuation?.amounts?.plannedMinOutput
                }
                outputLabel={completed ? 'actual out' : 'expected out'}
                meta={
                  <>
                    <time dateTime={leg.updatedAt}>{formatDate(leg.updatedAt)}</time>
                  </>
                }
              />
            )
          })
        )}
      </div>
    </div>
  )
}

function FundingFlowCard({
  order,
  amountLabel,
  showRuntimeState = false
}: {
  order: OrderFirehoseRow
  amountLabel: string
  showRuntimeState?: boolean
}) {
  const fundingDeposit = fundingDepositFlow(order)
  const waitStep = order.executionSteps.find((step) => step.stepType === 'wait_for_deposit')

  return (
    <FlowCard
      variant="root"
      title="Funding"
      subtitle="Source Deposit"
      status={showRuntimeState ? (waitStep?.status ?? order.status) : undefined}
      inputLabel={amountLabel}
      inputAsset={fundingDeposit.asset}
      inputAmount={fundingDeposit.amount}
      inputUsdValuation={fundingDeposit.usdValuation}
      meta={
        showRuntimeState ? (
          <time dateTime={waitStep?.updatedAt ?? order.updatedAt}>
            {formatDate(waitStep?.updatedAt ?? order.updatedAt)}
          </time>
        ) : undefined
      }
    />
  )
}

function FlowCard({
  index,
  variant,
  title,
  subtitle,
  status,
  inputAsset,
  outputAsset,
  inputAmount,
  outputAmount,
  inputUsdValuation,
  outputUsdValuation,
  inputLabel,
  outputLabel,
  meta,
  footer
}: {
  index?: number
  variant?: 'root'
  title: string
  subtitle: string
  status?: string
  inputAsset?: { chainId: string; assetId: string }
  outputAsset?: { chainId: string; assetId: string }
  inputAmount?: string
  outputAmount?: string
  inputUsdValuation?: UsdAmountValuation
  outputUsdValuation?: UsdAmountValuation
  inputLabel?: string
  outputLabel?: string
  meta?: React.ReactNode
  footer?: React.ReactNode
}) {
  const hasOutput = Boolean(outputAsset || outputAmount || outputUsdValuation)
  const detail = meta ?? footer
  return (
    <div className={`flow-card ${variant === 'root' ? 'root' : ''}`}>
      <div className="flow-card-top">
        {index === undefined ? null : <span className="step-index">#{index}</span>}
        <div>
          <strong>{title}</strong>
          <span>{subtitle}</span>
        </div>
        {status ? (
          <StatusPill status={status} />
        ) : (
          <span className="status-pill empty" aria-hidden="true">
            Completed
          </span>
        )}
      </div>
      <div className={`flow-amounts ${hasOutput ? '' : 'single'}`}>
        <AmountColumn
          label={inputLabel ?? 'in'}
          amount={inputAmount}
          asset={inputAsset}
          usdValuation={inputUsdValuation}
        />
        {hasOutput ? (
          <>
            <span className="flow-arrow">to</span>
            <AmountColumn
              label={outputLabel ?? 'out'}
              amount={outputAmount}
              asset={outputAsset}
              usdValuation={outputUsdValuation}
            />
          </>
        ) : null}
      </div>
      <div className={`flow-meta ${detail ? '' : 'empty'}`}>{detail}</div>
    </div>
  )
}

function AmountColumn({
  label,
  amount,
  asset,
  usdValuation
}: {
  label: string
  amount?: string
  asset?: { chainId: string; assetId: string }
  usdValuation?: UsdAmountValuation
}) {
  const formattedAmount = formatAmount(amount, asset)
  return (
    <div className="amount-column">
      <span>{label}</span>
      <code title={rawAmountTitle(amount, asset)}>{formattedAmount}</code>
      <UsdValue valuation={usdValuation} />
      {asset ? <AssetLabel asset={asset} /> : <em>unknown asset</em>}
    </div>
  )
}

function UsdValue({ valuation }: { valuation?: UsdAmountValuation }) {
  if (!SHOW_USD_VALUES || !valuation?.amountUsdMicro) return null
  return (
    <span
      className="usd-value"
      title={`${valuation.amountUsdMicro} micro-USD at ${formatUsdMicro(
        valuation.unitUsdMicro
      )}/${valuation.canonical.toUpperCase()}`}
    >
      {formatUsdMicro(valuation.amountUsdMicro)}
    </span>
  )
}

function OrderMetadataPanel({ order }: { order: OrderFirehoseRow }) {
  return (
    <div className="detail-block metadata">
      <h3>Order Metadata</h3>
      <dl>
        <dt>Order</dt>
        <dd>
          <code title={order.id}>{order.id}</code>
        </dd>
        <dt>Status</dt>
        <dd>
          <StatusPill status={order.status} />
        </dd>
        <dt>Created</dt>
        <dd>
          <time dateTime={order.createdAt}>{formatDate(order.createdAt)}</time>
        </dd>
        <dt>Updated</dt>
        <dd>
          <time dateTime={order.updatedAt}>{formatDate(order.updatedAt)}</time>
        </dd>
        <dt>Action Timeout</dt>
        <dd>
          <time dateTime={order.actionTimeoutAt}>{formatDate(order.actionTimeoutAt)}</time>
        </dd>
        <dt>Recipient</dt>
        <dd>
          <ExplorerValue
            chainId={order.destination.chainId}
            value={order.recipientAddress}
            kind="address"
            full
          />
        </dd>
        <dt>Refund</dt>
        <dd>
          <ExplorerValue
            chainId={order.source.chainId}
            value={order.refundAddress}
            kind="address"
            full
          />
        </dd>
        <dt>Quote</dt>
        <dd>
          <code title={order.quoteId}>{order.quoteId ?? 'none'}</code>
        </dd>
        <dt>Trace</dt>
        <dd>
          <code title={order.workflowTraceId}>{order.workflowTraceId ?? 'none'}</code>
        </dd>
      </dl>
    </div>
  )
}

function StreamBadge({
  state,
  compact = false
}: {
  state: StreamState
  compact?: boolean
}) {
  const live = state === 'live'
  const Icon = live ? Wifi : WifiOff

  return (
    <span className={`stream-badge ${live ? 'live' : state}`}>
      <Icon size={compact ? 15 : 17} />
      {compact ? null : <span>{humanize(state)}</span>}
    </span>
  )
}

function StatusPill({ status }: { status: string }) {
  return <span className={`status-pill ${statusTone(status)}`}>{humanize(status)}</span>
}

type QuoteFlowLegUsdValuation = NonNullable<UsdValuation['legs']>[number]

type QuoteFlowLeg = {
  key: string
  index: number
  provider: string
  kind: string
  executionStepType?: string
  inputAsset?: AssetRef
  outputAsset?: AssetRef
  amountIn?: string
  amountOut?: string
  minAmountOut?: string
  maxAmountIn?: string
  inputUsdValuation?: UsdAmountValuation
  outputUsdValuation?: UsdAmountValuation
}

type JsonRecord = Record<string, unknown>

function fundingDepositFlow(order: OrderFirehoseRow) {
  return {
    asset: order.source,
    amount: order.quotedInputAmount ?? order.maxAmountIn,
    usdValuation: order.quotedInputAmount
      ? order.quoteUsdValuation?.amounts?.input
      : order.quoteUsdValuation?.amounts?.maxInput
  }
}

function quoteFlowLegs(order: OrderFirehoseRow): QuoteFlowLeg[] {
  if (order.executionLegs.length > 0) {
    return order.executionLegs.map((leg) => ({
      key: leg.id,
      index: leg.legIndex,
      provider: leg.provider,
      kind: leg.legType,
      inputAsset: leg.input,
      outputAsset: leg.output,
      amountIn: leg.amountIn,
      amountOut: leg.expectedAmountOut,
      minAmountOut: leg.minAmountOut,
      inputUsdValuation: leg.usdValuation?.amounts?.plannedInput,
      outputUsdValuation: leg.usdValuation?.amounts?.plannedMinOutput
    }))
  }

  const quote = asRecord(order.providerQuote)
  const rawLegs = asArray(quote?.legs)
  const groupedLegs: Array<{
    key: string
    legs: Array<{
      index: number
      leg?: JsonRecord
      raw?: JsonRecord
      usdValuation?: QuoteFlowLegUsdValuation
    }>
  }> = []
  const groupIndexes = new Map<string, number>()

  rawLegs.forEach((value, index) => {
    const leg = asRecord(value)
    const raw = asRecord(leg?.raw)
    const transitionDeclId = stringField(leg, 'transition_decl_id')
    const groupKey =
      stringField(leg, 'transition_parent_decl_id') ??
      transitionDeclId ??
      stringField(raw, 'id') ??
      `${index}`
    const usdValuation = order.quoteUsdValuation?.legs?.find(
      (candidate) => candidate.index === index || candidate.transitionDeclId === transitionDeclId
    )
    const groupIndex = groupIndexes.get(groupKey)
    if (groupIndex === undefined) {
      groupIndexes.set(groupKey, groupedLegs.length)
      groupedLegs.push({
        key: groupKey,
        legs: [{ index, leg, raw, usdValuation }]
      })
    } else {
      groupedLegs[groupIndex].legs.push({ index, leg, raw, usdValuation })
    }
  })

  return groupedLegs.map((group, index) => {
    const first = group.legs[0]
    const last = group.legs[group.legs.length - 1]
    const providers = [
      ...new Set(
        group.legs
          .map(({ leg }) => stringField(leg, 'provider'))
          .filter((provider): provider is string => Boolean(provider))
      )
    ]
    const firstLeg = first.leg
    const firstRaw = first.raw
    const lastLeg = last.leg
    const lastRaw = last.raw
    return {
      key: group.key,
      index,
      provider: providers.length === 1 ? providers[0] : (providers[0] ?? 'multi'),
      executionStepType: stringField(firstLeg, 'execution_step_type'),
      kind:
        stringField(firstLeg, 'transition_kind') ??
        stringField(firstLeg, 'execution_step_type') ??
        stringField(firstRaw, 'kind') ??
        stringField(firstRaw, 'operationType') ??
        'quote_leg',
      inputAsset: assetRef(firstLeg?.input_asset) ?? assetRef(firstRaw?.input_asset),
      outputAsset: assetRef(lastLeg?.output_asset) ?? assetRef(lastRaw?.output_asset),
      amountIn:
        stringField(firstLeg, 'amount_in') ??
        stringField(firstRaw, 'amount_in') ??
        stringField(firstRaw, 'inputAmount') ??
        stringField(firstRaw, 'srcAmount'),
      amountOut:
        stringField(lastLeg, 'amount_out') ??
        stringField(lastRaw, 'amount_out') ??
        stringField(lastRaw, 'expectedOutputAmount') ??
        stringField(lastRaw, 'destAmount'),
      minAmountOut:
        stringField(lastRaw, 'min_amount_out') ??
        stringField(lastRaw, 'minOutputAmount') ??
        stringField(lastLeg, 'min_amount_out'),
      maxAmountIn:
        stringField(firstRaw, 'max_amount_in') ??
        stringField(firstRaw, 'maxInputAmount') ??
        stringField(firstLeg, 'max_amount_in'),
      inputUsdValuation: first.usdValuation?.amounts?.input,
      outputUsdValuation: last.usdValuation?.amounts?.output
    }
  })
}

type ExecutedAmount = {
  amount: string
  asset: AssetRef
  usdValuation?: UsdAmountValuation
}

type ExecutedAmounts =
  | {
      input: ExecutedAmount
      output: ExecutedAmount
    }
  | {
      placeholder: string
    }

function executedAmounts(order: OrderFirehoseRow): ExecutedAmounts {
  if (order.status !== 'completed') {
    return {
      placeholder: order.executionLegs.length === 0 ? 'Not created yet' : 'Not done yet'
    }
  }

  const executableLegs = order.executionLegs
    .filter((leg) => leg.status === 'completed')
    .sort(
      (left, right) =>
        left.legIndex - right.legIndex ||
        left.createdAt.localeCompare(right.createdAt) ||
        left.id.localeCompare(right.id)
    )
  const firstLeg = executableLegs[0]
  const lastLeg = executableLegs[executableLegs.length - 1]
  if (!firstLeg || !lastLeg) return { placeholder: 'Not observed' }

  const inputAsset = firstLeg.input
  const outputAsset = lastLeg.output
  const inputAmount = firstLeg.actualAmountIn ?? firstLeg.amountIn
  const outputAmount = lastLeg.actualAmountOut

  if (!inputAsset || !outputAsset || !inputAmount || !outputAmount) {
    return { placeholder: 'Not observed' }
  }

  return {
    input: {
      amount: inputAmount,
      asset: inputAsset,
      usdValuation:
        valuationForAmount(inputAmount, inputAsset, firstLeg.usdValuation, [
          'actualInput',
          'plannedInput'
        ]) ??
        valuationForAmount(inputAmount, inputAsset, order.quoteUsdValuation, ['input'])
    },
    output: {
      amount: outputAmount,
      asset: outputAsset,
      usdValuation:
        valuationForAmount(outputAmount, outputAsset, lastLeg.usdValuation, [
          'actualOutput',
          'plannedOutput',
          'plannedMinOutput'
        ]) ??
        valuationForAmount(outputAmount, outputAsset, order.quoteUsdValuation, [
          'output',
          'minOutput'
        ])
    }
  }
}

function valuationForAmount(
  amount: string,
  asset: AssetRef,
  valuation: UsdValuation | undefined,
  preferredKeys: string[]
): UsdAmountValuation | undefined {
  const amounts = valuation?.amounts
  if (!amounts) return undefined
  const preferred = preferredKeys
    .map((key) => amounts[key])
    .filter((candidate): candidate is UsdAmountValuation => Boolean(candidate))
  const candidates = [
    ...preferred,
    ...Object.values(amounts).filter(
      (candidate): candidate is UsdAmountValuation => Boolean(candidate)
    )
  ]

  const exact = candidates.find(
    (candidate) => candidate.raw === amount && sameAsset(candidate.asset, asset)
  )
  if (exact) return exact

  const priced = candidates.find((candidate) => sameAsset(candidate.asset, asset))
  if (!priced) return undefined
  const amountUsdMicro = calculateUsdMicro(
    amount,
    priced.unitUsdMicro,
    priced.decimals
  )
  if (!amountUsdMicro) return undefined

  return {
    raw: amount,
    asset,
    canonical: priced.canonical,
    decimals: priced.decimals,
    unitUsdMicro: priced.unitUsdMicro,
    amountUsdMicro
  }
}

function calculateUsdMicro(
  rawAmount: string,
  unitUsdMicro: string,
  decimals: number
): string | undefined {
  if (!/^\d+$/.test(rawAmount) || !/^\d+$/.test(unitUsdMicro)) return undefined
  const denominator = 10n ** BigInt(decimals)
  if (denominator === 0n) return undefined
  return ((BigInt(rawAmount) * BigInt(unitUsdMicro)) / denominator).toString()
}

function sameAsset(left: AssetRef, right: AssetRef) {
  return (
    left.chainId.toLowerCase() === right.chainId.toLowerCase() &&
    left.assetId.toLowerCase() === right.assetId.toLowerCase()
  )
}

function primaryOperationForStep(order: OrderFirehoseRow, stepId: string) {
  return findLastOperation(
    order.providerOperations,
    (operation) => operation.executionStepId === stepId
  )
}

function findLastOperation(
  operations: ProviderOperation[],
  predicate: (operation: ProviderOperation) => boolean
) {
  for (let index = operations.length - 1; index >= 0; index -= 1) {
    if (predicate(operations[index])) return operations[index]
  }
  return undefined
}

function executionOutputAmount(
  step: OrderExecutionStep,
  operation: ProviderOperation | undefined
) {
  return (
    stringField(asRecord(step.response), 'amount_out') ??
    stringField(asRecord(step.response), 'amountOut') ??
    operationAmount(operation, 'output') ??
    step.minAmountOut
  )
}

function stepTransactionChain(step: OrderExecutionStep) {
  if (['across_bridge', 'cctp_receive', 'unit_withdrawal'].includes(step.stepType)) {
    return step.output?.chainId ?? step.input?.chainId
  }
  return step.input?.chainId ?? step.output?.chainId
}

function isWaitForDepositStep(step: OrderExecutionStep) {
  return step.stepType === WAIT_FOR_DEPOSIT_STEP_TYPE
}

function operationAmount(
  operation: ProviderOperation | undefined,
  side: 'input' | 'output'
) {
  if (!operation) return undefined
  const request = asRecord(operation.request)
  const response = asRecord(operation.response)
  if (side === 'input') {
    return (
      stringField(request, 'amount_in') ??
      stringField(request, 'amountIn') ??
      stringField(request, 'inputAmount') ??
      stringField(request, 'srcAmount') ??
      stringField(response, 'inputAmount') ??
      stringField(response, 'srcAmount')
    )
  }
  return (
    stringField(request, 'amount_out') ??
    stringField(request, 'amountOut') ??
    stringField(request, 'expectedOutputAmount') ??
    stringField(response, 'amount_out') ??
    stringField(response, 'amountOut') ??
    stringField(response, 'expectedOutputAmount') ??
    stringField(response, 'minOutputAmount') ??
    stringField(response, 'destAmount')
  )
}

function operationAsset(
  operation: ProviderOperation | undefined,
  side: 'input' | 'output'
) {
  if (!operation) return undefined
  const request = asRecord(operation.request)
  const response = asRecord(operation.response)
  if (side === 'input') {
    return (
      assetRef(request?.input_asset) ??
      assetRef(request?.inputAsset) ??
      bridgeNativeAsset(request, 'origin') ??
      assetFromTokenFields(request, 'src')
    )
  }
  return (
    assetRef(request?.output_asset) ??
    assetRef(request?.outputAsset) ??
    assetRef(response?.output_asset) ??
    assetRef(response?.outputAsset) ??
    bridgeNativeAsset(request, 'destination') ??
    assetFromTokenFields(request, 'dest')
  )
}

function bridgeNativeAsset(record: JsonRecord | undefined, side: 'origin' | 'destination') {
  const chainId = stringField(record, `${side}_chain_id`)
  return chainId ? { chainId: `evm:${chainId}`, assetId: 'native' } : undefined
}

function assetFromTokenFields(record: JsonRecord | undefined, prefix: 'src' | 'dest') {
  if (!record) return undefined
  const token = stringField(record, `${prefix}Token`)
  const network = stringField(record, 'network')
  if (!token || !network) return undefined
  return { chainId: `evm:${network}`, assetId: token }
}

function assetRef(value: unknown): AssetRef | undefined {
  const record = asRecord(value)
  if (!record) return undefined
  const chainId = stringField(record, 'chainId') ?? stringField(record, 'chain_id')
  const assetId =
    stringField(record, 'assetId') ??
    stringField(record, 'asset_id') ??
    stringField(record, 'asset')
  return chainId && assetId ? { chainId, assetId } : undefined
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : []
}

function asRecord(value: unknown): JsonRecord | undefined {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as JsonRecord)
    : undefined
}

function stringField(record: JsonRecord | undefined, key: string): string | undefined {
  const value = record?.[key]
  if (typeof value === 'string') return value
  if (typeof value === 'number' || typeof value === 'bigint') return String(value)
  return undefined
}

function chainDisplayName(chainId: string) {
  return CHAIN_DISPLAY_NAMES[chainId.toLowerCase()] ?? chainId
}

function assetDisplayName(asset: AssetRef) {
  return ASSET_DISPLAY_NAMES[assetDisplayKey(asset)] ?? asset.assetId
}

function assetDecimals(asset: AssetRef | undefined) {
  return asset ? ASSET_DECIMALS[assetDisplayKey(asset)] : undefined
}

function assetDisplayKey(asset: AssetRef) {
  return `${asset.chainId.toLowerCase()}|${asset.assetId.toLowerCase()}`
}

function explorerUrl(
  chainId: string | undefined,
  value: string,
  kind: 'address' | 'tx' | 'auto'
) {
  if (!chainId) return undefined
  const normalizedChainId = chainId.toLowerCase()
  const normalizedValue = value.trim()
  if (!normalizedValue) return undefined

  if (normalizedChainId === 'bitcoin') {
    if (kind === 'tx' || (kind === 'auto' && isBitcoinTxHash(normalizedValue))) {
      return `https://mempool.space/tx/${normalizedValue}`
    }
    if (kind === 'address' || kind === 'auto') {
      return `https://mempool.space/address/${normalizedValue}`
    }
    return undefined
  }

  const baseUrl = EVM_EXPLORER_BASE_URLS[normalizedChainId]
  if (!baseUrl) return undefined
  if (kind === 'tx' || (kind === 'auto' && isEvmTxHash(normalizedValue))) {
    return `${baseUrl}/tx/${normalizedValue}`
  }
  if (kind === 'address' || (kind === 'auto' && isEvmAddress(normalizedValue))) {
    return `${baseUrl}/address/${normalizedValue}`
  }
  return undefined
}

function explorerAddressUrl(chainId: string, value: string) {
  return explorerUrl(chainId, value, 'address')
}

function isEvmTxHash(value: string) {
  return /^0x[0-9a-fA-F]{64}$/.test(value)
}

function isEvmAddress(value: string) {
  return /^0x[0-9a-fA-F]{40}$/.test(value)
}

function isBitcoinTxHash(value: string) {
  return /^[0-9a-fA-F]{64}$/.test(value)
}

function sortOrders(nextOrders: OrderFirehoseRow[]) {
  return [...nextOrders].sort(compareOrdersByCreatedAt)
}

function upsertOrder(current: OrderFirehoseRow[], order: OrderFirehoseRow) {
  return mergeOrders(current, [order])
}

function mergeOrders(current: OrderFirehoseRow[], incoming: OrderFirehoseRow[]) {
  const byId = new Map(current.map((order) => [order.id, order]))
  for (const order of incoming) {
    const existing = byId.get(order.id)
    byId.set(order.id, preferredOrder(existing, order))
  }
  return sortOrders([...byId.values()])
}

function preferredOrder(
  existing: OrderFirehoseRow | undefined,
  incoming: OrderFirehoseRow
) {
  if (!existing) return incoming
  return Date.parse(incoming.updatedAt) >= Date.parse(existing.updatedAt)
    ? incoming
    : existing
}

function compareOrdersByCreatedAt(a: OrderFirehoseRow, b: OrderFirehoseRow) {
  const byCreatedAt = Date.parse(b.createdAt) - Date.parse(a.createdAt)
  return byCreatedAt === 0 ? b.id.localeCompare(a.id) : byCreatedAt
}

function formatDate(value: string) {
  return new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  }).format(new Date(value))
}

function formatKind(kind: string | undefined) {
  if (!kind) return 'Unknown'
  return kind === 'exact_in' ? 'Exact In' : kind === 'exact_out' ? 'Exact Out' : humanize(kind)
}

function formatAmount(value: string | undefined, asset?: AssetRef) {
  if (!value) return '-'
  const decimals = assetDecimals(asset)
  if (decimals !== undefined) return rawToDecimal(value, decimals)
  return formatRawAmount(value)
}

function rawAmountTitle(value: string | undefined, asset?: AssetRef) {
  if (!value || assetDecimals(asset) === undefined) return undefined
  return `${value} raw`
}

function formatRawAmount(value: string) {
  if (value.length <= 18) return value
  return `${value.slice(0, 10)}...${value.slice(-6)}`
}

function rawToDecimal(value: string, decimals: number) {
  const sign = value.startsWith('-') ? '-' : ''
  const digits = sign ? value.slice(1) : value
  if (!/^\d+$/.test(digits)) return formatRawAmount(value)

  const stripped = digits.replace(/^0+(?=\d)/, '')
  if (decimals === 0) return `${sign}${stripped}`

  const padded = stripped.padStart(decimals + 1, '0')
  const whole = padded.slice(0, -decimals).replace(/^0+(?=\d)/, '')
  const fractional = padded.slice(-decimals).replace(/0+$/, '')
  return fractional ? `${sign}${whole}.${fractional}` : `${sign}${whole}`
}

function formatCount(value: number) {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 0 }).format(value)
}

function formatUsdMicro(value: string) {
  if (!/^\d+$/.test(value)) return '$-'
  const padded = value.padStart(7, '0')
  const whole = padded.slice(0, -6).replace(/^0+(?=\d)/, '')
  const fractional = padded.slice(-6)
  const cents = fractional.slice(0, 2)
  if (whole === '0' && cents === '00') {
    const significant = fractional.replace(/0+$/, '')
    return significant ? `$0.${significant}` : '$0.00'
  }
  return `$${formatWholeNumber(whole)}.${cents}`
}

function formatWholeNumber(value: string) {
  return value.replace(/\B(?=(\d{3})+(?!\d))/g, ',')
}

function shortId(value: string) {
  if (value.length <= 14) return value
  return `${value.slice(0, 8)}...${value.slice(-6)}`
}

function statusTone(status: string) {
  if (['completed', 'processed', 'skipped'].includes(status)) return 'success'
  if (
    [
      'failed',
      'expired',
      'cancelled',
      'refund_required',
      'refund_manual_intervention_required'
    ].includes(status)
  ) {
    return 'danger'
  }
  if (['running', 'executing', 'submitted', 'waiting_external'].includes(status)) {
    return 'active'
  }
  return 'waiting'
}

function humanize(value: string) {
  return value
    .split('_')
    .filter(Boolean)
    .map((part) => `${part[0]?.toUpperCase() ?? ''}${part.slice(1)}`)
    .join(' ')
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : 'Unexpected dashboard error'
}

async function signOut() {
  await authClient.signOut()
  window.location.assign('/')
}

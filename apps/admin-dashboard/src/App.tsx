import {
  Activity,
  ArrowRight,
  BarChart3,
  ChevronDown,
  ChevronRight,
  Check,
  CircleAlert,
  Clock3,
  Copy,
  LogIn,
  LogOut,
  RefreshCw,
  Search,
  ShieldCheck,
  Wifi,
  WifiOff,
  X
} from 'lucide-react'
import { useCallback, useEffect, useRef, useState } from 'react'

import {
  fetchMe,
  fetchOrderById,
  fetchOrders,
  fetchVolumeAnalytics,
  parseMetricsEvent,
  parseRemoveEvent,
  parseSnapshotEvent,
  parseUpsertEvent
} from './api'
import { authClient } from './auth-client'
import type {
  AssetRef,
  MeResponse,
  OrderLifecycleFilter,
  OrderExecutionLeg,
  OrderExecutionStep,
  OrderFirehoseRow,
  OrderMetrics,
  OrderTypeFilter,
  UsdAmountValuation,
  UsdValuation,
  VolumeAnalyticsResponse,
  VolumeBucketSize,
  VolumeOrderTypeFilter
} from './types'

type StreamState = 'idle' | 'connecting' | 'live' | 'closed' | 'error'

const ORDER_LIMIT = 100
const SHOW_USD_VALUES = true
const MAX_VOLUME_CHART_BUCKETS = 1500
const WAIT_FOR_DEPOSIT_STEP_TYPE = 'wait_for_deposit'
const ACTIVE_STEP_STATUSES = new Set(['ready', 'running', 'waiting', 'submitted'])
const FAILED_STEP_STATUSES = new Set(['failed', 'cancelled'])
const TERMINAL_STEP_STATUSES = new Set(['completed', 'failed', 'skipped', 'cancelled'])
type StatusTone = 'success' | 'danger' | 'active' | 'waiting' | 'neutral'
type StatusDisplay = {
  label: string
  tone: StatusTone
  title?: string
}
const ORDER_STATUS_DISPLAY: Record<string, StatusDisplay> = {
  quoted: {
    label: 'Quoted',
    tone: 'waiting',
    title: 'Order exists from a quote; funding vault is not attached'
  },
  pending_funding: {
    label: 'Pending Funding',
    tone: 'waiting',
    title: 'Funding vault exists and is waiting for deposit'
  },
  funded: {
    label: 'Funded',
    tone: 'active',
    title: 'Deposit was observed; execution has not started'
  },
  executing: {
    label: 'Executing',
    tone: 'active',
    title: 'Venue execution is in progress'
  },
  completed: {
    label: 'Completed',
    tone: 'success',
    title: 'Execution finished successfully'
  },
  refund_required: {
    label: 'Refund Required',
    tone: 'danger',
    title: 'Execution cannot complete; automated refund recovery is queued'
  },
  refunding: {
    label: 'Refunding',
    tone: 'active',
    title: 'Automated refund recovery is running'
  },
  refunded: {
    label: 'Refunded',
    tone: 'neutral',
    title: 'Refund finished'
  },
  manual_intervention_required: {
    label: 'Manual Intervention',
    tone: 'danger',
    title: 'Execution state needs operator inspection'
  },
  refund_manual_intervention_required: {
    label: 'Manual Refund',
    tone: 'danger',
    title: 'Refund recovery needs operator action'
  },
  expired: {
    label: 'Expired',
    tone: 'danger',
    title: 'Funding deadline passed before valid funding was observed'
  }
}
const ORDER_TABS: Array<{ type: OrderTypeFilter; label: string }> = [
  { type: 'market_order', label: 'Market Orders' },
  { type: 'limit_order', label: 'Limit Orders' }
]
const ORDER_FILTERS: Array<{ value: OrderLifecycleFilter; label: string }> = [
  { value: 'firehose', label: 'Firehose' },
  { value: 'in_progress', label: 'In Progress' },
  { value: 'needs_attention', label: 'Needs Attention' },
  { value: 'expired', label: 'Expired' },
  { value: 'refunded', label: 'Refunded' }
]
const VOLUME_WINDOWS = [
  { value: '6h', label: '6H' },
  { value: '24h', label: '24H' },
  { value: '7d', label: '7D' },
  { value: '30d', label: '30D' },
  { value: '90d', label: '90D' }
] as const
type VolumeWindow = (typeof VOLUME_WINDOWS)[number]['value']
const CHAIN_DISPLAY_NAMES: Record<string, string> = {
  bitcoin: 'Bitcoin',
  'evm:1': 'Ethereum',
  'evm:42161': 'Arbitrum',
  'evm:8453': 'Base',
  hyperliquid: 'Hyperliquid'
}

const PROVIDER_DISPLAY_NAMES: Record<string, string> = {
  across: 'Across',
  cctp: 'CCTP',
  hyperliquid: 'Hyperliquid',
  internal: 'Funding',
  unit: 'Unit',
  universal_router: 'Uniswap',
  velora: 'Velora'
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
  'hyperliquid|native': 'USDC',
  'hyperliquid|ubtc': 'BTC',
  'hyperliquid|ueth': 'ETH'
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
  'hyperliquid|native': 6,
  'hyperliquid|ubtc': 8,
  'hyperliquid|ueth': 18
}

export function App() {
  const [me, setMe] = useState<MeResponse | null>(null)
  const [orders, setOrders] = useState<OrderFirehoseRow[]>([])
  const [orderTab, setOrderTab] = useState<OrderTypeFilter>('market_order')
  const [orderFilter, setOrderFilter] =
    useState<OrderLifecycleFilter>('firehose')
  const [searchInput, setSearchInput] = useState('')
  const [searchedOrder, setSearchedOrder] = useState<OrderFirehoseRow | null>(
    null
  )
  const [nextCursor, setNextCursor] = useState<string | undefined>()
  const [metrics, setMetrics] = useState<OrderMetrics | null>(null)
  const [volumeAnalytics, setVolumeAnalytics] =
    useState<VolumeAnalyticsResponse | null>(null)
  const [volumeWindow, setVolumeWindow] = useState<VolumeWindow>('24h')
  const [volumeBucketSize, setVolumeBucketSize] =
    useState<VolumeBucketSize>('hour')
  const [volumeOrderType, setVolumeOrderType] =
    useState<VolumeOrderTypeFilter>('all')
  const [expandedOrderId, setExpandedOrderId] = useState<string | null>(null)
  const [detailLoadingOrderIds, setDetailLoadingOrderIds] = useState<Set<string>>(
    () => new Set()
  )
  const [copiedOrderId, setCopiedOrderId] = useState<string | null>(null)
  const [flashingOrderIds, setFlashingOrderIds] = useState<Set<string>>(
    () => new Set()
  )
  const [streamState, setStreamState] = useState<StreamState>('idle')
  const [streamRefreshKey, setStreamRefreshKey] = useState(0)
  const [loading, setLoading] = useState(true)
  const [loadingMore, setLoadingMore] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const loadMoreRef = useRef<HTMLDivElement | null>(null)
  const expandedOrderIdRef = useRef<string | null>(null)
  const loadOrderDetailsRef = useRef<(orderId: string) => void>(() => undefined)
  const loadVolumeAnalyticsRef = useRef<() => Promise<void>>(async () => undefined)
  const copiedOrderTimeoutRef = useRef<number | undefined>(undefined)
  const rowFlashTimeoutsRef = useRef<Map<string, number>>(new Map())

  const loadSession = useCallback(async () => {
    setError(null)
    const session = await fetchMe()
    setMe(session)
    return session
  }, [])

  const loadVolumeAnalytics = useCallback(async () => {
    const range = volumeDateRange(volumeWindow)
    const response = await fetchVolumeAnalytics({
      bucketSize: volumeBucketSize,
      orderType: volumeOrderType,
      from: range.from.toISOString(),
      to: range.to.toISOString()
    })
    setVolumeAnalytics(response)
  }, [volumeBucketSize, volumeOrderType, volumeWindow])

  const loadMoreOrders = useCallback(async () => {
    if (!nextCursor || loadingMore || searchedOrder) return

    setLoadingMore(true)
    setError(null)
    try {
      const response = await fetchOrders(
        ORDER_LIMIT,
        nextCursor,
        orderTab,
        orderFilter,
        false
      )
      setOrders((current) => mergeOrders(current, response.orders))
      setNextCursor(response.nextCursor)
      if (response.metrics) setMetrics(response.metrics)
    } catch (loadError) {
      setError(errorMessage(loadError))
    } finally {
      setLoadingMore(false)
    }
  }, [loadingMore, nextCursor, orderFilter, orderTab, searchedOrder])

  const searchOrder = useCallback(async () => {
    const orderId = searchInput.trim()
    if (!orderId) {
      setSearchedOrder(null)
      return
    }
    if (!isUuid(orderId)) {
      setError('Enter a full order UUID')
      return
    }

    setError(null)
    const response = await fetchOrderById(orderId)
    setSearchedOrder(response.order)
    if (
      response.order.orderType === 'market_order' ||
      response.order.orderType === 'limit_order'
    ) {
      setOrderTab(response.order.orderType)
    }
    setExpandedOrderId(response.order.id)
  }, [searchInput])

  const clearSearch = useCallback(() => {
    setSearchInput('')
    setSearchedOrder(null)
    setExpandedOrderId(null)
  }, [])

  const loadOrderDetails = useCallback(async (orderId: string) => {
    setDetailLoadingOrderIds((current) => {
      const next = new Set(current)
      next.add(orderId)
      return next
    })
    try {
      const response = await fetchOrderById(orderId)
      setOrders((current) => upsertOrder(current, response.order))
      setSearchedOrder((current) =>
        current?.id === orderId ? response.order : current
      )
    } catch (loadError) {
      setError(errorMessage(loadError))
    } finally {
      setDetailLoadingOrderIds((current) => {
        const next = new Set(current)
        next.delete(orderId)
        return next
      })
    }
  }, [])

  const toggleOrderExpansion = useCallback(
    (order: OrderFirehoseRow) => {
      setExpandedOrderId((current) => (current === order.id ? null : order.id))
      if (expandedOrderId !== order.id && order.detailLevel !== 'full') {
        void loadOrderDetails(order.id)
      }
    },
    [expandedOrderId, loadOrderDetails]
  )

  const changeOrderTab = useCallback(
    (type: OrderTypeFilter) => {
      setOrderTab(type)
      clearSearch()
    },
    [clearSearch]
  )

  const changeOrderFilter = useCallback(
    (filter: OrderLifecycleFilter) => {
      setOrderFilter(filter)
      clearSearch()
    },
    [clearSearch]
  )

  const refreshOrderStream = useCallback(() => {
    setStreamRefreshKey((current) => current + 1)
  }, [])

  const copyOrderId = useCallback(async (orderId: string) => {
    try {
      await copyTextToClipboard(orderId)
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
    expandedOrderIdRef.current = expandedOrderId
  }, [expandedOrderId])

  useEffect(() => {
    loadOrderDetailsRef.current = (orderId: string) => {
      void loadOrderDetails(orderId)
    }
  }, [loadOrderDetails])

  useEffect(() => {
    loadVolumeAnalyticsRef.current = loadVolumeAnalytics
  }, [loadVolumeAnalytics])

  useEffect(() => {
    let cancelled = false

    const load = async () => {
      try {
        await loadSession()
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
  }, [loadSession])

 useEffect(() => {
   if (!me?.authorized || !me.analyticsConfigured) return
   let cancelled = false

    const load = async () => {
      try {
        const range = volumeDateRange(volumeWindow)
        const response = await fetchVolumeAnalytics({
          bucketSize: volumeBucketSize,
          orderType: volumeOrderType,
          from: range.from.toISOString(),
          to: range.to.toISOString()
        })
        if (!cancelled) setVolumeAnalytics(response)
      } catch (loadError) {
        if (!cancelled) setError(errorMessage(loadError))
      }
    }

   void load()
   return () => {
     cancelled = true
   }
 }, [
    me?.analyticsConfigured,
    me?.authorized,
    volumeBucketSize,
    volumeOrderType,
    volumeWindow
  ])

  useEffect(() => {
    if (!me?.authorized) return

    setStreamState('connecting')
    setOrders([])
    setNextCursor(undefined)
    setExpandedOrderId(null)
    setLoadingMore(false)
    setError(null)
    const params = new URLSearchParams({
      limit: String(ORDER_LIMIT),
      orderType: orderTab
    })
    if (orderFilter !== 'firehose') params.set('filter', orderFilter)
    const source = new EventSource(`/api/orders/events?${params.toString()}`, {
      withCredentials: true
    })
    const rejectStreamEvent = (streamError: unknown) => {
      console.error('admin dashboard order stream event rejected', streamError)
      setStreamState('error')
      setError(errorMessage(streamError))
      source.close()
    }

    source.addEventListener('open', () => setStreamState('live'))
    source.addEventListener('snapshot', (event) => {
      try {
        const snapshot = parseSnapshotEvent(event as MessageEvent<string>)
        setOrders((current) => mergeSnapshotOrders(current, snapshot.orders))
        setNextCursor(snapshot.nextCursor)
        if (snapshot.metrics) setMetrics(snapshot.metrics)
        setStreamState('live')
        if (expandedOrderIdRef.current) {
          const expandedSummary = snapshot.orders.find(
            (order) => order.id === expandedOrderIdRef.current
          )
          if (expandedSummary && expandedSummary.detailLevel !== 'full') {
            loadOrderDetailsRef.current(expandedSummary.id)
          }
        }
      } catch (streamError) {
        rejectStreamEvent(streamError)
      }
    })
    source.addEventListener('upsert', (event) => {
      try {
        const { order, metrics: nextMetrics, analyticsChanged } = parseUpsertEvent(
          event as MessageEvent<string>
        )
        setOrders((current) => upsertVisibleOrder(current, order))
        setSearchedOrder((current) => (current?.id === order.id ? order : current))
        if (nextMetrics) setMetrics(nextMetrics)
        markOrderFlashed(order.id)
        if (expandedOrderIdRef.current === order.id && order.detailLevel !== 'full') {
          loadOrderDetailsRef.current(order.id)
        }
        if (
          (analyticsChanged || order.status === 'completed') &&
          me.analyticsConfigured
        ) {
          void loadVolumeAnalyticsRef
            .current()
            .catch((loadError) => setError(errorMessage(loadError)))
        }
      } catch (streamError) {
        rejectStreamEvent(streamError)
      }
    })
    source.addEventListener('remove', (event) => {
      try {
        const { id, metrics: nextMetrics, analyticsChanged } = parseRemoveEvent(
          event as MessageEvent<string>
        )
        setOrders((current) => current.filter((order) => order.id !== id))
        setSearchedOrder((current) => (current?.id === id ? null : current))
        if (nextMetrics) setMetrics(nextMetrics)
        if (analyticsChanged && me.analyticsConfigured) {
          void loadVolumeAnalyticsRef
            .current()
            .catch((loadError) => setError(errorMessage(loadError)))
        }
      } catch (streamError) {
        rejectStreamEvent(streamError)
      }
    })
    source.addEventListener('metrics', (event) => {
      try {
        const { metrics: nextMetrics, analyticsChanged } = parseMetricsEvent(
          event as MessageEvent<string>
        )
        if (nextMetrics) setMetrics(nextMetrics)
        if (analyticsChanged && me.analyticsConfigured) {
          void loadVolumeAnalyticsRef
            .current()
            .catch((loadError) => setError(errorMessage(loadError)))
        }
      } catch (streamError) {
        rejectStreamEvent(streamError)
      }
    })
    source.addEventListener('error', () => {
      setStreamState(source.readyState === EventSource.CLOSED ? 'closed' : 'error')
    })

    return () => {
      source.close()
      setStreamState('closed')
    }
  }, [
    markOrderFlashed,
    me?.analyticsConfigured,
    me?.authorized,
    orderFilter,
    orderTab,
    streamRefreshKey
  ])

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

  const displayedOrders = searchedOrder ? [searchedOrder] : orders

  return (
    <Shell
      right={
        <UserMenu
          me={me}
          streamState={streamState}
          onRefresh={refreshOrderStream}
        />
      }
    >
      <main className="dashboard">
        {me.analyticsConfigured ? (
          <VolumePanel
            analytics={volumeAnalytics}
            window={volumeWindow}
            bucketSize={volumeBucketSize}
            orderType={volumeOrderType}
            onWindowChange={setVolumeWindow}
            onBucketSizeChange={setVolumeBucketSize}
            onOrderTypeChange={setVolumeOrderType}
          />
        ) : null}

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
        </section>

        {!me.routerAdminKeyConfigured ? <SystemDownToast /> : null}
        {error ? <div className="notice error">{error}</div> : null}

        <section className="orders-surface" aria-label="Orders">
          <div className="table-toolbar">
            <div>
              <h2>Orders</h2>
              <p>created_at DESC</p>
            </div>
            <div className="table-toolbar-actions">
              <OrderTabs active={orderTab} onChange={changeOrderTab} />
              <StreamBadge state={streamState} />
            </div>
          </div>

          <div className="table-controls">
            <OrderSearch
              value={searchInput}
              active={Boolean(searchedOrder)}
              onChange={setSearchInput}
              onSearch={searchOrder}
              onClear={clearSearch}
            />
            <OrderFilterButtons active={orderFilter} onChange={changeOrderFilter} />
          </div>

          <div className="table-scroll">
            <table className="orders-table">
              <thead>
                <tr>
                  <th aria-label="Expand"></th>
                  <th>Created</th>
                  <th>Order</th>
                  <th>Route</th>
                  <th>{orderTab === 'limit_order' ? 'Limit State' : 'Kind'}</th>
                  <th>Quote</th>
                  <th>Executed</th>
                  <th>{orderTab === 'limit_order' ? 'Backend Status' : 'Status'}</th>
                  <th>Venue Progress</th>
                </tr>
              </thead>
              <tbody>
                {displayedOrders.length === 0 ? (
                  <tr>
                    <td className="empty" colSpan={9}>
                      No orders found
                    </td>
                  </tr>
                ) : (
                  displayedOrders.map((order) => (
                    <OrderRows
                      key={order.id}
                      order={order}
                      expanded={expandedOrderId === order.id}
                      loadingDetails={detailLoadingOrderIds.has(order.id)}
                      flashing={flashingOrderIds.has(order.id)}
                      copied={copiedOrderId === order.id}
                      onToggle={() => toggleOrderExpansion(order)}
                      onCopyOrderId={() => void copyOrderId(order.id)}
                    />
                  ))
                )}
              </tbody>
            </table>
          </div>
          <div ref={loadMoreRef} className="pagination-sentinel">
            {searchedOrder ? (
              <span>Search result</span>
            ) : loadingMore ? (
              <>
                <RefreshCw size={15} className="spin" />
                <span>Loading older orders</span>
              </>
            ) : nextCursor ? (
              <span>Older orders available</span>
            ) : displayedOrders.length > 0 ? (
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

function SystemDownToast() {
  return (
    <div className="system-toast" role="alert">
      <CircleAlert size={17} />
      <div>
        <strong>System down</strong>
        <span>Router admin API key is not configured.</span>
      </div>
    </div>
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

function OrderTabs({
  active,
  onChange
}: {
  active: OrderTypeFilter
  onChange: (type: OrderTypeFilter) => void
}) {
  return (
    <div className="order-tabs" role="tablist" aria-label="Order type">
      {ORDER_TABS.map((tab) => (
        <button
          key={tab.type}
          type="button"
          role="tab"
          aria-selected={active === tab.type}
          className={active === tab.type ? 'active' : undefined}
          onClick={() => onChange(tab.type)}
        >
          {tab.label}
        </button>
      ))}
    </div>
  )
}

function OrderSearch({
  value,
  active,
  onChange,
  onSearch,
  onClear
}: {
  value: string
  active: boolean
  onChange: (value: string) => void
  onSearch: () => void
  onClear: () => void
}) {
  return (
    <form
      className={`order-search ${active ? 'active' : ''}`}
      onSubmit={(event) => {
        event.preventDefault()
        void onSearch()
      }}
    >
      <Search size={15} />
      <input
        value={value}
        placeholder="Search order ID"
        spellCheck={false}
        onChange={(event) => onChange(event.target.value)}
      />
      {value || active ? (
        <button type="button" className="search-clear" onClick={onClear} aria-label="Clear search">
          <X size={14} />
        </button>
      ) : null}
      <button type="submit">Search</button>
    </form>
  )
}

function OrderFilterButtons({
  active,
  onChange
}: {
  active: OrderLifecycleFilter
  onChange: (filter: OrderLifecycleFilter) => void
}) {
  return (
    <div className="filter-buttons" aria-label="Order filters">
      {ORDER_FILTERS.map((filter) => (
        <button
          key={filter.value}
          type="button"
          className={active === filter.value ? 'active' : undefined}
          onClick={() => onChange(filter.value)}
        >
          {filter.label}
        </button>
      ))}
    </div>
  )
}

function VolumePanel({
  analytics,
  window,
  bucketSize,
  orderType,
  onWindowChange,
  onBucketSizeChange,
  onOrderTypeChange
}: {
  analytics: VolumeAnalyticsResponse | null
  window: VolumeWindow
  bucketSize: VolumeBucketSize
  orderType: VolumeOrderTypeFilter
  onWindowChange: (value: VolumeWindow) => void
  onBucketSizeChange: (value: VolumeBucketSize) => void
  onOrderTypeChange: (value: VolumeOrderTypeFilter) => void
}) {
  const points = analytics?.buckets ?? []
  const totalUsdMicro = points.reduce(
    (sum, bucket) => sum + BigInt(bucket.volumeUsdMicro),
    0n
  )

  return (
    <section className="volume-panel" aria-label="Completed volume">
      <div className="volume-header">
        <div>
          <span className="section-kicker">
            <BarChart3 size={15} />
            Completed Volume
          </span>
          <strong>{formatUsdMicro(totalUsdMicro.toString())}</strong>
        </div>
        <div className="volume-controls">
          <SegmentedControl
            label="Window"
            value={window}
            options={VOLUME_WINDOWS}
            onChange={onWindowChange}
          />
          <select
            value={bucketSize}
            onChange={(event) =>
              onBucketSizeChange(event.target.value as VolumeBucketSize)
            }
            aria-label="Volume bucket size"
          >
            <option value="five_minute">5 Minute</option>
            <option value="hour">Hour</option>
            <option value="day">Day</option>
          </select>
          <select
            value={orderType}
            onChange={(event) =>
              onOrderTypeChange(event.target.value as VolumeOrderTypeFilter)
            }
            aria-label="Volume order type"
          >
            <option value="all">All Orders</option>
            <option value="market_order">Market</option>
            <option value="limit_order">Limit</option>
          </select>
        </div>
      </div>
      <VolumeChart analytics={analytics} />
    </section>
  )
}

function SegmentedControl<T extends string>({
  label,
  value,
  options,
  onChange
}: {
  label: string
  value: T
  options: ReadonlyArray<{ value: T; label: string }>
  onChange: (value: T) => void
}) {
  return (
    <div className="segmented-control" aria-label={label}>
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          className={value === option.value ? 'active' : undefined}
          onClick={() => onChange(option.value)}
        >
          {option.label}
        </button>
      ))}
    </div>
  )
}

function VolumeChart({
  analytics
}: {
  analytics: VolumeAnalyticsResponse | null
}) {
  if (!analytics) {
    return <div className="volume-empty">Loading volume buckets</div>
  }

  const buckets = normalizedVolumeBuckets(analytics)
  if (buckets.length === 0) {
    return <div className="volume-empty">No completed volume in this window</div>
  }

  const width = 720
  const height = 220
  const padding = { top: 22, right: 20, bottom: 46, left: 74 }
  const chartWidth = width - padding.left - padding.right
  const chartHeight = height - padding.top - padding.bottom
  const maxVolumeUsdMicro = buckets.reduce(
    (max, bucket) =>
      bucket.volumeUsdMicro > max ? bucket.volumeUsdMicro : max,
    0n
  )
  const maxVolume = maxVolumeUsdMicro > 0n ? maxVolumeUsdMicro : 1n
  const slotWidth = chartWidth / buckets.length
  const barGap = buckets.length > 180 ? 1 : 3
  const barWidth = Math.max(slotWidth - barGap, 1)
  const yTicks = volumeYAxisTicks(maxVolume, 4)
  const xTicks = volumeXAxisTicks(buckets, 6)

  return (
    <div className="volume-chart-wrap">
      <svg viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Volume chart">
        {yTicks.map((tick) => {
          const y =
            height -
            padding.bottom -
            volumeBarHeight(tick.value, maxVolume, chartHeight)
          return (
            <g key={tick.key}>
              <line
                x1={padding.left}
                y1={y}
                x2={width - padding.right}
                y2={y}
                className="volume-grid"
              />
              <text
                x={padding.left - 10}
                y={y}
                className="volume-y-label"
                textAnchor="end"
                dominantBaseline="middle"
              >
                {formatUsdMicro(tick.value.toString())}
              </text>
            </g>
          )
        })}
        <line
          x1={padding.left}
          y1={height - padding.bottom}
          x2={width - padding.right}
          y2={height - padding.bottom}
          className="volume-axis"
        />
        <line
          x1={padding.left}
          y1={padding.top}
          x2={padding.left}
          y2={height - padding.bottom}
          className="volume-axis"
        />
        {xTicks.map((tick) => {
          const x = padding.left + tick.index * slotWidth + slotWidth / 2
          const textAnchor =
            tick.index === 0
              ? 'start'
              : tick.index === buckets.length - 1
                ? 'end'
                : 'middle'
          return (
            <g key={tick.bucket.bucketStart}>
              <line
                x1={x}
                y1={height - padding.bottom}
                x2={x}
                y2={height - padding.bottom + 5}
                className="volume-axis"
              />
              <text
                x={x}
                y={height - 22}
                className="volume-x-label"
                textAnchor={textAnchor}
              >
                {formatVolumeAxisDate(tick.bucket.bucketStart, analytics.bucketSize)}
              </text>
            </g>
          )
        })}
        {buckets.map((bucket, index) => {
          const barHeight = volumeBarHeight(
            bucket.volumeUsdMicro,
            maxVolume,
            chartHeight
          )
          const slotX = padding.left + index * slotWidth
          const x = slotX + Math.max((slotWidth - barWidth) / 2, 0)
          const y = height - padding.bottom - barHeight
          const tooltip = volumeTooltipLayout(
            x + barWidth / 2,
            y,
            padding,
            width
          )
          const label = `${formatDate(bucket.bucketStart)}, ${formatUsdMicro(
            bucket.volumeUsdMicro.toString()
          )}, ${formatCount(bucket.orderCount)} orders`
          return (
            <g
              key={bucket.bucketStart}
              className="volume-bucket"
              tabIndex={0}
              aria-label={label}
            >
              <rect
                className="volume-hit-area"
                x={slotX}
                y={padding.top}
                width={slotWidth}
                height={chartHeight}
              />
              <rect
                className="volume-bar"
                x={x}
                y={y}
                width={barWidth}
                height={barHeight}
                rx={3}
              />
              <g
                className="volume-tooltip"
                transform={`translate(${tooltip.x} ${tooltip.y})`}
              >
                <rect width={tooltip.width} height={tooltip.height} rx={6} />
                <text x={10} y={17} className="volume-tooltip-value">
                  {formatUsdMicro(bucket.volumeUsdMicro.toString())}
                </text>
                <text x={10} y={33} className="volume-tooltip-meta">
                  {formatDate(bucket.bucketStart)}
                </text>
                <text
                  x={tooltip.width - 10}
                  y={33}
                  className="volume-tooltip-meta"
                  textAnchor="end"
                >
                  {formatCount(bucket.orderCount)}
                </text>
              </g>
            </g>
          )
        })}
      </svg>
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
  loadingDetails,
  flashing,
  copied,
  onToggle,
  onCopyOrderId
}: {
  order: OrderFirehoseRow
  expanded: boolean
  loadingDetails: boolean
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
          {order.orderType === 'limit_order' ? (
            <LimitStateCell order={order} />
          ) : (
            <span className="kind-pill">{formatKind(order.orderKind)}</span>
          )}
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
            {loadingDetails ? <DetailsLoading /> : <OrderDetails order={order} />}
          </td>
        </tr>
      ) : null}
    </>
  )
}

function DetailsLoading() {
  return (
    <div className="details-loading">
      <RefreshCw size={16} className="spin" />
      <span>Loading order details</span>
    </div>
  )
}

function LimitStateCell({ order }: { order: OrderFirehoseRow }) {
  const limitStatus = order.limitStatus
  if (!limitStatus) {
    return <span className="kind-pill">{formatKind(order.orderKind)}</span>
  }

  return (
    <div className="limit-state">
      <span className={`limit-state-pill ${limitStatus.tone}`}>
        {limitStatus.label}
      </span>
      {limitStatus.detail ? <em>{limitStatus.detail}</em> : null}
    </div>
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
          rel="noopener noreferrer"
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
      <code className="mono compact-id">{shortOrderId(orderId)}</code>
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
      rel="noopener noreferrer"
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
  const activeVenue = progressVenueLabel(order)
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
      {activeVenue ? <em>{activeVenue}</em> : null}
    </div>
  )
}

function OrderDetails({ order }: { order: OrderFirehoseRow }) {
  return (
    <div className="details-grid">
      <OrderTimelinePanel order={order} />
      <OrderMetadataPanel order={order} />
    </div>
  )
}

type TimelineLeg = {
  key: string
  index: number
  provider: string
  kind: string
  status: string
  inputAsset?: AssetRef
  outputAsset?: AssetRef
  quotedInput?: string
  quotedOutput?: string
  executedInput?: string
  executedOutput?: string
  quotedInputUsd?: UsdAmountValuation
  quotedOutputUsd?: UsdAmountValuation
  executedInputUsd?: UsdAmountValuation
  executedOutputUsd?: UsdAmountValuation
  minAmountOut?: string
  maxAmountIn?: string
  updatedAt?: string
  steps: OrderExecutionStep[]
  references: TimelineReference[]
}

type TimelineReference = {
  key: string
  label: string
  value: string
  chainId?: string
  kind: 'address' | 'tx' | 'auto'
}

function OrderTimelinePanel({ order }: { order: OrderFirehoseRow }) {
  const legs = timelineLegs(order)

  return (
    <div className="detail-block timeline-block">
      <div className="timeline-header">
        <div>
          <h3>Order Timeline</h3>
          <p>{chainDisplayName(order.source.chainId)} to {chainDisplayName(order.destination.chainId)}</p>
        </div>
        <StatusPill status={order.status} />
      </div>
      <div className="timeline-list">
        <FundingTimelineItem order={order} />
        {legs.length === 0 ? (
          <div className="timeline-empty">No route legs materialized</div>
        ) : (
          legs.map((leg) => <TimelineLegItem key={leg.key} leg={leg} />)
        )}
      </div>
    </div>
  )
}

function FundingTimelineItem({ order }: { order: OrderFirehoseRow }) {
  const fundingDeposit = fundingDepositFlow(order)
  const waitStep = order.executionSteps.find((step) => step.stepType === 'wait_for_deposit')
  const completed = waitStep?.status === 'completed'

  return (
    <div className="timeline-item root">
      <div className="timeline-rail">
        <span className="timeline-dot root" />
      </div>
      <div className="timeline-card root">
        <div className="timeline-card-header">
          <div>
            <strong>Funding</strong>
            <span>Source deposit</span>
          </div>
          <StatusPill status={waitStep?.status ?? 'planned'} />
        </div>
        <div className="timeline-compare single">
          <TimelineAmountGroup
            title="Required"
            input={{
              label: 'deposit',
              amount: fundingDeposit.amount,
              asset: fundingDeposit.asset,
              usdValuation: fundingDeposit.usdValuation
            }}
          />
          <TimelineAmountGroup
            title="Observed"
            muted={!completed}
            input={
              completed
                ? {
                    label: 'deposit',
                    amount: fundingDeposit.amount,
                    asset: fundingDeposit.asset,
                    usdValuation: fundingDeposit.usdValuation
                  }
                : undefined
            }
            placeholder={completed ? undefined : 'Not funded yet'}
          />
        </div>
        {waitStep ? (
          <div className="timeline-funding-addresses">
            <TimelineAction step={waitStep} />
          </div>
        ) : null}
      </div>
    </div>
  )
}

function TimelineLegItem({ leg }: { leg: TimelineLeg }) {
  const outputDelta = amountDelta(leg.quotedOutput, leg.executedOutput)
  const inputDelta = amountDelta(leg.quotedInput, leg.executedInput)
  const primaryDelta = outputDelta ?? inputDelta

  return (
    <div className="timeline-item">
      <div className="timeline-rail">
        <span className="timeline-dot">#{leg.index}</span>
      </div>
      <div className="timeline-card">
        <div className="timeline-card-header">
          <div>
            <strong>{humanize(leg.provider)}</strong>
            <span>{humanize(leg.kind)}</span>
          </div>
          <StatusPill status={leg.status} />
        </div>
        <div className="timeline-route">
          {leg.inputAsset ? <AssetLabel asset={leg.inputAsset} /> : <em>unknown input</em>}
          <span>to</span>
          {leg.outputAsset ? <AssetLabel asset={leg.outputAsset} /> : <em>unknown output</em>}
        </div>
        <div className="timeline-compare">
          <TimelineAmountGroup
            title="Quoted"
            input={{
              label: 'in',
              amount: leg.quotedInput,
              asset: leg.inputAsset,
              usdValuation: leg.quotedInputUsd
            }}
            output={{
              label: 'out',
              amount: leg.quotedOutput,
              asset: leg.outputAsset,
              usdValuation: leg.quotedOutputUsd
            }}
          />
          <TimelineAmountGroup
            title="Executed"
            muted={!leg.executedInput && !leg.executedOutput}
            input={
              leg.executedInput
                ? {
                    label: 'in',
                    amount: leg.executedInput,
                    asset: leg.inputAsset,
                    usdValuation: leg.executedInputUsd
                  }
                : undefined
            }
            output={
              leg.executedOutput
                ? {
                    label: 'out',
                    amount: leg.executedOutput,
                    asset: leg.outputAsset,
                    usdValuation: leg.executedOutputUsd
                  }
                : undefined
            }
            placeholder={
              leg.executedInput || leg.executedOutput
                ? undefined
                : timelineExecutionPlaceholder(leg)
            }
          />
        </div>
        <div className="timeline-card-footer">
          <div className="timeline-card-summary">
            {primaryDelta ? <DeltaPill delta={primaryDelta} /> : null}
            <TimelineGuardrails leg={leg} />
          </div>
          <TimelineActions steps={leg.steps} />
        </div>
      </div>
    </div>
  )
}

function TimelineAmountGroup({
  title,
  input,
  output,
  muted = false,
  placeholder
}: {
  title: string
  input?: AmountPairSide
  output?: AmountPairSide
  muted?: boolean
  placeholder?: string
}) {
  return (
    <div className={`timeline-amount-group ${muted ? 'muted' : ''}`}>
      <h4>{title}</h4>
      {placeholder ? <div className="timeline-placeholder">{placeholder}</div> : null}
      {input ? <TimelineAmountRow side={input} /> : null}
      {output ? <TimelineAmountRow side={output} /> : null}
    </div>
  )
}

function TimelineAmountRow({ side }: { side: AmountPairSide }) {
  return (
    <div className="timeline-amount-row">
      <span>{side.label}</span>
      <code title={rawAmountTitle(side.amount, side.asset)}>
        {formatAmount(side.amount, side.asset)}
      </code>
      {side.asset ? <AssetLabel asset={side.asset} /> : <em>unknown asset</em>}
      <UsdValue valuation={side.usdValuation} />
    </div>
  )
}

export function timelineExecutionPlaceholder(leg: TimelineLeg) {
  const hasSubmittedTx = leg.steps.some(
    (step) => Boolean(step.txHash) && !TERMINAL_STEP_STATUSES.has(step.status)
  )
  if (hasSubmittedTx) return 'Transaction submitted, awaiting settlement'
  return 'Not executed yet'
}

function TimelineGuardrails({ leg }: { leg: TimelineLeg }) {
  const guardrails = [
    leg.minAmountOut && leg.outputAsset
      ? {
          label: 'min out',
          amount: leg.minAmountOut,
          asset: leg.outputAsset
        }
      : undefined,
    leg.maxAmountIn && leg.inputAsset
      ? {
          label: 'max in',
          amount: leg.maxAmountIn,
          asset: leg.inputAsset
        }
      : undefined
  ].filter((guardrail): guardrail is { label: string; amount: string; asset: AssetRef } =>
    Boolean(guardrail)
  )

  if (guardrails.length === 0) return null

  return (
    <div className="timeline-guardrails">
      {guardrails.map((guardrail) => (
        <span key={guardrail.label}>
          {guardrail.label} <code title={rawAmountTitle(guardrail.amount, guardrail.asset)}>
            {formatAmount(guardrail.amount, guardrail.asset)}
          </code>
        </span>
      ))}
    </div>
  )
}

function TimelineActions({ steps }: { steps: OrderExecutionStep[] }) {
  const visibleSteps = steps.filter((step) => !isWaitForDepositStep(step))
  if (visibleSteps.length === 0) return <span className="timeline-actions empty">No actions</span>

  return (
    <div className="timeline-actions">
      {visibleSteps.map((step) => <TimelineAction key={step.id} step={step} />)}
    </div>
  )
}

function TimelineAction({ step }: { step: OrderExecutionStep }) {
  const addresses = stepActionAddresses(step)

  return (
    <div className="timeline-action">
      <TimelineActionIdentifier step={step} />
      <div className="timeline-action-flow">
        <TimelineActionEndpoint
          label="sender"
          address={addresses.sender}
          addresses={addresses.senders}
        />
        <ArrowRight className="timeline-action-arrow" size={14} aria-hidden="true" />
        <TimelineActionEndpoint label="recipient" address={addresses.recipient} />
      </div>
    </div>
  )
}

function TimelineActionIdentifier({ step }: { step: OrderExecutionStep }) {
  if (step.txHash || isWaitForDepositStep(step)) {
    return (
      <span className="timeline-action-identifier">
        <span>tx</span>
        {step.txHash ? (
          <TimelineLinkedValue
            chainId={stepTransactionChain(step)}
            value={step.txHash}
            kind="auto"
          />
        ) : (
          <span className="timeline-inline-value missing">unknown</span>
        )}
      </span>
    )
  }

  const venueId = stepVenueIdentifier(step)
  return (
    <span className="timeline-action-identifier">
      <span>venue id</span>
      {venueId ? (
        <TimelineLinkedValue
          chainId={stepTransactionChain(step)}
          value={venueId}
          kind="auto"
        />
      ) : (
        <span className="timeline-inline-value missing">not created</span>
      )}
    </span>
  )
}

function TimelineActionEndpoint({
  label,
  address,
  addresses
}: {
  label: string
  address?: { address: string; chainId?: string }
  addresses?: Array<{ address: string; chainId?: string }>
}) {
  const visibleAddress = addresses?.[0] ?? address
  const extraCount = Math.max((addresses?.length ?? 0) - 1, 0)

  return (
    <span className="timeline-action-endpoint">
      <span>{label}</span>
      {visibleAddress?.address ? (
        <span className="timeline-action-address-group">
          <TimelineLinkedValue
            chainId={visibleAddress.chainId}
            value={visibleAddress.address}
            kind="address"
          />
          {extraCount > 0 ? (
            <span className="timeline-inline-value">+{extraCount}</span>
          ) : null}
        </span>
      ) : (
        <span className="timeline-inline-value missing">unknown</span>
      )}
    </span>
  )
}

function TimelineLinkedValue({
  chainId,
  value,
  kind
}: {
  chainId?: string
  value: string
  kind: 'address' | 'tx' | 'auto'
}) {
  const url = explorerUrl(chainId, value, kind)
  const label = shortId(value)
  if (!url) {
    return (
      <span className="timeline-inline-value" title={value}>
        {label}
      </span>
    )
  }

  return (
    <a
      className="timeline-inline-value"
      href={url}
      title={value}
      target="_blank"
      rel="noopener noreferrer"
      onClick={(event) => event.stopPropagation()}
    >
      {label}
    </a>
  )
}

function TimelineReferences({ references }: { references: TimelineReference[] }) {
  if (references.length === 0) return null

  return (
    <div className="timeline-references">
      {references.map((reference) => (
        <span key={reference.key} className="timeline-reference">
          <span>{reference.label}</span>
          <ExplorerValue
            chainId={reference.chainId}
            value={reference.value}
            kind={reference.kind}
          />
        </span>
      ))}
    </div>
  )
}

function DeltaPill({ delta }: { delta: AmountDelta }) {
  return (
    <span className={`delta-pill ${delta.tone}`}>
      {delta.label}
    </span>
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
  const display = statusDisplay(status)
  return (
    <span className={`status-pill ${display.tone}`} title={display.title ?? status}>
      {display.label}
    </span>
  )
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

export function timelineLegs(order: OrderFirehoseRow): TimelineLeg[] {
  const stepsByLegId = new Map<string, OrderExecutionStep[]>()
  for (const step of order.executionSteps) {
    if (!step.executionLegId) continue
    const existing = stepsByLegId.get(step.executionLegId) ?? []
    existing.push(step)
    stepsByLegId.set(step.executionLegId, existing)
  }

  if (order.executionLegs.length > 0) {
    return [...order.executionLegs]
      .sort(
        (left, right) =>
          left.legIndex - right.legIndex ||
          left.createdAt.localeCompare(right.createdAt) ||
          left.id.localeCompare(right.id)
      )
      .map((leg) => {
        const steps = stepsByLegId.get(leg.id) ?? []
        const quoteValuation = quoteUsdValuationForExecutionLeg(order, leg)
        const executedInput = leg.actualAmountIn
        const executedOutput = leg.actualAmountOut
        return {
          key: leg.id,
          index: leg.legIndex,
          provider: leg.provider,
          kind: leg.legType,
          status: leg.status,
          inputAsset: leg.input,
          outputAsset: leg.output,
          quotedInput: leg.amountIn,
          quotedOutput: leg.expectedAmountOut,
          executedInput,
          executedOutput,
          quotedInputUsd:
            valuationForAmount(leg.amountIn, leg.input, leg.usdValuation, ['plannedInput']) ??
            valuationForAmount(leg.amountIn, leg.input, quoteValuation, ['input']),
          quotedOutputUsd:
            valuationForAmount(leg.expectedAmountOut, leg.output, leg.usdValuation, [
              'plannedOutput',
              'plannedMinOutput'
            ]) ??
            valuationForAmount(leg.expectedAmountOut, leg.output, quoteValuation, [
              'output',
              'minOutput'
            ]),
          executedInputUsd: executedInput
            ? valuationForAmount(
                executedInput,
                leg.input,
                leg.usdValuation,
                ['actualInput'],
                { allowDerived: false }
              )
            : undefined,
          executedOutputUsd: executedOutput
            ? valuationForAmount(
                executedOutput,
                leg.output,
                leg.usdValuation,
                ['actualOutput'],
                { allowDerived: false }
              )
            : undefined,
          minAmountOut: leg.minAmountOut,
          updatedAt: leg.updatedAt,
          steps,
          references: timelineReferences(leg.input, leg.output, steps)
        }
      })
  }

  return quoteFlowLegs(order).map((leg) => ({
    key: leg.key,
    index: leg.index,
    provider: leg.provider,
    kind: leg.kind,
    status: 'planned',
    inputAsset: leg.inputAsset,
    outputAsset: leg.outputAsset,
    quotedInput: leg.amountIn,
    quotedOutput: leg.amountOut,
    quotedInputUsd: leg.inputUsdValuation,
    quotedOutputUsd: leg.outputUsdValuation,
    minAmountOut: leg.minAmountOut,
    maxAmountIn: leg.maxAmountIn,
    steps: [],
    references: []
  }))
}

function quoteUsdValuationForExecutionLeg(
  order: OrderFirehoseRow,
  leg: OrderExecutionLeg
): UsdValuation | undefined {
  const quoteUsdValuation = order.quoteUsdValuation
  const quoteLegs = quoteUsdValuation?.legs
  if (!quoteUsdValuation || !quoteLegs || quoteLegs.length === 0) return undefined

  const details = asRecord(leg.details)
  const transitionIds = asArray(details?.quote_leg_transition_decl_ids).filter(
    (value): value is string => typeof value === 'string'
  )

  const matchedLegs =
    transitionIds.length > 0
      ? transitionIds
          .map((transitionId) =>
            quoteLegs.find((quoteLeg) => quoteLeg.transitionDeclId === transitionId)
          )
          .filter((quoteLeg): quoteLeg is QuoteFlowLegUsdValuation => Boolean(quoteLeg))
      : [
          quoteLegs.find((quoteLeg) => quoteLeg.transitionDeclId === leg.transitionDeclId) ??
            quoteLegs.find((quoteLeg) => quoteLeg.index === leg.legIndex)
        ].filter((quoteLeg): quoteLeg is QuoteFlowLegUsdValuation => Boolean(quoteLeg))

  const first = matchedLegs[0]
  const last = matchedLegs[matchedLegs.length - 1]
  if (!first && !last) return undefined

  return {
    schemaVersion: quoteUsdValuation.schemaVersion,
    pricing: quoteUsdValuation.pricing,
    amounts: {
      input: first?.amounts?.input,
      output: last?.amounts?.output,
      minOutput: last?.amounts?.minOutput,
      maxInput: first?.amounts?.maxInput
    }
  }
}

type TimelineReferenceField = {
  key: string
  label: string
  side: 'input' | 'output' | 'auto' | 'hyperliquid'
}

const TIMELINE_REFERENCE_FIELDS: TimelineReferenceField[] = [
  { key: 'source_custody_vault_address', label: 'Source vault', side: 'input' },
  { key: 'destination_custody_vault_address', label: 'Destination vault', side: 'output' },
  { key: 'recipient_custody_vault_address', label: 'Recipient vault', side: 'output' },
  { key: 'depositor_custody_vault_address', label: 'Depositor vault', side: 'input' },
  { key: 'depositor_address', label: 'Depositor', side: 'input' },
  { key: 'recipient_address', label: 'Recipient', side: 'output' },
  { key: 'refund_custody_vault_address', label: 'Refund vault', side: 'input' },
  { key: 'revert_custody_vault_address', label: 'Revert vault', side: 'input' },
  { key: 'refund_address', label: 'Refund', side: 'input' },
  { key: 'release_sweep_target_address', label: 'Paymaster', side: 'auto' },
  { key: 'paymaster_address', label: 'Paymaster', side: 'auto' },
  { key: 'hyperliquid_custody_vault_address', label: 'Hyperliquid vault', side: 'hyperliquid' }
]

function timelineReferences(
  inputAsset: AssetRef | undefined,
  outputAsset: AssetRef | undefined,
  steps: OrderExecutionStep[]
): TimelineReference[] {
  const references: TimelineReference[] = []
  const seen = new Set<string>()

  for (const step of steps) {
    for (const record of [
      asRecord(step.request),
      asRecord(step.response),
      asRecord(step.details)
    ]) {
      if (!record) continue

      for (const field of TIMELINE_REFERENCE_FIELDS) {
        const value = stringField(record, field.key)
        if (!value || !isReferenceLike(value)) continue

        const label = timelineReferenceLabel(field, record)
        const key = `${label}:${value.toLowerCase()}`
        if (seen.has(key)) continue

        references.push({
          key,
          label,
          value,
          chainId: timelineReferenceChain(field, value, inputAsset, outputAsset, step),
          kind: 'auto'
        })
        seen.add(key)
      }
    }
  }

  return references.slice(0, 8)
}

function timelineReferenceLabel(field: TimelineReferenceField, record: JsonRecord) {
  if (
    field.key === 'recipient_address' &&
    stringField(record, 'recipient_role') === 'paymaster_wallet'
  ) {
    return 'Paymaster'
  }
  if (
    field.key === 'recipient_address' &&
    stringField(record, 'recipient_custody_vault_role') === 'paymaster_wallet'
  ) {
    return 'Paymaster'
  }
  return field.label
}

function timelineReferenceChain(
  field: TimelineReferenceField,
  value: string,
  inputAsset: AssetRef | undefined,
  outputAsset: AssetRef | undefined,
  step: OrderExecutionStep
) {
  if (field.side === 'hyperliquid') return 'hyperliquid'

  const preferred =
    field.side === 'output'
      ? outputAsset?.chainId ?? step.output?.chainId
      : inputAsset?.chainId ?? step.input?.chainId
  const alternate =
    field.side === 'output'
      ? inputAsset?.chainId ?? step.input?.chainId
      : outputAsset?.chainId ?? step.output?.chainId

  if (isEvmAddress(value)) {
    return preferred?.startsWith('evm:')
      ? preferred
      : alternate?.startsWith('evm:')
        ? alternate
        : preferred
  }
  if (preferred === 'bitcoin' || alternate === 'bitcoin') return 'bitcoin'
  return preferred ?? alternate
}

function isReferenceLike(value: string) {
  return (
    isEvmAddress(value) ||
    isBitcoinTxHash(value) ||
    isBitcoinAddressLike(value)
  )
}

function stepActionAddresses(step: OrderExecutionStep) {
  const senders = normalizeStepAddresses(step.actionAddresses?.senders)
  const sender =
    normalizeStepAddress(step.actionAddresses?.sender) ??
    senders[0] ??
    stepAddressFromFields(step, 'sender')
  const recipient =
    normalizeStepAddress(step.actionAddresses?.recipient) ??
    stepAddressFromFields(step, 'recipient') ??
    (step.stepType === 'hyperliquid_trade' ? sender : undefined)

  return {
    sender,
    senders: senders.length > 0 ? senders : sender ? [sender] : [],
    recipient
  }
}

function stepVenueIdentifier(step: OrderExecutionStep) {
  return (
    step.providerRef ??
    stringField(asRecord(step.response), 'provider_ref') ??
    nestedStringField(step.response, 'providerRef') ??
    nestedStringField(step.response, 'operationId')
  )
}

function normalizeStepAddress(
  address: { address: string; chainId?: string } | undefined
) {
  if (!address?.address) return undefined
  return address
}

function normalizeStepAddresses(
  addresses: Array<{ address: string; chainId?: string }> | undefined
) {
  return (addresses ?? []).filter((address) => Boolean(address.address))
}

function stepAddressFromFields(
  step: OrderExecutionStep,
  side: 'sender' | 'recipient'
) {
  const records = [
    asRecord(step.request),
    asRecord(step.response),
    asRecord(step.details)
  ]
  const keys =
    side === 'sender'
      ? [
          'source_custody_vault_address',
          'depositor_custody_vault_address',
          'depositor_address',
          'sender_address',
          'hyperliquid_custody_vault_address',
          'sourceAddress'
        ]
      : [
          'recipient_custody_vault_address',
          'destination_custody_vault_address',
          'recipient_address',
          'protocolAddress',
          'provider_ref',
          'destinationAddress'
        ]

  for (const record of records) {
    for (const key of keys) {
      const value = stringField(record, key) ?? nestedStringField(record, key)
      if (value && isReferenceLike(value)) {
        return {
          address: value,
          chainId: stepAddressChain(step, side, value)
        }
      }
    }
  }

  return undefined
}

function nestedStringField(value: unknown, key: string): string | undefined {
  if (Array.isArray(value)) {
    for (const item of value) {
      const found = nestedStringField(item, key)
      if (found) return found
    }
    return undefined
  }

  const record = asRecord(value)
  if (!record) return undefined
  const direct = stringField(record, key)
  if (direct) return direct

  for (const child of Object.values(record)) {
    const found = nestedStringField(child, key)
    if (found) return found
  }
  return undefined
}

function stepAddressChain(
  step: OrderExecutionStep,
  side: 'sender' | 'recipient',
  value: string
) {
  if (step.stepType === 'unit_deposit' && side === 'recipient') {
    return step.input?.chainId ?? step.output?.chainId
  }
  if (step.stepType === 'hyperliquid_trade') return 'hyperliquid'
  if (isEvmAddress(value)) {
    return side === 'recipient'
      ? step.output?.chainId ?? step.input?.chainId
      : step.input?.chainId ?? step.output?.chainId
  }
  if (isBitcoinAddressLike(value)) {
    return 'bitcoin'
  }
  return side === 'recipient'
    ? step.output?.chainId ?? step.input?.chainId
    : step.input?.chainId ?? step.output?.chainId
}

type AmountDelta = {
  label: string
  tone: 'positive' | 'negative' | 'neutral'
}

function amountDelta(quoted: string | undefined, executed: string | undefined): AmountDelta | undefined {
  if (!quoted || !executed || !/^\d+$/.test(quoted) || !/^\d+$/.test(executed)) {
    return undefined
  }
  const quotedRaw = BigInt(quoted)
  if (quotedRaw === 0n) return undefined
  const executedRaw = BigInt(executed)
  const deltaBps = ((executedRaw - quotedRaw) * 10000n) / quotedRaw
  const tone = deltaBps > 0n ? 'positive' : deltaBps < 0n ? 'negative' : 'neutral'
  const sign = deltaBps > 0n ? '+' : deltaBps < 0n ? '-' : ''
  const absolute = deltaBps < 0n ? -deltaBps : deltaBps
  const whole = absolute / 100n
  const fraction = String(absolute % 100n).padStart(2, '0')
  return {
    label: `${sign}${whole}.${fraction}% vs quote`,
    tone
  }
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

export function executedAmounts(order: OrderFirehoseRow): ExecutedAmounts {
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
  const inputAmount = firstLeg.actualAmountIn
  const outputAmount = lastLeg.actualAmountOut

  if (!inputAsset || !outputAsset || !inputAmount || !outputAmount) {
    return { placeholder: 'Not observed' }
  }

  return {
    input: {
      amount: inputAmount,
      asset: inputAsset,
      usdValuation:
        valuationForAmount(
          inputAmount,
          inputAsset,
          firstLeg.usdValuation,
          ['actualInput'],
          { allowDerived: false }
        )
    },
    output: {
      amount: outputAmount,
      asset: outputAsset,
      usdValuation:
        valuationForAmount(
          outputAmount,
          outputAsset,
          lastLeg.usdValuation,
          ['actualOutput'],
          { allowDerived: false }
        )
    }
  }
}

function valuationForAmount(
  amount: string,
  asset: AssetRef,
  valuation: UsdValuation | undefined,
  preferredKeys: string[],
  options: { allowDerived?: boolean } = {}
): UsdAmountValuation | undefined {
  const amounts = valuation?.amounts
  if (!amounts) return undefined
  const preferred = preferredKeys
    .map((key) => amounts[key])
    .filter((candidate): candidate is UsdAmountValuation => Boolean(candidate))
  if (options.allowDerived === false) {
    return preferred.find(
      (candidate) => candidate.raw === amount && sameAsset(candidate.asset, asset)
    )
  }
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

function stepTransactionChain(step: OrderExecutionStep) {
  if (['across_bridge', 'cctp_receive', 'unit_withdrawal'].includes(step.stepType)) {
    return step.output?.chainId ?? step.input?.chainId
  }
  return step.input?.chainId ?? step.output?.chainId
}

function isWaitForDepositStep(step: OrderExecutionStep) {
  return step.stepType === WAIT_FOR_DEPOSIT_STEP_TYPE
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

  if (normalizedChainId === 'hyperliquid') {
    if (kind === 'tx' || (kind === 'auto' && isEvmTxHash(normalizedValue))) {
      return `https://app.hyperliquid.xyz/explorer/tx/${normalizedValue}`
    }
    if (kind === 'address' || (kind === 'auto' && isEvmAddress(normalizedValue))) {
      return `https://app.hyperliquid.xyz/explorer/address/${normalizedValue}`
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

function isBitcoinAddressLike(value: string) {
  const normalized = value.toLowerCase()
  return (
    normalized.startsWith('bc1') ||
    normalized.startsWith('tb1') ||
    normalized.startsWith('bcrt1') ||
    /^[123mn2][a-km-zA-HJ-NP-Z1-9]{25,34}$/.test(value)
  )
}

function isUuid(value: string) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
    value
  )
}

function sortOrders(nextOrders: OrderFirehoseRow[]) {
  return [...nextOrders].sort(compareOrdersByCreatedAt)
}

function upsertOrder(current: OrderFirehoseRow[], order: OrderFirehoseRow) {
  return mergeOrders(current, [order])
}

function upsertVisibleOrder(current: OrderFirehoseRow[], order: OrderFirehoseRow) {
  const existing = current.find((candidate) => candidate.id === order.id)
  if (existing || current.length === 0) return upsertOrder(current, order)

  const oldestVisible = current[current.length - 1]
  if (compareOrdersByCreatedAt(order, oldestVisible) > 0) return current

  return upsertOrder(current, order)
}

function mergeOrders(current: OrderFirehoseRow[], incoming: OrderFirehoseRow[]) {
  const byId = new Map(current.map((order) => [order.id, order]))
  for (const order of incoming) {
    const existing = byId.get(order.id)
    byId.set(order.id, preferredOrder(existing, order))
  }
  return sortOrders([...byId.values()])
}

export function mergeSnapshotOrders(
  current: OrderFirehoseRow[],
  snapshot: OrderFirehoseRow[]
) {
  const currentById = new Map(current.map((order) => [order.id, order]))
  return sortOrders(
    snapshot.map((order) => preferredOrder(currentById.get(order.id), order))
  )
}

function preferredOrder(
  existing: OrderFirehoseRow | undefined,
  incoming: OrderFirehoseRow
) {
  if (!existing) return incoming
  const incomingUpdatedAt = Date.parse(incoming.updatedAt)
  const existingUpdatedAt = Date.parse(existing.updatedAt)
  if (incomingUpdatedAt < existingUpdatedAt) {
    return existing
  }
  if (
    existing.detailLevel === 'full' &&
    incoming.detailLevel !== 'full'
  ) {
    return {
      ...incoming,
      detailLevel: 'full' as const,
      providerQuote: existing.providerQuote,
      executionSteps: existing.executionSteps,
      providerOperations: existing.providerOperations
    }
  }
  return incoming
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
  if (kind === 'exact_in') return 'Exact In'
  if (kind === 'exact_out') return 'Exact Out'
  if (kind === 'limit') return 'Limit'
  return humanize(kind)
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

export function normalizedVolumeBuckets(analytics: VolumeAnalyticsResponse) {
  const bucketsByStart = new Map(
    analytics.buckets.map((bucket) => [bucket.bucketStart, bucket])
  )
  const starts = bucketStarts(
    new Date(analytics.from),
    new Date(analytics.to),
    analytics.bucketSize
  )

  const buckets = starts.map((bucketStart) => {
        const key = bucketStart.toISOString()
        const bucket = bucketsByStart.get(key)
        return {
          bucketStart: key,
          volumeUsdMicro: bucket ? BigInt(bucket.volumeUsdMicro) : 0n,
          orderCount: bucket?.orderCount ?? 0
        }
      })

  return downsampleVolumeBuckets(buckets, MAX_VOLUME_CHART_BUCKETS)
}

function bucketStarts(
  from: Date,
  to: Date,
  bucketSize: VolumeBucketSize
) {
  const starts: Date[] = []
    const cursor = alignDateToBucket(from, bucketSize)
  while (cursor < to) {
    starts.push(new Date(cursor))
    if (bucketSize === 'five_minute') cursor.setUTCMinutes(cursor.getUTCMinutes() + 5)
    if (bucketSize === 'hour') cursor.setUTCHours(cursor.getUTCHours() + 1)
    if (bucketSize === 'day') cursor.setUTCDate(cursor.getUTCDate() + 1)
  }
  return starts
}

function downsampleVolumeBuckets(
  buckets: Array<{
    bucketStart: string
    volumeUsdMicro: bigint
    orderCount: number
  }>,
  maxBuckets: number
) {
  if (buckets.length <= maxBuckets) return buckets

  const chunkSize = Math.ceil(buckets.length / maxBuckets)
  const downsampled: Array<{
    bucketStart: string
    volumeUsdMicro: bigint
    orderCount: number
  }> = []
  for (let index = 0; index < buckets.length; index += chunkSize) {
    const chunk = buckets.slice(index, index + chunkSize)
    downsampled.push({
      bucketStart: chunk[0].bucketStart,
      volumeUsdMicro: chunk.reduce(
        (sum, bucket) => sum + bucket.volumeUsdMicro,
        0n
      ),
      orderCount: chunk.reduce((sum, bucket) => sum + bucket.orderCount, 0)
    })
  }
  return downsampled
}

export function volumeBarHeight(
  volumeUsdMicro: bigint,
  maxVolumeUsdMicro: bigint,
  chartHeight: number
) {
  if (volumeUsdMicro <= 0n || maxVolumeUsdMicro <= 0n) return 0
  const scale = 10_000n
  const scaledRatio = (volumeUsdMicro * scale) / maxVolumeUsdMicro
  return (Number(scaledRatio) / Number(scale)) * chartHeight
}

export function volumeYAxisTicks(maxVolumeUsdMicro: bigint, segments: number) {
  const safeSegments = Math.max(1, Math.floor(segments))
  const ticks: Array<{ key: string; value: bigint }> = []
  for (let index = 0; index <= safeSegments; index += 1) {
    const value = (maxVolumeUsdMicro * BigInt(index)) / BigInt(safeSegments)
    ticks.push({ key: `${index}-${value.toString()}`, value })
  }
  return ticks
}

export function volumeXAxisTicks<T extends { bucketStart: string }>(
  buckets: T[],
  maxTicks: number
) {
  if (buckets.length === 0) return []
  const tickCount = Math.min(Math.max(2, Math.floor(maxTicks)), buckets.length)
  if (tickCount === 1) return [{ index: 0, bucket: buckets[0] }]

  const indexes = new Set<number>()
  for (let tick = 0; tick < tickCount; tick += 1) {
    indexes.add(Math.round((tick * (buckets.length - 1)) / (tickCount - 1)))
  }
  return [...indexes]
    .sort((left, right) => left - right)
    .map((index) => ({ index, bucket: buckets[index] }))
}

function volumeTooltipLayout(
  anchorX: number,
  anchorY: number,
  padding: { top: number; right: number; bottom: number; left: number },
  chartWidth: number
) {
  const width = 156
  const height = 44
  return {
    width,
    height,
    x: clampNumber(anchorX - width / 2, padding.left, chartWidth - padding.right - width),
    y: Math.max(padding.top, anchorY - height - 8)
  }
}

function formatVolumeAxisDate(value: string, bucketSize: VolumeBucketSize) {
  const date = new Date(value)
  if (bucketSize === 'day') {
    return new Intl.DateTimeFormat(undefined, {
      month: 'short',
      day: '2-digit'
    }).format(date)
  }
  return new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: '2-digit',
    hour: '2-digit'
  }).format(date)
}

function clampNumber(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max)
}

function alignDateToBucket(value: Date, bucketSize: VolumeBucketSize) {
  const aligned = new Date(value)
  aligned.setUTCSeconds(0, 0)
  if (bucketSize === 'five_minute') {
    aligned.setUTCMinutes(Math.floor(aligned.getUTCMinutes() / 5) * 5)
  }
  if (bucketSize === 'hour' || bucketSize === 'day') aligned.setUTCMinutes(0)
  if (bucketSize === 'day') aligned.setUTCHours(0)
  return aligned
}

export function volumeDateRange(window: VolumeWindow, now = new Date()) {
  const to = new Date(now)
  const from = new Date(to)
  if (window === '6h') from.setUTCHours(from.getUTCHours() - 6)
  if (window === '24h') from.setUTCHours(from.getUTCHours() - 24)
  if (window === '7d') from.setUTCDate(from.getUTCDate() - 7)
  if (window === '30d') from.setUTCDate(from.getUTCDate() - 30)
  if (window === '90d') from.setUTCDate(from.getUTCDate() - 90)
  return { from, to }
}

function shortId(value: string) {
  if (value.length <= 14) return value
  return `${value.slice(0, 8)}...${value.slice(-6)}`
}

function shortOrderId(value: string) {
  if (value.length <= 10) return value
  return `${value.slice(0, 4)}...${value.slice(-4)}`
}

export function statusDisplay(status: string): StatusDisplay {
  return (
    ORDER_STATUS_DISPLAY[status] ?? {
      label: humanize(status),
      tone: statusTone(status)
    }
  )
}

function statusTone(status: string): StatusTone {
  if (['completed', 'processed', 'skipped'].includes(status)) return 'success'
  if (
    [
      'failed',
      'expired',
      'cancelled',
      'refund_required',
      'manual_intervention_required',
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

export function progressVenueLabel(order: OrderFirehoseRow) {
  const progress = order.progress
  if (progress.totalStages === 0) return undefined
  if (progress.completedStages === progress.totalStages && progress.failedStages === 0) {
    return undefined
  }

  const failedLeg = progress.failedStages
    ? order.executionLegs.find((leg) => FAILED_STEP_STATUSES.has(leg.status))
    : undefined
  if (failedLeg) return providerDisplayName(failedLeg.provider)

  const activeLeg = order.executionLegs.find((leg) =>
    ACTIVE_STEP_STATUSES.has(leg.status)
  )
  if (activeLeg) return providerDisplayName(activeLeg.provider)

  const activeStep =
    order.executionSteps.find(
      (step) => !isWaitForDepositStep(step) && ACTIVE_STEP_STATUSES.has(step.status)
    ) ??
    order.executionSteps.find(
      (step) => !isWaitForDepositStep(step) && FAILED_STEP_STATUSES.has(step.status)
    )
  if (activeStep) return providerDisplayName(activeStep.provider)

  return progress.activeStage ? activeProgressStageVenue(progress.activeStage) : undefined
}

function activeProgressStageVenue(stage: string) {
  const [venue, status] = stage.split('/')
  const normalizedStatus = status ? providerKey(status) : undefined
  if (!normalizedStatus) return undefined
  if (
    !ACTIVE_STEP_STATUSES.has(normalizedStatus) &&
    !FAILED_STEP_STATUSES.has(normalizedStatus) &&
    normalizedStatus !== 'waiting_external' &&
    normalizedStatus !== 'executing'
  ) {
    return undefined
  }
  return venue ? providerDisplayName(venue.trim()) : undefined
}

function providerDisplayName(provider: string) {
  const normalized = providerKey(provider)
  return (
    PROVIDER_DISPLAY_NAMES[normalized] ??
    knownProviderPrefix(normalized) ??
    humanize(normalized)
  )
}

function knownProviderPrefix(normalized: string) {
  const provider = Object.keys(PROVIDER_DISPLAY_NAMES).find(
    (key) => normalized.startsWith(`${key}_`)
  )
  return provider ? PROVIDER_DISPLAY_NAMES[provider] : undefined
}

function providerKey(provider: string) {
  return provider
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
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

async function copyTextToClipboard(value: string) {
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(value)
      return
    } catch {
      // Use the DOM fallback below when clipboard permissions are unavailable.
    }
  }

  const textarea = document.createElement('textarea')
  textarea.value = value
  textarea.setAttribute('readonly', '')
  textarea.style.position = 'fixed'
  textarea.style.left = '-9999px'
  textarea.style.top = '0'
  document.body.appendChild(textarea)
  textarea.select()

  try {
    const copied = document.execCommand('copy')
    if (!copied) throw new Error('clipboard copy was rejected')
  } finally {
    document.body.removeChild(textarea)
  }
}

async function signOut() {
  await authClient.signOut()
  window.location.assign('/')
}

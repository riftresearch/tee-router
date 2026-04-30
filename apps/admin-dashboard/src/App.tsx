import {
  Activity,
  ChevronDown,
  ChevronRight,
  CircleAlert,
  CircleCheck,
  Clock3,
  LogIn,
  LogOut,
  RefreshCw,
  ShieldCheck,
  Wifi,
  WifiOff
} from 'lucide-react'
import { useCallback, useEffect, useMemo, useState } from 'react'

import { fetchMe, fetchOrders, parseSnapshotEvent, parseUpsertEvent } from './api'
import { authClient } from './auth-client'
import type { MeResponse, OrderFirehoseRow } from './types'

type StreamState = 'idle' | 'connecting' | 'live' | 'closed' | 'error'

const ORDER_LIMIT = 200

export function App() {
  const [me, setMe] = useState<MeResponse | null>(null)
  const [orders, setOrders] = useState<OrderFirehoseRow[]>([])
  const [expandedOrderId, setExpandedOrderId] = useState<string | null>(null)
  const [streamState, setStreamState] = useState<StreamState>('idle')
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const loadSession = useCallback(async () => {
    setError(null)
    const session = await fetchMe()
    setMe(session)
    return session
  }, [])

  const loadOrders = useCallback(async () => {
    const response = await fetchOrders(ORDER_LIMIT)
    setOrders(sortOrders(response.orders))
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
      setOrders(sortOrders(parseSnapshotEvent(event as MessageEvent<string>).orders))
      setStreamState('live')
    })
    source.addEventListener('upsert', (event) => {
      const { order } = parseUpsertEvent(event as MessageEvent<string>)
      setOrders((current) => upsertOrder(current, order))
    })
    source.addEventListener('error', () => {
      setStreamState(source.readyState === EventSource.CLOSED ? 'closed' : 'error')
    })

    return () => {
      source.close()
      setStreamState('closed')
    }
  }, [me?.authorized])

  const activeOrders = useMemo(
    () =>
      orders.filter((order) =>
        ['pending_funding', 'funded', 'executing', 'refund_required', 'refunding'].includes(
          order.status
        )
      ).length,
    [orders]
  )

  const failedOrders = useMemo(
    () =>
      orders.filter((order) =>
        ['failed', 'expired', 'refund_manual_intervention_required'].includes(
          order.status
        )
      ).length,
    [orders]
  )

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
          <Metric icon={<Activity size={18} />} label="Orders" value={orders.length} />
          <Metric icon={<Clock3 size={18} />} label="Active" value={activeOrders} />
          <Metric
            icon={<CircleAlert size={18} />}
            label="Needs Attention"
            value={failedOrders}
            tone={failedOrders ? 'danger' : 'neutral'}
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
                  <th>Status</th>
                  <th>Venue Progress</th>
                </tr>
              </thead>
              <tbody>
                {orders.length === 0 ? (
                  <tr>
                    <td className="empty" colSpan={8}>
                      No orders found
                    </td>
                  </tr>
                ) : (
                  orders.map((order) => (
                    <OrderRows
                      key={order.id}
                      order={order}
                      expanded={expandedOrderId === order.id}
                      onToggle={() =>
                        setExpandedOrderId((current) =>
                          current === order.id ? null : order.id
                        )
                      }
                    />
                  ))
                )}
              </tbody>
            </table>
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
  tone = 'neutral'
}: {
  icon: React.ReactNode
  label: string
  value: string | number
  tone?: 'neutral' | 'success' | 'warning' | 'danger'
}) {
  return (
    <div className={`metric ${tone}`}>
      <div className="metric-icon">{icon}</div>
      <div>
        <span>{label}</span>
        <strong>{value}</strong>
      </div>
    </div>
  )
}

function OrderRows({
  order,
  expanded,
  onToggle
}: {
  order: OrderFirehoseRow
  expanded: boolean
  onToggle: () => void
}) {
  return (
    <>
      <tr className="order-row" onClick={onToggle}>
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
          <code className="mono compact-id">{shortId(order.id)}</code>
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
          <StatusPill status={order.status} />
        </td>
        <td>
          <ProgressCell order={order} />
        </td>
      </tr>
      {expanded ? (
        <tr className="details-row">
          <td colSpan={8}>
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

function AssetLabel({ asset }: { asset: { chainId: string; assetId: string } }) {
  return (
    <span className="asset-label">
      <span>{asset.chainId}</span>
      <code>{asset.assetId}</code>
    </span>
  )
}

function QuoteCell({ order }: { order: OrderFirehoseRow }) {
  return (
    <div className="quote-cell">
      <span>
        in <code>{formatAmount(order.quotedInputAmount)}</code>
      </span>
      <span>
        out <code>{formatAmount(order.quotedOutputAmount)}</code>
      </span>
    </div>
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
      <div className="detail-block">
        <h3>Execution Steps</h3>
        <div className="timeline">
          {order.executionSteps.length === 0 ? (
            <div className="empty-detail">No execution steps</div>
          ) : (
            order.executionSteps.map((step) => (
              <div className="timeline-item" key={step.id}>
                <StatusDot status={step.status} />
                <div>
                  <div className="timeline-title">
                    <strong>{humanize(step.provider)}</strong>
                    <span>{humanize(step.stepType)}</span>
                    <StatusPill status={step.status} />
                  </div>
                  <div className="timeline-meta">
                    <span>#{step.stepIndex}</span>
                    {step.txHash ? <code>{shortId(step.txHash)}</code> : null}
                    {step.providerRef ? <code>{shortId(step.providerRef)}</code> : null}
                    <time dateTime={step.updatedAt}>{formatDate(step.updatedAt)}</time>
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      </div>

      <div className="detail-block">
        <h3>Provider Operations</h3>
        <div className="timeline">
          {order.providerOperations.length === 0 ? (
            <div className="empty-detail">No provider operations</div>
          ) : (
            order.providerOperations.map((operation) => (
              <div className="timeline-item" key={operation.id}>
                <StatusDot status={operation.status} />
                <div>
                  <div className="timeline-title">
                    <strong>{humanize(operation.provider)}</strong>
                    <span>{humanize(operation.operationType)}</span>
                    <StatusPill status={operation.status} />
                  </div>
                  <div className="timeline-meta">
                    {operation.providerRef ? (
                      <code>{shortId(operation.providerRef)}</code>
                    ) : null}
                    <time dateTime={operation.updatedAt}>
                      {formatDate(operation.updatedAt)}
                    </time>
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      </div>

      <div className="detail-block metadata">
        <h3>Order Metadata</h3>
        <dl>
          <dt>Recipient</dt>
          <dd>
            <code>{order.recipientAddress}</code>
          </dd>
          <dt>Refund</dt>
          <dd>
            <code>{order.refundAddress}</code>
          </dd>
          <dt>Quote</dt>
          <dd>
            <code>{order.quoteId ?? 'none'}</code>
          </dd>
          <dt>Trace</dt>
          <dd>
            <code>{order.workflowTraceId ?? 'none'}</code>
          </dd>
        </dl>
      </div>
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

function StatusDot({ status }: { status: string }) {
  const ok = statusTone(status)
  return ok === 'success' ? (
    <CircleCheck className={`status-dot ${ok}`} size={18} />
  ) : (
    <span className={`status-dot ${ok}`} />
  )
}

function sortOrders(nextOrders: OrderFirehoseRow[]) {
  return [...nextOrders].sort(compareOrdersByCreatedAt)
}

function upsertOrder(current: OrderFirehoseRow[], order: OrderFirehoseRow) {
  const index = current.findIndex((candidate) => candidate.id === order.id)
  const next =
    index === -1
      ? [...current, order]
      : current.map((candidate) => (candidate.id === order.id ? order : candidate))
  return sortOrders(next)
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

function formatAmount(value: string | undefined) {
  if (!value) return '-'
  if (value.length <= 18) return value
  return `${value.slice(0, 10)}...${value.slice(-6)}`
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

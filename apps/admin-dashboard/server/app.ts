import { Hono, type Context } from 'hono'
import { cors } from 'hono/cors'
import { secureHeaders } from 'hono/secure-headers'
import { serveStatic } from 'hono/bun'

import { createDashboardAuth, type DashboardAuthRuntime } from './auth'
import {
  ALLOWED_ADMIN_EMAILS,
  type AdminDashboardConfig,
  isAllowedAdminEmail,
  loadConfig,
  normalizeAdminEmail
} from './config'
import { createReplicaDatabase, type ReplicaDatabaseRuntime } from './db'
import { OrderCdcBroker, type OrderCdcUpsert } from './order-cdc'
import {
  fetchOrderFirehose,
  fetchOrderMetrics,
  type OrderFirehoseRow,
  type OrderPageCursor
} from './orders'

type AppBindings = {
  Variables: {
    adminEmail: string
  }
}

const LOCAL_DEV_ADMIN_EMAIL = ALLOWED_ADMIN_EMAILS[0]

export type AdminDashboardRuntime = {
  app: Hono<AppBindings>
  close: () => Promise<void>
}

export function createApp(
  config: AdminDashboardConfig = loadConfig()
): AdminDashboardRuntime {
  const app = new Hono<AppBindings>()
  const authRuntime = config.production ? createDashboardAuth(config) : null
  const replicaRuntime = createReplicaDatabase(config)
  const orderCdcBroker = replicaRuntime
    ? new OrderCdcBroker(replicaRuntime.pool, config)
    : null
  orderCdcBroker?.start()

  app.use(
    '*',
    secureHeaders({
      contentSecurityPolicy: {
        defaultSrc: ["'self'"],
        scriptSrc: ["'self'"],
        styleSrc: ["'self'"],
        imgSrc: ["'self'", 'https:', 'data:'],
        connectSrc: ["'self'", config.webOrigin, config.authBaseUrl],
        frameAncestors: ["'none'"],
        objectSrc: ["'none'"],
        baseUri: ["'self'"]
      },
      crossOriginEmbedderPolicy: false
    })
  )

  app.use(
    '/api/*',
    cors({
      origin: (origin) =>
        !origin || config.trustedOrigins.includes(origin)
          ? origin || config.webOrigin
          : config.webOrigin,
      credentials: true,
      allowHeaders: ['Content-Type', 'Authorization'],
      allowMethods: ['GET', 'POST', 'OPTIONS'],
      exposeHeaders: ['Content-Length'],
      maxAge: 600
    })
  )

  app.on(['GET', 'POST'], '/api/auth/*', (c) => {
    if (!config.production) {
      return c.json({ error: 'auth_disabled_in_development' }, 404)
    }

    if (!authRuntime) {
      return c.json(
        {
          error: 'auth_not_configured',
          missing: config.missingAuthConfig
        },
        503
      )
    }

    return authRuntime.auth.handler(c.req.raw)
  })

  app.get('/api/status', (c) =>
    c.json({
      status: 'ok',
      version: config.version,
      authConfigured: config.production ? Boolean(authRuntime) : true,
      authMode: config.production ? 'google' : 'development_bypass',
      replicaConfigured: Boolean(replicaRuntime)
    })
  )

  app.get('/api/me', async (c) => {
    if (!config.production) {
      return c.json({
        authenticated: true,
        authorized: true,
        user: {
          id: 'local-dev-admin',
          name: 'Local Dev Admin',
          email: LOCAL_DEV_ADMIN_EMAIL,
          emailVerified: true,
          image: null
        },
        allowedEmails: ALLOWED_ADMIN_EMAILS,
        routerAdminKeyConfigured: Boolean(config.routerAdminApiKey),
        authMode: 'development_bypass'
      })
    }

    if (!authRuntime) {
      return c.json(
        {
          authenticated: false,
          authorized: false,
          missingAuthConfig: config.missingAuthConfig,
          allowedEmails: ALLOWED_ADMIN_EMAILS
        },
        503
      )
    }

    const session = await authRuntime.auth.api.getSession({
      headers: c.req.raw.headers
    })
    const email = normalizeAdminEmail(session?.user.email)
    const emailVerified = session?.user.emailVerified === true
    const authorized = Boolean(
      session && email && emailVerified && isAllowedAdminEmail(email)
    )

    return c.json({
      authenticated: Boolean(session),
      authorized,
      user: session
        ? {
            id: session.user.id,
            name: session.user.name,
            email,
            emailVerified,
            image: session.user.image
          }
        : null,
      allowedEmails: ALLOWED_ADMIN_EMAILS,
      routerAdminKeyConfigured: authorized
        ? Boolean(config.routerAdminApiKey)
        : undefined,
      authMode: 'google'
    })
  })

  app.get('/api/orders', async (c) => {
    const admin = await requireAdmin(c, config, authRuntime)
    if (admin instanceof Response) return admin

    if (!replicaRuntime) {
      return c.json({ error: 'replica_database_not_configured' }, 503)
    }

    const limit = parseLimit(c.req.query('limit'), config.orderLimit)
    const cursor = parseOrderCursor(c.req.query('cursor'))
    if (cursor instanceof Response) return cursor

    const page = await fetchOrderPage(replicaRuntime, limit, cursor)
    return c.json({
      orders: page.orders,
      nextCursor: page.nextCursor,
      total: page.metrics.total,
      metrics: page.metrics,
      sort: 'created_at_desc'
    })
  })

  app.get('/api/orders/events', async (c) => {
    const admin = await requireAdmin(c, config, authRuntime)
    if (admin instanceof Response) return admin

    if (!replicaRuntime) {
      return c.json({ error: 'replica_database_not_configured' }, 503)
    }

    const limit = parseLimit(c.req.query('limit'), config.orderLimit)
    return createOrderEventStream(
      c.req.raw.signal,
      replicaRuntime,
      limit,
      orderCdcBroker
    )
  })

  if (config.serveStatic) {
    app.use('/assets/*', serveStatic({ root: './dist' }))
    app.get('*', serveStatic({ path: './dist/index.html' }))
  }

  return {
    app,
    close: async () => {
      await Promise.all([
        authRuntime?.close() ?? Promise.resolve(),
        orderCdcBroker?.close() ?? Promise.resolve(),
        replicaRuntime?.close() ?? Promise.resolve()
      ])
    }
  }
}

async function requireAdmin(
  c: Context<AppBindings>,
  config: AdminDashboardConfig,
  authRuntime: DashboardAuthRuntime | null
): Promise<{ email: string } | Response> {
  if (!config.production) {
    c.set('adminEmail', LOCAL_DEV_ADMIN_EMAIL)
    return { email: LOCAL_DEV_ADMIN_EMAIL }
  }

  if (!authRuntime) {
    return c.json({ error: 'auth_not_configured' }, 503)
  }

  const session = await authRuntime.auth.api.getSession({
    headers: c.req.raw.headers
  })
  const email = normalizeAdminEmail(session?.user.email)

  if (!session) {
    return c.json({ error: 'unauthenticated' }, 401)
  }

  if (!email || session.user.emailVerified !== true || !isAllowedAdminEmail(email)) {
    return c.json({ error: 'forbidden' }, 403)
  }

  c.set('adminEmail', email)
  return { email }
}

function parseLimit(value: string | undefined, defaultLimit: number): number {
  if (!value) return defaultLimit
  const parsed = Number(value)
  if (!Number.isInteger(parsed) || parsed <= 0) return defaultLimit
  return Math.min(parsed, 500)
}

async function fetchOrderPage(
  replicaRuntime: ReplicaDatabaseRuntime,
  limit: number,
  cursor: OrderPageCursor | undefined
) {
  const [ordersWithLookahead, metrics] = await Promise.all([
    fetchOrderFirehose(replicaRuntime.pool, limit + 1, cursor),
    fetchOrderMetrics(replicaRuntime.pool)
  ])
  const orders = ordersWithLookahead.slice(0, limit)
  const nextCursor =
    ordersWithLookahead.length > limit
      ? encodeOrderCursor(orders[orders.length - 1])
      : undefined

  return {
    orders,
    nextCursor,
    metrics
  }
}

function parseOrderCursor(value: string | undefined): OrderPageCursor | undefined | Response {
  if (!value) return undefined

  try {
    const parsed = JSON.parse(fromBase64Url(value)) as unknown
    if (!isOrderCursor(parsed)) throw new Error('invalid cursor shape')
    return parsed
  } catch (_error) {
    return Response.json({ error: 'invalid_cursor' }, { status: 400 })
  }
}

function encodeOrderCursor(order: OrderFirehoseRow | undefined): string | undefined {
  if (!order) return undefined
  return toBase64Url(
    JSON.stringify({
      createdAt: order.createdAt,
      id: order.id
    } satisfies OrderPageCursor)
  )
}

function isOrderCursor(value: unknown): value is OrderPageCursor {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  const cursor = value as Record<string, unknown>
  return (
    typeof cursor.createdAt === 'string' &&
    Number.isFinite(Date.parse(cursor.createdAt)) &&
    typeof cursor.id === 'string' &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
      cursor.id
    )
  )
}

function toBase64Url(value: string): string {
  return btoa(value).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/u, '')
}

function fromBase64Url(value: string): string {
  const normalized = value.replace(/-/g, '+').replace(/_/g, '/')
  const padded = normalized.padEnd(Math.ceil(normalized.length / 4) * 4, '=')
  return atob(padded)
}

function createOrderEventStream(
  signal: AbortSignal,
  replicaRuntime: ReplicaDatabaseRuntime,
  limit: number,
  orderCdcBroker: OrderCdcBroker | null
): Response {
  const encoder = new TextEncoder()
  let closed = false
  let abortListener: (() => void) | undefined
  let unsubscribe: (() => void) | undefined

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const close = () => {
        closed = true
        unsubscribe?.()
        unsubscribe = undefined
        try {
          controller.close()
        } catch (_error) {
          // The client may have already closed the stream.
        }
      }

      abortListener = close
      signal.addEventListener('abort', close, { once: true })

      const write = (event: string, data: unknown) => {
        if (closed) return
        controller.enqueue(
          encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
        )
      }

      const writeComment = (comment: string) => {
        if (closed) return
        controller.enqueue(encoder.encode(`: ${comment}\n\n`))
      }

      const loop = async () => {
        try {
          const pending: OrderCdcUpsert[] = []
          let snapshotSent = false
          unsubscribe = orderCdcBroker?.subscribe((event) => {
            if (!snapshotSent) {
              pending.push(event)
              return
            }
            write('upsert', event)
          })

          const [initialOrders, initialMetrics] = await Promise.all([
            fetchOrderFirehose(replicaRuntime.pool, limit),
            fetchOrderMetrics(replicaRuntime.pool)
          ])
          write('snapshot', {
            orders: initialOrders,
            total: initialMetrics.total,
            metrics: initialMetrics,
            sort: 'created_at_desc'
          })
          snapshotSent = true
          for (const event of pending) write('upsert', event)
          pending.length = 0

          while (!closed) {
            await waitForAbort(signal)
            writeComment('closing')
          }
        } catch (error) {
          write('error', {
            message:
              error instanceof Error ? error.message : 'order stream failed'
          })
          close()
        }
      }

      void loop()
    },
    cancel() {
      closed = true
      unsubscribe?.()
      if (abortListener) {
        signal.removeEventListener('abort', abortListener)
      }
    }
  })

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no'
    }
  })
}

function waitForAbort(signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve()
  return new Promise((resolve) => {
    signal.addEventListener('abort', () => resolve(), { once: true })
  })
}

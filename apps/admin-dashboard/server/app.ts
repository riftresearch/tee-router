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
import {
  fetchOrderFirehose,
  orderFingerprint,
  type OrderFirehoseRow
} from './orders'

type AppBindings = {
  Variables: {
    adminEmail: string
  }
}

export type AdminDashboardRuntime = {
  app: Hono<AppBindings>
  close: () => Promise<void>
}

export function createApp(
  config: AdminDashboardConfig = loadConfig()
): AdminDashboardRuntime {
  const app = new Hono<AppBindings>()
  const authRuntime = createDashboardAuth(config)
  const replicaRuntime = createReplicaDatabase(config)

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
      authConfigured: Boolean(authRuntime),
      replicaConfigured: Boolean(replicaRuntime)
    })
  )

  app.get('/api/me', async (c) => {
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
        : undefined
    })
  })

  app.get('/api/orders', async (c) => {
    const admin = await requireAdmin(c, authRuntime)
    if (admin instanceof Response) return admin

    if (!replicaRuntime) {
      return c.json({ error: 'replica_database_not_configured' }, 503)
    }

    const limit = parseLimit(c.req.query('limit'), config.orderLimit)
    const orders = await fetchOrderFirehose(replicaRuntime.pool, limit)
    return c.json({
      orders,
      sort: 'created_at_desc'
    })
  })

  app.get('/api/orders/events', async (c) => {
    const admin = await requireAdmin(c, authRuntime)
    if (admin instanceof Response) return admin

    if (!replicaRuntime) {
      return c.json({ error: 'replica_database_not_configured' }, 503)
    }

    const limit = parseLimit(c.req.query('limit'), config.orderLimit)
    return createOrderEventStream(
      c.req.raw.signal,
      replicaRuntime,
      limit,
      config.orderPollIntervalMs
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
        replicaRuntime?.close() ?? Promise.resolve()
      ])
    }
  }
}

async function requireAdmin(
  c: Context<AppBindings>,
  authRuntime: DashboardAuthRuntime | null
): Promise<{ email: string } | Response> {
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

function createOrderEventStream(
  signal: AbortSignal,
  replicaRuntime: ReplicaDatabaseRuntime,
  limit: number,
  pollIntervalMs: number
): Response {
  const encoder = new TextEncoder()
  const seen = new Map<string, string>()
  let closed = false
  let abortListener: (() => void) | undefined

  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const close = () => {
        closed = true
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
          const initialOrders = await fetchOrderFirehose(
            replicaRuntime.pool,
            limit
          )
          for (const order of initialOrders) {
            seen.set(order.id, orderFingerprint(order))
          }
          write('snapshot', { orders: initialOrders, sort: 'created_at_desc' })

          while (!closed) {
            await sleep(pollIntervalMs)
            writeComment('ping')

            const orders = await fetchOrderFirehose(replicaRuntime.pool, limit)
            const changedOrders: OrderFirehoseRow[] = []

            for (const order of orders) {
              const fingerprint = orderFingerprint(order)
              if (seen.get(order.id) !== fingerprint) {
                seen.set(order.id, fingerprint)
                changedOrders.push(order)
              }
            }

            for (const order of changedOrders) {
              write('upsert', { order, sort: 'created_at_desc' })
            }
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

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

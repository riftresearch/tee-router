import type { AdminDashboardConfig } from './config'

const MAX_RESPONSE_BYTES = 4 * 1024 * 1024
const REQUEST_TIMEOUT_MS = 15_000

export type RouteGraphProxyConfig = Pick<
  AdminDashboardConfig,
  'routerInternalBaseUrl' | 'routerAdminApiKey'
>

export class RouteGraphProxyError extends Error {
  readonly status: number
  readonly upstreamStatus?: number
  readonly upstreamBody?: string

  constructor(
    status: number,
    message: string,
    upstream?: { status: number; body?: string }
  ) {
    super(message)
    this.name = 'RouteGraphProxyError'
    this.status = status
    this.upstreamStatus = upstream?.status
    this.upstreamBody = upstream?.body
  }
}

export function ensureRouteGraphConfigured(config: RouteGraphProxyConfig): void {
  if (!config.routerInternalBaseUrl) {
    throw new RouteGraphProxyError(503, 'router_internal_base_url_not_configured')
  }
  if (!config.routerAdminApiKey) {
    throw new RouteGraphProxyError(503, 'router_admin_api_key_not_configured')
  }
}

export async function fetchRouteGraph(
  config: RouteGraphProxyConfig
): Promise<unknown> {
  ensureRouteGraphConfigured(config)
  return requestUpstream(
    config,
    'GET',
    `${config.routerInternalBaseUrl}/internal/v1/route-graph`
  )
}

export async function explainRoute(
  config: RouteGraphProxyConfig,
  body: unknown
): Promise<unknown> {
  ensureRouteGraphConfigured(config)
  return requestUpstream(
    config,
    'POST',
    `${config.routerInternalBaseUrl}/internal/v1/route-explain`,
    body
  )
}

async function requestUpstream(
  config: RouteGraphProxyConfig,
  method: 'GET' | 'POST',
  url: string,
  body?: unknown
): Promise<unknown> {
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS)
  let response: Response
  try {
    response = await fetch(url, {
      method,
      headers: {
        'content-type': 'application/json',
        authorization: `Bearer ${config.routerAdminApiKey ?? ''}`
      },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: controller.signal
    })
  } catch (error) {
    if (error instanceof Error && error.name === 'AbortError') {
      throw new RouteGraphProxyError(504, 'upstream_timeout')
    }
    throw new RouteGraphProxyError(502, 'upstream_unreachable')
  } finally {
    clearTimeout(timer)
  }

  const text = await readLimitedText(response, MAX_RESPONSE_BYTES)
  if (!response.ok) {
    throw new RouteGraphProxyError(
      mapUpstreamStatus(response.status),
      'upstream_error',
      { status: response.status, body: text.slice(0, 1024) }
    )
  }
  if (!text) return null
  try {
    return JSON.parse(text) as unknown
  } catch (_error) {
    throw new RouteGraphProxyError(502, 'invalid_upstream_json', {
      status: response.status,
      body: text.slice(0, 1024)
    })
  }
}

async function readLimitedText(response: Response, maxBytes: number) {
  if (!response.body) return ''
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  const chunks: string[] = []
  let bytes = 0
  for (;;) {
    const result = await reader.read()
    if (result.done) break
    bytes += result.value.byteLength
    if (bytes > maxBytes) {
      await reader.cancel().catch(() => undefined)
      throw new RouteGraphProxyError(502, 'upstream_response_too_large')
    }
    chunks.push(decoder.decode(result.value, { stream: true }))
  }
  chunks.push(decoder.decode())
  return chunks.join('')
}

function mapUpstreamStatus(status: number): number {
  if (status === 400) return 400
  if (status === 401 || status === 403) return 502
  if (status === 404) return 404
  return 502
}

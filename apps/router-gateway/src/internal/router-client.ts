import { UpstreamHttpError } from '../errors'

export type FetchLike = (
  input: string | URL | Request,
  init?: RequestInit
) => Promise<Response>

export type InternalDepositAsset = {
  chain: string
  asset: string
}

export type InternalQuoteEnvelope = {
  quote: InternalRouterOrderQuote
}

export type InternalOrderEnvelope = {
  order: {
    id: string
    status: string
    source_asset: InternalDepositAsset
    destination_asset: InternalDepositAsset
    recipient_address: string
    refund_address: string
  }
  quote: InternalRouterOrderQuote
  funding_vault?: {
    vault: {
      deposit_vault_address: string
    }
  } | null
  cancellation_secret?: string
}

export type InternalRouterOrderQuote = {
  type: 'market_order'
  payload: InternalMarketOrderQuote
}

export type InternalMarketOrderQuote = {
  id: string
  order_id?: string | null
  source_asset: InternalDepositAsset
  destination_asset: InternalDepositAsset
  recipient_address: string
  provider_id: string
  order_kind: 'exact_in' | 'exact_out'
  amount_in: string
  amount_out: string
  min_amount_out?: string | null
  max_amount_in?: string | null
  slippage_bps: number
  expires_at: string
  created_at: string
}

export type CreateQuoteRequest = {
  type: 'market_order'
  from_asset: InternalDepositAsset
  to_asset: InternalDepositAsset
  recipient_address: string
} & (
  | {
      kind: 'exact_in'
      amount_in: string
      slippage_bps: number
    }
  | {
      kind: 'exact_out'
      amount_out: string
      slippage_bps: number
    }
)

export type CreateOrderRequest = {
  quote_id: string
  refund_address: string
  cancel_after?: string
  idempotency_key?: string
  metadata?: Record<string, unknown>
}

export type CreateOrderCancellationRequest = {
  cancellation_secret: string
}

export type RouterClientOptions = {
  baseUrl: string
  fetch?: FetchLike
  timeoutMs: number
}

export class RouterClient {
  private readonly fetcher: FetchLike
  private readonly baseUrl: string
  private readonly timeoutMs: number

  constructor(options: RouterClientOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, '')
    this.fetcher = options.fetch ?? fetch
    this.timeoutMs = options.timeoutMs
  }

  createQuote(request: CreateQuoteRequest): Promise<InternalQuoteEnvelope> {
    return this.request('POST', '/api/v1/quotes', request)
  }

  getQuote(quoteId: string): Promise<InternalQuoteEnvelope> {
    return this.request('GET', `/api/v1/quotes/${encodeURIComponent(quoteId)}`)
  }

  createOrder(request: CreateOrderRequest): Promise<InternalOrderEnvelope> {
    return this.request('POST', '/api/v1/orders', request)
  }

  cancelOrder(
    orderId: string,
    request: CreateOrderCancellationRequest
  ): Promise<InternalOrderEnvelope> {
    return this.request(
      'POST',
      `/api/v1/orders/${encodeURIComponent(orderId)}/cancellations`,
      request
    )
  }

  private async request<T>(
    method: string,
    path: string,
    body?: unknown
  ): Promise<T> {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs)

    try {
      const response = await this.fetcher(`${this.baseUrl}${path}`, {
        method,
        headers: {
          accept: 'application/json',
          ...(body === undefined ? {} : { 'content-type': 'application/json' })
        },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: controller.signal
      })

      const responseBody = await readResponseBody(response)
      if (!response.ok) {
        throw new UpstreamHttpError(
          response.status,
          upstreamErrorMessage(response.status, responseBody),
          responseBody
        )
      }

      return responseBody as T
    } catch (error) {
      if (error instanceof UpstreamHttpError) throw error
      const message = error instanceof Error ? error.message : 'upstream request failed'
      throw new UpstreamHttpError(502, message, undefined)
    } finally {
      clearTimeout(timeout)
    }
  }
}

async function readResponseBody(response: Response): Promise<unknown> {
  const text = await response.text()
  if (!text) return undefined

  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}

function upstreamErrorMessage(status: number, body: unknown): string {
  if (isObject(body)) {
    const error = body.error
    if (typeof error === 'string') return error
    if (isObject(error) && typeof error.message === 'string') return error.message
    if (typeof body.message === 'string') return body.message
  }

  return `upstream router API returned ${status}`
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

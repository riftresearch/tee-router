import { GatewayConfigurationError, UpstreamHttpError } from '../errors'
import { readLimitedResponseText } from './http-body'

const MAX_UPSTREAM_SUCCESS_BODY_BYTES = 1024 * 1024
const MAX_UPSTREAM_ERROR_BODY_BYTES = 64 * 1024
const MAX_INTERNAL_ID_LENGTH = 128
const MAX_INTERNAL_STATUS_LENGTH = 64
const MAX_INTERNAL_ASSET_FIELD_LENGTH = 128
const MAX_INTERNAL_ADDRESS_LENGTH = 128
const MAX_INTERNAL_AMOUNT_LENGTH = 96
const MAX_INTERNAL_DATETIME_LENGTH = 64
const MAX_INTERNAL_SECRET_LENGTH = 512
const MAX_UINT256 =
  115792089237316195423570985008687907853269984665640564039457584007913129639935n
const INTEGER_STRING_PATTERN = /^[0-9]+$/

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

export type InternalRouterOrderQuote =
  | {
      type: 'market_order'
      payload: InternalMarketOrderQuote
    }
  | {
      type: 'limit_order'
      payload: InternalLimitOrderQuote
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

export type InternalLimitOrderQuote = {
  id: string
  order_id?: string | null
  source_asset: InternalDepositAsset
  destination_asset: InternalDepositAsset
  recipient_address: string
  provider_id: string
  input_amount: string
  output_amount: string
  residual_policy: 'refund'
  provider_quote?: unknown
  expires_at: string
  created_at: string
}

export type CreateQuoteRequest =
  | ({
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
    ))
  | {
      type: 'limit_order'
      from_asset: InternalDepositAsset
      to_asset: InternalDepositAsset
      recipient_address: string
      input_amount: string
      output_amount: string
    }

export type CreateOrderRequest = {
  quote_id: string
  refund_address: string
  cancel_after?: string
  idempotency_key: string
  metadata?: Record<string, unknown>
}

export type CreateOrderCancellationRequest = {
  cancellation_secret: string
}

export type RouterClientOptions = {
  baseUrl: string
  apiKey?: string
  fetch?: FetchLike
  timeoutMs: number
}

export class RouterClient {
  private readonly fetcher: FetchLike
  private readonly baseUrl: string
  private readonly apiKey: string | undefined
  private readonly timeoutMs: number

  constructor(options: RouterClientOptions) {
    this.baseUrl = normalizeRouterBaseUrl(options.baseUrl)
    this.apiKey = options.apiKey
    this.fetcher = options.fetch ?? fetch
    this.timeoutMs = options.timeoutMs
  }

  async createQuote(request: CreateQuoteRequest): Promise<InternalQuoteEnvelope> {
    return requireQuoteEnvelope(
      await this.request('POST', '/api/v1/quotes', request)
    )
  }

  async getQuote(quoteId: string): Promise<InternalQuoteEnvelope> {
    return requireQuoteEnvelope(
      await this.request('GET', `/api/v1/quotes/${encodeURIComponent(quoteId)}`)
    )
  }

  async createOrder(request: CreateOrderRequest): Promise<InternalOrderEnvelope> {
    return requireOrderEnvelope(
      await this.request('POST', '/api/v1/orders', request)
    )
  }

  cancelOrder(
    orderId: string,
    request: CreateOrderCancellationRequest
  ): Promise<InternalOrderEnvelope> {
    return this.request(
      'POST',
      `/api/v1/orders/${encodeURIComponent(orderId)}/cancellations`,
      request
    ).then(requireOrderEnvelope)
  }

  private async request(
    method: string,
    path: string,
    body?: unknown
  ): Promise<unknown> {
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs)

    try {
      const response = await this.fetcher(`${this.baseUrl}${path}`, {
        method,
        headers: {
          accept: 'application/json',
          ...(this.apiKey ? { authorization: `Bearer ${this.apiKey}` } : {}),
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

      return responseBody
    } catch (error) {
      if (error instanceof UpstreamHttpError) throw error
      if (isAbortError(error)) {
        throw new UpstreamHttpError(
          504,
          `upstream router API timed out after ${this.timeoutMs}ms`,
          undefined
        )
      }
      void error
      throw new UpstreamHttpError(502, 'upstream router API request failed', undefined)
    } finally {
      clearTimeout(timeout)
    }
  }
}

function normalizeRouterBaseUrl(value: string): string {
  const trimmed = value.trim()
  if (!trimmed) {
    throw new GatewayConfigurationError('router base URL is required')
  }
  try {
    const url = new URL(trimmed)
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      throw new Error('expected http or https URL')
    }
    if (url.username || url.password) {
      throw new Error('URL credentials are not allowed')
    }
    if (url.search || url.hash) {
      throw new Error('URL query strings and fragments are not allowed')
    }
    return url.toString().replace(/\/+$/, '')
  } catch {
    throw new GatewayConfigurationError(
      'router base URL must be an absolute HTTP(S) URL without credentials, query string, or fragment'
    )
  }
}

function requireQuoteEnvelope(value: unknown): InternalQuoteEnvelope {
  if (isQuoteEnvelope(value)) return value
  throw malformedUpstreamResponse('quote')
}

function requireOrderEnvelope(value: unknown): InternalOrderEnvelope {
  if (isOrderEnvelope(value)) return value
  throw malformedUpstreamResponse('order')
}

function malformedUpstreamResponse(kind: string): UpstreamHttpError {
  return new UpstreamHttpError(
    502,
    `internal router API returned malformed ${kind} response`,
    { malformed: true }
  )
}

function isOrderEnvelope(value: unknown): value is InternalOrderEnvelope {
  if (!isObject(value)) return false
  if (!isObject(value.order)) return false
  if (!isQuote(value.quote, { strictAmounts: false })) return false
  if (!isObject(value.funding_vault)) return false
  if (!isObject(value.funding_vault.vault)) return false
  return (
    isBoundedString(value.order.id, MAX_INTERNAL_ID_LENGTH) &&
    isBoundedString(value.order.status, MAX_INTERNAL_STATUS_LENGTH) &&
    isDepositAsset(value.order.source_asset) &&
    isDepositAsset(value.order.destination_asset) &&
    isBoundedString(value.order.recipient_address, MAX_INTERNAL_ADDRESS_LENGTH) &&
    isBoundedString(value.order.refund_address, MAX_INTERNAL_ADDRESS_LENGTH) &&
    isBoundedString(
      value.funding_vault.vault.deposit_vault_address,
      MAX_INTERNAL_ADDRESS_LENGTH
    ) &&
    optionalBoundedString(value.cancellation_secret, MAX_INTERNAL_SECRET_LENGTH)
  )
}

function isQuoteEnvelope(value: unknown): value is InternalQuoteEnvelope {
  return isObject(value) && isQuote(value.quote)
}

function isQuote(
  value: unknown,
  options: { strictAmounts: boolean } = { strictAmounts: true }
): value is InternalRouterOrderQuote {
  if (!isObject(value)) return false
  if (value.type === 'market_order') return isMarketQuote(value.payload, options)
  if (value.type === 'limit_order') return isLimitQuote(value.payload, options)
  return false
}

function isMarketQuote(
  value: unknown,
  options: { strictAmounts: boolean }
): value is InternalMarketOrderQuote {
  if (!isObject(value)) return false
  const amount = options.strictAmounts ? isRawUint256String : isBoundedAmountString
  const optionalAmount = options.strictAmounts
    ? optionalRawUint256String
    : optionalBoundedAmountString
  return (
    isBoundedString(value.id, MAX_INTERNAL_ID_LENGTH) &&
    optionalBoundedString(value.order_id, MAX_INTERNAL_ID_LENGTH) &&
    isDepositAsset(value.source_asset) &&
    isDepositAsset(value.destination_asset) &&
    isBoundedString(value.recipient_address, MAX_INTERNAL_ADDRESS_LENGTH) &&
    isBoundedString(value.provider_id, MAX_INTERNAL_ID_LENGTH) &&
    (value.order_kind === 'exact_in' || value.order_kind === 'exact_out') &&
    amount(value.amount_in) &&
    amount(value.amount_out) &&
    optionalAmount(value.min_amount_out) &&
    optionalAmount(value.max_amount_in) &&
    isSlippageBps(value.slippage_bps) &&
    isBoundedString(value.expires_at, MAX_INTERNAL_DATETIME_LENGTH) &&
    isBoundedString(value.created_at, MAX_INTERNAL_DATETIME_LENGTH)
  )
}

function isLimitQuote(
  value: unknown,
  options: { strictAmounts: boolean }
): value is InternalLimitOrderQuote {
  if (!isObject(value)) return false
  const amount = options.strictAmounts ? isRawUint256String : isBoundedAmountString
  return (
    isBoundedString(value.id, MAX_INTERNAL_ID_LENGTH) &&
    optionalBoundedString(value.order_id, MAX_INTERNAL_ID_LENGTH) &&
    isDepositAsset(value.source_asset) &&
    isDepositAsset(value.destination_asset) &&
    isBoundedString(value.recipient_address, MAX_INTERNAL_ADDRESS_LENGTH) &&
    isBoundedString(value.provider_id, MAX_INTERNAL_ID_LENGTH) &&
    amount(value.input_amount) &&
    amount(value.output_amount) &&
    value.residual_policy === 'refund' &&
    isBoundedString(value.expires_at, MAX_INTERNAL_DATETIME_LENGTH) &&
    isBoundedString(value.created_at, MAX_INTERNAL_DATETIME_LENGTH)
  )
}

function isDepositAsset(value: unknown): value is InternalDepositAsset {
  return (
    isObject(value) &&
    isBoundedString(value.chain, MAX_INTERNAL_ASSET_FIELD_LENGTH) &&
    isBoundedString(value.asset, MAX_INTERNAL_ASSET_FIELD_LENGTH)
  )
}

function isBoundedString(value: unknown, maxLength: number): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= maxLength
}

function optionalBoundedString(value: unknown, maxLength: number): boolean {
  return value === undefined || value === null || isBoundedString(value, maxLength)
}

function isBoundedAmountString(value: unknown): value is string {
  return isBoundedString(value, MAX_INTERNAL_AMOUNT_LENGTH)
}

function optionalBoundedAmountString(value: unknown): boolean {
  return value === undefined || value === null || isBoundedAmountString(value)
}

function isRawUint256String(value: unknown): value is string {
  if (!isBoundedAmountString(value)) return false
  if (!INTEGER_STRING_PATTERN.test(value)) return false
  const raw = BigInt(value)
  return raw > 0n && raw <= MAX_UINT256
}

function optionalRawUint256String(value: unknown): boolean {
  return value === undefined || value === null || isRawUint256String(value)
}

function isSlippageBps(value: unknown): value is number {
  return (
    typeof value === 'number' &&
    Number.isSafeInteger(value) &&
    value >= 0 &&
    value <= 10_000
  )
}

async function readResponseBody(response: Response): Promise<unknown> {
  const maxBytes = response.ok
    ? MAX_UPSTREAM_SUCCESS_BODY_BYTES
    : MAX_UPSTREAM_ERROR_BODY_BYTES
  const { text, truncated } = await readLimitedResponseText(response, maxBytes)
  if (truncated) {
    if (response.ok) {
      throw new UpstreamHttpError(
        502,
        `upstream router API response exceeded ${maxBytes} bytes`,
        {
          truncated: true,
          maxBytes
        }
      )
    }
    return {
      truncated: true,
      maxBytes
    }
  }
  if (!text) return undefined

  try {
    return JSON.parse(text)
  } catch {
    return text
  }
}

function upstreamErrorMessage(status: number, _body: unknown): string {
  return `upstream router API returned ${status}`
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === 'AbortError'
}

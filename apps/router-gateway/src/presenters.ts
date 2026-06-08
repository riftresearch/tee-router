import {
  assetIdentifierFromInternal,
  formatPositiveAmount,
  type AmountFormat
} from './assets'
import { GatewayValidationError } from './errors'
import type {
  InternalLimitOrderQuote,
  InternalMarketOrderQuote,
  InternalOrderEnvelope,
  InternalQuoteEnvelope
} from './internal/router-client'

export type PublicQuoteResponse = {
  quoteId: string
  orderType: 'market_order' | 'limit_order'
  from: string
  to: string
  expiry: string
  estimatedOut: string
  fees?: PublicQuoteFee[]
  expectedSwapTimeMs?: number
  venues: string[]
  quoteCandidates?: unknown
  amountFormat: AmountFormat
}

export type PublicQuoteFee = {
  kind: string
  label: string
  asset: string
  amount: string
  amountRaw: string
}

export type PublicOrderResponse = PublicQuoteResponse & {
  orderId: string
  orderAddress: string
  amountToSend: string
  status: string
}

export type PublicLimitOrderResponse = {
  orderId: string
  orderAddress: string
  amountToSend: string
  quoteId: string
  from: string
  to: string
  status: string
  expiry: string
  outputAmount: string
  amountFormat: AmountFormat
}

export function presentQuoteEnvelope(
  envelope: InternalQuoteEnvelope,
  amountFormat: AmountFormat
): PublicQuoteResponse {
  return presentQuote(routerQuoteFromEnvelope(envelope), amountFormat)
}

export function presentOrderEnvelope(
  envelope: InternalOrderEnvelope,
  amountFormat: AmountFormat
): PublicOrderResponse {
  const quote = routerQuoteFromEnvelope({ quote: envelope.quote })
  const source = assetIdentifierFromInternal(quote.source_asset)
  const amountToSendRaw =
    envelope.quote.type === 'market_order'
      ? envelope.quote.payload.amount_in
      : envelope.quote.payload.input_amount
  const orderAddress = envelope.funding_vault?.vault.deposit_vault_address

  if (!orderAddress) {
    throw new GatewayValidationError('router order response is missing funding vault address')
  }

  return {
    ...presentQuote(quote, amountFormat),
    orderId: envelope.order.id,
    orderAddress,
    amountToSend: formatPositiveAmount(amountToSendRaw, source, amountFormat),
    status: presentOrderStatus(envelope.order.status)
  }
}

export function presentAnyOrderEnvelope(
  envelope: InternalOrderEnvelope,
  amountFormat: AmountFormat
): PublicOrderResponse | PublicLimitOrderResponse {
  if (envelope.quote.type === 'limit_order') {
    return presentLimitOrderEnvelope(envelope, amountFormat)
  }

  return presentOrderEnvelope(envelope, amountFormat)
}

export function presentLimitOrderEnvelope(
  envelope: InternalOrderEnvelope,
  amountFormat: AmountFormat
): PublicLimitOrderResponse {
  const quote = limitQuoteFromEnvelope({ quote: envelope.quote })
  const source = assetIdentifierFromInternal(quote.source_asset)
  const destination = assetIdentifierFromInternal(quote.destination_asset)
  const orderAddress = envelope.funding_vault?.vault.deposit_vault_address

  if (!orderAddress) {
    throw new GatewayValidationError('router order response is missing funding vault address')
  }

  return {
    orderId: envelope.order.id,
    orderAddress,
    amountToSend: formatPositiveAmount(quote.input_amount, source, amountFormat),
    quoteId: quote.id,
    from: source.id,
    to: destination.id,
    status: presentOrderStatus(envelope.order.status),
    expiry: quote.expires_at,
    outputAmount: formatPositiveAmount(quote.output_amount, destination, amountFormat),
    amountFormat
  }
}

export function marketQuoteFromEnvelope(
  envelope: InternalQuoteEnvelope
): InternalMarketOrderQuote {
  if (envelope.quote.type !== 'market_order') {
    throw new GatewayValidationError('router returned an unsupported quote type')
  }

  return envelope.quote.payload
}

export function limitQuoteFromEnvelope(
  envelope: InternalQuoteEnvelope
): InternalLimitOrderQuote {
  if (envelope.quote.type !== 'limit_order') {
    throw new GatewayValidationError('router returned an unsupported quote type')
  }

  return envelope.quote.payload
}

export function routerQuoteFromEnvelope(
  envelope: InternalQuoteEnvelope
): InternalMarketOrderQuote | InternalLimitOrderQuote {
  return envelope.quote.payload
}

function presentQuote(
  quote: InternalMarketOrderQuote | InternalLimitOrderQuote,
  amountFormat: AmountFormat
): PublicQuoteResponse {
  const source = assetIdentifierFromInternal(quote.source_asset)
  const destination = assetIdentifierFromInternal(quote.destination_asset)
  const venues = quoteVenues(quote.provider_quote)
  if ('input_amount' in quote) {
    return {
      quoteId: quote.id,
      orderType: 'limit_order',
      from: source.id,
      to: destination.id,
      expiry: quote.expires_at,
      estimatedOut: formatPositiveAmount(
        quote.output_amount,
        destination,
        amountFormat
      ),
      ...(quote.expected_swap_time_ms === null ||
      quote.expected_swap_time_ms === undefined
        ? {}
        : { expectedSwapTimeMs: quote.expected_swap_time_ms }),
      venues,
      amountFormat
    }
  }

  return {
    quoteId: quote.id,
    orderType: 'market_order',
    from: source.id,
    to: destination.id,
    expiry: quote.expires_at,
    estimatedOut: formatPositiveAmount(
      quote.estimated_amount_out,
      destination,
      amountFormat
    ),
    ...(quote.expected_swap_time_ms === null ||
    quote.expected_swap_time_ms === undefined
      ? {}
      : { expectedSwapTimeMs: quote.expected_swap_time_ms }),
    ...(quote.quote_candidates === undefined
      ? {}
      : { quoteCandidates: quote.quote_candidates }),
    venues,
    ...quoteFees(quote.provider_quote, amountFormat),
    amountFormat
  }
}

function presentOrderStatus(status: string): string {
  if (status === 'refund_required' || status === 'refunding') {
    return 'refund_pending'
  }
  return status
}

function quoteVenues(providerQuote: unknown): string[] {
  const quote = isRecord(providerQuote) ? providerQuote : undefined
  const directVenues = Array.isArray(quote?.venues) ? quote.venues : []
  if (directVenues.length > 0) {
    return directVenues.filter((venue): venue is string => typeof venue === 'string' && venue.length > 0)
  }
  const transitions = Array.isArray(quote?.transitions)
    ? quote.transitions
    : []
  const venues: string[] = []
  for (const transition of transitions) {
    if (!isRecord(transition)) continue
    const provider = transition.provider
    if (typeof provider === 'string' && provider.length > 0) {
      venues.push(provider)
    }
  }
  return venues
}

function quoteFees(
  providerQuote: unknown,
  amountFormat: AmountFormat
): { fees?: PublicQuoteFee[] } {
  const fees: PublicQuoteFee[] = []
  const quote = isRecord(providerQuote) ? providerQuote : undefined
  const legs = Array.isArray(quote?.legs) ? quote.legs : []
  for (const leg of legs) {
    if (!isRecord(leg)) continue
    const raw = isRecord(leg.raw) ? leg.raw : undefined
    const activationFee = isRecord(raw?.hyperliquid_core_activation_fee)
      ? raw.hyperliquid_core_activation_fee
      : undefined
    if (!activationFee) continue
    const amountRaw =
      typeof activationFee.amount_raw === 'string'
        ? activationFee.amount_raw
        : undefined
    const amountDecimal =
      typeof activationFee.amount_decimal === 'string'
        ? activationFee.amount_decimal
        : undefined
    const quoteAsset =
      typeof activationFee.quote_asset === 'string'
        ? activationFee.quote_asset
        : 'USDC'
    if (!amountRaw || !amountDecimal) continue
    fees.push({
      kind: 'hyperliquid_core_activation_fee',
      label: 'Hyperliquid activation fee',
      asset: `Hyperliquid.${quoteAsset}`,
      amount: amountFormat === 'raw' ? amountRaw : amountDecimal,
      amountRaw
    })
  }
  return fees.length === 0 ? {} : { fees }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

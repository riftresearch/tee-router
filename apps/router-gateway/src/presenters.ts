import {
  assetIdentifierFromInternal,
  formatPositiveAmount,
  formatSlippage,
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
  expectedOut: string
  expectedSlippage?: string
  minOut?: string
  maxIn?: string
  maxSlippage?: string
  fees?: PublicQuoteFee[]
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
  minOut: string
  amountFormat: AmountFormat
  cancellationSecret?: string
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
      ? envelope.quote.payload.max_amount_in ?? envelope.quote.payload.amount_in
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
    status: envelope.order.status
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
    status: envelope.order.status,
    expiry: quote.expires_at,
    minOut: formatPositiveAmount(quote.output_amount, destination, amountFormat),
    amountFormat,
    ...(envelope.cancellation_secret
      ? { cancellationSecret: envelope.cancellation_secret }
      : {})
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
  if ('input_amount' in quote) {
    return {
      quoteId: quote.id,
      orderType: 'limit_order',
      from: source.id,
      to: destination.id,
      expiry: quote.expires_at,
      expectedOut: formatPositiveAmount(
        quote.output_amount,
        destination,
        amountFormat
      ),
      maxIn: formatPositiveAmount(quote.input_amount, source, amountFormat),
      maxSlippage: formatSlippage(0, amountFormat),
      amountFormat
    }
  }

  const minOut =
    quote.min_amount_out === null || quote.min_amount_out === undefined
      ? undefined
      : formatPositiveAmount(quote.min_amount_out, destination, amountFormat)
  const maxIn =
    quote.max_amount_in === null || quote.max_amount_in === undefined
      ? undefined
      : formatPositiveAmount(quote.max_amount_in, source, amountFormat)

  return {
    quoteId: quote.id,
    orderType: 'market_order',
    from: source.id,
    to: destination.id,
    expiry: quote.expires_at,
    expectedOut: formatPositiveAmount(quote.amount_out, destination, amountFormat),
    ...(minOut === undefined ? {} : { minOut }),
    ...(maxIn === undefined ? {} : { maxIn }),
    ...(quote.slippage_bps === null || quote.slippage_bps === undefined
      ? {}
      : { maxSlippage: formatSlippage(quote.slippage_bps, amountFormat) }),
    ...quoteFees(quote.provider_quote, amountFormat),
    amountFormat
  }
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

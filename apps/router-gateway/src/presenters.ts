import {
  assetIdentifierFromInternal,
  formatAmount,
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
  from: string
  to: string
  expiry: string
  expectedOut: string
  expectedSlippage?: string
  minOut?: string
  maxIn?: string
  maxSlippage: string
  amountFormat: AmountFormat
}

export type PublicOrderResponse = PublicQuoteResponse & {
  orderId: string
  orderAddress: string
  amountToSend: string
  status: string
  cancellationSecret?: string
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
  return presentQuote(marketQuoteFromEnvelope(envelope), amountFormat)
}

export function presentOrderEnvelope(
  envelope: InternalOrderEnvelope,
  amountFormat: AmountFormat
): PublicOrderResponse {
  const quote = marketQuoteFromEnvelope({ quote: envelope.quote })
  const source = assetIdentifierFromInternal(quote.source_asset)
  const amountToSendRaw = quote.max_amount_in ?? quote.amount_in
  const orderAddress = envelope.funding_vault?.vault.deposit_vault_address

  if (!orderAddress) {
    throw new GatewayValidationError('router order response is missing funding vault address')
  }

  return {
    ...presentQuote(quote, amountFormat),
    orderId: envelope.order.id,
    orderAddress,
    amountToSend: formatAmount(amountToSendRaw, source, amountFormat),
    status: envelope.order.status,
    ...(envelope.cancellation_secret
      ? { cancellationSecret: envelope.cancellation_secret }
      : {})
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
    amountToSend: formatAmount(quote.input_amount, source, amountFormat),
    quoteId: quote.id,
    from: source.id,
    to: destination.id,
    status: envelope.order.status,
    expiry: quote.expires_at,
    minOut: formatAmount(quote.output_amount, destination, amountFormat),
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

function presentQuote(
  quote: InternalMarketOrderQuote,
  amountFormat: AmountFormat
): PublicQuoteResponse {
  const source = assetIdentifierFromInternal(quote.source_asset)
  const destination = assetIdentifierFromInternal(quote.destination_asset)
  const minOut =
    quote.min_amount_out === null || quote.min_amount_out === undefined
      ? undefined
      : formatAmount(quote.min_amount_out, destination, amountFormat)
  const maxIn =
    quote.max_amount_in === null || quote.max_amount_in === undefined
      ? undefined
      : formatAmount(quote.max_amount_in, source, amountFormat)

  return {
    quoteId: quote.id,
    from: source.id,
    to: destination.id,
    expiry: quote.expires_at,
    expectedOut: formatAmount(quote.amount_out, destination, amountFormat),
    ...(minOut === undefined ? {} : { minOut }),
    ...(maxIn === undefined ? {} : { maxIn }),
    maxSlippage: formatSlippage(quote.slippage_bps, amountFormat),
    amountFormat
  }
}

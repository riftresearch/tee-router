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
  orderType: 'market_order' | 'limit_order'
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
    amountToSend: formatAmount(amountToSendRaw, source, amountFormat),
    status: envelope.order.status,
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
      expectedOut: formatAmount(quote.output_amount, destination, amountFormat),
      maxIn: formatAmount(quote.input_amount, source, amountFormat),
      maxSlippage: formatSlippage(0, amountFormat),
      amountFormat
    }
  }

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
    orderType: 'market_order',
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

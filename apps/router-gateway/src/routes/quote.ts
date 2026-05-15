import { createRoute, z } from '@hono/zod-openapi'

import {
  assertAddressMatchesChain,
  parseAmount,
  resolveAssetIdentifier,
  type AmountFormat
} from '../assets'
import type { GatewayConfig } from '../config'
import { normalizeError } from '../errors'
import { presentQuoteEnvelope } from '../presenters'
import { routerClientFor, type GatewayDeps } from './deps'
import {
  AmountFormatSchema,
  AddressSchema,
  AmountStringSchema,
  AssetIdentifierSchema,
  ErrorResponses,
  QuoteResponseSchema
} from './schemas'

export const QuoteRequestSchema = z
  .object({
    from: AssetIdentifierSchema.openapi({
      example: 'Bitcoin.BTC'
    }),
    to: AssetIdentifierSchema.openapi({
      example: 'Ethereum.USDC'
    }),
    amountFormat: AmountFormatSchema.optional(),
    toAddress: AddressSchema.openapi({
      description:
        'Current Rust router quote bridge field. The upstream router requires a recipient address at quote time.',
      example: '0x1111111111111111111111111111111111111111'
    }),
    orderType: z
      .enum([
        'market_order',
        'market',
        // Limit orders are temporarily disabled at the gateway.
        // 'limit_order',
        // 'limit'
      ])
      .optional()
      .openapi({
        description: 'Order type. Defaults to market_order.',
        example: 'market_order'
      }),
    fromAmount: AmountStringSchema.optional().openapi({
      example: '10'
    })
  })
  .strict()
  .superRefine((value, ctx) => {
    const orderType = normalizeOrderType(value.orderType)
    if (orderType === 'market_order' && value.fromAmount === undefined) {
      ctx.addIssue({
        code: 'custom',
        message: 'fromAmount is required',
        path: ['fromAmount']
      })
    }
    if (orderType === 'limit_order' && value.fromAmount === undefined) {
      ctx.addIssue({
        code: 'custom',
        message: 'fromAmount is required',
        path: ['fromAmount']
      })
    }
  })
  .openapi('QuoteRequest')

export const quoteRoute = createRoute({
  method: 'post',
  path: '/quote',
  tags: ['Quotes'],
  summary: 'Create a quote for a market order',
  request: {
    body: {
      required: true,
      content: {
        'application/json': {
          schema: QuoteRequestSchema
        }
      }
    }
  },
  responses: {
    201: {
      description: 'Market quote created by the internal router API.',
      content: {
        'application/json': {
          schema: QuoteResponseSchema
        }
      }
    },
    ...ErrorResponses
  }
})

export function createQuoteHandler(
  config: GatewayConfig,
  deps: GatewayDeps = {}
) {
  return async (c: any) => {
    try {
      const request = c.req.valid('json')
      const amountFormat: AmountFormat = request.amountFormat ?? 'readable'
      const source = resolveAssetIdentifier(request.from)
      const destination = resolveAssetIdentifier(request.to)
      const orderType = normalizeOrderType(request.orderType)
      assertAddressMatchesChain(
        destination.internal.chain,
        request.toAddress,
        'toAddress',
        { bitcoinAddressNetworks: config.bitcoinAddressNetworks }
      )

      if (orderType === 'limit_order') {
        const envelope = await routerClientFor(config, deps).createQuote({
          type: 'limit_order',
          from_asset: source.internal,
          to_asset: destination.internal,
          recipient_address: request.toAddress,
          input_amount: parseAmount(
            request.fromAmount as string,
            source,
            amountFormat,
            'fromAmount'
          ),
          output_amount: parseAmount(
            request.fromAmount as string,
            destination,
            amountFormat,
            'fromAmount'
          )
        })

        return c.json(presentQuoteEnvelope(envelope, amountFormat), 201)
      }

      const envelope = await routerClientFor(config, deps).createQuote({
        type: 'market_order',
        from_asset: source.internal,
        to_asset: destination.internal,
        recipient_address: request.toAddress,
        amount_in: parseAmount(
          request.fromAmount as string,
          source,
          amountFormat,
          'fromAmount'
        )
      })

      return c.json(presentQuoteEnvelope(envelope, amountFormat), 201)
    } catch (error) {
      const normalized = normalizeError(error)
      return c.json(normalized.body, normalized.status)
    }
  }
}

function normalizeOrderType(value: string | undefined): 'market_order' | 'limit_order' {
  if (value === 'limit' || value === 'limit_order') return 'limit_order'
  return 'market_order'
}

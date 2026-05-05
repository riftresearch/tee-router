import { createRoute, z } from '@hono/zod-openapi'

import {
  assertAddressMatchesChain,
  parseAmount,
  parseSlippageBps,
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
  QuoteResponseSchema,
  SlippageStringSchema
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
      .enum(['market_order', 'market', 'limit_order', 'limit'])
      .optional()
      .openapi({
        description: 'Order type. Defaults to market_order.',
        example: 'market_order'
      }),
    fromAmount: AmountStringSchema.optional().openapi({
      example: '10'
    }),
    toAmount: AmountStringSchema.optional().openapi({
      example: '100000'
    }),
    maxSlippage: SlippageStringSchema.optional().openapi({
      example: '1.5'
    })
  })
  .strict()
  .superRefine((value, ctx) => {
    const orderType = normalizeOrderType(value.orderType)
    const amountFields = [value.fromAmount, value.toAmount].filter(
      (amount) => amount !== undefined
    )
    if (orderType === 'market_order' && amountFields.length !== 1) {
      ctx.addIssue({
        code: 'custom',
        message: 'exactly one of fromAmount or toAmount is required',
        path: ['fromAmount']
      })
    }
    if (orderType === 'market_order' && value.maxSlippage === undefined) {
      ctx.addIssue({
        code: 'custom',
        message: 'maxSlippage is required for market orders',
        path: ['maxSlippage']
      })
    }
    if (
      orderType === 'limit_order' &&
      (value.fromAmount === undefined || value.toAmount === undefined)
    ) {
      ctx.addIssue({
        code: 'custom',
        message: 'fromAmount and toAmount are required for limit orders',
        path: ['fromAmount']
      })
    }
  })
  .openapi('QuoteRequest')

export const quoteRoute = createRoute({
  method: 'post',
  path: '/quote',
  tags: ['Quotes'],
  summary: 'Create a quote for a market or limit order',
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
      description: 'Quote created by the internal router API.',
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
            request.toAmount as string,
            destination,
            amountFormat,
            'toAmount'
          )
        })

        return c.json(presentQuoteEnvelope(envelope, amountFormat), 201)
      }

      const slippageBps = parseSlippageBps(request.maxSlippage as string, amountFormat)
      const orderKind =
        request.fromAmount !== undefined
          ? {
              kind: 'exact_in' as const,
              amount_in: parseAmount(
                request.fromAmount,
                source,
                amountFormat,
                'fromAmount'
              ),
              slippage_bps: slippageBps
            }
          : {
              kind: 'exact_out' as const,
              amount_out: parseAmount(
                request.toAmount as string,
                destination,
                amountFormat,
                'toAmount'
              ),
              slippage_bps: slippageBps
            }

      const envelope = await routerClientFor(config, deps).createQuote({
        type: 'market_order',
        from_asset: source.internal,
        to_asset: destination.internal,
        recipient_address: request.toAddress,
        ...orderKind
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

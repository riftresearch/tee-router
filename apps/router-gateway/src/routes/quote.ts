import { createRoute, z } from '@hono/zod-openapi'

import {
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
  ErrorResponses,
  QuoteResponseSchema
} from './schemas'

export const QuoteRequestSchema = z
  .object({
    from: z.string().min(1).openapi({
      example: 'Bitcoin.BTC'
    }),
    to: z.string().min(1).openapi({
      example: 'Ethereum.USDC'
    }),
    amountFormat: AmountFormatSchema.optional(),
    toAddress: z.string().min(1).openapi({
      description:
        'Current Rust router quote bridge field. The upstream router requires a recipient address at quote time.',
      example: '0x1111111111111111111111111111111111111111'
    }),
    fromAmount: z.string().min(1).optional().openapi({
      example: '10'
    }),
    toAmount: z.string().min(1).optional().openapi({
      example: '100000'
    }),
    maxSlippage: z.string().min(1).openapi({
      example: '1.5'
    })
  })
  .strict()
  .superRefine((value, ctx) => {
    const amountFields = [value.fromAmount, value.toAmount].filter(
      (amount) => amount !== undefined
    )
    if (amountFields.length !== 1) {
      ctx.addIssue({
        code: 'custom',
        message: 'exactly one of fromAmount or toAmount is required',
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
      const slippageBps = parseSlippageBps(request.maxSlippage, amountFormat)
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

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
import { decimalsResolverFor, routerClientFor, type GatewayDeps } from './deps'
import {
  AmountFormatSchema,
  AddressSchema,
  AmountStringSchema,
  AssetIdentifierSchema,
  ErrorResponses,
  QuoteResponseSchema
} from './schemas'

const ProviderIdSchema = z.enum([
  'across',
  'cctp',
  'unit',
  'hyperliquid_bridge',
  'hyperliquid',
  'velora'
])

const QuoteRoutingSchema = z
  .object({
    providerSequence: z.array(ProviderIdSchema).min(1).max(5).optional().openapi({
      description:
        'Exact provider sequence the selected transition path must use. This filters candidate routes before quote hydration; it does not bypass provider quoting or execution validation.',
      example: ['velora', 'cctp']
    })
  })
  .strict()
  .openapi('QuoteRouting')

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
    }),
    routing: QuoteRoutingSchema.optional().openapi({
      description:
        'Optional route-selection constraints. providerSequence is an exact transition-provider sequence.'
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
  summary: 'Get a market quote',
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
      // Readable amounts need decimals for both sides (input parse + output
      // formatting). For tokens addressed by contract address, resolve decimals
      // on-chain (cached) so readable works without the caller knowing them.
      if (amountFormat === 'readable') {
        const decimalsResolver = decimalsResolverFor(config, deps)
        await decimalsResolver.ensure(source)
        await decimalsResolver.ensure(destination)
      }
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

      const routing =
        request.routing?.providerSequence === undefined
          ? undefined
          : {
              provider_sequence: request.routing.providerSequence
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
        ),
        ...(routing === undefined ? {} : { routing })
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

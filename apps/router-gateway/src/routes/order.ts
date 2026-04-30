import { createRoute, z } from '@hono/zod-openapi'

import type { AmountFormat } from '../assets'
import type { GatewayConfig } from '../config'
import { GatewayValidationError, normalizeError } from '../errors'
import { marketQuoteFromEnvelope, presentOrderEnvelope } from '../presenters'
import {
  cancellationServiceFor,
  routerClientFor,
  type GatewayDeps
} from './deps'
import {
  AmountFormatSchema,
  CancellationOwnershipSchema,
  ErrorResponses,
  OrderResponseSchema
} from './schemas'

export const OrderMarketRequestSchema = z
  .object({
    quoteId: z.string().uuid(),
    fromAddress: z.string().min(1),
    toAddress: z.string().min(1),
    refundAddress: z.string().min(1).optional(),
    integrator: z.string().min(1).optional(),
    idempotencyKey: z.string().min(1).optional(),
    cancelAfter: z.string().datetime().optional(),
    cancellation: CancellationOwnershipSchema.optional(),
    amountFormat: AmountFormatSchema.optional()
  })
  .strict()
  .openapi('OrderMarketRequest')

export const orderMarketRoute = createRoute({
  method: 'post',
  path: '/order/market',
  tags: ['Orders'],
  summary: 'Create a market order from a quote',
  request: {
    body: {
      required: true,
      content: {
        'application/json': {
          schema: OrderMarketRequestSchema
        }
      }
    }
  },
  responses: {
    201: {
      description: 'Market order created by the internal router API.',
      content: {
        'application/json': {
          schema: OrderResponseSchema
        }
      }
    },
    ...ErrorResponses
  }
})

export const OrderCancelRequestSchema = z
  .object({
    cancellationSecret: z.string().min(1).optional(),
    cancellationToken: z.string().min(1).optional(),
    amountFormat: AmountFormatSchema.optional()
  })
  .strict()
  .superRefine((value, ctx) => {
    const credentials = [value.cancellationSecret, value.cancellationToken].filter(
      (credential) => credential !== undefined
    )
    if (credentials.length !== 1) {
      ctx.addIssue({
        code: 'custom',
        message: 'exactly one of cancellationSecret or cancellationToken is required',
        path: ['cancellationSecret']
      })
    }
  })
  .openapi('OrderCancelRequest')

export const orderCancelRoute = createRoute({
  method: 'post',
  path: '/order/{orderId}/cancel',
  tags: ['Orders'],
  summary: 'Cancel an order with a cancellation secret',
  request: {
    params: z.object({
      orderId: z.string().uuid()
    }),
    body: {
      required: true,
      content: {
        'application/json': {
          schema: OrderCancelRequestSchema
        }
      }
    }
  },
  responses: {
    200: {
      description: 'Order cancellation accepted by the internal router API.',
      content: {
        'application/json': {
          schema: OrderResponseSchema
        }
      }
    },
    ...ErrorResponses
  }
})

export function createOrderMarketHandler(
  config: GatewayConfig,
  deps: GatewayDeps = {}
) {
  return async (c: any) => {
    try {
      const request = c.req.valid('json')
      const amountFormat: AmountFormat = request.amountFormat ?? 'readable'
      const cancellationMode = request.cancellation?.mode ?? 'user_secret'
      const routerClient = routerClientFor(config, deps)
      const quoteEnvelope = await routerClient.getQuote(request.quoteId)
      const quote = marketQuoteFromEnvelope(quoteEnvelope)

      if (
        !addressesMatch(
          quote.destination_asset.chain,
          quote.recipient_address,
          request.toAddress
        )
      ) {
        throw new GatewayValidationError(
          'toAddress must match the recipient address used when the quote was created',
          {
            quoteId: request.quoteId,
            quoteRecipientAddress: quote.recipient_address,
            toAddress: request.toAddress
          }
        )
      }

      const envelope = await routerClient.createOrder({
        quote_id: request.quoteId,
        refund_address: request.refundAddress ?? request.fromAddress,
        ...(request.cancelAfter ? { cancel_after: request.cancelAfter } : {}),
        ...(request.idempotencyKey
          ? { idempotency_key: request.idempotencyKey }
          : {}),
        metadata: {
          ...(request.integrator ? { integrator: request.integrator } : {}),
          from_address: request.fromAddress,
          to_address: request.toAddress,
          gateway: 'router-gateway'
        }
      })
      const response = presentOrderEnvelope(envelope, amountFormat)

      if (cancellationMode === 'gateway_managed_token') {
        if (!envelope.cancellation_secret) {
          throw new GatewayValidationError(
            'router order response is missing cancellationSecret'
          )
        }

        const cancellationToken = await cancellationServiceFor(
          config,
          deps
        ).createGatewayManagedToken({
          routerOrderId: envelope.order.id,
          cancellationSecret: envelope.cancellation_secret,
          authPolicy: { mode: 'gateway_managed_token' }
        })

        delete response.cancellationSecret
        return c.json(
          {
            ...response,
            cancellationMode,
            cancellationToken
          },
          201
        )
      }

      return c.json(
        {
          ...response,
          cancellationMode
        },
        201
      )
    } catch (error) {
      const normalized = normalizeError(error)
      return c.json(normalized.body, normalized.status)
    }
  }
}

export function createOrderCancelHandler(
  config: GatewayConfig,
  deps: GatewayDeps = {}
) {
  return async (c: any) => {
    try {
      const request = c.req.valid('json')
      const amountFormat: AmountFormat = request.amountFormat ?? 'readable'
      const cancellationSecret =
        request.cancellationSecret ??
        (await cancellationServiceFor(
          config,
          deps
        ).resolveTokenCancellationSecret(
          c.req.valid('param').orderId,
          request.cancellationToken as string
        ))
      const envelope = await routerClientFor(config, deps).cancelOrder(
        c.req.valid('param').orderId,
        {
          cancellation_secret: cancellationSecret
        }
      )

      return c.json(presentOrderEnvelope(envelope, amountFormat), 200)
    } catch (error) {
      const normalized = normalizeError(error)
      return c.json(normalized.body, normalized.status)
    }
  }
}

function addressesMatch(chainId: string, left: string, right: string): boolean {
  if (chainId.startsWith('evm:')) {
    return left.toLowerCase() === right.toLowerCase()
  }

  return left === right
}

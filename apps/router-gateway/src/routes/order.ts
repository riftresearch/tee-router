import { createRoute, z } from '@hono/zod-openapi'

import type { AmountFormat } from '../assets'
import type { GatewayConfig } from '../config'
import { GatewayValidationError, normalizeError } from '../errors'
import { marketQuoteFromEnvelope, presentOrderEnvelope } from '../presenters'
import {
  refundAuthorizationServiceFor,
  routerClientFor,
  type GatewayDeps
} from './deps'
import {
  AmountFormatSchema,
  ErrorResponses,
  OrderResponseSchema,
  RefundModeSchema
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
    refundMode: RefundModeSchema.optional(),
    refundAuthorizer: z.string().min(1).nullable(),
    amountFormat: AmountFormatSchema.optional()
  })
  .strict()
  .superRefine((value, ctx) => {
    const refundMode = value.refundMode ?? 'evmSignature'
    if (refundMode === 'evmSignature' && !value.refundAuthorizer) {
      ctx.addIssue({
        code: 'custom',
        message: 'refundAuthorizer is required when refundMode is evmSignature',
        path: ['refundAuthorizer']
      })
    }

    if (refundMode === 'token' && value.refundAuthorizer !== null) {
      ctx.addIssue({
        code: 'custom',
        message: 'refundAuthorizer must be null when refundMode is token',
        path: ['refundAuthorizer']
      })
    }
  })
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
    refundToken: z.string().min(1).optional(),
    refundSignature: z.string().regex(/^0x[a-fA-F0-9]+$/).optional(),
    refundSignatureDeadline: z.number().int().positive().optional(),
    amountFormat: AmountFormatSchema.optional()
  })
  .strict()
  .superRefine((value, ctx) => {
    const hasToken = value.refundToken !== undefined
    const hasSignature = value.refundSignature !== undefined
    const credentials = [hasToken, hasSignature].filter(Boolean)
    if (credentials.length !== 1) {
      ctx.addIssue({
        code: 'custom',
        message: 'exactly one of refundToken or refundSignature is required',
        path: ['refundToken']
      })
    }

    if (hasSignature && value.refundSignatureDeadline === undefined) {
      ctx.addIssue({
        code: 'custom',
        message: 'refundSignatureDeadline is required with refundSignature',
        path: ['refundSignatureDeadline']
      })
    }
  })
  .openapi('OrderCancelRequest')

export const orderCancelRoute = createRoute({
  method: 'post',
  path: '/order/{orderId}/cancel',
  tags: ['Orders'],
  summary: 'Cancel an order with its configured refund authorization',
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
      const refundMode = request.refundMode ?? 'evmSignature'
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

      if (!envelope.cancellation_secret) {
        throw new GatewayValidationError(
          'router order response is missing cancellationSecret'
        )
      }

      const refundAuthorization = await refundAuthorizationServiceFor(
        config,
        deps
      ).createRefundAuthorization({
        routerOrderId: envelope.order.id,
        cancellationSecret: envelope.cancellation_secret,
        refundMode,
        refundAuthorizer: request.refundAuthorizer ?? null
      })

      delete response.cancellationSecret
      return c.json(
        {
          ...response,
          ...refundAuthorization
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
      const orderId = c.req.valid('param').orderId
      const refundAuthorizationService = refundAuthorizationServiceFor(config, deps)
      const cancellationSecret = request.refundToken
        ? await refundAuthorizationService.resolveTokenCancellationSecret(
            orderId,
            request.refundToken
          )
        : await refundAuthorizationService.resolveEvmSignatureCancellationSecret({
            routerOrderId: orderId,
            refundSignature: request.refundSignature as string,
            refundSignatureDeadline: request.refundSignatureDeadline as number
          })
      const envelope = await routerClientFor(config, deps).cancelOrder(
        orderId,
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

import { createRoute, z } from '@hono/zod-openapi'

import { assertAddressMatchesChain, type AmountFormat } from '../assets'
import type { GatewayConfig } from '../config'
import {
  GatewayConflictError,
  GatewayValidationError,
  UpstreamHttpError,
  normalizeError
} from '../errors'
import { presentOrderEnvelope, routerQuoteFromEnvelope } from '../presenters'
import {
  refundAuthorizationServiceFor,
  routerClientFor,
  type GatewayDeps
} from './deps'
import { logError } from '../internal/logging'
import {
  AddressSchema,
  AmountFormatSchema,
  DateTimeSchema,
  EvmSignatureSchema,
  ErrorResponses,
  IdempotencyKeySchema,
  IntegratorSchema,
  OrderResponseSchema,
  RefundTokenSchema,
  RefundModeSchema
} from './schemas'

export const OrderMarketRequestSchema = z
  .object({
    quoteId: z.string().uuid(),
    fromAddress: AddressSchema,
    toAddress: AddressSchema,
    refundAddress: AddressSchema.optional(),
    integrator: IntegratorSchema.optional(),
    idempotencyKey: IdempotencyKeySchema,
    cancelAfter: DateTimeSchema.optional(),
    refundMode: RefundModeSchema.optional(),
    refundAuthorizer: AddressSchema.nullable(),
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
  summary: 'Create an order from a quote',
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
      description: 'Order created by the internal router API.',
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
    refundToken: RefundTokenSchema.optional(),
    refundSignature: EvmSignatureSchema.optional(),
    refundSignatureDeadline: z.number().int().positive().safe().optional(),
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
  summary: 'Cancel an order via specified authentication',
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
      const quote = routerQuoteFromEnvelope(quoteEnvelope)
      assertAddressMatchesChain(
        quote.source_asset.chain,
        request.fromAddress,
        'fromAddress',
        { bitcoinAddressNetworks: config.bitcoinAddressNetworks }
      )
      assertAddressMatchesChain(
        quote.source_asset.chain,
        request.refundAddress ?? request.fromAddress,
        'refundAddress',
        { bitcoinAddressNetworks: config.bitcoinAddressNetworks }
      )
      assertAddressMatchesChain(
        quote.destination_asset.chain,
        request.toAddress,
        'toAddress',
        { bitcoinAddressNetworks: config.bitcoinAddressNetworks }
      )

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
        idempotency_key: request.idempotencyKey,
        metadata: {
          ...(request.integrator ? { integrator: request.integrator } : {}),
          from_address: request.fromAddress,
          to_address: request.toAddress,
          gateway: 'router-gateway'
        }
      })

      if (!envelope.cancellation_secret) {
        throw new UpstreamHttpError(
          502,
          'internal router API returned order response without cancellation secret',
          undefined
        )
      }

      let response!: ReturnType<typeof presentOrderEnvelope>
      try {
        response = presentOrderEnvelope(envelope, amountFormat)
      } catch (presentationError) {
        await cancelUnpersistedGatewayOrder(
          routerClient,
          envelope.order.id,
          envelope.cancellation_secret
        )
        throw malformedOrderPresentationError(presentationError)
      }

      let refundAuthorization
      try {
        refundAuthorization = await refundAuthorizationServiceFor(
          config,
          deps
        ).createRefundAuthorization({
          routerOrderId: envelope.order.id,
          cancellationSecret: envelope.cancellation_secret,
          refundMode,
          refundAuthorizer: request.refundAuthorizer ?? null
        })
      } catch (authorizationError) {
        if (!(authorizationError instanceof GatewayConflictError)) {
          await cancelUnpersistedGatewayOrder(
            routerClient,
            envelope.order.id,
            envelope.cancellation_secret
          )
        }
        throw authorizationError
      }

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
      const cancellationAuthorization = request.refundToken
        ? await refundAuthorizationService.resolveTokenCancellationSecret(
            orderId,
            request.refundToken
          )
        : await refundAuthorizationService.resolveEvmSignatureCancellationSecret({
            routerOrderId: orderId,
            refundSignature: request.refundSignature as string,
            refundSignatureDeadline: request.refundSignatureDeadline as number
          })
      let envelope
      try {
        envelope = await routerClientFor(config, deps).cancelOrder(orderId, {
          cancellation_secret: cancellationAuthorization.cancellationSecret
        })
      } catch (error) {
        await refundAuthorizationService
          .releaseCancellationAttempt(orderId, cancellationAuthorization.claimId)
          .catch((releaseError) => {
            logError(
              'router gateway failed to release cancellation authorization claim',
              releaseError
            )
          })
        throw error
      }
      await refundAuthorizationService.markCancellationForwarded(
        orderId,
        cancellationAuthorization.claimId
      )

      let response!: ReturnType<typeof presentOrderEnvelope>
      try {
        response = presentOrderEnvelope(envelope, amountFormat)
      } catch (presentationError) {
        throw malformedOrderPresentationError(presentationError)
      }

      return c.json(response, 200)
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

async function cancelUnpersistedGatewayOrder(
  routerClient: ReturnType<typeof routerClientFor>,
  orderId: string,
  cancellationSecret: string
) {
  try {
    await routerClient.cancelOrder(orderId, {
      cancellation_secret: cancellationSecret
    })
  } catch (cleanupError) {
    logError(
      'router gateway failed to cancel order after refund authorization persistence failed',
      cleanupError
    )
    throw new Error(
      'failed to persist refund authorization and failed to cancel newly-created order'
    )
  }
}

function malformedOrderPresentationError(error: unknown) {
  if (error instanceof GatewayValidationError) {
    return new UpstreamHttpError(
      502,
      'internal router API returned malformed order response',
      { malformed: true, reason: error.message }
    )
  }
  return error
}

import { createRoute, z } from '@hono/zod-openapi'

import {
  assertAddressMatchesChain,
  parseAmount,
  resolveAssetIdentifier,
  type AmountFormat,
  type ResolvedAsset
} from '../assets'
import type { GatewayConfig } from '../config'
import {
  GatewayConflictError,
  GatewayValidationError,
  UpstreamHttpError,
  normalizeError
} from '../errors'
import {
  limitQuoteFromEnvelope,
  presentOrderEnvelope,
  routerQuoteFromEnvelope
} from '../presenters'
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
  AssetIdentifierSchema,
  AmountStringSchema,
  IdempotencyKeySchema,
  IntegratorSchema,
  OrderCancellationResponseSchema,
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

export const OrderLimitRequestSchema = z
  .object({
    from: AssetIdentifierSchema.openapi({
      example: 'Bitcoin.BTC'
    }),
    to: AssetIdentifierSchema.openapi({
      example: 'Ethereum.USDC'
    }),
    amountFormat: AmountFormatSchema.optional(),
    fromAddress: AddressSchema.openapi({
      example: 'bc1qexample000000000000000000000000000000'
    }),
    toAddress: AddressSchema.openapi({
      example: '0x1111111111111111111111111111111111111111'
    }),
    fromAmount: AmountStringSchema.optional().openapi({
      description:
        'Source amount. Provide exactly two of fromAmount, toAmount, and price; Rift computes the omitted value from the other two.',
      example: '10'
    }),
    toAmount: AmountStringSchema.optional().openapi({
      description:
        'Destination amount. Provide exactly two of fromAmount, toAmount, and price; Rift computes the omitted value from the other two.',
      example: '1000000'
    }),
    price: AmountStringSchema.optional().openapi({
      description:
        'Readable destination asset amount per one readable source asset. For Bitcoin.BTC to Ethereum.USDC, "100000" means 100,000 USDC per 1 BTC. Provide exactly two of fromAmount, toAmount, and price; Rift computes the omitted value from the other two.',
      example: '100000'
    }),
    expiration: DateTimeSchema.optional().openapi({
      example: '2026-05-04T13:00:00Z'
    }),
    refundAddress: AddressSchema.optional().openapi({
      example: 'bc1qrefund0000000000000000000000000000000'
    }),
    refundMode: RefundModeSchema.optional(),
    refundAuthorizer: AddressSchema.nullable().openapi({
      example: '0x2222222222222222222222222222222222222222'
    }),
    integrator: IntegratorSchema.optional().openapi({
      example: 'partner-a'
    }),
    idempotencyKey: IdempotencyKeySchema.openapi({
      example: 'partner-a-limit-order-0001'
    })
  })
  .strict()
  .superRefine((value, ctx) => {
    const amountTerms = [value.fromAmount, value.toAmount, value.price].filter(
      (term) => term !== undefined
    )
    if (amountTerms.length !== 2) {
      ctx.addIssue({
        code: 'custom',
        message: 'exactly two of fromAmount, toAmount, and price are required',
        path: ['fromAmount']
      })
    }

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
  .openapi('OrderLimitRequest', {
    description:
      'Limit orders require exactly two of fromAmount, toAmount, and price. Price is a readable destination-per-source ratio; for example, Bitcoin.BTC to Ethereum.USDC with price "100000" means 100,000 USDC per 1 BTC. amountFormat applies to fromAmount and toAmount, not to price.'
  })

export const orderLimitRoute = createRoute({
  method: 'post',
  path: '/order/limit',
  tags: ['Orders'],
  summary: 'Create a limit order',
  request: {
    body: {
      required: true,
      content: {
        'application/json': {
          schema: OrderLimitRequestSchema
        }
      }
    }
  },
  responses: {
    201: {
      description: 'Limit order created by the internal router API.',
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
      orderId: z.string().uuid().openapi({
        example: '00000000-0000-4000-8000-000000000002'
      })
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
          schema: OrderCancellationResponseSchema
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

export function createOrderLimitHandler(
  config: GatewayConfig,
  deps: GatewayDeps = {}
) {
  return async (c: any) => {
    try {
      const request = c.req.valid('json')
      const amountFormat: AmountFormat = request.amountFormat ?? 'readable'
      const refundMode = request.refundMode ?? 'evmSignature'
      const fromAsset = resolveAssetIdentifier(request.from)
      const toAsset = resolveAssetIdentifier(request.to)
      assertAddressMatchesChain(
        fromAsset.internal.chain,
        request.fromAddress,
        'fromAddress',
        { bitcoinAddressNetworks: config.bitcoinAddressNetworks }
      )
      assertAddressMatchesChain(
        fromAsset.internal.chain,
        request.refundAddress ?? request.fromAddress,
        'refundAddress',
        { bitcoinAddressNetworks: config.bitcoinAddressNetworks }
      )
      assertAddressMatchesChain(
        toAsset.internal.chain,
        request.toAddress,
        'toAddress',
        { bitcoinAddressNetworks: config.bitcoinAddressNetworks }
      )
      const { inputAmount, outputAmount } = limitOrderAmounts({
        fromAmount: request.fromAmount,
        toAmount: request.toAmount,
        price: request.price,
        fromAsset,
        toAsset,
        amountFormat
      })

      const routerClient = routerClientFor(config, deps)
      const quoteEnvelope = await routerClient.createQuote({
        type: 'limit_order',
        from_asset: fromAsset.internal,
        to_asset: toAsset.internal,
        recipient_address: request.toAddress,
        input_amount: inputAmount,
        output_amount: outputAmount
      })

      const quoteId = limitQuoteFromEnvelope(quoteEnvelope).id
      const envelope = await routerClient.createOrder({
        quote_id: quoteId,
        refund_address: request.refundAddress ?? request.fromAddress,
        ...(request.expiration ? { cancel_after: request.expiration } : {}),
        idempotency_key: request.idempotencyKey,
        metadata: {
          ...(request.integrator ? { integrator: request.integrator } : {}),
          from_address: request.fromAddress,
          to_address: request.toAddress,
          gateway: 'router-gateway',
          order_type: 'limit_order'
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

type LimitOrderAmountInput = {
  fromAmount?: string
  toAmount?: string
  price?: string
  fromAsset: ResolvedAsset
  toAsset: ResolvedAsset
  amountFormat: AmountFormat
}

function limitOrderAmounts(input: LimitOrderAmountInput): {
  inputAmount: string
  outputAmount: string
} {
  if (input.fromAmount !== undefined && input.toAmount !== undefined) {
    return {
      inputAmount: parseAmount(
        input.fromAmount,
        input.fromAsset,
        input.amountFormat,
        'fromAmount'
      ),
      outputAmount: parseAmount(
        input.toAmount,
        input.toAsset,
        input.amountFormat,
        'toAmount'
      )
    }
  }

  if (input.fromAmount !== undefined && input.price !== undefined) {
    const inputAmount = parseAmount(
      input.fromAmount,
      input.fromAsset,
      input.amountFormat,
      'fromAmount'
    )
    return {
      inputAmount,
      outputAmount: rawOutputFromReadablePrice(
        inputAmount,
        input.fromAsset,
        input.toAsset,
        input.price
      )
    }
  }

  if (input.toAmount !== undefined && input.price !== undefined) {
    const outputAmount = parseAmount(
      input.toAmount,
      input.toAsset,
      input.amountFormat,
      'toAmount'
    )
    return {
      inputAmount: rawInputFromReadablePrice(
        outputAmount,
        input.fromAsset,
        input.toAsset,
        input.price
      ),
      outputAmount
    }
  }

  throw new GatewayValidationError('unsupported limit order amount shape')
}

const DECIMAL_STRING_PATTERN = /^[0-9]+(?:\.[0-9]+)?$/

function rawOutputFromReadablePrice(
  inputAmount: string,
  fromAsset: ResolvedAsset,
  toAsset: ResolvedAsset,
  price: string
): string {
  const fromDecimals = knownDecimals(fromAsset, 'price')
  const toDecimals = knownDecimals(toAsset, 'price')
  const decimal = parsePositiveDecimal(price, 'price')
  const numerator =
    BigInt(inputAmount) * decimal.integer * pow10(toDecimals)
  const denominator = pow10(fromDecimals + decimal.scale)
  const raw = numerator / denominator
  assertPositive(raw, 'price')
  return raw.toString()
}

function rawInputFromReadablePrice(
  outputAmount: string,
  fromAsset: ResolvedAsset,
  toAsset: ResolvedAsset,
  price: string
): string {
  const fromDecimals = knownDecimals(fromAsset, 'price')
  const toDecimals = knownDecimals(toAsset, 'price')
  const decimal = parsePositiveDecimal(price, 'price')
  const numerator =
    BigInt(outputAmount) * pow10(fromDecimals + decimal.scale)
  const denominator = decimal.integer * pow10(toDecimals)
  const raw = ceilDiv(numerator, denominator)
  assertPositive(raw, 'price')
  return raw.toString()
}

function parsePositiveDecimal(value: string, field: string): {
  integer: bigint
  scale: number
} {
  if (!DECIMAL_STRING_PATTERN.test(value)) {
    throw new GatewayValidationError(
      `${field} must be a decimal string without separators`,
      { field }
    )
  }

  const [whole, fractional = ''] = value.split('.')
  if (fractional.length > 18 || value.length > 128) {
    throw new GatewayValidationError(
      `${field} supports at most 18 decimal places and 128 characters`,
      { field }
    )
  }

  const integer = BigInt(`${whole}${fractional}`.replace(/^0+(?=\d)/, ''))
  if (integer <= 0n) {
    throw new GatewayValidationError(`${field} must be greater than zero`, {
      field
    })
  }

  return {
    integer,
    scale: fractional.length
  }
}

function knownDecimals(asset: ResolvedAsset, field: string): number {
  if (asset.decimals !== undefined) return asset.decimals
  throw new GatewayValidationError(
    `${field} conversion requires known decimals for ${asset.id}; provide fromAmount and toAmount with amountFormat raw`,
    { field, asset: asset.id }
  )
}

function pow10(exponent: number): bigint {
  return 10n ** BigInt(exponent)
}

function ceilDiv(numerator: bigint, denominator: bigint): bigint {
  return (numerator + denominator - 1n) / denominator
}

function assertPositive(value: bigint, field: string) {
  if (value <= 0n) {
    throw new GatewayValidationError(`${field} resolves to a zero-sized amount`, {
      field
    })
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

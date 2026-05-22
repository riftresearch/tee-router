import { z } from '@hono/zod-openapi'

export const MAX_ASSET_IDENTIFIER_LENGTH = 96
export const MAX_ADDRESS_LENGTH = 128
export const MAX_AMOUNT_LENGTH = 96
export const MAX_INTEGRATOR_LENGTH = 128
export const MIN_IDEMPOTENCY_KEY_LENGTH = 16
export const MAX_IDEMPOTENCY_KEY_LENGTH = 128
export const MAX_DATETIME_LENGTH = 64
const IDEMPOTENCY_KEY_PATTERN = /^[A-Za-z0-9._:-]+$/

export const AssetIdentifierSchema = z.string().min(1).max(MAX_ASSET_IDENTIFIER_LENGTH)
export const AddressSchema = z.string().min(1).max(MAX_ADDRESS_LENGTH)
export const AmountStringSchema = z.string().min(1).max(MAX_AMOUNT_LENGTH)
export const IntegratorSchema = z.string().min(1).max(MAX_INTEGRATOR_LENGTH)
export const IdempotencyKeySchema = z
  .string()
  .min(MIN_IDEMPOTENCY_KEY_LENGTH)
  .max(MAX_IDEMPOTENCY_KEY_LENGTH)
  .regex(
    IDEMPOTENCY_KEY_PATTERN,
    'idempotencyKey must contain only letters, numbers, dot, underscore, colon, or hyphen'
  )
export const DateTimeSchema = z.string().max(MAX_DATETIME_LENGTH).datetime()
export const EvmSignatureSchema = z
  .string()
  .regex(/^0x[a-fA-F0-9]{130}$/)

export const AmountFormatSchema = z
  .enum(['readable', 'raw'])
  .default('readable')
  .openapi('AmountFormat', {
    example: 'readable'
  })

export const ErrorResponseSchema = z
  .object({
    error: z.object({
      code: z.string().openapi({
        example: 'invalid_request'
      }),
      message: z.string().openapi({
        example: 'Invalid gateway request.'
      }),
      details: z.unknown().optional().openapi({
        example: {
          field: 'fromAmount'
        }
      })
    })
  })
  .openapi('ErrorResponse')

export const QuoteResponseSchema = z
  .object({
    quoteId: z.string().openapi({
      example: '00000000-0000-4000-8000-000000000001'
    }),
    orderType: z.enum([
      'market_order',
      // Limit-order response shapes are intentionally not advertised while
      // limit-order creation is disabled.
      // 'limit_order'
    ]).openapi({
      example: 'market_order'
    }),
    from: z.string().openapi({
      example: 'Bitcoin.BTC'
    }),
    to: z.string().openapi({
      example: 'Ethereum.USDC'
    }),
    expiry: z.string().openapi({
      example: '2026-05-04T12:10:00Z'
    }),
    estimatedOut: z.string().openapi({
      example: '100000'
    }),
    fees: z
      .array(
        z.object({
          kind: z.string().openapi({
            example: 'hyperliquid_core_activation_fee'
          }),
          label: z.string().openapi({
            example: 'Hyperliquid activation fee'
          }),
          asset: z.string().openapi({
            example: 'Hyperliquid.USDC'
          }),
          amount: z.string().openapi({
            example: '1'
          }),
          amountRaw: z.string().openapi({
            example: '1000000'
          })
        })
      )
      .optional(),
    amountFormat: AmountFormatSchema
  })
  .openapi('QuoteResponse')

export const OrderResponseSchema = z
  .object({
    orderId: z.string().openapi({
      example: '00000000-0000-4000-8000-000000000002'
    }),
    orderAddress: z.string().openapi({
      example: 'bc1qorderaddress0000000000000000000000000'
    }),
    amountToSend: z.string().openapi({
      example: '10'
    }),
    quoteId: z.string().openapi({
      example: '00000000-0000-4000-8000-000000000001'
    }),
    orderType: z.enum([
      'market_order',
      // Limit-order response shapes are intentionally not advertised while
      // limit-order creation is disabled.
      // 'limit_order'
    ]).openapi({
      example: 'market_order'
    }),
    from: z.string().openapi({
      example: 'Bitcoin.BTC'
    }),
    to: z.string().openapi({
      example: 'Ethereum.USDC'
    }),
    status: z.string().openapi({
      example: 'quoted'
    }),
    expiry: z.string().openapi({
      example: '2026-05-04T12:10:00Z'
    }),
    estimatedOut: z.string().openapi({
      example: '100000'
    }),
    fees: z
      .array(
        z.object({
          kind: z.string().openapi({
            example: 'hyperliquid_core_activation_fee'
          }),
          label: z.string().openapi({
            example: 'Hyperliquid activation fee'
          }),
          asset: z.string().openapi({
            example: 'Hyperliquid.USDC'
          }),
          amount: z.string().openapi({
            example: '1'
          }),
          amountRaw: z.string().openapi({
            example: '1000000'
          })
        })
      )
      .optional(),
    amountFormat: AmountFormatSchema
  })
  .openapi('OrderResponse')

export const ErrorResponses = {
  400: {
    description: 'Invalid gateway request.',
    content: {
      'application/json': {
        schema: ErrorResponseSchema
      }
    }
  },
  401: {
    description: 'Gateway authentication failed.',
    content: {
      'application/json': {
        schema: ErrorResponseSchema
      }
    }
  },
  409: {
    description: 'Gateway request conflicts with an in-progress or completed operation.',
    content: {
      'application/json': {
        schema: ErrorResponseSchema
      }
    }
  },
  413: {
    description: 'Gateway request body is too large.',
    content: {
      'application/json': {
        schema: ErrorResponseSchema
      }
    }
  },
  500: {
    description: 'Unexpected gateway error.',
    content: {
      'application/json': {
        schema: ErrorResponseSchema
      }
    }
  },
  502: {
    description: 'Internal router API error.',
    content: {
      'application/json': {
        schema: ErrorResponseSchema
      }
    }
  },
  504: {
    description: 'Internal router API timed out.',
    content: {
      'application/json': {
        schema: ErrorResponseSchema
      }
    }
  },
  503: {
    description: 'Gateway is not configured for this operation.',
    content: {
      'application/json': {
        schema: ErrorResponseSchema
      }
    }
  }
} as const

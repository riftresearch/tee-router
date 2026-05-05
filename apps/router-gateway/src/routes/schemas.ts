import { z } from '@hono/zod-openapi'

export const MAX_ASSET_IDENTIFIER_LENGTH = 96
export const MAX_ADDRESS_LENGTH = 128
export const MAX_AMOUNT_LENGTH = 96
export const MAX_SLIPPAGE_LENGTH = 16
export const MAX_INTEGRATOR_LENGTH = 128
export const MIN_IDEMPOTENCY_KEY_LENGTH = 16
export const MAX_IDEMPOTENCY_KEY_LENGTH = 128
export const MAX_REFUND_TOKEN_LENGTH = 128
export const MAX_DATETIME_LENGTH = 64
const IDEMPOTENCY_KEY_PATTERN = /^[A-Za-z0-9._:-]+$/
const REFUND_TOKEN_PATTERN = /^[A-Za-z0-9_-]+$/

export const AssetIdentifierSchema = z.string().min(1).max(MAX_ASSET_IDENTIFIER_LENGTH)
export const AddressSchema = z.string().min(1).max(MAX_ADDRESS_LENGTH)
export const AmountStringSchema = z.string().min(1).max(MAX_AMOUNT_LENGTH)
export const SlippageStringSchema = z.string().min(1).max(MAX_SLIPPAGE_LENGTH)
export const IntegratorSchema = z.string().min(1).max(MAX_INTEGRATOR_LENGTH)
export const IdempotencyKeySchema = z
  .string()
  .min(MIN_IDEMPOTENCY_KEY_LENGTH)
  .max(MAX_IDEMPOTENCY_KEY_LENGTH)
  .regex(
    IDEMPOTENCY_KEY_PATTERN,
    'idempotencyKey must contain only letters, numbers, dot, underscore, colon, or hyphen'
  )
export const RefundTokenSchema = z
  .string()
  .min(1)
  .max(MAX_REFUND_TOKEN_LENGTH)
  .regex(REFUND_TOKEN_PATTERN, 'refundToken must be base64url text')
export const DateTimeSchema = z.string().max(MAX_DATETIME_LENGTH).datetime()
export const EvmSignatureSchema = z
  .string()
  .regex(/^0x[a-fA-F0-9]{130}$/)

export const AmountFormatSchema = z
  .enum(['readable', 'raw'])
  .default('readable')
  .openapi('AmountFormat')

export const RefundModeSchema = z
  .enum(['evmSignature', 'token'])
  .default('evmSignature')
  .openapi('RefundMode')

export const ErrorResponseSchema = z
  .object({
    error: z.object({
      code: z.string(),
      message: z.string(),
      details: z.unknown().optional()
    })
  })
  .openapi('ErrorResponse')

export const QuoteResponseSchema = z
  .object({
    quoteId: z.string(),
    orderType: z.enum(['market_order', 'limit_order']),
    from: z.string(),
    to: z.string(),
    expiry: z.string(),
    expectedOut: z.string(),
    expectedSlippage: z.string().optional(),
    minOut: z.string().optional(),
    maxIn: z.string().optional(),
    maxSlippage: z.string(),
    amountFormat: AmountFormatSchema
  })
  .openapi('QuoteResponse')

export const OrderResponseSchema = z
  .object({
    orderId: z.string(),
    orderAddress: z.string(),
    amountToSend: z.string(),
    quoteId: z.string(),
    orderType: z.enum(['market_order', 'limit_order']),
    from: z.string(),
    to: z.string(),
    status: z.string(),
    expiry: z.string(),
    expectedOut: z.string(),
    expectedSlippage: z.string().optional(),
    minOut: z.string().optional(),
    maxIn: z.string().optional(),
    maxSlippage: z.string(),
    amountFormat: AmountFormatSchema,
    refundMode: RefundModeSchema.optional(),
    refundAuthorizer: z.string().nullable().optional(),
    refundToken: z.string().optional()
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

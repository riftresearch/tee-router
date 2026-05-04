import { z } from '@hono/zod-openapi'

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

export const LimitOrderResponseSchema = z
  .object({
    orderId: z.string(),
    orderAddress: z.string(),
    amountToSend: z.string(),
    quoteId: z.string(),
    from: z.string(),
    to: z.string(),
    status: z.string(),
    expiry: z.string(),
    minOut: z.string(),
    amountFormat: AmountFormatSchema,
    refundMode: RefundModeSchema.optional(),
    refundAuthorizer: z.string().nullable().optional(),
    refundToken: z.string().optional()
  })
  .openapi('LimitOrderResponse')

export const OrderCancellationResponseSchema = z
  .union([OrderResponseSchema, LimitOrderResponseSchema])
  .openapi('OrderCancellationResponse')

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
  503: {
    description: 'Gateway is not configured for this operation.',
    content: {
      'application/json': {
        schema: ErrorResponseSchema
      }
    }
  }
} as const

import { z } from '@hono/zod-openapi'

export const AmountFormatSchema = z
  .enum(['readable', 'raw'])
  .default('readable')
  .openapi('AmountFormat')

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
    cancellationMode: z
      .enum(['user_secret', 'gateway_managed_token'])
      .optional(),
    cancellationSecret: z.string().optional(),
    cancellationToken: z.string().optional()
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

export const CancellationOwnershipSchema = z
  .discriminatedUnion('mode', [
    z.object({
      mode: z.literal('user_secret')
    }),
    z.object({
      mode: z.literal('gateway_managed_token')
    })
  ])
  .openapi('CancellationOwnership')

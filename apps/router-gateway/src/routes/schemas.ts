import { z } from '@hono/zod-openapi'

export const AmountFormatSchema = z
  .enum(['readable', 'raw'])
  .default('readable')
  .openapi('AmountFormat', {
    example: 'readable'
  })

export const RefundModeSchema = z
  .enum(['evmSignature', 'token'])
  .default('evmSignature')
  .openapi('RefundMode', {
    example: 'evmSignature'
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
    from: z.string().openapi({
      example: 'Bitcoin.BTC'
    }),
    to: z.string().openapi({
      example: 'Ethereum.USDC'
    }),
    expiry: z.string().openapi({
      example: '2026-05-04T12:10:00Z'
    }),
    expectedOut: z.string().openapi({
      example: '100000'
    }),
    expectedSlippage: z.string().optional().openapi({
      example: '0.2'
    }),
    minOut: z.string().optional().openapi({
      example: '99000'
    }),
    maxIn: z.string().optional().openapi({
      example: '10.1'
    }),
    maxSlippage: z.string().openapi({
      example: '1.5'
    }),
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
    expectedOut: z.string().openapi({
      example: '100000'
    }),
    expectedSlippage: z.string().optional().openapi({
      example: '0.2'
    }),
    minOut: z.string().optional().openapi({
      example: '99000'
    }),
    maxIn: z.string().optional().openapi({
      example: '10.1'
    }),
    maxSlippage: z.string().openapi({
      example: '1.5'
    }),
    amountFormat: AmountFormatSchema,
    refundMode: RefundModeSchema.optional(),
    refundAuthorizer: z.string().nullable().optional().openapi({
      example: '0x2222222222222222222222222222222222222222'
    }),
    refundToken: z.string().optional().openapi({
      example: 'rgt_abcdefghijklmnopqrstuvwxyz'
    })
  })
  .openapi('OrderResponse')

export const LimitOrderResponseSchema = z
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
      example: '00000000-0000-4000-8000-000000000003'
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
    minOut: z.string().openapi({
      example: '100000'
    }),
    amountFormat: AmountFormatSchema,
    refundMode: RefundModeSchema.optional(),
    refundAuthorizer: z.string().nullable().optional().openapi({
      example: '0x2222222222222222222222222222222222222222'
    }),
    refundToken: z.string().optional().openapi({
      example: 'rgt_abcdefghijklmnopqrstuvwxyz'
    })
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

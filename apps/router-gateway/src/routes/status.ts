import { createRoute, z } from '@hono/zod-openapi'
import type { RouteHandler } from '@hono/zod-openapi'

import type { GatewayConfig } from '../config'

export const StatusResponseSchema = z
  .object({
    status: z.literal('ok'),
    service: z.literal('router-gateway'),
    version: z.string(),
    upstream: z.object({
      configured: z.boolean(),
      baseUrl: z.string().optional(),
      queryApiConfigured: z.boolean(),
      queryApiBaseUrl: z.string().optional()
    })
  })
  .openapi('StatusResponse')

export const statusRoute = createRoute({
  method: 'get',
  path: '/status',
  tags: ['System'],
  summary: 'Gateway status',
  responses: {
    200: {
      description: 'Gateway status and upstream configuration state.',
      content: {
        'application/json': {
          schema: StatusResponseSchema
        }
      }
    }
  }
})

export function createStatusHandler(
  config: GatewayConfig
): RouteHandler<typeof statusRoute> {
  return (c) =>
    c.json(
      {
        status: 'ok',
        service: 'router-gateway',
        version: config.version,
        upstream: {
          configured: Boolean(config.routerInternalBaseUrl),
          ...(config.routerInternalBaseUrl
            ? { baseUrl: config.routerInternalBaseUrl }
            : {}),
          queryApiConfigured: Boolean(config.routerQueryApiBaseUrl),
          ...(config.routerQueryApiBaseUrl
            ? { queryApiBaseUrl: config.routerQueryApiBaseUrl }
            : {})
        }
      },
      200
    )
}

import { createRoute, z } from '@hono/zod-openapi'
import type { RouteHandler } from '@hono/zod-openapi'

import type { DependencyHealthMonitor } from '../health'

export const HealthResponseSchema = z
  .object({
    status: z.literal('ok'),
    timestamp: z.string().datetime()
  })
  .openapi('HealthResponse')

export const DependencyHealthResponseSchema = z
  .object({
    status: z.enum(['ok', 'degraded']),
    timestamp: z.string().datetime(),
    dependencies: z.array(
      z.object({
        name: z.string(),
        status: z.enum(['reachable', 'unreachable', 'unknown']),
        checkedAt: z.string().datetime().optional()
      })
    )
  })
  .openapi('DependencyHealthResponse')

export const healthRoute = createRoute({
  method: 'get',
  path: '/health',
  tags: ['Status'],
  summary: 'Get Rift online status',
  description: 'Returns the online status of the Rift API.',
  responses: {
    200: {
      description: 'Rift online status response',
      content: {
        'application/json': {
          schema: HealthResponseSchema
        }
      }
    }
  }
})

export const dependencyHealthRoute = createRoute({
  method: 'get',
  path: '/providers',
  tags: ['Status'],
  summary: 'Get execution provider online status',
  description:
    'Returns cached execution provider online checks from the router worker.',
  responses: {
    200: {
      description: 'Cached execution provider online status response',
      content: {
        'application/json': {
          schema: DependencyHealthResponseSchema
        }
      }
    }
  }
})

export function createHealthHandler(): RouteHandler<typeof healthRoute> {
  return (c) =>
    c.json(
      {
        status: 'ok',
        timestamp: new Date().toISOString()
      },
      200
    )
}

export function createDependencyHealthHandler(
  monitor: DependencyHealthMonitor
): RouteHandler<typeof dependencyHealthRoute> {
  return (c) =>
    c.json(monitor.snapshot(), 200)
}

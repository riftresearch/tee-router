import { OpenAPIHono } from '@hono/zod-openapi'
import { bodyLimit } from 'hono/body-limit'
import { cors } from 'hono/cors'
import { HTTPException } from 'hono/http-exception'

import { type GatewayConfig, loadConfig } from './config'
import { errorBody } from './errors'
import {
  createOrderCancelHandler,
  createOrderMarketHandler,
  orderCancelRoute,
  orderMarketRoute
} from './routes/order'
import { type GatewayDeps } from './routes/deps'
import { createDependencyHealthMonitor } from './health'
import {
  createDependencyHealthHandler,
  createHealthHandler,
  dependencyHealthRoute,
  healthRoute
} from './routes/health'
import { createQuoteHandler, quoteRoute } from './routes/quote'
import { logError } from './internal/logging'

export const MAX_GATEWAY_JSON_BODY_BYTES = 16 * 1024

export function createApp(
  config: GatewayConfig = loadConfig(),
  deps: GatewayDeps = {}
) {
  const app = new OpenAPIHono({
    defaultHook: (result, c) => {
      if (result.success) return

      const issues = result.error.issues.map((issue) => ({
        path: issue.path.map(String).join('.'),
        message: issue.message
      }))
      return c.json(
        errorBody(
          'VALIDATION_ERROR',
          issues[0]?.message ?? 'Invalid gateway request',
          {
            target: result.target,
            issues
          }
        ),
        400
      )
    }
  })
  const dependencyHealthMonitor =
    deps.dependencyHealthMonitor ?? createDependencyHealthMonitor(config, deps.fetch)

  app.onError((error, c) => {
    if (error instanceof HTTPException && error.status === 400) {
      return c.json(errorBody('VALIDATION_ERROR', error.message), 400)
    }

    logError('router gateway uncaught request error', error)
    return c.json(errorBody('BAD_GATEWAY', 'Unexpected gateway error'), 500)
  })

  app.use(
    '*',
    cors({
      origin: '*',
      allowMethods: ['GET', 'POST', 'OPTIONS'],
      allowHeaders: ['*'],
      exposeHeaders: ['*']
    })
  )
  app.use(
    '*',
    bodyLimit({
      maxSize: MAX_GATEWAY_JSON_BODY_BYTES,
      onError: (c) =>
        c.json(
          errorBody(
            'PAYLOAD_TOO_LARGE',
            `request body must be at most ${MAX_GATEWAY_JSON_BODY_BYTES} bytes`
          ),
          413
        )
    })
  )

  // zod-openapi currently narrows handler return types to success responses.
  // These handlers also return the error responses declared on each route.
  app.openapi(quoteRoute, createQuoteHandler(config, deps) as never)
  app.openapi(orderMarketRoute, createOrderMarketHandler(config, deps) as never)
  app.openapi(orderCancelRoute, createOrderCancelHandler(config, deps) as never)
  app.openapi(healthRoute, createHealthHandler())
  app.openapi(
    dependencyHealthRoute,
    createDependencyHealthHandler(dependencyHealthMonitor)
  )

  app.doc31('/openapi.json', {
    openapi: '3.1.0',
    info: {
      title: 'TEE Router Gateway API',
      version: config.version,
      description:
        'Public TypeScript API gateway for quote and order access to the TEE router protocol.'
    },
    servers: [
      {
        url: config.publicBaseUrl ?? `http://localhost:${config.port}`,
        description: config.publicBaseUrl ? 'Production' : 'Local development'
      }
    ]
  })

  return app
}

export type RouterGatewayApp = ReturnType<typeof createApp>

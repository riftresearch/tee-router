import { OpenAPIHono } from '@hono/zod-openapi'
import { cors } from 'hono/cors'

import { type GatewayConfig, loadConfig } from './config'
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

export function createApp(
  config: GatewayConfig = loadConfig(),
  deps: GatewayDeps = {}
) {
  const app = new OpenAPIHono()
  const dependencyHealthMonitor =
    deps.dependencyHealthMonitor ?? createDependencyHealthMonitor(config, deps.fetch)

  app.use(
    '*',
    cors({
      origin: '*',
      allowMethods: ['GET', 'POST', 'OPTIONS'],
      allowHeaders: ['*'],
      exposeHeaders: ['*']
    })
  )

  app.openapi(healthRoute, createHealthHandler())
  app.openapi(
    dependencyHealthRoute,
    createDependencyHealthHandler(dependencyHealthMonitor)
  )
  // zod-openapi currently narrows handler return types to success responses.
  // These handlers also return the error responses declared on each route.
  app.openapi(quoteRoute, createQuoteHandler(config, deps) as never)
  app.openapi(orderMarketRoute, createOrderMarketHandler(config, deps) as never)
  app.openapi(orderCancelRoute, createOrderCancelHandler(config, deps) as never)

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

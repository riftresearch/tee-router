import { OpenAPIHono } from '@hono/zod-openapi'

import { type GatewayConfig, loadConfig } from './config'
import { createStatusHandler, statusRoute } from './routes/status'

export function createApp(config: GatewayConfig = loadConfig()) {
  const app = new OpenAPIHono()

  app.openapi(statusRoute, createStatusHandler(config))

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
        url: `http://localhost:${config.port}`,
        description: 'Local development'
      }
    ]
  })

  return app
}

export type RouterGatewayApp = ReturnType<typeof createApp>

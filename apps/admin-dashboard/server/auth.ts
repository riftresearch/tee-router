import { betterAuth } from 'better-auth'
import { APIError } from 'better-auth/api'
import { Pool } from 'pg'

import {
  type AdminDashboardConfig,
  isAllowedAdminEmail,
  normalizeAdminEmail
} from './config'

export type DashboardAuthSession = {
  user: {
    id: string
    name: string
    email: string
    emailVerified: boolean
    image?: string | null
  }
}

export type DashboardAuth = {
  handler: (request: Request) => Response | Promise<Response>
  api: {
    getSession: (input: { headers: Headers }) => Promise<DashboardAuthSession | null>
  }
  options: unknown
}

export type DashboardAuthRuntime = {
  auth: DashboardAuth
  close: () => Promise<void>
}

export function createDashboardAuth(
  config: AdminDashboardConfig
): DashboardAuthRuntime | null {
  if (
    !config.authDatabaseUrl ||
    !config.googleClientId ||
    !config.googleClientSecret ||
    !config.betterAuthSecret
  ) {
    return null
  }

  const pool = new Pool({
    connectionString: config.authDatabaseUrl,
    max: 5,
    application_name: 'rift-admin-dashboard-auth'
  })

  const auth = betterAuth({
    appName: 'Rift Admin Dashboard',
    baseURL: config.authBaseUrl,
    trustedOrigins: config.trustedOrigins,
    secret: config.betterAuthSecret,
    database: pool,
    socialProviders: {
      google: {
        clientId: config.googleClientId,
        clientSecret: config.googleClientSecret
      }
    },
    databaseHooks: {
      user: {
        create: {
          before: async (user) => {
            const email = normalizeAdminEmail(user.email)
            if (!isAllowedAdminEmail(email)) {
              throw new APIError('FORBIDDEN', {
                message: 'This Google account is not allowed to access Rift admin.'
              })
            }

            return {
              data: user
            }
          }
        }
      }
    },
    advanced: {
      cookiePrefix: 'rift-admin',
      useSecureCookies: config.secureCookies,
      defaultCookieAttributes: {
        httpOnly: true,
        secure: config.secureCookies,
        sameSite: 'lax'
      }
    },
    rateLimit: {
      enabled: true
    }
  }) as DashboardAuth

  return {
    auth,
    close: () => pool.end()
  }
}

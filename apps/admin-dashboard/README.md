# Rift Admin Dashboard

Vite + React dashboard for tee-router order operations. The backend owns auth,
the router admin key, and read-only replica access. The browser never receives
`ROUTER_ADMIN_API_KEY`.

## Required Runtime Variables

```env
ADMIN_DASHBOARD_PRODUCTION=true
ADMIN_DASHBOARD_AUTH_DATABASE_URL=postgres://...
ADMIN_DASHBOARD_REPLICA_DATABASE_URL=postgres://...
BETTER_AUTH_SECRET=...
BETTER_AUTH_URL=https://admin.example.com
GOOGLE_CLIENT_ID=...
GOOGLE_CLIENT_SECRET=...
ROUTER_ADMIN_API_KEY=...
ADMIN_DASHBOARD_CDC_SLOT_NAME=admin_dashboard_orders_cdc
ROUTER_CDC_PUBLICATION_NAME=router_cdc_publication
ROUTER_CDC_MESSAGE_PREFIX=rift.router.change
```

`ADMIN_DASHBOARD_REPLICA_DATABASE_URL` may be replaced with
`ROUTER_REPLICA_DATABASE_URL`. Do not point either value at the Phala primary.

The auth database must be writable because Better Auth stores users, sessions,
OAuth state, and PKCE there. The replica database is read by normal queries and
by one `pgoutput` CDC decoding stream. Router migrations install the
`pg_logical_emit_message` triggers and publication that drive the SSE firehose.

`ADMIN_DASHBOARD_PRODUCTION` defaults to `true` when `NODE_ENV=production`.
Set it to `false` only for local development or deterministic compose stacks.
In non-production mode the backend bypasses Google sign-in and returns a local
admin session server-side; production still requires a verified Google account
from the allowlist.

Local non-production auth bypass binds to `127.0.0.1` by default. If a
deterministic compose stack needs to bind `HOST=0.0.0.0`, it must also set
`ADMIN_DASHBOARD_ALLOW_INSECURE_DEV_AUTH_BYPASS=true` so the unauthenticated
development surface is explicit.

## Local Development

```sh
bun install
bun admin-dashboard:dev
```

The dashboard server runs Better Auth schema migrations automatically during
startup when auth configuration is present.

Local defaults:

- API: `http://localhost:3000`
- Vite: `http://localhost:5173`

Use `ADMIN_DASHBOARD_WEB_ORIGIN` and `ADMIN_DASHBOARD_TRUSTED_ORIGINS` when the
frontend and API are served from different origins.

## Access Policy

Only verified Google sessions for these emails are authorized:

- `cliff@rift.trade`
- `samee@rift.trade`
- `tristan@rift.trade`

Authorization is enforced server-side on every dashboard API and SSE route.

## Railway

Use `railway/admin-dashboard/Dockerfile` for the Railway service build. The
service should expose port `3000` and set `BETTER_AUTH_URL` plus
`ADMIN_DASHBOARD_WEB_ORIGIN` to the public dashboard origin.

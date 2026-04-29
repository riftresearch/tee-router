# Router Gateway

Bun + Hono public API gateway for the TEE router protocol.

The gateway owns the public TypeScript API contract and serves the current
OpenAPI 3.1 document at `/openapi.json`. The Rust `router-api` remains the
internal protocol service.

## Commands

```sh
bun run router-gateway:dev
bun run router-gateway:typecheck
```

## Environment

- `PORT`: public gateway listen port. Defaults to `3000`.
- `HOST`: public gateway listen host. Defaults to `0.0.0.0`.
- `ROUTER_INTERNAL_BASE_URL`: upstream Rust router API base URL.

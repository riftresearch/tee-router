# RIFT Explorer

Transaction explorer for RIFT swaps. Search and track swap transactions by ID.

## Development

```bash
bun run explorer:dev
```

Open http://localhost:3000.

By default, Explorer reads order info from:

```text
https://api.rift.trade
```

Override it locally with `NEXT_PUBLIC_RIFT_API_URL` when needed.

## Deployment

The Railway service should build from the repository root with `railway/explorer/Dockerfile`.

Target domain: `explorer.rift.trade`.

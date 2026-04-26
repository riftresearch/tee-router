# HyperUnit SOCKS5 on Railway

This helper creates a dedicated SOCKS5 proxy service for HyperUnit egress inside a Railway project.

Default behavior:

- uses the published upstream image `serjs/go-socks5-proxy`
- pins the service to Railway Europe West using CLI region flag `europe-west4`
- requires SOCKS5 authentication
- binds the proxy to port `1080`
- only allows outbound destinations matching `^api\.hyperunit\.xyz$`
- assumes the router will reach it over Railway internal networking

Why this shape:

- the router now accepts `HYPERUNIT_PROXY_URL`
- we explicitly reject `socks5h://`; use `socks5://`
- HyperUnit is the only provider that should use this proxy
- Railway's Europe deployment region is EU West Metal in Amsterdam

## Create the service

First authenticate Railway CLI:

```bash
railway login
```

Then create the service:

```bash
RAILWAY_PROJECT="rift v2" \
RAILWAY_ENVIRONMENT=production \
PROXY_USER=router \
PROXY_PASSWORD='replace-me' \
./deploy/railway/hyperunit-socks5/create-service.sh
```

If you want Railway to build directly from the upstream GitHub repository instead of using the published Docker image:

```bash
SOURCE_MODE=repo \
RAILWAY_PROJECT="rift v2" \
RAILWAY_ENVIRONMENT=production \
PROXY_USER=router \
PROXY_PASSWORD='replace-me' \
./deploy/railway/hyperunit-socks5/create-service.sh
```

## Router configuration

Point only the HyperUnit path at the internal proxy:

```bash
HYPERUNIT_PROXY_URL=socks5://router:replace-me@hyperunit-socks5.railway.internal:1080
```

Keep `HYPERUNIT_API_URL=https://api.hyperunit.xyz`.

## Notes

- Do not expose this service publicly unless you actually need public ingress.
- If you need stricter ingress control, set `ALLOWED_IPS`.
- If the Railway service name changes, the internal hostname changes with it.
- Railway docs currently show the Europe deployment region as `EU West Metal` in Amsterdam, with region identifier `europe-west4-drams3a`, and the CLI scale flag pattern as `--europe-west4`.

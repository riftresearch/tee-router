# syntax=docker/dockerfile:1.7
FROM oven/bun:1.3.8-alpine

WORKDIR /app

COPY package.json bun.lock tsconfig.base.json ./
COPY apps/admin-dashboard/package.json ./apps/admin-dashboard/package.json
COPY apps/explorer/package.json ./apps/explorer/package.json
COPY apps/router-gateway/package.json ./apps/router-gateway/package.json

RUN --mount=type=cache,id=tee-router-bun-cache,target=/root/.bun/install/cache,sharing=locked \
    bun install --frozen-lockfile

COPY apps/router-gateway ./apps/router-gateway

ENV HOST=0.0.0.0

CMD ["bun", "run", "router-gateway:start"]

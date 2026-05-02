FROM oven/bun:1.3.8-alpine

WORKDIR /app

COPY package.json bun.lock tsconfig.base.json ./
COPY apps/admin-dashboard/package.json ./apps/admin-dashboard/package.json
COPY apps/router-gateway/package.json ./apps/router-gateway/package.json

RUN bun install --frozen-lockfile

COPY apps/router-gateway ./apps/router-gateway

ENV HOST=0.0.0.0

CMD ["bun", "run", "router-gateway:start"]

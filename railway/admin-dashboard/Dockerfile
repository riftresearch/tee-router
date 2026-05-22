FROM oven/bun:1.3.8-alpine

WORKDIR /app

COPY package.json bun.lock tsconfig.base.json ./
COPY apps/admin-dashboard/package.json ./apps/admin-dashboard/package.json
COPY apps/explorer/package.json ./apps/explorer/package.json
COPY apps/router-gateway/package.json ./apps/router-gateway/package.json

RUN bun install --frozen-lockfile

COPY apps/admin-dashboard ./apps/admin-dashboard

ENV NODE_ENV=production
ENV HOST=0.0.0.0

EXPOSE 3000

RUN bun run admin-dashboard:build

CMD ["bun", "run", "admin-dashboard:start"]

FROM oven/bun:1.3.8-alpine

WORKDIR /app

RUN apk add --no-cache nodejs

COPY package.json bun.lock tsconfig.base.json ./
COPY apps/explorer/package.json ./apps/explorer/package.json

RUN bun install

COPY apps/explorer ./apps/explorer

RUN bun run explorer:build

ENV HOSTNAME=0.0.0.0
ENV PORT=3000

CMD ["bun", "run", "explorer:start"]

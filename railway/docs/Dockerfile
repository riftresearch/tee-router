# syntax=docker/dockerfile:1.7
FROM node:22-alpine

RUN --mount=type=cache,id=tee-router-npm-cache,target=/root/.npm,sharing=locked \
    apk add --no-cache socat \
  && npm install -g mint@latest

WORKDIR /app
COPY apps/docs/ ./

ENV MINT_TELEMETRY_DISABLED=1

CMD sh -lc 'mint dev --no-open & socat TCP-LISTEN:${PORT:-8080},fork,reuseaddr,bind=0.0.0.0 TCP:127.0.0.1:3000'

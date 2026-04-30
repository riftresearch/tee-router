FROM node:22-alpine

RUN apk add --no-cache socat \
  && npm install -g mint@latest

WORKDIR /app
COPY apps/docs/ ./

ENV MINT_TELEMETRY_DISABLED=1

CMD sh -lc 'mint dev --no-open & socat TCP-LISTEN:${PORT:-8080},fork,reuseaddr,bind=0.0.0.0 TCP:127.0.0.1:3000'

# redis-v3 — forever (no-TTL) decimals cache for router-gateway-v3.
# Build context = repo root.
FROM redis:7-alpine

# Persist the cache across restarts on a Railway volume mounted at /data so the
# on-chain decimals() lookups survive deploys (entries are immutable per token).
EXPOSE 6379
CMD ["redis-server", "--appendonly", "yes", "--dir", "/data"]

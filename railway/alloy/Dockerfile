# alloy-v3 — production observability ingress.
# Build context = repo root (Railway monorepo: dockerfilePath set per service).
FROM grafana/alloy:latest

COPY etc/alloy.railway.alloy /etc/alloy/config.alloy

# Railway injects PORT for the public endpoint; Alloy's OTLP HTTP receiver
# binds [::]:4318 (see config). The server UI binds 12345.
EXPOSE 4318 12345
ENTRYPOINT ["/bin/alloy"]
CMD ["run", "--server.http.listen-addr=0.0.0.0:12345", "/etc/alloy/config.alloy"]

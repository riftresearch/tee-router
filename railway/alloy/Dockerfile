# alloy-v3 — production observability ingress.
# Build context = repo root (Railway monorepo: dockerfilePath set per service).
# rev: force-rebuild after 0.0.0.0:4318 bind fix
FROM grafana/alloy:latest

COPY etc/alloy.railway.alloy /etc/alloy/config.alloy

# Bootstrap creates the public Railway domain on port 4318 for the OTLP HTTP
# receiver. Alloy's admin UI binds 12345 and should stay internal.
EXPOSE 4318 12345
ENTRYPOINT ["/bin/alloy"]
CMD ["run", "--server.http.listen-addr=0.0.0.0:12345", "/etc/alloy/config.alloy"]

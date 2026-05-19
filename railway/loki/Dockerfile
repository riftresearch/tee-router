# loki-v3 — Railway-private log store. Build context = repo root.
FROM grafana/loki:latest

COPY etc/loki.railway.yml /etc/loki/config.yaml

# Persistent Railway volume mounts at /loki (set in bootstrap).
EXPOSE 3100
ENTRYPOINT ["/usr/bin/loki"]
CMD ["-config.file=/etc/loki/config.yaml"]

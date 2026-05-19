# grafana-v3 — observability UI. Build context = repo root.
# Bundles the repo's dashboards + VictoriaMetrics/Loki datasource
# provisioning so the UI is usable on first boot.
FROM grafana/grafana:10.4.2

COPY etc/grafana/grafana.ini /etc/grafana/grafana.ini
COPY etc/grafana/dashboards /var/lib/grafana/dashboards
COPY railway/grafana/provisioning /etc/grafana/provisioning

# Grafana listens on 3000; expose via a Railway public domain (bootstrap).
EXPOSE 3000

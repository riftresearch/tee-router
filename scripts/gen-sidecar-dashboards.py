#!/usr/bin/env python3
"""Emit per-sidecar Grafana dashboards from a shared template.

Each sidecar service (bitcoin-indexer, bitcoin-receipt-watcher, evm-token-indexer,
hl-shim-indexer) gets one dashboard scoped via the `service` label introduced in
the Stage 2 metric refactor. The EVM receipt watcher is emitted once with a
$chain variable so the same dashboard serves ethereum / base / arbitrum.

Output: etc/grafana/dashboards/tee-router-<slug>.json (one file per service).
"""

import json
import pathlib

OUT = pathlib.Path(__file__).resolve().parent.parent / "etc" / "grafana" / "dashboards"

DATASOURCE = {"type": "prometheus", "uid": "victoriametrics"}
LOKI = {"type": "loki", "uid": "loki"}


def panel_up(panel_id, service_filter, x=0, y=0, w=6, h=4):
    return {
        "datasource": DATASOURCE,
        "description": "Is the Prometheus scrape target reachable?",
        "fieldConfig": {
            "defaults": {
                "mappings": [
                    {"options": {"0": {"color": "red", "text": "DOWN"}}, "type": "value"},
                    {"options": {"1": {"color": "green", "text": "UP"}}, "type": "value"},
                ],
                "thresholds": {"mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "green", "value": 1}]},
                "unit": "none",
            },
            "overrides": [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {"colorMode": "background", "graphMode": "none", "justifyMode": "center", "orientation": "auto", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "textMode": "value_and_name"},
        "targets": [{"datasource": DATASOURCE, "editorMode": "code", "expr": f"up{{{service_filter}}}", "legendFormat": "{{instance}}", "refId": "A"}],
        "title": "Scrape Health",
        "type": "stat",
    }


def panel_request_rate(panel_id, service_filter, x=6, y=0, w=9, h=8):
    return {
        "datasource": DATASOURCE,
        "description": "Outbound request rate broken down by endpoint and status_class. From the upstream metric introduced in the kind={trading_venue,sidecar_service} refactor.",
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "palette-classic"},
                "custom": {"axisLabel": "req/s", "drawStyle": "line", "fillOpacity": 25, "lineWidth": 2, "showPoints": "never", "stacking": {"group": "A", "mode": "normal"}},
                "unit": "reqps",
            },
            "overrides": [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {"legend": {"calcs": ["lastNotNull", "max"], "displayMode": "table", "placement": "bottom", "showLegend": True}, "tooltip": {"mode": "multi", "sort": "desc"}},
        "targets": [{"datasource": DATASOURCE, "editorMode": "code", "expr": f"sum by (endpoint, status_class) (rate(tee_router_upstream_requests_total{{{service_filter}}}[1m]))", "legendFormat": "{{endpoint}} {{status_class}}", "refId": "A"}],
        "title": "Request Rate",
        "type": "timeseries",
    }


def panel_error_rate(panel_id, service_filter, x=15, y=0, w=9, h=8):
    return {
        "datasource": DATASOURCE,
        "description": "Non-2xx response rate by endpoint. Anything sustained here means the upstream is failing for the router.",
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "palette-classic"},
                "custom": {"axisLabel": "err/s", "drawStyle": "line", "fillOpacity": 25, "lineWidth": 2, "showPoints": "never"},
                "unit": "reqps",
            },
            "overrides": [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {"legend": {"calcs": ["lastNotNull", "max"], "displayMode": "table", "placement": "bottom", "showLegend": True}, "tooltip": {"mode": "multi", "sort": "desc"}},
        "targets": [{"datasource": DATASOURCE, "editorMode": "code", "expr": f'sum by (endpoint, status_class) (rate(tee_router_upstream_requests_total{{{service_filter}, status_class!="2xx"}}[5m]))', "legendFormat": "{{endpoint}} {{status_class}}", "refId": "A"}],
        "title": "Error Rate (5m)",
        "type": "timeseries",
    }


def panel_p95_latency(panel_id, service_filter, x=0, y=8, w=12, h=8):
    return {
        "datasource": DATASOURCE,
        "description": "p95 client-observed latency for outbound HTTP to this service.",
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "palette-classic"},
                "custom": {"axisLabel": "seconds", "drawStyle": "line", "fillOpacity": 10, "lineWidth": 2, "showPoints": "never"},
                "unit": "s",
            },
            "overrides": [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {"legend": {"calcs": ["lastNotNull", "max"], "displayMode": "table", "placement": "bottom", "showLegend": True}, "tooltip": {"mode": "multi", "sort": "desc"}},
        "targets": [{"datasource": DATASOURCE, "editorMode": "code", "expr": f'avg by (endpoint) (tee_router_upstream_request_duration_seconds{{{service_filter}, quantile="0.95"}})', "legendFormat": "{{endpoint}}", "refId": "A"}],
        "title": "p95 Latency by Endpoint",
        "type": "timeseries",
    }


def panel_success_ratio(panel_id, service_filter, x=12, y=8, w=12, h=8):
    return {
        "datasource": DATASOURCE,
        "description": "2xx response ratio (5m). Healthy is 100%.",
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "custom": {"axisLabel": "% 2xx", "drawStyle": "line", "fillOpacity": 10, "lineWidth": 2, "showPoints": "never"},
                "max": 100,
                "min": 0,
                "thresholds": {"mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "yellow", "value": 95}, {"color": "green", "value": 99}]},
                "unit": "percent",
            },
            "overrides": [],
        },
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {"legend": {"calcs": ["lastNotNull", "min"], "displayMode": "table", "placement": "bottom", "showLegend": True}, "tooltip": {"mode": "single"}},
        "targets": [{"datasource": DATASOURCE, "editorMode": "code", "expr": f'100 * (sum by (endpoint) (rate(tee_router_upstream_requests_total{{{service_filter}, status_class="2xx"}}[5m])) / sum by (endpoint) (rate(tee_router_upstream_requests_total{{{service_filter}}}[5m])))', "legendFormat": "{{endpoint}}", "refId": "A"}],
        "title": "Success Ratio (5m)",
        "type": "timeseries",
    }


def panel_recent_logs(panel_id, service_filter_lq, x=0, y=16, w=24, h=10):
    return {
        "datasource": LOKI,
        "description": "Recent service logs from Loki.",
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "id": panel_id,
        "options": {"dedupStrategy": "none", "enableLogDetails": True, "prettifyLogMessage": False, "showCommonLabels": False, "showLabels": False, "showTime": True, "sortOrder": "Descending", "wrapLogMessage": False},
        "targets": [{"datasource": LOKI, "editorMode": "code", "expr": f'{{{service_filter_lq}}} | json', "queryType": "range", "refId": "A"}],
        "title": "Recent Logs",
        "type": "logs",
    }


def build_dashboard(title, uid, service_label_value, scrape_filter=None, loki_service_name=None, templating=None):
    """`scrape_filter` is a raw label-matcher string for the up{} query, e.g.
    `job="bitcoin-indexer"` or `job=~"evm-token-indexer.*"`. If omitted, falls back
    to the upstream service filter."""
    service_filter = f'service="{service_label_value}"'
    scrape_filter = scrape_filter or service_filter
    panels = [
        panel_up(1, scrape_filter),
        panel_request_rate(2, service_filter),
        panel_error_rate(3, service_filter),
        panel_p95_latency(4, service_filter),
        panel_success_ratio(5, service_filter),
    ]
    if loki_service_name:
        panels.append(panel_recent_logs(6, f'service_name="{loki_service_name}"'))
    dashboard = {
        "annotations": {"list": [{"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True, "hide": True, "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts", "type": "dashboard"}]},
        "editable": True,
        "fiscalYearStartMonth": 0,
        "graphTooltip": 0,
        "id": None,
        "links": [{"asDropdown": True, "icon": "external link", "includeVars": False, "keepTime": True, "tags": ["tee-router"], "targetBlank": False, "title": "Tee Router dashboards", "type": "dashboards"}],
        "liveNow": False,
        "panels": panels,
        "refresh": "30s",
        "schemaVersion": 39,
        "tags": ["tee-router", "sidecar"],
        "templating": {"list": templating or []},
        "time": {"from": "now-30m", "to": "now"},
        "timepicker": {},
        "timezone": "browser",
        "title": title,
        "uid": uid,
        "version": 1,
    }
    return dashboard


def evm_receipt_watcher_dashboard():
    """Parameterized by $chain variable so one dashboard serves all 3 chains."""
    chain_var = {
        "current": {"selected": False, "text": "ethereum", "value": "ethereum"},
        "datasource": DATASOURCE,
        "definition": 'label_values(up{job=~"evm-receipt-watcher.*"}, job)',
        "hide": 0,
        "includeAll": False,
        "label": "Chain",
        "multi": False,
        "name": "chain",
        "options": [],
        "query": {"qryType": 1, "query": 'label_values(up{job=~"evm-receipt-watcher.*"}, job)', "refId": "PrometheusVariableQueryEditor-VariableQuery"},
        "refresh": 2,
        "regex": "/evm-receipt-watcher-(.*)/",
        "skipUrlSync": False,
        "sort": 1,
        "type": "query",
    }
    # service is always "evm_receipt_watcher" but scrape job varies by chain
    return build_dashboard(
        title="Tee Router — EVM Receipt Watcher",
        uid="tee-router-evm-receipt-watcher",
        service_label_value="evm_receipt_watcher",
        scrape_filter='job="evm-receipt-watcher-$chain"',
        templating=[chain_var],
    )


DASHBOARDS = [
    ("Tee Router — Bitcoin Indexer", "tee-router-bitcoin-indexer", "bitcoin_indexer", 'job="bitcoin-indexer"'),
    ("Tee Router — Bitcoin Receipt Watcher", "tee-router-bitcoin-receipt-watcher", "bitcoin_receipt_watcher", 'job="bitcoin-receipt-watcher"'),
    ("Tee Router — EVM Token Indexer", "tee-router-evm-token-indexer", "evm_token_indexer", 'job=~"evm-token-indexer.*"'),
    # hl-shim-indexer is hand-written (uses self-emitted metrics, not the upstream pattern)
    # because hl-shim-client doesn't yet instrument outbound calls.
]


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    for title, uid, service, scrape_filter in DASHBOARDS:
        d = build_dashboard(title, uid, service, scrape_filter=scrape_filter)
        path = OUT / f"{uid}.json"
        path.write_text(json.dumps(d, indent=2) + "\n")
        print(f"wrote {path}")
    d = evm_receipt_watcher_dashboard()
    path = OUT / "tee-router-evm-receipt-watcher.json"
    path.write_text(json.dumps(d, indent=2) + "\n")
    print(f"wrote {path}")


if __name__ == "__main__":
    main()

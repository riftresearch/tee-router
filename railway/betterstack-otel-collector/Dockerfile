FROM otel/opentelemetry-collector-contrib:0.150.1

COPY otelcol.railway.yml /etc/otelcol-contrib/config.yaml

CMD ["--config=/etc/otelcol-contrib/config.yaml"]

FROM debian:bookworm-slim

RUN apt-get update -qq \
  && apt-get install -y -qq --no-install-recommends \
    ca-certificates \
    gettext-base \
    netcat-openbsd \
    stunnel4 \
  && rm -rf /var/lib/apt/lists/*

COPY railway/router-replica-stunnel/stunnel.conf.template /etc/stunnel/stunnel.conf.template
COPY railway/router-replica-stunnel/entrypoint.sh /entrypoint.sh

RUN chmod 0755 /entrypoint.sh

CMD ["/entrypoint.sh"]

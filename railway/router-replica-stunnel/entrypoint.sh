#!/usr/bin/env bash
set -euo pipefail

: "${PHALA_DB_SNI:?PHALA_DB_SNI must be set}"
export LISTEN_PORT="${PORT:-5432}"

envsubst < /etc/stunnel/stunnel.conf.template > /etc/stunnel/stunnel.conf
exec stunnel /etc/stunnel/stunnel.conf

DROP PUBLICATION IF EXISTS router_cdc_publication;

CREATE PUBLICATION router_cdc_publication;

COMMENT ON PUBLICATION router_cdc_publication IS
    'Message-only CDC publication. Router triggers emit compact pg_logical_emit_message payloads; consumers do not need row-level pgoutput relation changes.';

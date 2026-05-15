CREATE OR REPLACE FUNCTION public.router_emit_cdc_message()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    record_id uuid;
    event_order_id uuid;
    event_watch_id uuid;
    event_provider_operation_id uuid;
    event_order_updated_at timestamptz;
    event_updated_at timestamptz;
    payload jsonb;
BEGIN
    IF TG_OP = 'DELETE' THEN
        CASE TG_TABLE_NAME
            WHEN 'router_orders' THEN
                record_id := OLD.id;
                event_order_id := OLD.id;
                event_order_updated_at := OLD.updated_at;
                event_updated_at := OLD.updated_at;

            WHEN 'market_order_quotes' THEN
                record_id := OLD.id;
                event_order_id := OLD.order_id;
                event_updated_at := OLD.created_at;

            WHEN 'market_order_actions' THEN
                event_order_id := OLD.order_id;
                record_id := OLD.order_id;
                event_updated_at := OLD.updated_at;

            WHEN 'limit_order_quotes' THEN
                record_id := OLD.id;
                event_order_id := OLD.order_id;
                event_updated_at := OLD.created_at;

            WHEN 'limit_order_actions' THEN
                event_order_id := OLD.order_id;
                record_id := OLD.order_id;
                event_updated_at := OLD.updated_at;

            WHEN 'order_execution_legs' THEN
                record_id := OLD.id;
                event_order_id := OLD.order_id;
                event_updated_at := OLD.updated_at;

            WHEN 'order_execution_steps' THEN
                record_id := OLD.id;
                event_order_id := OLD.order_id;
                event_updated_at := OLD.updated_at;

            WHEN 'order_provider_operations' THEN
                record_id := OLD.id;
                event_order_id := OLD.order_id;
                event_provider_operation_id := OLD.id;
                event_updated_at := OLD.updated_at;

            WHEN 'order_provider_addresses' THEN
                record_id := OLD.id;
                event_order_id := OLD.order_id;
                event_provider_operation_id := OLD.provider_operation_id;
                event_updated_at := OLD.updated_at;

            WHEN 'deposit_vaults' THEN
                record_id := OLD.id;
                event_watch_id := OLD.id;
                event_updated_at := OLD.updated_at;
                SELECT cv.order_id
                INTO event_order_id
                FROM public.custody_vaults cv
                WHERE cv.id = OLD.id;

            WHEN 'custody_vaults' THEN
                record_id := OLD.id;
                event_order_id := OLD.order_id;
                event_watch_id := OLD.id;
                event_updated_at := OLD.updated_at;

            ELSE
                RETURN OLD;
        END CASE;
    ELSE
        CASE TG_TABLE_NAME
            WHEN 'router_orders' THEN
                record_id := NEW.id;
                event_order_id := NEW.id;
                event_order_updated_at := NEW.updated_at;
                event_updated_at := NEW.updated_at;

            WHEN 'market_order_quotes' THEN
                record_id := NEW.id;
                event_order_id := NEW.order_id;
                event_updated_at := NEW.created_at;

            WHEN 'market_order_actions' THEN
                event_order_id := NEW.order_id;
                record_id := NEW.order_id;
                event_updated_at := NEW.updated_at;

            WHEN 'limit_order_quotes' THEN
                record_id := NEW.id;
                event_order_id := NEW.order_id;
                event_updated_at := NEW.created_at;

            WHEN 'limit_order_actions' THEN
                event_order_id := NEW.order_id;
                record_id := NEW.order_id;
                event_updated_at := NEW.updated_at;

            WHEN 'order_execution_legs' THEN
                record_id := NEW.id;
                event_order_id := NEW.order_id;
                event_updated_at := NEW.updated_at;

            WHEN 'order_execution_steps' THEN
                record_id := NEW.id;
                event_order_id := NEW.order_id;
                event_updated_at := NEW.updated_at;

            WHEN 'order_provider_operations' THEN
                record_id := NEW.id;
                event_order_id := NEW.order_id;
                event_provider_operation_id := NEW.id;
                event_updated_at := NEW.updated_at;

            WHEN 'order_provider_addresses' THEN
                record_id := NEW.id;
                event_order_id := NEW.order_id;
                event_provider_operation_id := NEW.provider_operation_id;
                event_updated_at := NEW.updated_at;

            WHEN 'deposit_vaults' THEN
                record_id := NEW.id;
                event_watch_id := NEW.id;
                event_updated_at := NEW.updated_at;
                SELECT cv.order_id
                INTO event_order_id
                FROM public.custody_vaults cv
                WHERE cv.id = NEW.id;

            WHEN 'custody_vaults' THEN
                record_id := NEW.id;
                event_order_id := NEW.order_id;
                event_watch_id := NEW.id;
                event_updated_at := NEW.updated_at;

            ELSE
                RETURN NEW;
        END CASE;
    END IF;

    IF TG_TABLE_NAME IN ('market_order_quotes', 'limit_order_quotes')
        AND event_order_id IS NULL
    THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RETURN NEW;
    END IF;

    payload := jsonb_build_object(
        'version', 1,
        'schema', TG_TABLE_SCHEMA,
        'table', TG_TABLE_NAME,
        'op', TG_OP,
        'id', record_id,
        'orderId', event_order_id,
        'orderUpdatedAt', event_order_updated_at,
        'eventUpdatedAt', event_updated_at,
        'watchId', event_watch_id,
        'providerOperationId', event_provider_operation_id
    );

    PERFORM pg_catalog.pg_logical_emit_message(
        true,
        'rift.router.change',
        payload::text
    );

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

DROP PUBLICATION IF EXISTS router_cdc_publication;

CREATE PUBLICATION router_cdc_publication;

COMMENT ON PUBLICATION router_cdc_publication IS
    'Message-only CDC publication. Router triggers emit compact pg_logical_emit_message payloads; consumers do not need row-level pgoutput relation changes.';

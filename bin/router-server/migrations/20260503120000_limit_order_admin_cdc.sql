CREATE OR REPLACE FUNCTION public.router_emit_cdc_message()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    record_id uuid;
    event_order_id uuid;
    event_watch_id uuid;
    event_provider_operation_id uuid;
    payload jsonb;
BEGIN
    CASE TG_TABLE_NAME
        WHEN 'router_orders' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := record_id;

        WHEN 'market_order_quotes' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);

        WHEN 'market_order_actions' THEN
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            record_id := event_order_id;

        WHEN 'limit_order_quotes' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);

        WHEN 'limit_order_actions' THEN
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            record_id := event_order_id;

        WHEN 'order_execution_legs' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);

        WHEN 'order_execution_steps' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);

        WHEN 'order_provider_operations' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            event_watch_id := record_id;
            event_provider_operation_id := record_id;

        WHEN 'order_provider_addresses' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            event_provider_operation_id := COALESCE(NEW.provider_operation_id, OLD.provider_operation_id);
            event_watch_id := event_provider_operation_id;

        WHEN 'deposit_vaults' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_watch_id := record_id;
            SELECT cv.order_id
            INTO event_order_id
            FROM public.custody_vaults cv
            WHERE cv.id = record_id;

        WHEN 'custody_vaults' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            event_watch_id := record_id;

        ELSE
            IF TG_OP = 'DELETE' THEN
                RETURN OLD;
            END IF;
            RETURN NEW;
    END CASE;

    payload := jsonb_build_object(
        'version', 1,
        'schema', TG_TABLE_SCHEMA,
        'table', TG_TABLE_NAME,
        'op', TG_OP,
        'id', record_id,
        'orderId', event_order_id,
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

DROP TRIGGER IF EXISTS router_cdc_message_limit_order_quotes ON public.limit_order_quotes;
CREATE TRIGGER router_cdc_message_limit_order_quotes
AFTER INSERT OR UPDATE OR DELETE ON public.limit_order_quotes
FOR EACH ROW EXECUTE FUNCTION public.router_emit_cdc_message();
ALTER TABLE public.limit_order_quotes ENABLE ALWAYS TRIGGER router_cdc_message_limit_order_quotes;

DROP TRIGGER IF EXISTS router_cdc_message_limit_order_actions ON public.limit_order_actions;
CREATE TRIGGER router_cdc_message_limit_order_actions
AFTER INSERT OR UPDATE OR DELETE ON public.limit_order_actions
FOR EACH ROW EXECUTE FUNCTION public.router_emit_cdc_message();
ALTER TABLE public.limit_order_actions ENABLE ALWAYS TRIGGER router_cdc_message_limit_order_actions;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_catalog.pg_publication
        WHERE pubname = 'router_cdc_publication'
    ) THEN
        ALTER PUBLICATION router_cdc_publication SET TABLE
            public.router_orders,
            public.market_order_quotes,
            public.market_order_actions,
            public.limit_order_quotes,
            public.limit_order_actions,
            public.order_execution_legs,
            public.order_execution_steps,
            public.order_provider_operations,
            public.order_provider_addresses,
            public.deposit_vaults,
            public.custody_vaults;
    END IF;
END;
$$;

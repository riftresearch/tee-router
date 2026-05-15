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
    CASE TG_TABLE_NAME
        WHEN 'router_orders' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := record_id;
            event_order_updated_at := COALESCE(NEW.updated_at, OLD.updated_at);
            event_updated_at := COALESCE(NEW.updated_at, OLD.updated_at);

        WHEN 'market_order_quotes' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            event_updated_at := COALESCE(NEW.created_at, OLD.created_at);

        WHEN 'market_order_actions' THEN
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            record_id := event_order_id;
            event_updated_at := COALESCE(NEW.updated_at, OLD.updated_at);

        WHEN 'order_execution_steps' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            event_updated_at := COALESCE(NEW.updated_at, OLD.updated_at);

        WHEN 'order_provider_operations' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            event_provider_operation_id := record_id;
            event_updated_at := COALESCE(NEW.updated_at, OLD.updated_at);

        WHEN 'order_provider_addresses' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            event_provider_operation_id := COALESCE(NEW.provider_operation_id, OLD.provider_operation_id);
            event_updated_at := COALESCE(NEW.updated_at, OLD.updated_at);

        WHEN 'deposit_vaults' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_watch_id := record_id;
            event_updated_at := COALESCE(NEW.updated_at, OLD.updated_at);
            SELECT cv.order_id
            INTO event_order_id
            FROM public.custody_vaults cv
            WHERE cv.id = record_id;

        WHEN 'custody_vaults' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            event_watch_id := record_id;
            event_updated_at := COALESCE(NEW.updated_at, OLD.updated_at);

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

DROP TRIGGER IF EXISTS router_cdc_message_router_orders ON public.router_orders;
CREATE TRIGGER router_cdc_message_router_orders
AFTER INSERT OR UPDATE OR DELETE ON public.router_orders
FOR EACH ROW EXECUTE FUNCTION public.router_emit_cdc_message();
ALTER TABLE public.router_orders ENABLE ALWAYS TRIGGER router_cdc_message_router_orders;

DROP TRIGGER IF EXISTS router_cdc_message_market_order_quotes ON public.market_order_quotes;
CREATE TRIGGER router_cdc_message_market_order_quotes
AFTER INSERT OR UPDATE OR DELETE ON public.market_order_quotes
FOR EACH ROW EXECUTE FUNCTION public.router_emit_cdc_message();
ALTER TABLE public.market_order_quotes ENABLE ALWAYS TRIGGER router_cdc_message_market_order_quotes;

DROP TRIGGER IF EXISTS router_cdc_message_market_order_actions ON public.market_order_actions;
CREATE TRIGGER router_cdc_message_market_order_actions
AFTER INSERT OR UPDATE OR DELETE ON public.market_order_actions
FOR EACH ROW EXECUTE FUNCTION public.router_emit_cdc_message();
ALTER TABLE public.market_order_actions ENABLE ALWAYS TRIGGER router_cdc_message_market_order_actions;

DROP TRIGGER IF EXISTS router_cdc_message_order_execution_steps ON public.order_execution_steps;
CREATE TRIGGER router_cdc_message_order_execution_steps
AFTER INSERT OR UPDATE OR DELETE ON public.order_execution_steps
FOR EACH ROW EXECUTE FUNCTION public.router_emit_cdc_message();
ALTER TABLE public.order_execution_steps ENABLE ALWAYS TRIGGER router_cdc_message_order_execution_steps;

DROP TRIGGER IF EXISTS router_cdc_message_order_provider_operations ON public.order_provider_operations;
CREATE TRIGGER router_cdc_message_order_provider_operations
AFTER INSERT OR UPDATE OR DELETE ON public.order_provider_operations
FOR EACH ROW EXECUTE FUNCTION public.router_emit_cdc_message();
ALTER TABLE public.order_provider_operations ENABLE ALWAYS TRIGGER router_cdc_message_order_provider_operations;

DROP TRIGGER IF EXISTS router_cdc_message_order_provider_addresses ON public.order_provider_addresses;
CREATE TRIGGER router_cdc_message_order_provider_addresses
AFTER INSERT OR UPDATE OR DELETE ON public.order_provider_addresses
FOR EACH ROW EXECUTE FUNCTION public.router_emit_cdc_message();
ALTER TABLE public.order_provider_addresses ENABLE ALWAYS TRIGGER router_cdc_message_order_provider_addresses;

DROP TRIGGER IF EXISTS router_cdc_message_deposit_vaults ON public.deposit_vaults;
CREATE TRIGGER router_cdc_message_deposit_vaults
AFTER INSERT OR UPDATE OR DELETE ON public.deposit_vaults
FOR EACH ROW EXECUTE FUNCTION public.router_emit_cdc_message();
ALTER TABLE public.deposit_vaults ENABLE ALWAYS TRIGGER router_cdc_message_deposit_vaults;

DROP TRIGGER IF EXISTS router_cdc_message_custody_vaults ON public.custody_vaults;
CREATE TRIGGER router_cdc_message_custody_vaults
AFTER INSERT OR UPDATE OR DELETE ON public.custody_vaults
FOR EACH ROW EXECUTE FUNCTION public.router_emit_cdc_message();
ALTER TABLE public.custody_vaults ENABLE ALWAYS TRIGGER router_cdc_message_custody_vaults;

DROP PUBLICATION IF EXISTS router_cdc_publication;

CREATE PUBLICATION router_cdc_publication;

COMMENT ON PUBLICATION router_cdc_publication IS
    'Message-only CDC publication. Router triggers emit compact pg_logical_emit_message payloads; consumers do not need row-level pgoutput relation changes.';

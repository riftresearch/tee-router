CREATE OR REPLACE FUNCTION public.sauron_provider_operation_watch_notify()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  target_watch_id uuid;
  payload text;
BEGIN
  target_watch_id := COALESCE(NEW.id, OLD.id);
  payload := json_build_object('watchId', target_watch_id::text)::text;
  PERFORM pg_notify('sauron_watch_set_changed', payload);
  RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE OR REPLACE FUNCTION public.sauron_provider_address_watch_notify()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  target_watch_id uuid;
  payload text;
BEGIN
  target_watch_id := COALESCE(NEW.provider_operation_id, OLD.provider_operation_id);
  IF target_watch_id IS NULL THEN
    RETURN COALESCE(NEW, OLD);
  END IF;

  payload := json_build_object('watchId', target_watch_id::text)::text;
  PERFORM pg_notify('sauron_watch_set_changed', payload);
  RETURN COALESCE(NEW, OLD);
END;
$$;

DROP TRIGGER IF EXISTS sauron_provider_operation_watch_notify ON public.order_provider_operations;
DROP TRIGGER IF EXISTS sauron_provider_address_watch_notify ON public.order_provider_addresses;

CREATE TRIGGER sauron_provider_operation_watch_notify
AFTER INSERT OR UPDATE OR DELETE ON public.order_provider_operations
FOR EACH ROW
EXECUTE FUNCTION public.sauron_provider_operation_watch_notify();

CREATE TRIGGER sauron_provider_address_watch_notify
AFTER INSERT OR UPDATE OR DELETE ON public.order_provider_addresses
FOR EACH ROW
EXECUTE FUNCTION public.sauron_provider_address_watch_notify();

ALTER TABLE public.order_provider_operations
ENABLE ALWAYS TRIGGER sauron_provider_operation_watch_notify;

ALTER TABLE public.order_provider_addresses
ENABLE ALWAYS TRIGGER sauron_provider_address_watch_notify;

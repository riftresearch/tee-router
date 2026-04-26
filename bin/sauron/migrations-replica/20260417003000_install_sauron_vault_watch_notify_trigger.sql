CREATE OR REPLACE FUNCTION public.sauron_deposit_vault_watch_notify()
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

CREATE OR REPLACE FUNCTION public.sauron_custody_vault_watch_notify()
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

DROP TRIGGER IF EXISTS sauron_deposit_vault_watch_notify ON public.deposit_vaults;
DROP TRIGGER IF EXISTS sauron_custody_vault_watch_notify ON public.custody_vaults;

CREATE TRIGGER sauron_deposit_vault_watch_notify
AFTER INSERT OR UPDATE OR DELETE ON public.deposit_vaults
FOR EACH ROW
EXECUTE FUNCTION public.sauron_deposit_vault_watch_notify();

CREATE TRIGGER sauron_custody_vault_watch_notify
AFTER INSERT OR UPDATE OR DELETE ON public.custody_vaults
FOR EACH ROW
EXECUTE FUNCTION public.sauron_custody_vault_watch_notify();

ALTER TABLE public.deposit_vaults
ENABLE ALWAYS TRIGGER sauron_deposit_vault_watch_notify;

ALTER TABLE public.custody_vaults
ENABLE ALWAYS TRIGGER sauron_custody_vault_watch_notify;

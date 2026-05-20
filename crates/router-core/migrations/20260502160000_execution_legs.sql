CREATE TABLE public.order_execution_legs (
    id uuid NOT NULL,
    order_id uuid NOT NULL,
    execution_attempt_id uuid,
    transition_decl_id text,
    leg_index integer NOT NULL,
    leg_type text NOT NULL,
    provider text NOT NULL,
    status text NOT NULL,
    input_chain_id text NOT NULL,
    input_asset_id text NOT NULL,
    output_chain_id text NOT NULL,
    output_asset_id text NOT NULL,
    amount_in text NOT NULL,
    expected_amount_out text NOT NULL,
    min_amount_out text,
    actual_amount_in text,
    actual_amount_out text,
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    details_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    usd_valuation_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    CONSTRAINT order_execution_legs_pkey PRIMARY KEY (id),
    CONSTRAINT order_execution_legs_amount_in_digits CHECK (amount_in ~ '^[0-9]+$'::text),
    CONSTRAINT order_execution_legs_expected_amount_out_digits CHECK (expected_amount_out ~ '^[0-9]+$'::text),
    CONSTRAINT order_execution_legs_min_amount_out_digits CHECK ((min_amount_out IS NULL) OR (min_amount_out ~ '^[0-9]+$'::text)),
    CONSTRAINT order_execution_legs_actual_amount_in_digits CHECK ((actual_amount_in IS NULL) OR (actual_amount_in ~ '^[0-9]+$'::text)),
    CONSTRAINT order_execution_legs_actual_amount_out_digits CHECK ((actual_amount_out IS NULL) OR (actual_amount_out ~ '^[0-9]+$'::text)),
    CONSTRAINT order_execution_legs_details_object CHECK (jsonb_typeof(details_json) = 'object'::text),
    CONSTRAINT order_execution_legs_usd_valuation_object CHECK (jsonb_typeof(usd_valuation_json) = 'object'::text),
    CONSTRAINT order_execution_legs_lifecycle_timestamps CHECK ((completed_at IS NULL) OR (started_at IS NULL) OR (completed_at >= started_at)),
    CONSTRAINT order_execution_legs_provider_not_empty CHECK (provider <> ''::text),
    CONSTRAINT order_execution_legs_transition_decl_id_not_empty CHECK ((transition_decl_id IS NULL) OR (transition_decl_id <> ''::text)),
    CONSTRAINT order_execution_legs_type_not_empty CHECK (leg_type <> ''::text),
    CONSTRAINT order_execution_legs_status_check CHECK (status = ANY (ARRAY['planned'::text, 'waiting'::text, 'ready'::text, 'running'::text, 'completed'::text, 'failed'::text, 'skipped'::text, 'cancelled'::text])),
    CONSTRAINT order_execution_legs_leg_index_nonnegative CHECK (leg_index >= 0)
);

ALTER TABLE public.order_execution_steps
    ADD COLUMN execution_leg_id uuid;

ALTER TABLE ONLY public.order_execution_legs
    ADD CONSTRAINT order_execution_legs_execution_attempt_id_fkey
    FOREIGN KEY (execution_attempt_id) REFERENCES public.order_execution_attempts(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.order_execution_legs
    ADD CONSTRAINT order_execution_legs_order_id_fkey
    FOREIGN KEY (order_id) REFERENCES public.router_orders(id) ON DELETE CASCADE;

ALTER TABLE ONLY public.order_execution_steps
    ADD CONSTRAINT order_execution_steps_execution_leg_id_fkey
    FOREIGN KEY (execution_leg_id) REFERENCES public.order_execution_legs(id) ON DELETE SET NULL;

CREATE UNIQUE INDEX idx_order_execution_legs_attempt_index
    ON public.order_execution_legs USING btree (execution_attempt_id, leg_index)
    WHERE execution_attempt_id IS NOT NULL;

CREATE UNIQUE INDEX idx_order_execution_legs_order_index
    ON public.order_execution_legs USING btree (order_id, leg_index)
    WHERE execution_attempt_id IS NULL;

CREATE INDEX idx_order_execution_legs_order_id
    ON public.order_execution_legs USING btree (order_id);

CREATE INDEX idx_order_execution_legs_status_next
    ON public.order_execution_legs USING btree (status, updated_at)
    WHERE status = ANY (ARRAY['planned'::text, 'waiting'::text, 'ready'::text, 'running'::text]);

CREATE INDEX idx_order_execution_steps_execution_leg_id
    ON public.order_execution_steps USING btree (execution_leg_id)
    WHERE execution_leg_id IS NOT NULL;

CREATE TRIGGER update_order_execution_legs_updated_at
    BEFORE UPDATE ON public.order_execution_legs
    FOR EACH ROW EXECUTE FUNCTION public.update_updated_at_column();

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

        WHEN 'order_execution_legs' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            event_updated_at := COALESCE(NEW.updated_at, OLD.updated_at);

        WHEN 'order_execution_steps' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            event_updated_at := COALESCE(NEW.updated_at, OLD.updated_at);

        WHEN 'order_provider_operations' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            event_watch_id := record_id;
            event_provider_operation_id := record_id;
            event_updated_at := COALESCE(NEW.updated_at, OLD.updated_at);

        WHEN 'order_provider_addresses' THEN
            record_id := COALESCE(NEW.id, OLD.id);
            event_order_id := COALESCE(NEW.order_id, OLD.order_id);
            event_provider_operation_id := COALESCE(NEW.provider_operation_id, OLD.provider_operation_id);
            event_watch_id := event_provider_operation_id;
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

DROP TRIGGER IF EXISTS router_cdc_message_order_execution_legs ON public.order_execution_legs;
CREATE TRIGGER router_cdc_message_order_execution_legs
AFTER INSERT OR UPDATE OR DELETE ON public.order_execution_legs
FOR EACH ROW EXECUTE FUNCTION public.router_emit_cdc_message();
ALTER TABLE public.order_execution_legs ENABLE ALWAYS TRIGGER router_cdc_message_order_execution_legs;

DROP PUBLICATION IF EXISTS router_cdc_publication;

CREATE PUBLICATION router_cdc_publication;

COMMENT ON PUBLICATION router_cdc_publication IS
    'Message-only CDC publication. Router triggers emit compact pg_logical_emit_message payloads; consumers do not need row-level pgoutput relation changes.';

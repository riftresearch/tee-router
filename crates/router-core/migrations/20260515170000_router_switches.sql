CREATE TABLE public.router_switches (
    name text NOT NULL,
    enabled boolean NOT NULL DEFAULT false,
    reason text DEFAULT ''::text NOT NULL,
    updated_by text DEFAULT 'unknown'::text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT router_switches_name_check CHECK ((name = ANY (ARRAY['refund_only_mode'::text])))
);

ALTER TABLE ONLY public.router_switches
    ADD CONSTRAINT router_switches_pkey PRIMARY KEY (name);

INSERT INTO public.router_switches (name, enabled, reason, updated_by)
VALUES ('refund_only_mode', false, '', 'migration')
ON CONFLICT (name) DO NOTHING;

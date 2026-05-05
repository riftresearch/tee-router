ALTER TABLE public.deposit_vaults
    ADD COLUMN funding_tx_hash text,
    ADD COLUMN funding_sender_address text,
    ADD COLUMN funding_sender_addresses jsonb DEFAULT '[]'::jsonb NOT NULL,
    ADD COLUMN funding_recipient_address text,
    ADD COLUMN funding_transfer_index bigint,
    ADD COLUMN funding_observed_amount text,
    ADD COLUMN funding_confirmation_state text,
    ADD COLUMN funding_observed_at timestamp with time zone,
    ADD COLUMN funding_evidence_json jsonb DEFAULT '{}'::jsonb NOT NULL;

ALTER TABLE public.deposit_vaults
    ADD CONSTRAINT deposit_vault_funding_sender_addresses_array
    CHECK (jsonb_typeof(funding_sender_addresses) = 'array');

ALTER TABLE public.deposit_vaults
    ADD CONSTRAINT deposit_vault_funding_evidence_object
    CHECK (jsonb_typeof(funding_evidence_json) = 'object');

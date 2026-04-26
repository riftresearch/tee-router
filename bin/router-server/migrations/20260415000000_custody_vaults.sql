CREATE TABLE custody_vaults (
    id UUID PRIMARY KEY,
    order_id UUID REFERENCES router_orders(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    visibility TEXT NOT NULL,
    chain_id TEXT NOT NULL,
    asset_id TEXT,
    address TEXT NOT NULL,
    control_type TEXT NOT NULL,
    derivation_salt BYTEA,
    signer_ref TEXT,
    status TEXT NOT NULL,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT custody_vaults_role_check CHECK (
        role IN (
            'source_deposit',
            'across_refund',
            'destination_execution',
            'unit_recovery'
        )
    ),
    CONSTRAINT custody_vaults_visibility_check CHECK (
        visibility IN (
            'user_facing',
            'internal',
            'watch_only'
        )
    ),
    CONSTRAINT custody_vaults_control_type_check CHECK (
        control_type IN (
            'router_derived_key',
            'paymaster_wallet',
            'hyperliquid_master_signer',
            'external_user',
            'watch_only'
        )
    ),
    CONSTRAINT custody_vaults_status_check CHECK (
        status IN (
            'planned',
            'active',
            'released',
            'failed'
        )
    ),
    CONSTRAINT custody_vaults_chain_id_not_empty CHECK (chain_id <> ''),
    CONSTRAINT custody_vaults_asset_id_not_empty CHECK (asset_id IS NULL OR asset_id <> ''),
    CONSTRAINT custody_vaults_address_not_empty CHECK (address <> ''),
    CONSTRAINT custody_vaults_signer_ref_not_empty CHECK (
        signer_ref IS NULL OR signer_ref <> ''
    ),
    CONSTRAINT custody_vaults_derivation_salt_len CHECK (
        derivation_salt IS NULL OR octet_length(derivation_salt) = 32
    ),
    CONSTRAINT custody_vaults_metadata_object CHECK (
        jsonb_typeof(metadata_json) = 'object'
    )
);

INSERT INTO custody_vaults (
    id,
    order_id,
    role,
    visibility,
    chain_id,
    asset_id,
    address,
    control_type,
    derivation_salt,
    signer_ref,
    status,
    metadata_json,
    created_at,
    updated_at
)
SELECT
    id,
    order_id,
    'source_deposit',
    'user_facing',
    deposit_chain_id,
    deposit_asset_id,
    deposit_vault_address,
    'router_derived_key',
    deposit_vault_salt,
    'deposit_vault:' || id::text,
    CASE
        WHEN status IN ('completed', 'refunded') THEN 'released'
        ELSE 'active'
    END,
    jsonb_build_object(
        'deposit_vault_id',
        id,
        'role_reason',
        'user_funding_source'
    ),
    created_at,
    updated_at
FROM deposit_vaults;

INSERT INTO custody_vaults (
    id,
    order_id,
    role,
    visibility,
    chain_id,
    asset_id,
    address,
    control_type,
    derivation_salt,
    signer_ref,
    status,
    metadata_json,
    created_at,
    updated_at
)
SELECT
    oca.id,
    oca.order_id,
    oca.role,
    CASE
        WHEN oca.role = 'source_deposit' THEN 'user_facing'
        ELSE 'internal'
    END,
    oca.chain_id,
    NULL,
    oca.address,
    'router_derived_key',
    NULL,
    oca.signer_ref,
    oca.status,
    oca.metadata_json,
    oca.created_at,
    oca.updated_at
FROM order_custody_accounts oca
WHERE oca.role NOT IN ('unit_deposit', 'hyperliquid_destination')
  AND NOT EXISTS (
    SELECT 1
    FROM custody_vaults cv
    WHERE cv.chain_id = oca.chain_id
      AND cv.address = oca.address
);

UPDATE order_execution_steps
SET details_json =
    details_json
    - 'custody_account_id'
    - 'custody_account_role'
    || jsonb_build_object(
        'custody_vault_id',
        details_json->'vault_id',
        'custody_vault_role',
        'source_deposit'
    )
WHERE step_type = 'wait_for_deposit'
  AND details_json ? 'vault_id';

UPDATE order_execution_steps
SET details_json =
    details_json
    - 'destination_custody_account_id'
    || jsonb_build_object(
        'destination_custody_vault_id',
        details_json->'destination_custody_account_id'
    )
WHERE details_json ? 'destination_custody_account_id';

DROP TABLE order_custody_accounts;

DROP INDEX IF EXISTS idx_deposit_vaults_order_id;
DROP INDEX IF EXISTS idx_deposit_vaults_chain_id_address;

ALTER TABLE deposit_vaults
    ADD CONSTRAINT deposit_vaults_custody_vault_fkey
        FOREIGN KEY (id) REFERENCES custody_vaults(id) ON DELETE CASCADE;

ALTER TABLE deposit_vaults
    DROP COLUMN order_id,
    DROP COLUMN deposit_chain_id,
    DROP COLUMN deposit_asset_id,
    DROP COLUMN deposit_vault_salt,
    DROP COLUMN deposit_vault_address;

CREATE UNIQUE INDEX idx_custody_vaults_chain_address
    ON custody_vaults (chain_id, address);

CREATE INDEX idx_custody_vaults_order_id
    ON custody_vaults (order_id)
    WHERE order_id IS NOT NULL;

CREATE INDEX idx_custody_vaults_order_role
    ON custody_vaults (order_id, role)
    WHERE order_id IS NOT NULL;

CREATE INDEX idx_custody_vaults_role
    ON custody_vaults (role);

ALTER TABLE order_execution_steps
    DROP CONSTRAINT order_execution_steps_step_type_check;

ALTER TABLE order_execution_steps
    ADD CONSTRAINT order_execution_steps_step_type_check CHECK (
        step_type IN (
            'wait_for_deposit',
            'across_bridge',
            'unit_deposit',
            'unit_withdrawal',
            'hyperliquid_trade',
            'refund'
        )
    );

CREATE TRIGGER update_custody_vaults_updated_at BEFORE UPDATE ON custody_vaults
    FOR EACH ROW EXECUTE PROCEDURE update_updated_at_column();

ALTER TABLE deposit_vaults
    ADD COLUMN deposit_chain_id TEXT,
    ADD COLUMN deposit_asset_id TEXT;

UPDATE deposit_vaults
SET
    deposit_chain_id = CASE deposit_chain
        WHEN 'bitcoin' THEN 'bitcoin'
        WHEN 'ethereum' THEN 'evm:1'
        WHEN 'base' THEN 'evm:8453'
        ELSE NULL
    END,
    deposit_asset_id = CASE
        WHEN deposit_token->>'type' = 'Native' THEN 'native'
        WHEN deposit_token->>'type' = 'Address' THEN lower(deposit_token->>'data')
        ELSE NULL
    END;

ALTER TABLE deposit_vaults
    ALTER COLUMN deposit_chain_id SET NOT NULL,
    ALTER COLUMN deposit_asset_id SET NOT NULL;

DROP INDEX IF EXISTS idx_deposit_vaults_chain_address;

CREATE UNIQUE INDEX idx_deposit_vaults_chain_id_address
    ON deposit_vaults (deposit_chain_id, deposit_vault_address);

ALTER TABLE deposit_vaults
    DROP COLUMN deposit_chain,
    DROP COLUMN deposit_token,
    DROP COLUMN deposit_decimals;

CREATE UNIQUE INDEX idx_custody_vaults_order_role_chain_asset
    ON custody_vaults (order_id, role, chain_id, (COALESCE(asset_id, '')))
    WHERE order_id IS NOT NULL;

ALTER TABLE custody_vaults DROP CONSTRAINT custody_vaults_role_check;

ALTER TABLE custody_vaults ADD CONSTRAINT custody_vaults_role_check CHECK (
    role IN (
        'source_deposit',
        'destination_execution',
        'hyperliquid_spot'
    )
);

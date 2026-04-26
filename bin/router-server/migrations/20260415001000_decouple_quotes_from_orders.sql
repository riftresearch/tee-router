ALTER TABLE market_order_quotes
    ADD COLUMN source_chain_id TEXT,
    ADD COLUMN source_asset_id TEXT,
    ADD COLUMN destination_chain_id TEXT,
    ADD COLUMN destination_asset_id TEXT,
    ADD COLUMN recipient_address TEXT;

UPDATE market_order_quotes moq
SET
    source_chain_id = ro.source_chain_id,
    source_asset_id = ro.source_asset_id,
    destination_chain_id = ro.destination_chain_id,
    destination_asset_id = ro.destination_asset_id,
    recipient_address = ro.recipient_address
FROM router_orders ro
WHERE moq.order_id = ro.id;

ALTER TABLE market_order_quotes
    ALTER COLUMN order_id DROP NOT NULL,
    ALTER COLUMN source_chain_id SET NOT NULL,
    ALTER COLUMN source_asset_id SET NOT NULL,
    ALTER COLUMN destination_chain_id SET NOT NULL,
    ALTER COLUMN destination_asset_id SET NOT NULL,
    ALTER COLUMN recipient_address SET NOT NULL,
    ADD CONSTRAINT market_order_quotes_source_chain_not_empty CHECK (source_chain_id <> ''),
    ADD CONSTRAINT market_order_quotes_source_asset_not_empty CHECK (source_asset_id <> ''),
    ADD CONSTRAINT market_order_quotes_destination_chain_not_empty CHECK (destination_chain_id <> ''),
    ADD CONSTRAINT market_order_quotes_destination_asset_not_empty CHECK (destination_asset_id <> ''),
    ADD CONSTRAINT market_order_quotes_recipient_address_not_empty CHECK (recipient_address <> '');

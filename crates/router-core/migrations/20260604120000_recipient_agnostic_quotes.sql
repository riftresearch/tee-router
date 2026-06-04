-- Quotes are now recipient-agnostic: the real recipient is supplied only on the
-- ORDER request and stored on router_orders.recipient_address. The quote-level
-- recipient_address columns are no longer written or read by the quote path, so
-- relax them to NULLABLE and drop their not-empty CHECK constraints. The columns
-- are kept (vestigial) to preserve historical rows.

ALTER TABLE public.market_order_quotes
    ALTER COLUMN recipient_address DROP NOT NULL;

ALTER TABLE public.market_order_quotes
    DROP CONSTRAINT IF EXISTS market_order_quotes_recipient_address_not_empty;

ALTER TABLE public.limit_order_quotes
    ALTER COLUMN recipient_address DROP NOT NULL;

ALTER TABLE public.limit_order_quotes
    DROP CONSTRAINT IF EXISTS limit_order_quotes_recipient_address_not_empty;

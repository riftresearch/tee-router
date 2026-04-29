CREATE TABLE IF NOT EXISTS public.sauron_backend_cursors (
    backend TEXT PRIMARY KEY,
    height BIGINT NOT NULL,
    hash TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sauron_backend_cursors_backend_not_empty CHECK (backend <> ''),
    CONSTRAINT sauron_backend_cursors_height_nonnegative CHECK (height >= 0),
    CONSTRAINT sauron_backend_cursors_hash_not_empty CHECK (hash <> '')
);

CREATE TABLE IF NOT EXISTS public.sauron_cdc_checkpoints (
    consumer TEXT PRIMARY KEY,
    slot_name TEXT NOT NULL,
    last_lsn TEXT,
    last_xid BIGINT,
    last_batch_size BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT sauron_cdc_checkpoints_consumer_not_empty CHECK (consumer <> ''),
    CONSTRAINT sauron_cdc_checkpoints_slot_name_not_empty CHECK (slot_name <> ''),
    CONSTRAINT sauron_cdc_checkpoints_batch_size_nonnegative CHECK (last_batch_size >= 0)
);

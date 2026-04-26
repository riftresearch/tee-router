CREATE TABLE IF NOT EXISTS public.sauron_backend_cursors (
    backend TEXT PRIMARY KEY,
    height BIGINT NOT NULL,
    hash TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT sauron_backend_cursors_backend_not_empty CHECK (backend <> ''),
    CONSTRAINT sauron_backend_cursors_height_nonnegative CHECK (height >= 0),
    CONSTRAINT sauron_backend_cursors_hash_not_empty CHECK (hash <> '')
);

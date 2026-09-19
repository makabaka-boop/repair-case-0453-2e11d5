-- Batch sealing service schema.
-- Applied at API startup; idempotent so any instance can boot first.

CREATE TABLE IF NOT EXISTS batches (
    id               CHAR(32)     PRIMARY KEY,
    expected_chunks  INTEGER      NOT NULL CHECK (expected_chunks BETWEEN 1 AND 10000),
    status           TEXT         NOT NULL DEFAULT 'OPEN'
                                   CHECK (status IN ('OPEN', 'SEALED')),
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    sealed_at        TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS chunks (
    batch_id    CHAR(32)     NOT NULL
        REFERENCES batches (id) ON DELETE CASCADE,
    seq         INTEGER      NOT NULL CHECK (seq >= 1),
    payload     BYTEA        NOT NULL,
    received_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, seq)
);

-- Covered by the composite primary key, but make reverse lookups explicit.
CREATE INDEX IF NOT EXISTS idx_chunks_batch ON chunks (batch_id);

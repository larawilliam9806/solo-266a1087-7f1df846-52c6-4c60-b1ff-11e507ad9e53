"""SQLite metadata layer.

Three tables:

* ``files``          -- registry of every parquet file that belongs to a
                        daily partition.  ``state`` is ``active`` (visible to
                        readers) or ``retired`` (replaced by compaction,
                        waiting for in-flight readers to drain before the
                        bytes are unlinked).
* ``compactions``    -- one row per compaction job.  A partial unique index
                        guarantees at most one ``running`` job per day, which
                        is what lets us reject concurrent compactions for the
                        same day atomically (even across a process restart).
* ``upload_batches`` -- one row per committed client upload batch, keyed by
                        the client-supplied batch id (PRIMARY KEY, so racing
                        first uploads of the same id register exactly once).
                        The receipt (original file name + row count) lives
                        here -- not in ``files`` -- so it stays valid after
                        compaction retires and reclaims the original parquet.

The compaction *switch* (retire inputs + register output + mark job done)
happens in a single SQLite transaction, so a crash can never leave the
registry half-switched.  Likewise, an idempotent upload commits its batch
row and its file row in one transaction, so a crash or a failed attempt
can never hold a batch id without the corresponding file being registered.
"""

from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    day        TEXT NOT NULL,
    path       TEXT NOT NULL UNIQUE,
    kind       TEXT NOT NULL CHECK (kind IN ('upload', 'compacted')),
    state      TEXT NOT NULL CHECK (state IN ('active', 'retired')),
    rows       INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_files_day_state ON files(day, state);

CREATE TABLE IF NOT EXISTS compactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    day         TEXT NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('running', 'done', 'failed')),
    input_paths TEXT NOT NULL,          -- JSON array, snapshot taken at job start
    input_rows  INTEGER NOT NULL,
    output_path TEXT,
    output_rows INTEGER,
    error       TEXT,
    created_at  TEXT NOT NULL,
    finished_at TEXT
);
-- At most one running compaction per day (enforced by the DB itself).
CREATE UNIQUE INDEX IF NOT EXISTS ux_compactions_one_running_per_day
    ON compactions(day) WHERE status = 'running';

-- Idempotent upload registry: one row per committed client batch.
-- ``sha256`` is the hex digest of the raw CSV bytes; ``file_name``/``rows``
-- are the receipt of the first registration and are kept even after the
-- parquet file itself is retired and reclaimed by compaction.
CREATE TABLE IF NOT EXISTS upload_batches (
    batch_id   TEXT PRIMARY KEY,
    day        TEXT NOT NULL,
    sha256     TEXT NOT NULL,
    file_name  TEXT NOT NULL,
    rows       INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    """Open a short-lived connection with sane durability/concurrency pragmas."""
    conn = sqlite3.connect(str(db_path), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    # FULL so a committed upload / compaction switch survives a power cut.
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_schema(db_path: str) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()

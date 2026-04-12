"""
database_schema.py

DDL statements for the Pickfair SQLite schema.
Extracted from Database._init_db() to keep database.py focused on
data-access logic rather than schema declaration.

Usage inside Database._init_db():
    from database_schema import SCHEMA_DDL
    with self.transaction() as conn:
        for stmt in SCHEMA_DDL:
            conn.execute(stmt)
"""

from __future__ import annotations

SCHEMA_DDL: tuple[str, ...] = (
    # ── core settings ──────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    """,

    # ── telegram ───────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS telegram_chats (
        chat_id TEXT PRIMARY KEY,
        title TEXT DEFAULT '',
        is_active INTEGER NOT NULL DEFAULT 1
    )
    """,

    # ── incoming signals ───────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS received_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        selection TEXT DEFAULT '',
        action TEXT DEFAULT '',
        price REAL NOT NULL DEFAULT 0.0,
        stake REAL NOT NULL DEFAULT 0.0,
        status TEXT DEFAULT '',
        signal_json TEXT NOT NULL DEFAULT '{}',
        received_at TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,

    # ── outbound telegram log ──────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS telegram_outbox_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        chat_id TEXT,
        message_text TEXT,
        status TEXT DEFAULT '',
        created_at TEXT NOT NULL
    )
    """,

    # ── signal pattern rules ───────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS signal_patterns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        label TEXT NOT NULL DEFAULT '',
        pattern TEXT NOT NULL DEFAULT '',
        enabled INTEGER NOT NULL DEFAULT 1,
        bet_side TEXT DEFAULT '',
        market_type TEXT DEFAULT 'MATCH_ODDS',
        selection_template TEXT DEFAULT '',
        min_minute INTEGER,
        max_minute INTEGER,
        min_score INTEGER,
        max_score INTEGER,
        live_only INTEGER NOT NULL DEFAULT 0,
        priority INTEGER NOT NULL DEFAULT 100,
        extra_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,

    # ── simulation ─────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS simulation_state (
        state_key TEXT PRIMARY KEY,
        state_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS simulation_bets (
        bet_id TEXT PRIMARY KEY,
        market_id TEXT NOT NULL,
        selection_id TEXT NOT NULL,
        side TEXT NOT NULL DEFAULT 'BACK',
        price REAL NOT NULL DEFAULT 0.0,
        size REAL NOT NULL DEFAULT 0.0,
        matched_size REAL NOT NULL DEFAULT 0.0,
        avg_price_matched REAL NOT NULL DEFAULT 0.0,
        status TEXT NOT NULL DEFAULT 'EXECUTABLE',
        event_key TEXT DEFAULT '',
        table_id INTEGER,
        batch_id TEXT DEFAULT '',
        event_name TEXT DEFAULT '',
        market_name TEXT DEFAULT '',
        runner_name TEXT DEFAULT '',
        payload_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,

    # ── sagas / orders ─────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS order_saga (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_ref TEXT NOT NULL UNIQUE,
        batch_id TEXT DEFAULT '',
        event_key TEXT DEFAULT '',
        table_id INTEGER,
        market_id TEXT NOT NULL,
        selection_id TEXT NOT NULL,
        bet_type TEXT NOT NULL,
        price REAL NOT NULL DEFAULT 0.0,
        stake REAL NOT NULL DEFAULT 0.0,
        status TEXT NOT NULL DEFAULT 'PENDING',
        bet_id TEXT DEFAULT '',
        error_text TEXT DEFAULT '',
        payload_json TEXT DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_ref TEXT NOT NULL,
        correlation_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'INFLIGHT',
        payload_json TEXT NOT NULL DEFAULT '{}',
        response_json TEXT,
        outcome TEXT,
        reason TEXT,
        last_error TEXT,
        ambiguity_reason TEXT,
        finalized INTEGER NOT NULL DEFAULT 0,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts REAL NOT NULL,
        event_json TEXT NOT NULL DEFAULT '{}'
    )
    """,

    # ── dutching ───────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS dutching_batches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT NOT NULL UNIQUE,
        event_key TEXT NOT NULL,
        market_id TEXT NOT NULL,
        event_name TEXT DEFAULT '',
        market_name TEXT DEFAULT '',
        table_id INTEGER,
        strategy TEXT DEFAULT 'DUTCHING',
        status TEXT NOT NULL DEFAULT 'PENDING',
        total_legs INTEGER NOT NULL DEFAULT 0,
        placed_legs INTEGER NOT NULL DEFAULT 0,
        matched_legs INTEGER NOT NULL DEFAULT 0,
        failed_legs INTEGER NOT NULL DEFAULT 0,
        cancelled_legs INTEGER NOT NULL DEFAULT 0,
        batch_exposure REAL NOT NULL DEFAULT 0.0,
        avg_profit REAL NOT NULL DEFAULT 0.0,
        book_pct REAL NOT NULL DEFAULT 0.0,
        payload_json TEXT DEFAULT '{}',
        notes TEXT DEFAULT '',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        closed_at TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS dutching_batch_legs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id TEXT NOT NULL,
        leg_index INTEGER NOT NULL,
        customer_ref TEXT DEFAULT '',
        market_id TEXT NOT NULL,
        selection_id TEXT NOT NULL,
        side TEXT NOT NULL DEFAULT 'BACK',
        price REAL NOT NULL DEFAULT 0.0,
        stake REAL NOT NULL DEFAULT 0.0,
        liability REAL NOT NULL DEFAULT 0.0,
        bet_id TEXT DEFAULT '',
        status TEXT NOT NULL DEFAULT 'CREATED',
        error_text TEXT DEFAULT '',
        raw_response_json TEXT DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(batch_id, leg_index)
    )
    """,

    # ── observability ──────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS observability_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at REAL NOT NULL,
        payload_json TEXT NOT NULL DEFAULT '{}'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS diagnostics_exports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at REAL NOT NULL,
        export_path TEXT NOT NULL
    )
    """,

    # ── indexes ────────────────────────────────────────────────────────
    "CREATE INDEX IF NOT EXISTS idx_received_signals_created_at ON received_signals(created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_signal_patterns_enabled_priority ON signal_patterns(enabled, priority, id)",
    "CREATE INDEX IF NOT EXISTS idx_order_saga_status ON order_saga(status)",
    "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)",
    "CREATE INDEX IF NOT EXISTS idx_orders_customer_ref ON orders(customer_ref)",
    "CREATE INDEX IF NOT EXISTS idx_orders_correlation_id ON orders(correlation_id)",
    "CREATE INDEX IF NOT EXISTS idx_order_saga_batch_id ON order_saga(batch_id)",
    "CREATE INDEX IF NOT EXISTS idx_order_saga_event_key ON order_saga(event_key)",
    "CREATE INDEX IF NOT EXISTS idx_dutching_batches_status ON dutching_batches(status)",
    "CREATE INDEX IF NOT EXISTS idx_dutching_legs_batch_id ON dutching_batch_legs(batch_id)",
    "CREATE INDEX IF NOT EXISTS idx_observability_snapshots_created_at ON observability_snapshots(created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_diagnostics_exports_created_at ON diagnostics_exports(created_at DESC)",
)

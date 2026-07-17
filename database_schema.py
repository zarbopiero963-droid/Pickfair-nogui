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

    # ── telegram multi-bot (Bot API, epica #374 PR-3) ──────────────────
    # Persistenza di N bot, ognuno col proprio bot_token (CIFRATO a riposo,
    # formato enc:v1: via SecretCipher — la cifratura NON e' automatica su una
    # colonna reale: la fa esplicitamente il CRUD in database.py) e il proprio
    # set di chat. Tabelle ADDITIVE: `telegram_chats` legacy resta intatta, il
    # runtime single-bot non cambia (nessun wiring in PR-3).
    """
    CREATE TABLE IF NOT EXISTS telegram_bots (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        label      TEXT NOT NULL DEFAULT '',
        bot_token  TEXT NOT NULL DEFAULT '',
        is_active  INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS telegram_bot_chats (
        bot_id     INTEGER NOT NULL REFERENCES telegram_bots(id) ON DELETE CASCADE,
        chat_id    TEXT NOT NULL,
        title      TEXT DEFAULT '',
        is_active  INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (bot_id, chat_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_telegram_bot_chats_bot ON telegram_bot_chats(bot_id)",

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

    # ── (A) CATALOGO CACHE — volatile, rinfrescato ogni giorno ──────────
    """
    CREATE TABLE IF NOT EXISTS bf_events (
        event_id          TEXT PRIMARY KEY,
        name              TEXT NOT NULL,
        competition_id    TEXT,
        competition_name  TEXT,
        event_type_id     TEXT NOT NULL,
        open_date         TEXT,
        last_seen         TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_bf_events_name ON bf_events(name)",
    "CREATE INDEX IF NOT EXISTS ix_bf_events_open ON bf_events(open_date)",
    """
    CREATE TABLE IF NOT EXISTS bf_markets (
        market_id         TEXT PRIMARY KEY,
        event_id          TEXT NOT NULL REFERENCES bf_events(event_id) ON DELETE CASCADE,
        market_name       TEXT,
        market_type       TEXT,
        total_matched     REAL,
        open_date         TEXT,
        last_seen         TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_bf_markets_event ON bf_markets(event_id)",
    "CREATE INDEX IF NOT EXISTS ix_bf_markets_type  ON bf_markets(market_type)",
    """
    CREATE TABLE IF NOT EXISTS bf_runners (
        market_id         TEXT NOT NULL REFERENCES bf_markets(market_id) ON DELETE CASCADE,
        selection_id      TEXT NOT NULL,
        runner_name       TEXT,
        handicap          REAL DEFAULT 0,
        sort_priority     INTEGER,
        last_seen         TEXT NOT NULL,
        PRIMARY KEY (market_id, selection_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_bf_runners_name ON bf_runners(market_id, runner_name)",
    """
    CREATE TABLE IF NOT EXISTS sync_meta (
        id                INTEGER PRIMARY KEY CHECK (id = 1),
        last_sync_at      TEXT,
        last_sync_id      TEXT,
        events_count      INTEGER DEFAULT 0,
        markets_count     INTEGER DEFAULT 0,
        runners_count     INTEGER DEFAULT 0
    )
    """,

    # ── (B) MAPPE DUREVOLI — tenute per sempre ─────────────────────────
    """
    CREATE TABLE IF NOT EXISTS providers (
        id    INTEGER PRIMARY KEY AUTOINCREMENT,
        name  TEXT NOT NULL UNIQUE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS name_aliases (
        provider_id   INTEGER NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
        country       TEXT,
        betfair_name  TEXT NOT NULL,
        alias_norm    TEXT NOT NULL,
        PRIMARY KEY (provider_id, alias_norm)
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_name_aliases_betfair ON name_aliases(provider_id, betfair_name)",
    """
    CREATE TABLE IF NOT EXISTS market_aliases (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        provider_id     INTEGER NOT NULL REFERENCES providers(id) ON DELETE CASCADE,
        start_after     TEXT,
        end_before      TEXT,
        phrase_norm     TEXT NOT NULL,
        market_type     TEXT NOT NULL,
        market_name     TEXT,
        selection_name  TEXT
    )
    """,
    "CREATE INDEX IF NOT EXISTS ix_market_aliases_provider ON market_aliases(provider_id)",

    # ── Parser personalizzati (v2) ─────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS parsers (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        name          TEXT NOT NULL UNIQUE,
        provider_id   INTEGER REFERENCES providers(id) ON DELETE SET NULL,
        definition    TEXT NOT NULL,
        enabled       INTEGER NOT NULL DEFAULT 1
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS parser_by_chat (
        chat_id    TEXT PRIMARY KEY,
        parser_id  INTEGER NOT NULL REFERENCES parsers(id) ON DELETE CASCADE
    )
    """,

    # ── signal pattern rules (Legacy / Simple) ─────────────────────────
    """
    CREATE TABLE IF NOT EXISTS signal_patterns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        label TEXT NOT NULL DEFAULT '',
        pattern TEXT NOT NULL DEFAULT '',
        enabled INTEGER NOT NULL DEFAULT 1,
        action TEXT NOT NULL DEFAULT 'QUICK_BET',
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
        logical_key TEXT DEFAULT '',
        reason_code TEXT DEFAULT '',
        outcome TEXT DEFAULT '',
        matched_size REAL DEFAULT 0.0,
        avg_price_matched REAL DEFAULT 0.0,
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

    # ── risk history ───────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS risk_position_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_key TEXT DEFAULT '',
        market_id TEXT DEFAULT '',
        selection_id TEXT DEFAULT '',
        side TEXT DEFAULT '',
        stake REAL NOT NULL DEFAULT 0.0,
        net_pnl REAL NOT NULL DEFAULT 0.0,
        outcome TEXT DEFAULT '',
        table_id INTEGER,
        payload_json TEXT NOT NULL DEFAULT '{}',
        closed_at TEXT NOT NULL,
        created_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_risk_position_history_closed_at ON risk_position_history(closed_at DESC)",

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

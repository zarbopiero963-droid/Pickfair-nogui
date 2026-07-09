"""
betfair_market_api.py  —  Pickfair Market Sync API
====================================================
Avvia un server REST locale (FastAPI) che:
  • usa il BetfairClient esistente nel repo (cert login, keepalive, circuit breaker)
  • estende il client con listEvents + listMarketCatalogue
  • sincronizza in background ogni SYNC_INTERVAL secondi
  • salva tutto su SQLite (o PostgreSQL se DATABASE_URL è impostato)
  • espone endpoint REST per leggere eventi, mercati e squadre

Avvio: python betfair_market_api.py
       oppure doppio click su AVVIA_API.bat (Windows)

Configurazione: .env nella stessa cartella (vedi .env.example)
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── Carica .env se presente ────────────────────────────────────────────────────
_ENV_FILE = Path(__file__).parent / ".env"
if _ENV_FILE.exists():
    for _line in _ENV_FILE.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

# ── Config ─────────────────────────────────────────────────────────────────────
BF_USERNAME       = os.getenv("BF_USERNAME", "")
BF_PASSWORD       = os.getenv("BF_PASSWORD", "")
BF_APP_KEY        = os.getenv("BF_APP_KEY", "")
BF_CERT_PEM       = os.getenv("BF_CERT_PEM", "client-2048.crt")   # path al certificato
BF_KEY_PEM        = os.getenv("BF_KEY_PEM",  "client-2048.key")   # path alla chiave privata
SYNC_INTERVAL     = int(os.getenv("SYNC_INTERVAL", "120"))          # secondi tra sync
API_HOST          = os.getenv("API_HOST", "0.0.0.0")
API_PORT          = int(os.getenv("API_PORT", "8765"))
DATABASE_URL      = os.getenv("DATABASE_URL", "")                   # PostgreSQL opzionale
DB_FILE           = os.getenv("DB_FILE", "pickfair_markets.db")     # SQLite fallback

# Sport da sincronizzare (1 = Soccer/Football)
EVENT_TYPE_IDS    = os.getenv("EVENT_TYPE_IDS", "1").split(",")
# Quante ore nel futuro includere
HOURS_AHEAD       = int(os.getenv("HOURS_AHEAD", "48"))

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("betfair_market_api.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("bf_market_api")

# ── Import BetfairClient dal repo ──────────────────────────────────────────────
_REPO_ROOT = Path(__file__).parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from betfair_client import BetfairClient as _BaseBetfairClient
    log.info("✅ BetfairClient importato dal repo.")
except ImportError as _e:
    log.critical("❌ Impossibile importare BetfairClient: %s", _e)
    log.critical("   Assicurati di eseguire questo script dalla root del repo Pickfair-nogui.")
    sys.exit(1)


# ── Estende BetfairClient con metodi di discovery ──────────────────────────────
class MarketBetfairClient(_BaseBetfairClient):
    """Aggiunge listEvents, listMarketCatalogue e listMarketBook batch."""

    def list_events(
        self,
        event_type_ids: List[str],
        *,
        in_play_only: bool = False,
        max_results: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Ritorna tutti gli eventi per gli sport richiesti."""
        filter_: Dict[str, Any] = {
            "eventTypeIds": event_type_ids,
        }
        if in_play_only:
            filter_["inPlayOnly"] = True
        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listEvents",
            {"filter": filter_, "maxResults": max_results},
        )
        return result if isinstance(result, list) else []

    def list_market_catalogue(
        self,
        event_type_ids: List[str],
        event_ids: Optional[List[str]] = None,
        *,
        max_results: int = 5000,
    ) -> List[Dict[str, Any]]:
        """Ritorna il catalogo mercati con runner, competition e orario."""
        filter_: Dict[str, Any] = {"eventTypeIds": event_type_ids}
        if event_ids:
            filter_["eventIds"] = event_ids
        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listMarketCatalogue",
            {
                "filter": filter_,
                "marketProjection": [
                    "EVENT",
                    "COMPETITION",
                    "MARKET_START_TIME",
                    "RUNNER_DESCRIPTION",
                    "MARKET_DESCRIPTION",
                ],
                "sort": "FIRST_TO_START",
                "maxResults": max_results,
            },
        )
        return result if isinstance(result, list) else []

    def list_in_play_markets(self) -> List[Dict[str, Any]]:
        """Ritorna tutti i mercati attualmente in gioco."""
        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listMarketCatalogue",
            {
                "filter": {
                    "eventTypeIds": EVENT_TYPE_IDS,
                    "inPlayOnly": True,
                },
                "marketProjection": [
                    "EVENT",
                    "COMPETITION",
                    "MARKET_START_TIME",
                    "RUNNER_DESCRIPTION",
                    "MARKET_DESCRIPTION",
                ],
                "sort": "FIRST_TO_START",
                "maxResults": 1000,
            },
        )
        return result if isinstance(result, list) else []

    def list_event_types(self) -> List[Dict[str, Any]]:
        """Ritorna tutti i tipi di sport disponibili su Betfair."""
        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listEventTypes",
            {"filter": {}},
        )
        return result if isinstance(result, list) else []

    def list_competitions(
        self, event_type_ids: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Ritorna tutte le competizioni disponibili."""
        f: Dict[str, Any] = {}
        if event_type_ids:
            f["eventTypeIds"] = event_type_ids
        result = self._post_jsonrpc(
            self.BETTING_URL,
            "SportsAPING/v1.0/listCompetitions",
            {"filter": f},
        )
        return result if isinstance(result, list) else []


# ── Database ───────────────────────────────────────────────────────────────────
class Database:
    """
    Usa PostgreSQL se DATABASE_URL è impostato, altrimenti SQLite locale.
    """

    def __init__(self):
        self._use_pg = bool(DATABASE_URL)
        if self._use_pg:
            import psycopg2
            from psycopg2.extras import execute_values
            self._psycopg2 = psycopg2
            self._execute_values = execute_values
            self._conn = psycopg2.connect(DATABASE_URL)
            self._conn.autocommit = False
            log.info("🗄️  Database: PostgreSQL (%s)", DATABASE_URL[:30] + "...")
        else:
            self._conn = sqlite3.connect(DB_FILE, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            log.info("🗄️  Database: SQLite (%s)", DB_FILE)
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self):
        ddl = self._sqlite_ddl() if not self._use_pg else self._pg_ddl()
        with self._lock:
            cur = self._conn.cursor()
            cur.executescript(ddl) if not self._use_pg else [
                cur.execute(stmt) for stmt in ddl.split(";") if stmt.strip()
            ]
            self._conn.commit()
        log.info("📦 Schema DB inizializzato.")

    @staticmethod
    def _sqlite_ddl() -> str:
        return """
        CREATE TABLE IF NOT EXISTS bf_events (
            event_id        TEXT PRIMARY KEY,
            event_name      TEXT,
            competition_id  TEXT,
            competition     TEXT,
            country_code    TEXT,
            venue           TEXT,
            open_date       TEXT,
            is_live         INTEGER DEFAULT 0,
            updated_at      TEXT
        );
        CREATE TABLE IF NOT EXISTS bf_markets (
            market_id       TEXT PRIMARY KEY,
            event_id        TEXT,
            market_name     TEXT,
            market_type     TEXT,
            status          TEXT,
            in_play         INTEGER DEFAULT 0,
            total_matched   REAL DEFAULT 0,
            start_time      TEXT,
            updated_at      TEXT,
            FOREIGN KEY(event_id) REFERENCES bf_events(event_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS bf_runners (
            runner_id       INTEGER,
            market_id       TEXT,
            runner_name     TEXT,
            sort_priority   INTEGER DEFAULT 0,
            PRIMARY KEY(runner_id, market_id),
            FOREIGN KEY(market_id) REFERENCES bf_markets(market_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS bf_teams (
            team_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            team_name       TEXT UNIQUE NOT NULL,
            first_seen      TEXT,
            last_seen       TEXT
        );
        CREATE TABLE IF NOT EXISTS bf_competitions (
            competition_id  TEXT PRIMARY KEY,
            competition_name TEXT,
            region          TEXT,
            market_count    INTEGER DEFAULT 0,
            updated_at      TEXT
        );
        """

    @staticmethod
    def _pg_ddl() -> str:
        return """
        CREATE TABLE IF NOT EXISTS bf_events (
            event_id        TEXT PRIMARY KEY,
            event_name      TEXT,
            competition_id  TEXT,
            competition     TEXT,
            country_code    TEXT,
            venue           TEXT,
            open_date       TIMESTAMPTZ,
            is_live         BOOLEAN DEFAULT FALSE,
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS bf_markets (
            market_id       TEXT PRIMARY KEY,
            event_id        TEXT REFERENCES bf_events(event_id) ON DELETE CASCADE,
            market_name     TEXT,
            market_type     TEXT,
            status          TEXT,
            in_play         BOOLEAN DEFAULT FALSE,
            total_matched   NUMERIC(16,2) DEFAULT 0,
            start_time      TIMESTAMPTZ,
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS bf_runners (
            runner_id       BIGINT,
            market_id       TEXT REFERENCES bf_markets(market_id) ON DELETE CASCADE,
            runner_name     TEXT,
            sort_priority   INT DEFAULT 0,
            PRIMARY KEY (runner_id, market_id)
        );
        CREATE TABLE IF NOT EXISTS bf_teams (
            team_id         SERIAL PRIMARY KEY,
            team_name       TEXT UNIQUE NOT NULL,
            first_seen      TIMESTAMPTZ DEFAULT NOW(),
            last_seen       TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE TABLE IF NOT EXISTS bf_competitions (
            competition_id  TEXT PRIMARY KEY,
            competition_name TEXT,
            region          TEXT,
            market_count    INTEGER DEFAULT 0,
            updated_at      TIMESTAMPTZ DEFAULT NOW()
        )
        """

    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def upsert_events(self, rows: list):
        if not rows:
            return
        now = self._now()
        with self._lock:
            cur = self._conn.cursor()
            for r in rows:
                if self._use_pg:
                    cur.execute("""
                        INSERT INTO bf_events
                            (event_id,event_name,competition_id,competition,country_code,venue,open_date,is_live,updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT (event_id) DO UPDATE SET
                            event_name=EXCLUDED.event_name,
                            competition_id=EXCLUDED.competition_id,
                            competition=EXCLUDED.competition,
                            country_code=EXCLUDED.country_code,
                            venue=EXCLUDED.venue,
                            open_date=EXCLUDED.open_date,
                            is_live=EXCLUDED.is_live,
                            updated_at=NOW()
                    """, r)
                else:
                    cur.execute("""
                        INSERT OR REPLACE INTO bf_events
                            (event_id,event_name,competition_id,competition,country_code,venue,open_date,is_live,updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?)
                    """, (*r, now))
            self._conn.commit()

    def upsert_markets(self, rows: list):
        if not rows:
            return
        now = self._now()
        with self._lock:
            cur = self._conn.cursor()
            for r in rows:
                if self._use_pg:
                    cur.execute("""
                        INSERT INTO bf_markets
                            (market_id,event_id,market_name,market_type,status,in_play,total_matched,start_time,updated_at)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                        ON CONFLICT (market_id) DO UPDATE SET
                            event_id=EXCLUDED.event_id,
                            market_name=EXCLUDED.market_name,
                            market_type=EXCLUDED.market_type,
                            status=EXCLUDED.status,
                            in_play=EXCLUDED.in_play,
                            total_matched=EXCLUDED.total_matched,
                            start_time=EXCLUDED.start_time,
                            updated_at=NOW()
                    """, r)
                else:
                    cur.execute("""
                        INSERT OR REPLACE INTO bf_markets
                            (market_id,event_id,market_name,market_type,status,in_play,total_matched,start_time,updated_at)
                        VALUES (?,?,?,?,?,?,?,?,?)
                    """, (*r, now))
            self._conn.commit()

    def upsert_runners(self, rows: list):
        if not rows:
            return
        with self._lock:
            cur = self._conn.cursor()
            for r in rows:
                if self._use_pg:
                    cur.execute("""
                        INSERT INTO bf_runners (runner_id,market_id,runner_name,sort_priority)
                        VALUES (%s,%s,%s,%s)
                        ON CONFLICT (runner_id,market_id) DO UPDATE SET
                            runner_name=EXCLUDED.runner_name,
                            sort_priority=EXCLUDED.sort_priority
                    """, r)
                else:
                    cur.execute("""
                        INSERT OR REPLACE INTO bf_runners (runner_id,market_id,runner_name,sort_priority)
                        VALUES (?,?,?,?)
                    """, r)
            self._conn.commit()

    def upsert_teams(self, names: List[str]):
        if not names:
            return
        now = self._now()
        with self._lock:
            cur = self._conn.cursor()
            for name in names:
                if self._use_pg:
                    cur.execute("""
                        INSERT INTO bf_teams (team_name,first_seen,last_seen)
                        VALUES (%s,NOW(),NOW())
                        ON CONFLICT (team_name) DO UPDATE SET last_seen=NOW()
                    """, (name,))
                else:
                    cur.execute("""
                        INSERT INTO bf_teams (team_name,first_seen,last_seen)
                        VALUES (?,?,?)
                        ON CONFLICT(team_name) DO UPDATE SET last_seen=excluded.last_seen
                    """, (name, now, now))
            self._conn.commit()

    def upsert_competitions(self, rows: list):
        if not rows:
            return
        now = self._now()
        with self._lock:
            cur = self._conn.cursor()
            for r in rows:
                if self._use_pg:
                    cur.execute("""
                        INSERT INTO bf_competitions (competition_id,competition_name,region,market_count,updated_at)
                        VALUES (%s,%s,%s,%s,NOW())
                        ON CONFLICT (competition_id) DO UPDATE SET
                            competition_name=EXCLUDED.competition_name,
                            region=EXCLUDED.region,
                            market_count=EXCLUDED.market_count,
                            updated_at=NOW()
                    """, r)
                else:
                    cur.execute("""
                        INSERT OR REPLACE INTO bf_competitions
                            (competition_id,competition_name,region,market_count,updated_at)
                        VALUES (?,?,?,?,?)
                    """, (*r, now))
            self._conn.commit()

    def delete_stale_events(self, current_ids: List[str]):
        if not current_ids:
            return
        with self._lock:
            cur = self._conn.cursor()
            if self._use_pg:
                cur.execute("""
                    DELETE FROM bf_events
                    WHERE event_id NOT IN %s
                      AND open_date < NOW() - INTERVAL '3 hours'
                """, (tuple(current_ids),))
            else:
                placeholders = ",".join("?" * len(current_ids))
                cur.execute(f"""
                    DELETE FROM bf_events
                    WHERE event_id NOT IN ({placeholders})
                      AND open_date < datetime('now', '-3 hours')
                """, current_ids)
            deleted = cur.rowcount
            self._conn.commit()
        if deleted:
            log.info("🗑️  Eliminati %d eventi scaduti.", deleted)

    def query(self, sql: str, params=None) -> List[Dict]:
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(sql, params or [])
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass


# ── Sync Engine ────────────────────────────────────────────────────────────────
_sync_state: Dict[str, Any] = {
    "last_sync": None,
    "last_error": None,
    "events_count": 0,
    "markets_count": 0,
    "teams_count": 0,
    "competitions_count": 0,
    "running": False,
}


def _do_sync(client: MarketBetfairClient, db: Database):
    log.info("🔄 Avvio sincronizzazione Betfair...")
    _sync_state["running"] = True

    # 1. Competizioni
    try:
        comps_raw = client.list_competitions(EVENT_TYPE_IDS)
        comp_rows = []
        for c in comps_raw:
            comp = c.get("competition", {})
            cid = comp.get("id")
            if cid:
                comp_rows.append((
                    cid,
                    comp.get("name", ""),
                    c.get("competitionRegion", ""),
                    c.get("marketCount", 0),
                ))
        db.upsert_competitions(comp_rows)
        _sync_state["competitions_count"] = len(comp_rows)
        log.info("🏆 Competizioni: %d", len(comp_rows))
    except Exception as e:
        log.warning("⚠️  Errore fetch competizioni: %s", e)

    # 2. Eventi (live + prossime 48h)
    try:
        events_raw = client.list_events(EVENT_TYPE_IDS, max_results=1000)
    except Exception as e:
        log.error("❌ Errore fetch eventi: %s", e)
        _sync_state["last_error"] = str(e)
        _sync_state["running"] = False
        return

    event_rows = []
    event_ids = []
    for item in events_raw:
        ev = item.get("event", {})
        comp = item.get("competition", {})
        eid = ev.get("id")
        if not eid:
            continue
        event_ids.append(eid)
        event_rows.append((
            eid,
            ev.get("name", ""),
            comp.get("id", ""),
            comp.get("name", ""),
            ev.get("countryCode", ""),
            ev.get("venue", ""),
            ev.get("openDate", ""),
            0,  # is_live aggiornato dopo
        ))

    db.upsert_events(event_rows)
    _sync_state["events_count"] = len(event_rows)
    log.info("📅 Eventi: %d", len(event_rows))

    # 3. Mercati in gioco + imposta is_live
    try:
        live_markets = client.list_in_play_markets()
        live_event_ids = {
            m.get("event", {}).get("id")
            for m in live_markets
            if m.get("event")
        }
        if live_event_ids:
            db_conn = db._conn
            with db._lock:
                cur = db_conn.cursor()
                if db._use_pg:
                    cur.execute(
                        "UPDATE bf_events SET is_live=TRUE WHERE event_id=ANY(%s)",
                        (list(live_event_ids),)
                    )
                else:
                    phs = ",".join("?" * len(live_event_ids))
                    cur.execute(
                        f"UPDATE bf_events SET is_live=1 WHERE event_id IN ({phs})",
                        list(live_event_ids)
                    )
                db_conn.commit()
    except Exception as e:
        log.warning("⚠️  Errore fetch live: %s", e)

    # 4. Catalogo mercati (in batch da 500 eventi)
    all_market_rows = []
    all_runner_rows = []
    all_teams: set = set()

    batch_size = 500
    for i in range(0, len(event_ids), batch_size):
        batch = event_ids[i : i + batch_size]
        try:
            markets_raw = client.list_market_catalogue(
                EVENT_TYPE_IDS, event_ids=batch, max_results=5000
            )
        except Exception as e:
            log.warning("⚠️  Errore batch mercati %d-%d: %s", i, i + batch_size, e)
            continue

        for m in markets_raw:
            mid = m.get("marketId")
            ev = m.get("event", {})
            desc = m.get("description") or {}
            if not mid:
                continue

            mtype = desc.get("marketType", "")
            status = desc.get("status", "ACTIVE")
            in_play = bool(desc.get("inPlay", False))
            start = m.get("marketStartTime", "")
            total_matched = float(m.get("totalMatched") or 0)

            all_market_rows.append((
                mid,
                ev.get("id", ""),
                m.get("marketName", ""),
                mtype,
                status,
                1 if in_play else 0,
                total_matched,
                start,
            ))

            for runner in m.get("runners") or []:
                rname = runner.get("runnerName", "")
                rid = runner.get("selectionId")
                if rid:
                    all_runner_rows.append((
                        rid,
                        mid,
                        rname,
                        runner.get("sortPriority", 0),
                    ))
                if rname and rname.lower() not in ("the draw", "draw", ""):
                    all_teams.add(rname)

    db.upsert_markets(all_market_rows)
    db.upsert_runners(all_runner_rows)
    db.upsert_teams(list(all_teams))
    db.delete_stale_events(event_ids)

    _sync_state["markets_count"] = len(all_market_rows)
    _sync_state["teams_count"] = len(all_teams)
    _sync_state["last_sync"] = datetime.now(timezone.utc).isoformat()
    _sync_state["last_error"] = None
    _sync_state["running"] = False

    log.info(
        "✅ Sync completata — Eventi: %d | Mercati: %d | Runner: %d | Squadre: %d",
        len(event_rows), len(all_market_rows), len(all_runner_rows), len(all_teams),
    )


def _sync_loop(client: MarketBetfairClient, db: Database):
    """Thread di sync in background."""
    while True:
        try:
            _do_sync(client, db)
        except Exception as e:
            log.error("❌ Errore sync loop: %s", e)
            _sync_state["last_error"] = str(e)
            _sync_state["running"] = False
        log.info("⏳ Prossima sync tra %ds...", SYNC_INTERVAL)
        time.sleep(SYNC_INTERVAL)


# ── FastAPI REST Server ────────────────────────────────────────────────────────
def build_app(db: Database, client: MarketBetfairClient):
    try:
        from fastapi import FastAPI, Query
        from fastapi.middleware.cors import CORSMiddleware
    except ImportError:
        log.critical("❌ FastAPI non installato. Esegui: pip install fastapi uvicorn")
        sys.exit(1)

    app = FastAPI(
        title="Pickfair Market API",
        description="API interna che espone dati Betfair Exchange sincronizzati in tempo reale.",
        version="1.0.0",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/status", summary="Stato della sincronizzazione")
    def status():
        return {
            "ok": True,
            "connected": client.connected,
            **_sync_state,
            "sync_interval_seconds": SYNC_INTERVAL,
        }

    @app.post("/sync", summary="Forza una sincronizzazione immediata")
    def force_sync():
        if _sync_state["running"]:
            return {"ok": False, "message": "Sync già in corso."}
        t = threading.Thread(target=_do_sync, args=(client, db), daemon=True)
        t.start()
        return {"ok": True, "message": "Sync avviata."}

    @app.get("/events", summary="Lista eventi football")
    def get_events(
        live_only: bool = Query(False, description="Solo partite live"),
        limit: int = Query(500, le=5000),
        offset: int = Query(0),
    ):
        where = "WHERE is_live=1" if live_only else ""
        if db._use_pg:
            where = "WHERE is_live=TRUE" if live_only else ""
        rows = db.query(
            f"SELECT * FROM bf_events {where} ORDER BY open_date ASC LIMIT ? OFFSET ?"
            if not db._use_pg else
            f"SELECT * FROM bf_events {where} ORDER BY open_date ASC LIMIT %s OFFSET %s",
            [limit, offset],
        )
        return {"count": len(rows), "events": rows}

    @app.get("/events/{event_id}", summary="Dettaglio evento con mercati")
    def get_event(event_id: str):
        evs = db.query(
            "SELECT * FROM bf_events WHERE event_id=?" if not db._use_pg
            else "SELECT * FROM bf_events WHERE event_id=%s",
            [event_id],
        )
        if not evs:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Evento non trovato")
        markets = db.query(
            "SELECT * FROM bf_markets WHERE event_id=? ORDER BY market_name" if not db._use_pg
            else "SELECT * FROM bf_markets WHERE event_id=%s ORDER BY market_name",
            [event_id],
        )
        return {"event": evs[0], "markets": markets}

    @app.get("/markets", summary="Lista mercati")
    def get_markets(
        in_play: bool = Query(False, description="Solo mercati in gioco"),
        market_type: str = Query("", description="Filtro tipo mercato (es. MATCH_ODDS)"),
        event_id: str = Query("", description="Filtro per event_id"),
        limit: int = Query(500, le=5000),
        offset: int = Query(0),
    ):
        conds, params = [], []
        ph = "?" if not db._use_pg else "%s"
        if in_play:
            conds.append(f"in_play={'1' if not db._use_pg else 'TRUE'}")
        if market_type:
            conds.append(f"market_type={ph}")
            params.append(market_type.upper())
        if event_id:
            conds.append(f"event_id={ph}")
            params.append(event_id)
        where = ("WHERE " + " AND ".join(conds)) if conds else ""
        params += [limit, offset]
        rows = db.query(
            f"SELECT * FROM bf_markets {where} ORDER BY start_time ASC LIMIT {ph} OFFSET {ph}",
            params,
        )
        return {"count": len(rows), "markets": rows}

    @app.get("/markets/{market_id}", summary="Dettaglio mercato con runner")
    def get_market(market_id: str):
        ph = "?" if not db._use_pg else "%s"
        mkts = db.query(f"SELECT * FROM bf_markets WHERE market_id={ph}", [market_id])
        if not mkts:
            from fastapi import HTTPException
            raise HTTPException(status_code=404, detail="Mercato non trovato")
        runners = db.query(
            f"SELECT * FROM bf_runners WHERE market_id={ph} ORDER BY sort_priority",
            [market_id],
        )
        return {"market": mkts[0], "runners": runners}

    @app.get("/teams", summary="Lista squadre conosciute")
    def get_teams(
        search: str = Query("", description="Cerca per nome"),
        limit: int = Query(200, le=2000),
        offset: int = Query(0),
    ):
        ph = "?" if not db._use_pg else "%s"
        if search:
            like = f"%{search}%"
            op = "LIKE" if not db._use_pg else "ILIKE"
            rows = db.query(
                f"SELECT * FROM bf_teams WHERE team_name {op} {ph} ORDER BY last_seen DESC LIMIT {ph} OFFSET {ph}",
                [like, limit, offset],
            )
        else:
            rows = db.query(
                f"SELECT * FROM bf_teams ORDER BY last_seen DESC LIMIT {ph} OFFSET {ph}",
                [limit, offset],
            )
        return {"count": len(rows), "teams": rows}

    @app.get("/competitions", summary="Lista competizioni")
    def get_competitions():
        rows = db.query("SELECT * FROM bf_competitions ORDER BY market_count DESC")
        return {"count": len(rows), "competitions": rows}

    @app.get("/summary", summary="Riepilogo database")
    def get_summary():
        ph = "?" if not db._use_pg else "%s"
        total_events = db.query("SELECT COUNT(*) as n FROM bf_events")[0]["n"]
        live_events  = db.query(
            f"SELECT COUNT(*) as n FROM bf_events WHERE is_live={'1' if not db._use_pg else 'TRUE'}"
        )[0]["n"]
        total_markets = db.query("SELECT COUNT(*) as n FROM bf_markets")[0]["n"]
        live_markets  = db.query(
            f"SELECT COUNT(*) as n FROM bf_markets WHERE in_play={'1' if not db._use_pg else 'TRUE'}"
        )[0]["n"]
        total_teams   = db.query("SELECT COUNT(*) as n FROM bf_teams")[0]["n"]
        total_comps   = db.query("SELECT COUNT(*) as n FROM bf_competitions")[0]["n"]
        return {
            "events": {"total": total_events, "live": live_events},
            "markets": {"total": total_markets, "live": live_markets},
            "teams": total_teams,
            "competitions": total_comps,
            "last_sync": _sync_state["last_sync"],
            "last_error": _sync_state["last_error"],
        }

    return app


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("  Pickfair Market API  —  avvio...")
    log.info("=" * 60)

    # Validazione config
    missing = []
    if not BF_USERNAME:   missing.append("BF_USERNAME")
    if not BF_PASSWORD:   missing.append("BF_PASSWORD")
    if not BF_APP_KEY:    missing.append("BF_APP_KEY")
    if not Path(BF_CERT_PEM).exists(): missing.append(f"BF_CERT_PEM (file: {BF_CERT_PEM})")
    if not Path(BF_KEY_PEM).exists():  missing.append(f"BF_KEY_PEM  (file: {BF_KEY_PEM})")
    if missing:
        log.critical("❌ Configurazione mancante: %s", ", ".join(missing))
        log.critical("   Crea o completa il file .env (vedi .env.example)")
        sys.exit(1)

    # Crea client Betfair
    client = MarketBetfairClient(
        username=BF_USERNAME,
        app_key=BF_APP_KEY,
        cert_pem=BF_CERT_PEM,
        key_pem=BF_KEY_PEM,
    )

    # Login iniziale
    log.info("🔐 Login Betfair come '%s'...", BF_USERNAME)
    try:
        result = client.login(password=BF_PASSWORD)
        if not result.get("connected"):
            log.critical("❌ Login fallito: %s", result)
            sys.exit(1)
        log.info("✅ Login riuscito. Token valido fino a: %s", result.get("expiry", "N/A"))
    except Exception as e:
        log.critical("❌ Errore login: %s", e)
        sys.exit(1)

    # Crea database
    db = Database()

    # Prima sync immediata
    try:
        _do_sync(client, db)
    except Exception as e:
        log.error("❌ Prima sync fallita: %s. Il server si avvia comunque.", e)

    # Thread sync in background
    sync_thread = threading.Thread(target=_sync_loop, args=(client, db), daemon=True)
    sync_thread.start()
    log.info("🔄 Thread sync avviato (ogni %ds).", SYNC_INTERVAL)

    # Avvia FastAPI
    try:
        import uvicorn
    except ImportError:
        log.critical("❌ uvicorn non installato. Esegui: pip install uvicorn")
        sys.exit(1)

    app = build_app(db, client)

    log.info("🚀 API server su http://%s:%d", API_HOST, API_PORT)
    log.info("📖 Docs: http://localhost:%d/docs", API_PORT)
    log.info("-" * 60)
    log.info("  Endpoints disponibili:")
    log.info("  GET  /summary        — riepilogo")
    log.info("  GET  /events         — eventi (?live_only=true)")
    log.info("  GET  /events/{id}    — evento + mercati")
    log.info("  GET  /markets        — mercati (?in_play=true&market_type=MATCH_ODDS)")
    log.info("  GET  /markets/{id}   — mercato + runner")
    log.info("  GET  /teams          — squadre (?search=juventus)")
    log.info("  GET  /competitions   — competizioni")
    log.info("  POST /sync           — forza sync immediata")
    log.info("  GET  /status         — stato sync")
    log.info("-" * 60)

    uvicorn.run(
        app,
        host=API_HOST,
        port=API_PORT,
        log_level="info",
        access_log=False,
    )


if __name__ == "__main__":
    main()

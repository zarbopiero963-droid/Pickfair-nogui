"""Proiezioni di SOLA LETTURA dello stato del runtime per la Control API (ponte del core).

Appartiene a Pickfair-nogui: è l'unico punto che legge il ``RuntimeController``. Il server,
l'autenticazione e il contratto sono nel package ``pickfair_control_api`` (repository
pickfair-mcp-), che non conosce il motore.

Vincoli (verificati dai test in tests/control_api/):
- nessuna chiamata a ``RuntimeController.get_status()``: quel metodo legge i fondi Betfair,
  aggiorna lo stato della cassa e fa girare il monitor della perdita giornaliera, quindi NON
  è una lettura pura;
- ogni attributo è letto una sola volta e controllato per tipo; tipo inatteso o attributo
  assente => il campo è ``None`` ed elencato in ``unavailable`` (mai un default);
- i motivi testuali del core diventano token MAIUSCOLI: un testo libero diventa
  ``UNSTRUCTURED`` e non esce mai;
- nessuna eccezione esce da qui: una lettura fallita rende il campo indisponibile.

Limite noto: i campi non sono letti in un'unica sezione critica del runtime (prenderne il
lock rallenterebbe il motore). Ogni campo è coerente in sé; due campi letti in istanti
vicini possono riflettere stati consecutivi. ``observed_at`` dichiara l'istante della lettura.
"""

from __future__ import annotations

import math
import re
import time
from collections.abc import Callable
from typing import Any, Final

CODE_RE: Final = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
UNSTRUCTURED: Final = "UNSTRUCTURED"
MAX_CODES: Final = 32

RUNTIME_STATES: Final = frozenset({"STOPPED", "ACTIVE", "PAUSED", "LOCKDOWN"})
EXECUTION_MODES: Final = {"SIMULATION": "SIM", "LIVE": "LIVE"}
READINESS_LEVELS: Final = frozenset({"READY", "DEGRADED", "NOT_READY"})

CONSISTENCY: Final = "PER_FIELD_NON_ATOMIC"  # status e reconciliation: letture campo per campo
_MISSING: Final = object()


class _Unavailable(Exception):
    """Il campo non è dimostrabile dal runtime."""


def to_code(value: object) -> str:
    """Token MAIUSCOLO dal prefisso prima di «:»; altrimenti UNSTRUCTURED."""
    if not isinstance(value, str):
        return UNSTRUCTURED
    head = value.strip().split(":", 1)[0]
    return head if CODE_RE.fullmatch(head) else UNSTRUCTURED


def _optional_code(value: object) -> str | None:
    if value is None or value == "":
        return None
    return to_code(value)


def _attr(obj: object, name: str) -> Any:
    try:
        value = vars(obj).get(name, _MISSING)
    except TypeError:
        value = _MISSING
    if value is _MISSING:
        raise _Unavailable(name)
    return value


def _bool(obj: object, name: str) -> bool:
    value = _attr(obj, name)
    if type(value) is not bool:
        raise _Unavailable(name)
    return value


def _count(value: object) -> int:
    if type(value) is not int or value < 0:
        raise _Unavailable("count")
    return value


def _collect(fields: dict[str, Callable[[], Any]]) -> tuple[dict[str, Any], list[str]]:
    values: dict[str, Any] = {}
    unavailable: list[str] = []
    for name, read in fields.items():
        try:
            values[name] = read()
        except Exception:  # noqa: BLE001 - qualunque guasto rende il campo indisponibile
            values[name] = None
        if values[name] is None:
            unavailable.append(name)
    return values, unavailable


# ---------------------------------------------------------------- GET /v1/status


def _runtime_state(rc: object) -> str:
    value = getattr(_attr(rc, "mode"), "value", None)
    if value not in RUNTIME_STATES:
        raise _Unavailable("mode")
    return value


def _execution_mode(rc: object) -> str:
    raw = _attr(rc, "execution_mode")
    mapped = EXECUTION_MODES.get(raw) if isinstance(raw, str) else None
    if mapped is None:
        raise _Unavailable("execution_mode")
    return mapped


def _readiness(rc: object) -> dict[str, Any]:
    report = rc.evaluate_live_readiness()  # type: ignore[attr-defined]
    if not isinstance(report, dict):
        raise _Unavailable("readiness")
    ready, level, blockers = report.get("ready"), report.get("level"), report.get("blockers")
    if type(ready) is not bool or level not in READINESS_LEVELS or not isinstance(blockers, list):
        raise _Unavailable("readiness")
    codes = sorted({to_code(b) for b in blockers})
    if len(codes) > MAX_CODES or ready != (level == "READY") or (ready and codes):
        raise _Unavailable("readiness")
    # La readiness MCP-aware (PF-API-0) non esiste ancora nel core: dichiarato, non simulato.
    return {"level": level, "ready": ready, "blockers": codes, "mcp_aware": False}


def _stop(rc: object) -> dict[str, Any]:
    reason = _attr(rc, "_emergency_reason")
    risk_reason = _attr(rc, "_risk_stop_reason")
    if not isinstance(reason, str) or not isinstance(risk_reason, str):
        raise _Unavailable("stop")
    kill = rc._is_kill_switch_active()  # type: ignore[attr-defined]
    if type(kill) is not bool:
        raise _Unavailable("stop")
    return {
        "emergency_stopped": _bool(rc, "_emergency_stopped"),
        "emergency_reason": _optional_code(reason),
        "risk_stop_reason": _optional_code(risk_reason),
        "daily_loss_pending_stop": _bool(rc, "_daily_loss_pending_stop"),
        "kill_switch_active": kill,
    }


def status_view(rc: object, *, clock: Callable[[], float] = time.time) -> dict[str, Any]:
    values, unavailable = _collect(
        {
            "runtime_state": lambda: _runtime_state(rc),
            "execution_mode": lambda: _execution_mode(rc),
            "live_enabled": lambda: _bool(rc, "live_enabled"),
            "readiness": lambda: _readiness(rc),
            "stop": lambda: _stop(rc),
            "engine": lambda: _engine_diagnostics(rc),
            "protections": lambda: protections_view(rc),
        }
    )
    return {
        "source": "core",
        "observed_at": float(clock()),
        "consistency": CONSISTENCY,
        **values,
        "unavailable": unavailable,
    }


# ------------------------------------------------- GET /v1/reconciliation/status


def _startup(rc: object) -> dict[str, Any]:
    required = vars(rc).get("_reconciliation_required", _MISSING)
    if required is _MISSING:
        # La riconciliazione d'avvio avviene in start(): prima non è mai stata eseguita.
        return {"state": "NOT_RUN", "reason": None, "recovery_failure": None}
    if type(required) is not bool:
        raise _Unavailable("startup")
    rebuild = vars(rc).get("_last_position_rebuild", {})
    failure = vars(rc).get("_startup_recovery_failure", "")
    reason = rebuild.get("reason") if isinstance(rebuild, dict) and required else None
    return {
        "state": "REQUIRED" if required else "OK",
        "reason": _optional_code(reason) if required else None,
        "recovery_failure": _optional_code(failure),
    }


def _engine(rc: object) -> object:
    engine = _attr(rc, "reconciliation_engine")
    if engine is None:
        raise _Unavailable("reconciliation_engine")
    return engine


def _ambiguities(rc: object) -> int:
    snapshot = _engine(rc).ambiguity_snapshot()  # type: ignore[attr-defined]
    if not isinstance(snapshot, dict):
        raise _Unavailable("ambiguities")
    return len(snapshot)


def _ghosts(rc: object) -> dict[str, int]:
    evidence = _engine(rc).ghost_evidence_snapshot()  # type: ignore[attr-defined]
    if not isinstance(evidence, dict):
        raise _Unavailable("ghosts")
    return {
        "ghost_orders": _count(evidence.get("ghost_orders_count")),
        "suspected_ghosts": _count(evidence.get("suspected_ghost_count")),
        "unconfirmed_inflight": _count(evidence.get("unconfirmed_inflight_count")),
    }


def _recovery_state(rc: object) -> dict[str, bool]:
    return {
        "unreadable": _bool(rc, "_recovery_state_unreadable"),
        "persist_failed": _bool(rc, "_recovery_persist_failed"),
    }


def reconciliation_view(rc: object, *, clock: Callable[[], float] = time.time) -> dict[str, Any]:
    values, unavailable = _collect(
        {
            "startup": lambda: _startup(rc),
            "ambiguities_pending": lambda: _ambiguities(rc),
            "ghosts": lambda: _ghosts(rc),
            "hedge_registry_unreliable": lambda: _bool(rc, "_hedged_markets_unreliable"),
            "recovery_state": lambda: _recovery_state(rc),
            "settlement": lambda: _settlement(rc),
        }
    )
    return {
        "source": "core",
        "observed_at": float(clock()),
        "consistency": CONSISTENCY,
        **values,
        "unavailable": unavailable,
    }


# ------------------------------------------------- MCP-01e: GET /v1/account e GET /v1/exposure
#
# Contratto 1.2.0, consistenza PER_SECTION_LOCKED: le sezioni vengono da chiamate del core sotto il
# lock del componente che le possiede, preso UNA volta per vista: `bankroll` e `cash` nella stessa
# acquisizione del lock del RiskDesk; `tables`, `total_exposure` e `recovery` nella stessa
# acquisizione del lock dei tavoli. Lo snapshot dei fondi è un solo valore sostituito per intero.
#
# Le letture non attendono il motore: il lock è richiesto con un'attesa massima di LOCK_WAIT_S;
# se il runtime lo tiene più a lungo (es. controllo-e-modifica con scrittura durevole), le sezioni
# di quel lock sono indisponibili (null + `unavailable`), mai bloccate né stimate. Nessuna lettura dell'exchange, nessun DB, nessun
# metodo che modifica stato (vietati: get_status, pending_exposure, read_funds_*, snapshot del
# broker SIM, che aggiorna cache interne). Importi riportati come li tiene il core, mai ricalcolati.

SECTION_CONSISTENCY: Final = "PER_SECTION_LOCKED"
MONEY_LIMIT: Final = 1_000_000_000.0
TABLE_STATUSES: Final = frozenset({"FREE", "ACTIVE", "RECOVERY", "LOCKED"})
DESK_MODES: Final = frozenset({"NORMAL", "EXPANSION", "DEFENSE", "LOCKDOWN"})
IDENTIFIER_RE: Final = re.compile(r"^[0-9A-Za-z._-]{1,32}$")
MAX_TABLES: Final = 256
MAX_FUNDS_AGE_S: Final = 31_536_000.0
LOCK_WAIT_S: Final = 0.25


def _acquire(lock: object) -> bool:
    acquire = getattr(lock, "acquire", None)
    if not callable(acquire):
        return False
    return bool(acquire(timeout=LOCK_WAIT_S))


def _under(lock: object, fields: dict[str, Callable[[], Any]]) -> tuple[dict[str, Any], list[str]]:
    """`_collect` sotto il lock dato, con attesa limitata; lock non ottenuto => tutto indisponibile."""
    try:
        acquired = _acquire(lock)
    except Exception:  # noqa: BLE001
        acquired = False
    if not acquired:
        return {name: None for name in fields}, list(fields)
    try:
        return _collect(fields)
    finally:
        lock.release()  # type: ignore[attr-defined]


def _desk_lock(rc: object) -> object:
    try:
        return vars(_attr(rc, "risk_desk")).get("_lock")
    except Exception:  # noqa: BLE001
        return None


def _tables_lock(rc: object) -> object:
    try:
        return _attr(rc, "table_manager").locked()
    except Exception:  # noqa: BLE001
        return None


def _money(value: object, *, non_negative: bool = False) -> float:
    if type(value) not in (int, float):  # bool escluso: type() esatto
        raise _Unavailable("money")
    number = float(value)  # type: ignore[arg-type]
    if not math.isfinite(number) or abs(number) > MONEY_LIMIT or (non_negative and number < 0):
        raise _Unavailable("money")
    return number


def _bankroll(rc: object) -> dict[str, float]:
    snap = _attr(rc, "risk_desk").debug_snapshot()  # una sola lettura sotto il lock del RiskDesk
    if not isinstance(snap, dict):
        raise _Unavailable("bankroll")
    out = {
        name: _money(snap.get(name))
        for name in (
            "bankroll_start",
            "bankroll_current",
            "equity_peak",
            "equity_current",
            "realized_pnl",
            "unrealized_pnl",
        )
    }
    drawdown = _money(snap.get("drawdown_pct"), non_negative=True)
    if drawdown > 100.0:
        raise _Unavailable("drawdown")
    out["drawdown_pct"] = drawdown
    return out


def _cash(rc: object) -> dict[str, Any]:
    desk = _attr(rc, "risk_desk")
    basis = desk.cash_basis()  # cassa e impegnato della STESSA lettura, sotto lock
    if not isinstance(basis, tuple) or len(basis) != 3:
        raise _Unavailable("cash")
    cash, committed, _read_at = basis
    verified = cash is not None
    reason = None
    if not verified:
        raw = vars(desk).get("cash_equity_reason")
        reason = _optional_code(raw) if isinstance(raw, str) else None
    return {
        "verified": verified,
        "cash_equity": _money(cash, non_negative=True) if verified else None,
        "committed_exposure": _money(committed, non_negative=True),
        "unverified_reason": reason,
    }


def _exchange_funds(rc: object, monotonic: Callable[[], float]) -> dict[str, Any]:
    snap = _attr(rc, "_account_snapshot")  # esiste solo dopo la prima lettura dei fondi
    read_at = vars(rc).get("_account_snapshot_read_at")
    if not isinstance(snap, dict) or type(snap.get("ok")) is not bool:
        raise _Unavailable("exchange_funds")
    ok = snap["ok"]
    source = snap.get("source")
    if not isinstance(source, str):
        raise _Unavailable("exchange_funds")
    reason = None if ok else (_optional_code(snap.get("reason")) or UNSTRUCTURED)
    age = None
    if type(read_at) in (int, float) and math.isfinite(read_at):
        age = max(0.0, float(monotonic()) - float(read_at))
        if age > MAX_FUNDS_AGE_S:
            age = None
    return {
        "ok": ok,
        "available": _money(snap.get("available"), non_negative=True),
        "committed": _money(snap.get("committed"), non_negative=True),
        "cash_equity": _money(snap.get("cash_equity"), non_negative=True),
        "reason": reason,
        "source": to_code(source.upper()),
        "age_s": age,
    }


def _desk_mode(rc: object, bankroll: dict[str, float] | None) -> str:
    if bankroll is None:
        raise _Unavailable("desk_mode")
    # Stessa funzione e stessi argomenti di RuntimeController._desk_mode(), ma sui valori della
    # sezione `bankroll` (letti insieme sotto lock): desk mode coerente con i numeri riportati.
    mode = _attr(rc, "mm").determine_desk_mode(
        bankroll_current=bankroll["bankroll_current"], equity_peak=bankroll["equity_peak"]
    )
    value = getattr(mode, "value", None)
    if value not in DESK_MODES:
        raise _Unavailable("desk_mode")
    return value


def account_view(
    rc: object,
    *,
    clock: Callable[[], float] = time.time,
    monotonic: Callable[[], float] = time.monotonic,
) -> dict[str, Any]:
    mode, mode_missing = _collect({"execution_mode": lambda: _execution_mode(rc)})
    desk, desk_missing = _under(
        _desk_lock(rc), {"bankroll": lambda: _bankroll(rc), "cash": lambda: _cash(rc)}
    )
    funds, funds_missing = _collect({"exchange_funds": lambda: _exchange_funds(rc, monotonic)})
    derived, derived_missing = _collect({"desk_mode": lambda: _desk_mode(rc, desk["bankroll"])})
    return {
        "source": "core",
        "observed_at": float(clock()),
        "consistency": SECTION_CONSISTENCY,
        **mode,
        **desk,
        **funds,
        **derived,
        "unavailable": mode_missing + desk_missing + funds_missing + derived_missing,
    }


def _identifier(value: object) -> str | None:
    if value == "":
        return None
    if not isinstance(value, str) or not IDENTIFIER_RE.fullmatch(value):
        raise _Unavailable("identifier")
    return value


def _tables(rc: object) -> list[dict[str, Any]]:
    rows = _attr(rc, "table_manager").snapshot()  # tutti i tavoli sotto il lock dei tavoli
    if not isinstance(rows, list) or len(rows) > MAX_TABLES:
        raise _Unavailable("tables")
    tables = []
    for row in rows:
        if not isinstance(row, dict):
            raise _Unavailable("tables")
        table_id, status = row.get("table_id"), row.get("status")
        event_key, in_recovery = row.get("current_event_key"), row.get("in_recovery")
        if type(table_id) is not int or not 1 <= table_id <= 10_000:
            raise _Unavailable("tables")
        if status not in TABLE_STATUSES or not isinstance(event_key, str):
            raise _Unavailable("tables")
        if type(in_recovery) is not bool:
            raise _Unavailable("tables")
        tables.append(
            {
                "table_id": table_id,
                "status": status,
                "occupied": bool(event_key),  # il nome dell'evento (testo libero) non esce
                "in_recovery": in_recovery,
                "market_id": _identifier(row.get("market_id")),
                "selection_id": _identifier(row.get("selection_id")),
                "current_exposure": _money(row.get("current_exposure")),
                "loss_amount": _money(row.get("loss_amount")),
                "sl_tp": _sl_tp(row.get("meta")),  # solo le chiavi SL/TP: il resto del meta non esce
            }
        )
    if len({t["table_id"] for t in tables}) != len(tables):
        raise _Unavailable("tables")
    return sorted(tables, key=lambda t: t["table_id"])


def _total_exposure(rc: object) -> float:
    # Stesso valore del cap max_open_exposure: non finito nel core (stato corrotto) => indisponibile.
    return _money(_attr(rc, "table_manager").total_exposure(), non_negative=True)


def _recovery(rc: object) -> dict[str, Any]:
    config = _attr(rc, "config")
    active = rc._active_recovery_count()  # type: ignore[attr-defined]  # fail-closed del core
    maximum = getattr(config, "max_recovery_tables", None)
    allow = getattr(config, "allow_recovery", None)
    if type(allow) is not bool:
        raise _Unavailable("recovery")
    return {
        "active_count": _count(active),
        "max_recovery_tables": _count(maximum),
        "allow_recovery": allow,
    }


def exposure_view(rc: object, *, clock: Callable[[], float] = time.time) -> dict[str, Any]:
    values, unavailable = _under(
        _tables_lock(rc),
        {
            "tables": lambda: _tables(rc),
            "total_exposure": lambda: _total_exposure(rc),
            "recovery": lambda: _recovery(rc),
            "_table_refs": lambda: _table_refs(rc),  # stessa acquisizione del lock: uso interno
        },
    )
    table_refs = values.pop("_table_refs")
    unavailable = [name for name in unavailable if name != "_table_refs"]
    return {
        "source": "core",
        "observed_at": float(clock()),
        "consistency": SECTION_CONSISTENCY,
        **values,
        "unavailable": unavailable,
        "sl_tp_closes": _sl_tp_closes(rc, table_refs),  # MCP-01i
    }


# ------------------------------------------------- MCP-01f: diagnostica, protezioni, posizioni
#
# Contratto 1.3.0. Regole in più:
# - importi e quote come stringhe decimali canoniche (`decimal_str`): lossless dal Decimal del
#   core; dal float del core la rappresentazione più corta che lo identifica (repr);
# - ogni protezione è ACTIVE/INACTIVE/TRIGGERED/UNKNOWN/UNAVAILABLE; una lettura fallita o un
#   lock occupato diventa UNKNOWN, mai ACTIVE; ciò che il core non espone è UNAVAILABLE;
# - le regole sono quelle del core, chiamate direttamente (`_entry_funds_reason`,
#   `validators.risk_pct_reason`, `risk_stop.exposure_stop_reason`, `_session_loss_block_reason`,
#   `determine_desk_mode`): il ponte non ricalcola soglie e non introduce limiti.

from datetime import datetime, timezone  # noqa: E402
from decimal import Decimal  # noqa: E402

DECIMAL_RE: Final = re.compile(
    r"^(?:-?[1-9][0-9]{0,14}(?:\.[0-9]{1,28})?|0(?:\.[0-9]{1,28})?|-0\.[0-9]{0,27}[1-9])$"
)
COMPONENT_RE: Final = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
ENGINE_STATES: Final = frozenset({"READY", "DEGRADED", "NOT_READY"})
CLIENT_KINDS: Final = frozenset({"SIMULATION", "LIVE"})
MAX_POSITIONS: Final = 512


def decimal_str(value: object) -> str:
    """Stringa decimale canonica (senza esponente, senza zeri finali); errore se non rappresentabile."""
    if isinstance(value, bool):
        raise _Unavailable("decimal")
    if isinstance(value, Decimal):
        number = value
    elif isinstance(value, int):
        number = Decimal(value)
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise _Unavailable("decimal")
        number = Decimal(repr(value))  # la stringa più corta che identifica il float del core
    else:
        raise _Unavailable("decimal")
    if not number.is_finite():
        raise _Unavailable("decimal")
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in ("-0", ""):
        text = "0"
    if not DECIMAL_RE.fullmatch(text):
        raise _Unavailable("decimal")
    return text


def _code_of(value: object) -> str:
    """Motivo del core (minuscolo, con dettagli dopo «:») come token MAIUSCOLO."""
    if not isinstance(value, str) or not value.strip():
        return UNSTRUCTURED
    return to_code(value.strip().split(":", 1)[0].upper().replace("-", "_"))


def _epoch(value: object) -> float | None:
    """Istante del core (ISO 8601 UTC senza fuso, oppure epoch) come epoch; None se ignoto."""
    if type(value) in (int, float) and math.isfinite(value) and 0 < value < 1e11:  # type: ignore[operator]
        return float(value)  # type: ignore[arg-type]
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        stamp = parsed.timestamp()
        return stamp if 0 < stamp < 1e11 else None
    return None


# ---------------------------------------------------------------- diagnostica del motore


def _trading_engine(rc: object) -> dict[str, Any]:
    engine = _attr(rc, "trading_engine")
    if engine is None:
        raise _Unavailable("trading_engine")
    report = engine.readiness()  # type: ignore[attr-defined]
    health = report.get("health") if isinstance(report, dict) else None
    state = report.get("state") if isinstance(report, dict) else None
    if state not in ENGINE_STATES or not isinstance(health, dict) or len(health) > 32:
        raise _Unavailable("trading_engine")
    components = []
    for name, entry in sorted(health.items()):
        if not isinstance(name, str) or not COMPONENT_RE.fullmatch(name):
            raise _Unavailable("trading_engine")
        if not isinstance(entry, dict) or entry.get("state") not in ENGINE_STATES:
            raise _Unavailable("trading_engine")
        reason = entry.get("reason")
        components.append(
            {"name": name, "state": entry["state"],
             "reason": None if reason is None else _code_of(reason)}
        )
    if state == "READY" and any(c["state"] != "READY" for c in components):
        raise _Unavailable("trading_engine")
    wired = engine.risk_gate_wired  # type: ignore[attr-defined]
    if type(wired) is not bool:
        raise _Unavailable("trading_engine")
    return {"state": state, "evaluated_at": "ENGINE_START", "risk_gate_wired": wired,
            "components": components}


def _service_state(rc: object) -> tuple[bool, bool, str, bool]:
    """Stato del servizio Betfair dai suoi attributi (nessun metodo: né rete né `get_client`).

    Stessa logica di `BetfairService.status()` / `get_client()`: in simulazione il cliente è il
    broker simulato, altrimenti il cliente LIVE; `connected` vale solo con un cliente presente.
    """
    service = vars(_attr(rc, "betfair_service"))
    simulated, connected = service.get("simulation_mode"), service.get("connected")
    if type(simulated) is not bool or type(connected) is not bool:
        raise _Unavailable("betfair")
    client = service.get("simulation_broker") if simulated else service.get("client")
    has_client = client is not None
    return connected and has_client, has_client, "SIMULATION" if simulated else "LIVE", bool(
        service.get("last_error")
    )


def _betfair(rc: object) -> dict[str, Any]:
    connected, has_client, kind, error_present = _service_state(rc)
    keep = _attr(rc, "betfair_service").keepalive_status()  # type: ignore[attr-defined]
    if not isinstance(keep, dict) or type(keep.get("running")) is not bool:
        raise _Unavailable("betfair")
    return {
        "connected": connected,
        "has_client": has_client,
        "client_kind": kind,
        "error_present": error_present,  # il testo dell'errore non esce
        "keepalive": {
            "running": keep["running"],
            "failure_count": _count(keep.get("failure_count")),
            "last_ok_at": _epoch(keep.get("last_ok_ts")),
            "error_present": bool(keep.get("last_error")),
        },
    }


def _stream(rc: object) -> dict[str, Any]:
    feed = vars(rc).get("streaming_feed")
    if feed is None:
        raise _Unavailable("stream")  # il feed esiste solo dopo l'avvio con streaming
    s = feed.status()
    if not isinstance(s, dict):
        raise _Unavailable("stream")
    flags = [s.get(k) for k in ("running", "connected", "degraded_503", "auth_degraded")]
    if any(type(f) is not bool for f in flags):
        raise _Unavailable("stream")
    return {
        "running": s["running"],
        "connected": s["connected"],
        "degraded": s["degraded_503"],
        "auth_degraded": s["auth_degraded"],
        "auth_failure_count": _count(s.get("auth_failure_count")),
        "keepalive_failure_count": _count(s.get("keepalive_failure_count")),
        "error_present": bool(s.get("last_auth_error") or s.get("last_transport_error")
                              or s.get("last_keepalive_error")),
        "last_message_at": _epoch(s.get("last_message_at")),
    }


def _io(rc: object) -> dict[str, Any]:
    io = rc.runtime_io_snapshot()  # type: ignore[attr-defined]
    if not isinstance(io, dict):
        raise _Unavailable("io")
    latency = io.get("last_latency_ms")
    if type(latency) not in (int, float) or not math.isfinite(latency) or not 0 <= latency <= 1e7:
        raise _Unavailable("io")
    return {
        "last_status": _code_of(io.get("last_status")),
        "total_count": _count(io.get("total_count")),
        "slow_count": _count(io.get("slow_count")),
        "degraded_count": _count(io.get("degraded_count")),
        "unavailable_count": _count(io.get("unavailable_count")),
        "last_latency_ms": float(latency),
        "error_present": bool(io.get("last_error")),
    }


def _event_bus(rc: object) -> dict[str, Any]:
    p = _attr(rc, "bus").pressure_snapshot()
    if not isinstance(p, dict) or type(p.get("running")) is not bool or type(p.get("accepting")) is not bool:
        raise _Unavailable("event_bus")
    since = p.get("seconds_since_last_dequeue")
    if since is not None and (type(since) not in (int, float) or not 0 <= since <= 1e9):
        raise _Unavailable("event_bus")
    return {
        "queue_depth": _count(p.get("queue_depth")),
        "queue_high_watermark": _count(p.get("queue_high_watermark")),
        "enqueued_total": _count(p.get("enqueued_total")),
        "dequeued_total": _count(p.get("dequeued_total")),
        "running": p["running"],
        "accepting": p["accepting"],
        "seconds_since_last_dequeue": None if since is None else float(since),
    }


def _queues(rc: object) -> dict[str, int]:
    inflight = _attr(rc, "_cashout_inflight")
    uncertain = _attr(rc, "_cashout_uncertain")
    if not isinstance(inflight, dict) or not isinstance(uncertain, set):
        raise _Unavailable("queues")
    return {
        "cashout_inflight": len(inflight),
        "cashout_uncertain": len(uncertain),
        "cashout_barriers": len(_attr(rc, "_cashout_barriers")),
        "direct_unmatched": len(rc.direct_unmatched_bet_ids),  # type: ignore[attr-defined]  # property
    }


def _modes(rc: object) -> dict[str, Any]:
    _connected, has_client, kind, _error = _service_state(rc)
    return {
        "configured_execution_mode": _execution_mode(rc),
        "simulation_flag": _bool(rc, "simulation_mode"),
        "live_enabled": _bool(rc, "live_enabled"),
        "live_readiness_ok": _bool(rc, "live_readiness_ok"),
        "client_kind": kind if has_client else "NONE",
        "read_only_mode": "NOT_IN_CORE",
    }


def _last_signal_at(rc: object) -> float | None:
    return _epoch(_attr(rc, "last_signal_at"))


def _engine_diagnostics(rc: object) -> dict[str, Any]:
    values, unavailable = _collect(
        {
            "trading_engine": lambda: _trading_engine(rc),
            "betfair": lambda: _betfair(rc),
            "stream": lambda: _stream(rc),
            "io": lambda: _io(rc),
            "event_bus": lambda: _event_bus(rc),
            "queues": lambda: _queues(rc),
            "modes": lambda: _modes(rc),
            "last_signal_at": lambda: _last_signal_at(rc),
        }
    )
    return {**values, "unavailable": unavailable}


# ---------------------------------------------------------------- protezioni finanziarie


def _p(state: str, reason: str | None = None, *, source: str, limit: str | None = None,
       as_of: float | None = None) -> dict[str, Any]:
    return {"state": state, "reason": reason, "source": source, "limit": limit, "as_of": as_of}


def _unknown(source: str, reason: str = "READ_FAILED") -> dict[str, Any]:
    return _p("UNKNOWN", reason, source=source)


def _config_value(rc: object, name: str) -> Any:
    return getattr(_attr(rc, "config"), name)


def _optional_cap(rc: object, name: str) -> dict[str, Any]:
    """Cap opzionale: None = non configurato (INACTIVE); valore illeggibile o <= 0 = UNKNOWN."""
    raw = _config_value(rc, name)
    if raw is None:
        return _p("INACTIVE", "NOT_CONFIGURED", source="CONFIG")
    from core import validators

    value = validators.finite_number(raw)
    if value is None or value <= 0:
        return _p("UNKNOWN", "INVALID_LIMIT", source="CONFIG")
    return _p("ACTIVE", source="CONFIG", limit=decimal_str(raw))


def _risk_pct(rc: object, name: str) -> dict[str, Any]:
    """Percentuale obbligatoria: il core nega ogni nuova entrata se assente o non valida."""
    from core import validators

    raw = _config_value(rc, name)
    reason = validators.risk_pct_reason(name, raw)
    if reason:
        return _p("TRIGGERED", _code_of(reason), source="CONFIG")
    return _p("ACTIVE", source="CONFIG", limit=decimal_str(raw))


def _kill_switch(rc: object) -> dict[str, Any]:
    safe_mode = vars(rc).get("safe_mode")
    if safe_mode is None:
        return _p("UNAVAILABLE", "SAFE_MODE_NOT_WIRED", source="NONE")
    active = rc._is_kill_switch_active()  # type: ignore[attr-defined]
    info = safe_mode.get_status_info() if callable(getattr(safe_mode, "get_status_info", None)) else {}
    as_of = _epoch(info.get("triggered_at")) if isinstance(info, dict) else None
    if type(active) is not bool:
        return _unknown("SAFE_MODE")
    if active:
        return _p("TRIGGERED", "SAFE_MODE_TRIGGERED", source="SAFE_MODE", as_of=as_of)
    return _p("ACTIVE", source="SAFE_MODE")


def _emergency(rc: object) -> dict[str, Any]:
    stopped = _bool(rc, "_emergency_stopped")
    if stopped:
        return _p("TRIGGERED", _code_of(_attr(rc, "_emergency_reason")), source="RUNTIME",
                  as_of=_epoch(vars(rc).get("_emergency_stopped_at")))
    return _p("ACTIVE", source="RUNTIME")


def _lockdown_reason(rc: object) -> str | None:
    """Motivo del LOCKDOWN del runtime, oppure None se il runtime non è in LOCKDOWN."""
    if getattr(_attr(rc, "mode"), "value", None) != "LOCKDOWN":
        return None
    return _code_of(vars(rc).get("last_error"))


def _runtime_lockdown(rc: object) -> dict[str, Any]:
    reason = _lockdown_reason(rc)
    if reason is not None:
        return _p("TRIGGERED", reason, source="RUNTIME")
    return _p("ACTIVE", source="RUNTIME")


def _desk_lockdown(rc: object) -> dict[str, Any]:
    desk = _attr(rc, "risk_desk")
    lock = vars(desk).get("_lock")
    if not _acquire(lock):
        return _unknown("MONEY_MANAGEMENT", "LOCK_BUSY")
    try:
        bankroll, peak = desk.bankroll_current, desk.equity_peak  # type: ignore[attr-defined]
    finally:
        lock.release()  # type: ignore[union-attr]
    mode = _attr(rc, "mm").determine_desk_mode(bankroll_current=bankroll, equity_peak=peak)
    limit = decimal_str(_config_value(rc, "lockdown_drawdown_pct"))
    if getattr(mode, "value", None) == "LOCKDOWN":
        return _p("TRIGGERED", "DESK_LOCKDOWN", source="MONEY_MANAGEMENT", limit=limit)
    if getattr(mode, "value", None) not in DESK_MODES:
        return _unknown("MONEY_MANAGEMENT")
    return _p("ACTIVE", source="MONEY_MANAGEMENT", limit=limit)


def _drawdown_stop(rc: object, name: str, trigger: str, *, live_only: bool) -> dict[str, Any]:
    cap = _optional_cap(rc, name)
    if cap["state"] != "ACTIVE":
        return cap
    if _lockdown_reason(rc) == trigger:
        return _p("TRIGGERED", trigger, source="RUNTIME", limit=cap["limit"])
    if live_only and _execution_mode(rc) != "LIVE":
        # Il core applica lo stop percentuale solo in LIVE (runtime_controller, Enforcement A1 #320).
        return _p("INACTIVE", "ENFORCED_ONLY_IN_LIVE", source="CONFIG", limit=cap["limit"])
    return cap


def _session_loss(rc: object) -> dict[str, Any]:
    raw = _config_value(rc, "max_session_loss")
    if raw is None:
        return _p("INACTIVE", "NOT_CONFIGURED", source="CONFIG")
    reason = rc._session_loss_block_reason()  # type: ignore[attr-defined]
    limit = decimal_str(raw) if not isinstance(raw, bool) and isinstance(raw, int | float) else None
    if not isinstance(reason, str):
        return _unknown("LOSS_LIMITS")
    if reason.startswith("session_loss_breached"):
        return _p("TRIGGERED", "SESSION_LOSS_BREACHED", source="LOSS_LIMITS", limit=limit)
    if reason:
        return _p("UNKNOWN", _code_of(reason), source="LOSS_LIMITS", limit=limit)
    return _p("ACTIVE", source="LOSS_LIMITS", limit=limit)


def _daily_loss(rc: object) -> dict[str, Any]:
    from core import loss_limits

    lock = vars(rc).get("_daily_loss_state_lock")
    if not _acquire(lock):
        return _unknown("DAILY_LOSS_MONITOR", "LOCK_BUSY")
    try:
        state = dict(_attr(rc, "_daily_loss_monitor_state"))
    finally:
        lock.release()  # type: ignore[union-attr]
    threshold = state.get("threshold")
    as_of = _epoch(state.get("last_checked_at"))
    if loss_limits.persist_failed(vars(rc).get("_daily_loss_persisted")):
        return _p("TRIGGERED", "DAILY_LOSS_STATE_NOT_PERSISTED", source="DAILY_LOSS_MONITOR",
                  as_of=as_of)
    if threshold is None:
        return _p("INACTIVE", "NOT_CONFIGURED", source="DAILY_LOSS_MONITOR", as_of=as_of)
    limit = decimal_str(threshold)
    if state.get("breached") is True or _bool(rc, "_daily_loss_pending_stop"):
        return _p("TRIGGERED", _code_of(state.get("last_status")), source="DAILY_LOSS_MONITOR",
                  limit=limit, as_of=_epoch(state.get("breached_at")) or as_of)
    if state.get("breached") is not False:
        return _unknown("DAILY_LOSS_MONITOR")
    return _p("ACTIVE", source="DAILY_LOSS_MONITOR", limit=limit, as_of=as_of)


def _risk_stop(rc: object) -> dict[str, Any]:
    reason = _attr(rc, "_risk_stop_reason")
    if not isinstance(reason, str):
        return _unknown("RUNTIME")
    if reason:
        return _p("TRIGGERED", _code_of(reason), source="RUNTIME")
    return _p("ACTIVE", source="RUNTIME")


def _exposure_stop(rc: object) -> dict[str, Any]:
    from core import risk_stop

    raw = _config_value(rc, "max_exposure_stop")
    if raw is None:
        return _p("INACTIVE", "NOT_CONFIGURED", source="CONFIG")
    tables = _attr(rc, "table_manager")
    lock = tables.locked()
    if not _acquire(lock):
        return _unknown("RISK_STOP", "LOCK_BUSY")
    try:
        total = tables.total_exposure()
    finally:
        lock.release()
    reason = risk_stop.exposure_stop_reason(raw, total)
    limit = decimal_str(raw) if not isinstance(raw, bool) and isinstance(raw, int | float) else None
    if reason.startswith("exposure_stop_reached"):
        return _p("TRIGGERED", "EXPOSURE_STOP_REACHED", source="RISK_STOP", limit=limit)
    if reason:
        return _p("UNKNOWN", _code_of(reason), source="RISK_STOP", limit=limit)
    return _p("ACTIVE", source="RISK_STOP", limit=limit)


def _funds_limit(rc: object, ttl: object) -> str | None:
    """Età massima dei fondi dichiarata come limite, senza ricalcolare la regola del core (MCP-01k).

    - accessore unico del core ``funds_max_age_s()`` (richiesto a Claude D per F-16): il suo valore;
    - soglia F-16 ``LIVE_FUNDS_MAX_AGE_S`` presente senza accessore: limite non dichiarato (None), perché
      quello effettivo è una combinazione decisa dal core e non va replicata qui;
    - altrimenti (``main`` attuale): il TTL delle tracce delle riserve, l'unico limite del core.
    Lo stato della protezione viene sempre e solo dalla regola del core (``_entry_funds_reason``).
    """
    cls = type(rc)
    accessor = getattr(cls, "funds_max_age_s", None)
    if callable(accessor):
        return decimal_str(accessor())
    if hasattr(cls, "LIVE_FUNDS_MAX_AGE_S"):
        return None
    return decimal_str(ttl)


def _funds_freshness(rc: object, monotonic: Callable[[], float]) -> dict[str, Any]:
    from core.table_manager import TableManager

    account = vars(rc).get("_account_snapshot")
    read_at = vars(rc).get("_account_snapshot_read_at")
    limit = _funds_limit(rc, TableManager.PLACED_TOMBSTONE_TTL_S)
    # Regola del core per le nuove entrate (riserve pendenti escluse: lettura pura).
    reason = rc._entry_funds_reason(account, 0.0, read_at=read_at)  # type: ignore[attr-defined]
    if not isinstance(reason, str):
        return _unknown("FUNDS_GATE")
    as_of = None
    if type(read_at) in (int, float) and math.isfinite(read_at):
        as_of_age = max(0.0, float(monotonic()) - float(read_at))
        as_of = time.time() - as_of_age
    if reason:
        return _p("TRIGGERED", _code_of(reason), source="FUNDS_GATE", limit=limit, as_of=as_of)
    if as_of is None:
        # Senza l'istante della lettura il core non controlla l'età dei fondi: non verificabile.
        return _p("UNKNOWN", "FUNDS_READ_TIME_UNKNOWN", source="FUNDS_GATE", limit=limit)
    return _p("ACTIVE", source="FUNDS_GATE", limit=limit, as_of=as_of)


def _book_guard(rc: object) -> dict[str, Any]:
    from core import validators

    raw = _config_value(rc, "book_block")
    value = validators.finite_number(raw)
    if value is None or value <= 0:
        return _unknown("CONFIG", "INVALID_LIMIT")
    return _p("ACTIVE", source="CONFIG", limit=decimal_str(raw))


def _anti_duplication(rc: object) -> dict[str, Any]:
    enabled = _config_value(rc, "anti_duplication_enabled")
    if type(enabled) is not bool:
        return _unknown("CONFIG")
    if not enabled:
        return _p("INACTIVE", "DISABLED_BY_CONFIG", source="CONFIG")
    return _p("ACTIVE", source="DUPLICATION_GUARD")


def _order_breaker(rc: object) -> dict[str, Any]:
    engine = vars(rc).get("trading_engine")
    breaker = vars(engine).get("_order_submission_breaker") if engine is not None else None
    if breaker is None:
        return _p("UNAVAILABLE", "ENGINE_NOT_WIRED", source="NONE")
    # Lettura per attributi: `snapshot()`/`is_open()` NON sono puri (OPEN -> HALF_OPEN).
    state = getattr(vars(breaker).get("state"), "value", None)
    opened_at = vars(breaker).get("opened_at")
    if state == "CLOSED":
        return _p("ACTIVE", source="CIRCUIT_BREAKER")
    if state in ("OPEN", "HALF_OPEN"):
        return _p("TRIGGERED", f"BREAKER_{state}", source="CIRCUIT_BREAKER",
                  as_of=_epoch(opened_at))
    return _unknown("CIRCUIT_BREAKER")


def _recovery_guard(rc: object) -> dict[str, Any]:
    unreadable = vars(rc).get("_recovery_state_unreadable", False)
    failed = vars(rc).get("_recovery_persist_failed", _MISSING)
    if unreadable is True:
        return _p("TRIGGERED", "RECOVERY_STATE_UNREADABLE", source="RUNTIME")
    if failed is True:
        return _p("TRIGGERED", "RECOVERY_STATE_NOT_PERSISTED", source="RUNTIME")
    if failed is _MISSING:
        # Nasce solo alla prima scrittura dello stato di recovery: prima non è dimostrabile.
        return _p("UNKNOWN", "RECOVERY_PERSIST_STATE_NOT_INITIALIZED", source="RUNTIME")
    if unreadable is not False or failed is not False:
        return _unknown("RUNTIME")
    return _p("ACTIVE", source="RUNTIME")


PROTECTION_ORDER: Final = (
    "kill_switch", "emergency_stop", "runtime_lockdown", "desk_lockdown",
    "drawdown_pct_hard_stop", "drawdown_abs_hard_stop", "session_loss", "daily_loss",
    "risk_stop", "exposure_stop", "max_open_exposure", "max_order_exposure",
    "max_market_exposure", "max_event_exposure_abs", "max_total_exposure_pct",
    "max_event_exposure_pct", "funds_freshness", "book_guard", "market_tradability",
    "anti_duplication", "order_circuit_breaker", "recovery_state",
)
UNVERIFIABLE: Final = frozenset({"UNKNOWN", "UNAVAILABLE"})


def protections_view(rc: object, *, monotonic: Callable[[], float] = time.monotonic) -> dict[str, Any]:
    readers: dict[str, Callable[[], dict[str, Any]]] = {
        "kill_switch": lambda: _kill_switch(rc),
        "emergency_stop": lambda: _emergency(rc),
        "runtime_lockdown": lambda: _runtime_lockdown(rc),
        "desk_lockdown": lambda: _desk_lockdown(rc),
        "drawdown_pct_hard_stop": lambda: _drawdown_stop(
            rc, "max_drawdown_hard_stop_pct", "DRAWDOWN_HARD_STOP_TRIGGERED", live_only=True),
        "drawdown_abs_hard_stop": lambda: _drawdown_stop(
            rc, "max_drawdown_abs", "DRAWDOWN_ABS_HARD_STOP_TRIGGERED", live_only=False),
        "session_loss": lambda: _session_loss(rc),
        "daily_loss": lambda: _daily_loss(rc),
        "risk_stop": lambda: _risk_stop(rc),
        "exposure_stop": lambda: _exposure_stop(rc),
        "max_open_exposure": lambda: _optional_cap(rc, "max_open_exposure"),
        "max_order_exposure": lambda: _optional_cap(rc, "max_order_exposure"),
        "max_market_exposure": lambda: _optional_cap(rc, "max_market_exposure"),
        "max_event_exposure_abs": lambda: _optional_cap(rc, "max_event_exposure_abs"),
        "max_total_exposure_pct": lambda: _risk_pct(rc, "max_total_exposure_pct"),
        "max_event_exposure_pct": lambda: _risk_pct(rc, "max_event_exposure_pct"),
        "funds_freshness": lambda: _funds_freshness(rc, monotonic),
        "book_guard": lambda: _book_guard(rc),
        # Stato negoziabile del mercato: correzione F-23 in corso nel core (Claude D).
        "market_tradability": lambda: _p("UNAVAILABLE", "CORE_PENDING_F23", source="NONE"),
        "anti_duplication": lambda: _anti_duplication(rc),
        "order_circuit_breaker": lambda: _order_breaker(rc),
        "recovery_state": lambda: _recovery_guard(rc),
    }
    items: dict[str, dict[str, Any]] = {}
    for name in PROTECTION_ORDER:
        try:
            item = readers[name]()
        except Exception:  # noqa: BLE001 - una lettura fallita è UNKNOWN, mai ACTIVE
            item = _unknown("RUNTIME")
        if item["state"] == "ACTIVE" and item["reason"] is not None:
            item = _unknown(item["source"])
        items[name] = item
    return {
        "items": items,
        "disabled": sorted(n for n, v in items.items() if v["state"] == "INACTIVE"),
        "unverifiable": sorted(n for n, v in items.items() if v["state"] in UNVERIFIABLE),
    }


# ---------------------------------------------------------------- GET /v1/positions


def _position_row(position: object, ledger: object) -> dict[str, Any]:
    if not isinstance(position, dict):
        raise _Unavailable("positions")
    selection = position.get("selection_id")
    selection = str(selection) if type(selection) is int else selection
    table_id = position.get("table_id")
    if table_id is not None and (type(table_id) is not int or not 1 <= table_id <= 10_000):
        raise _Unavailable("positions")
    row: dict[str, Any] = {
        "market_id": _identifier(position.get("market_id")),
        "selection_id": _identifier(selection),
        "table_id": table_id,
        "unrealized_basis": "LAST_MARK",
    }
    if row["market_id"] is None or row["selection_id"] is None:
        raise _Unavailable("positions")
    if ledger is not None:
        size = vars(ledger)["_open_size"]
        side = vars(ledger)["_open_side"]
        exposure, liability = ledger._residual_exposure_liability()  # type: ignore[attr-defined]
        if side not in ("BACK", "LAY", "") or not isinstance(size, Decimal):
            raise _Unavailable("positions")
        flat = side == "" or size <= 0
        row.update(
            side="FLAT" if flat else side,
            open_size=decimal_str(size),
            avg_entry_price=decimal_str(ledger._avg_entry_price()),  # type: ignore[attr-defined]
            realized_pnl=decimal_str(vars(ledger)["_realized_pnl"]),
            unrealized_pnl=decimal_str(vars(ledger)["_unrealized_pnl"]),
            exposure=decimal_str(exposure),
            liability=decimal_str(liability),
            amounts_source="LEDGER_DECIMAL",
            unavailable=[],
        )
        return row
    side = position.get("side")
    if side not in ("BACK", "LAY"):
        raise _Unavailable("positions")
    row.update(
        side=side,
        open_size=decimal_str(position.get("stake")),
        avg_entry_price=decimal_str(position.get("price")),
        realized_pnl=None,
        unrealized_pnl=None,
        exposure=None,
        liability=None,
        amounts_source="CORE_FLOAT",
        unavailable=["realized_pnl", "unrealized_pnl", "exposure", "liability"],
    )
    return row


def _positions(rc: object) -> list[dict[str, Any]]:
    engine = _attr(rc, "pnl_engine")
    positions = vars(engine)["_positions"]
    ledgers = vars(engine)["_position_ledgers"]
    if not isinstance(positions, dict) or not isinstance(ledgers, dict) or len(positions) > MAX_POSITIONS:
        raise _Unavailable("positions")
    # Chiavi interne (event_key, customer_ref, reservation_id, batch_id) non escono.
    return [_position_row(pos, ledgers.get(key)) for key, pos in positions.items()]


def _pnl_lock(rc: object) -> object:
    try:
        return vars(_attr(rc, "pnl_engine")).get("_state_lock")
    except Exception:  # noqa: BLE001
        return None


def positions_view(rc: object, *, clock: Callable[[], float] = time.time) -> dict[str, Any]:
    values, unavailable = _under(_pnl_lock(rc), {"positions": lambda: _positions(rc)})
    return {
        "source": "core",
        "observed_at": float(clock()),
        "consistency": SECTION_CONSISTENCY,
        **values,
        "unavailable": unavailable,
    }


# ------------------------------------------------- MCP-01g: GET /v1/orders
#
# Fonte: il ledger durevole `orders` del core, letto con una connessione SQLite DISTINTA e in SOLA
# LETTURA (`mode=ro` + `PRAGMA query_only`): a livello di database ogni scrittura è impossibile.
# Una sola transazione di lettura (WAL: istantanea coerente, nessuna attesa sugli scrittori),
# attesa massima DB_BUSY_TIMEOUT_S. Stati: mappa ESPLICITA dagli stati reali del core (nessuno
# stato inventato; APPROVED/CANCELLED/SETTLED non esistono nel ledger). Un AMBIGUOUS resta
# AMBIGUOUS; un valore mancante resta null (mai zero); il betId si cerca nelle stesse forme di
# `RuntimeController._open_order_candidates` (test di confronto).

import json  # noqa: E402
import sqlite3  # noqa: E402
from urllib.parse import quote  # noqa: E402

_ACCEPTED_FAMILY: Final = ("ACCEPTED", "UNMATCHED", "PARTIALLY_MATCHED", "MATCHED")
ORDER_STATUS_MAP: Final = {
    "INFLIGHT": ("PENDING",),
    "HEDGE_PENDING": ("PENDING",),
    "SUBMITTED": _ACCEPTED_FAMILY,
    "ACCEPTED_FOR_PROCESSING": _ACCEPTED_FAMILY,
    "HEDGE_PLACED": _ACCEPTED_FAMILY,
    "HEDGE_UNMATCHED": ("UNMATCHED",),
    "COMPLETED": ("MATCHED",),
    "FAILED": ("FAILED",),
    "HEDGE_FAILED": ("FAILED",),
    "DENIED": ("REJECTED",),
    "DUPLICATE_BLOCKED": ("DUPLICATE_BLOCKED",),
    "AMBIGUOUS": ("AMBIGUOUS",),
    "HEDGE_AMBIGUOUS": ("AMBIGUOUS",),
}
# Stati non terminali che la riconciliazione d'avvio del core considera aperti
# (`RuntimeController._open_order_candidates`; un test verifica che coincidano).
RECONCILE_STATUSES: Final = frozenset({"SUBMITTED", "AMBIGUOUS", "INFLIGHT"})
# MCP-01k: righe il cui esito non è confermato dal core. Un report presente nella loro risposta (es. la
# risposta senza esito conservata dal fix P0 di Claude D) non è un dato: gli importi abbinati/residui restano
# indisponibili. Il betId resta, perché è l'identità che la riconciliazione del core usa.
UNCONFIRMED_STATUSES: Final = frozenset({"AMBIGUOUS", "HEDGE_AMBIGUOUS", "INFLIGHT", "HEDGE_PENDING"})
MAX_ORDERS: Final = 500
RECENT_FINALIZED: Final = 100
DB_BUSY_TIMEOUT_S: Final = 0.25
ORDER_OPTIONAL: Final = ("bet_id", "market_id", "selection_id", "side", "price", "stake",
                         "size_matched", "avg_price_matched", "size_remaining", "table_id")
_ORDERS_SQL: Final = (
    "SELECT id, customer_ref, status, payload_json, response_json, ambiguity_reason, finalized, "
    "created_at, updated_at FROM orders "
    "WHERE finalized = 0 OR id IN (SELECT id FROM orders ORDER BY id DESC LIMIT ?) "
    "ORDER BY id DESC LIMIT ?"
)


class _SnapshotMissing(Exception):
    def __init__(self, state: str, reason: str) -> None:
        super().__init__(reason)
        self.state, self.reason = state, reason


def _ro_connection(rc: object) -> sqlite3.Connection:
    db = vars(rc).get("db")
    path = vars(db).get("db_path") if db is not None else None
    if not isinstance(path, str) or not path or path == ":memory:":
        raise _SnapshotMissing("UNAVAILABLE", "DB_NOT_CONFIGURED")
    try:
        conn = sqlite3.connect(f"file:{quote(path)}?mode=ro", uri=True,
                               timeout=DB_BUSY_TIMEOUT_S, check_same_thread=False)
    except sqlite3.Error:
        raise _SnapshotMissing("UNAVAILABLE", "DB_UNREADABLE") from None
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def _dec_or_none(value: object) -> str | None:
    """Importo/quota del core come Decimal canonico; testo numerico ammesso; altrimenti None."""
    if isinstance(value, str):
        try:
            value = Decimal(value.strip())
        except ArithmeticError:
            return None
    try:
        return decimal_str(value)
    except _Unavailable:
        return None


def _bet_id_from_response(resp: object) -> str | None:
    """Forme note del betId, nello stesso ordine di `RuntimeController._open_order_candidates`
    (l'identità che la riconciliazione usa al riavvio; un test confronta i due risultati).

    NON si usa `Database._order_bet_id`: per `{"result": {"betId": ...}}` restituisce "" mentre la
    riconciliazione trova il betId. Divergenza del core segnalata a Claude B e Claude D: un solo
    helper pubblico eliminerebbe questa copia.
    """
    if not isinstance(resp, dict):
        return None
    for container in (resp, resp.get("result"), resp.get("raw")):
        if not isinstance(container, dict):
            continue
        direct = container.get("betId") or container.get("bet_id")
        reports = container.get("instructionReports") or []
        first = reports[0] if reports and isinstance(reports[0], dict) else {}
        found = first.get("betId") or first.get("bet_id") or direct
        if found not in (None, ""):
            return str(found)
    return None


def _first_report(resp: object) -> dict[str, Any]:
    if not isinstance(resp, dict):
        return {}
    for container in (resp, resp.get("result"), resp.get("raw")):
        if isinstance(container, dict):
            reports = container.get("instructionReports") or []
            if reports and isinstance(reports[0], dict):
                return reports[0]
    return {}


def _ident_or_none(value: object) -> str | None:
    value = str(value) if type(value) is int else value
    if isinstance(value, str) and IDENTIFIER_RE.fullmatch(value):
        return value
    return None


def _contract_status(core_status: str, matched: str | None, remaining: str | None) -> str:
    allowed = ORDER_STATUS_MAP.get(core_status)
    if allowed is None:
        return "UNKNOWN"
    if allowed != _ACCEPTED_FAMILY:
        return allowed[0]
    if matched is None or remaining is None:
        return "ACCEPTED"
    m, r = Decimal(matched), Decimal(remaining)
    if r == 0 and m > 0:
        return "MATCHED"
    if m == 0 and r > 0:
        return "UNMATCHED"
    if m > 0 and r > 0:
        return "PARTIALLY_MATCHED"
    return "ACCEPTED"


def _order_row(row: sqlite3.Row, queue_ids: set[str], broker_ids: set[str] | None) -> dict[str, Any]:
    from core.hedge_ref import HEDGE_ROLE, is_hedge_order_ref

    local_id = row["id"]
    created, updated = row["created_at"], row["updated_at"]
    if type(local_id) is not int or local_id < 1:
        raise _Unavailable("order")
    if type(created) not in (int, float) or type(updated) not in (int, float):
        raise _Unavailable("order")
    payload = json.loads(row["payload_json"] or "{}")
    if not isinstance(payload, dict):
        raise _Unavailable("order")
    try:
        response = json.loads(row["response_json"]) if row["response_json"] else None
    except ValueError:
        response = None  # risposta illeggibile: quantità e betId restano indisponibili
    core_status = to_code(str(row["status"] or "").upper())
    report = {} if core_status in UNCONFIRMED_STATUSES else _first_report(response)
    bet_id = _ident_or_none(_bet_id_from_response(response))
    side = str(payload.get("bet_type") or payload.get("side") or "").upper()
    table = payload.get("table_id") if payload.get("table_id") is not None \
        else payload.get("reservation_table_id")
    values = {
        "bet_id": bet_id,
        "market_id": _ident_or_none(payload.get("market_id")),
        "selection_id": _ident_or_none(payload.get("selection_id")),
        "side": side if side in ("BACK", "LAY") else None,
        "price": _dec_or_none(payload.get("price")),
        "stake": _dec_or_none(payload.get("stake")),
        "size_matched": _dec_or_none(report.get("sizeMatched")) if "sizeMatched" in report else None,
        "avg_price_matched": (_dec_or_none(report.get("averagePriceMatched"))
                              if "averagePriceMatched" in report else None),
        "size_remaining": (_dec_or_none(report.get("sizeRemaining"))
                           if "sizeRemaining" in report else None),
        "table_id": table if type(table) is int and 1 <= table <= 10_000 else None,
    }
    if is_hedge_order_ref(row["customer_ref"]) or payload.get("role") == HEDGE_ROLE:
        role = "HEDGE"
    elif payload.get("batch_id"):
        role = "DUTCHING_LEG"
    else:
        role = "ENTRY"
    reason = row["ambiguity_reason"]
    if broker_ids is None or bet_id is None:
        broker_view = "NOT_CHECKED"
    else:
        broker_view = "PRESENT" if bet_id in broker_ids else "ABSENT"
    return {
        "local_id": local_id,
        "role": role,
        "core_status": core_status,
        "status": _contract_status(core_status, values["size_matched"], values["size_remaining"]),
        **values,
        "created_at": float(created),
        "updated_at": float(updated),
        "reconciliation": {
            "in_reconciliation": core_status in RECONCILE_STATUSES or core_status == "HEDGE_AMBIGUOUS",
            "in_engine_queue": str(local_id) in queue_ids,
            "finalized": bool(row["finalized"]),
            "ambiguity_reason": _optional_code(reason.upper()) if isinstance(reason, str) else None,
        },
        "broker_view": broker_view,
        "unavailable": [name for name in ORDER_OPTIONAL if values[name] is None],
    }


def _engine_queue_ids(rc: object) -> set[str]:
    engine = vars(rc).get("reconciliation_engine")
    if engine is None:
        return set()
    snapshot = engine.ambiguity_snapshot()  # copia sotto il lock del registro
    return {str(e.get("order_id")) for e in snapshot.values() if isinstance(e, dict)}


def _broker_bet_ids(rc: object) -> set[str] | None:
    """betId degli ordini nel broker SIM (attributi, attesa limitata); None in LIVE o se occupato."""
    service = vars(rc).get("betfair_service")
    service_vars = vars(service) if service is not None else {}
    broker = service_vars.get("simulation_broker")
    if service_vars.get("simulation_mode") is not True or broker is None:
        return None
    lock = vars(broker).get("_lock")
    if not _acquire(lock):
        return None
    try:
        return {str(o.bet_id) for o in vars(broker)["state"].orders.values() if o.bet_id}
    finally:
        lock.release()  # type: ignore[union-attr]


def _ro_transaction(rc: object, read: Callable[[sqlite3.Connection], Any]) -> Any:
    """Una transazione di lettura sulla connessione in sola lettura; DB occupato => UNKNOWN, altrimenti
    illeggibile => UNAVAILABLE. Usata da `/v1/orders` e dalle chiusure SL/TP (MCP-01i)."""
    conn = _ro_connection(rc)
    try:
        try:
            conn.execute("BEGIN")
            result = read(conn)
            conn.execute("COMMIT")
        except sqlite3.OperationalError as exc:
            text = str(exc).lower()
            if "locked" in text or "busy" in text:
                raise _SnapshotMissing("UNKNOWN", "DB_BUSY") from None
            raise _SnapshotMissing("UNAVAILABLE", "DB_UNREADABLE") from None
        except sqlite3.Error:
            raise _SnapshotMissing("UNAVAILABLE", "DB_UNREADABLE") from None
    finally:
        conn.close()
    return result


def _orders_snapshot(rc: object) -> dict[str, Any]:
    rows = _ro_transaction(
        rc, lambda conn: conn.execute(_ORDERS_SQL, (RECENT_FINALIZED, MAX_ORDERS + 1)).fetchall())
    truncated = len(rows) > MAX_ORDERS
    queue_ids = _engine_queue_ids(rc)
    broker_ids = _broker_bet_ids(rc)
    orders, omitted = [], 0
    for row in rows[:MAX_ORDERS]:
        try:
            orders.append(_order_row(row, queue_ids, broker_ids))
        except Exception:  # noqa: BLE001 - riga illeggibile: contata, mai nascosta
            omitted += 1
    if omitted or truncated:
        state, reason = "PARTIAL", ("ROWS_UNREADABLE" if omitted else "TRUNCATED")
    elif vars(rc).get("_reconciliation_required") is True:
        state, reason = "STALE", "STARTUP_RECONCILIATION_REQUIRED"
    else:
        state, reason = "OK", None
    return {"state": state, "reason": reason, "orders": orders, "omitted": omitted,
            "truncated": truncated}


def orders_view(rc: object, *, clock: Callable[[], float] = time.time) -> dict[str, Any]:
    try:
        snapshot = _orders_snapshot(rc)
    except _SnapshotMissing as missing:
        snapshot = {"state": missing.state, "reason": missing.reason}
    except Exception:  # noqa: BLE001 - mai un dato apparentemente valido
        snapshot = {"state": "UNAVAILABLE", "reason": "READ_FAILED"}
    return {
        "source": "core",
        "observed_at": float(clock()),
        "consistency": "SINGLE_READ_TRANSACTION",
        "snapshot": snapshot,
    }


# ------------------------------------------------- MCP-01h: SL/TP, settlement, barriere di cashout
#
# Contratto 1.5.0, estensioni di route esistenti. SL/TP: dal `meta` del tavolo (già nello
# snapshot sotto il lock dei tavoli), nelle chiavi che il core legge (`stop_loss`, `take_profit`) e
# in quelle che F-02 di Claude D aggiunge (`*_pct`, `sl_tp_triggered`). Settlement: solo conteggi,
# mai payload o riferimenti. Strutture create alla prima scrittura (`_unrecorded_settlements`,
# `_held_batch_ids`): assenti = vuote, come le tratta il core stesso (`getattr(...) or set()`).

_SL_TP_KEYS: Final = ("stop_loss", "take_profit", "stop_loss_pct", "take_profit_pct",
                      "trailing_stop_pct")
CHECKPOINT_STAGES: Final = frozenset({
    "SETTLEMENT_DETECTED", "BANKROLL_SYNC_DONE", "MM_DECISION_DONE", "NEXT_TRADE_SUBMIT_ATTEMPTED",
    "NEXT_TRADE_SUBMIT_CONFIRMED", "CYCLE_BLOCKED", "CYCLE_AMBIGUOUS",
})


def _non_negative_dec(value: object) -> str:
    text = decimal_str(value)
    if text.startswith("-"):
        raise _Unavailable("negative")
    return text


def _sl_tp(meta: object) -> dict[str, Any]:
    unknown = {"state": "UNKNOWN", **{k: None for k in _SL_TP_KEYS}}
    if not isinstance(meta, dict):
        return unknown
    values: dict[str, Any] = {}
    for key in _SL_TP_KEYS:
        raw = meta.get(key)
        if raw is None:
            values[key] = None
            continue
        try:
            values[key] = _non_negative_dec(raw)
        except _Unavailable:
            return unknown  # valore presente ma illeggibile: mai «non armato»
    armed = values["stop_loss"] is not None or values["take_profit"] is not None
    if meta.get("sl_tp_triggered"):
        state = "TRIGGERED" if armed else "UNKNOWN"
        if not armed:
            return unknown
    else:
        state = "ARMED" if armed else "NOT_ARMED"
    return {"state": state, **values}


def _settlement_db(rc: object) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Conteggi di `settlement_effects` e `cycle_recovery_checkpoints` in una sola lettura."""
    try:
        conn = _ro_connection(rc)
    except _SnapshotMissing:
        return None, None
    effects = checkpoints = None
    try:
        conn.execute("BEGIN")
        try:
            rows = conn.execute(
                "SELECT outcome, COUNT(*) AS n FROM settlement_effects GROUP BY outcome").fetchall()
            not_realized = conn.execute(
                "SELECT COUNT(*) FROM settlement_effects WHERE realized_applied = 0").fetchone()[0]
            by_outcome: dict[str, int] = {}
            for row in rows:
                code = to_code(str(row["outcome"] or "").upper())
                by_outcome[code] = by_outcome.get(code, 0) + int(row["n"])
            effects = {
                "held": by_outcome.get("HELD", 0),
                "not_realized": int(not_realized),
                "by_outcome": [{"outcome": k, "count": v} for k, v in sorted(by_outcome.items())],
            }
        except sqlite3.Error:
            effects = None
        try:
            rows = conn.execute(
                "SELECT checkpoint_stage, COUNT(*) AS n FROM cycle_recovery_checkpoints "
                "GROUP BY checkpoint_stage").fetchall()
            by_stage: dict[str, int] = {}
            for row in rows:
                stage = str(row["checkpoint_stage"] or "")
                stage = stage if stage in CHECKPOINT_STAGES else "UNKNOWN"
                by_stage[stage] = by_stage.get(stage, 0) + int(row["n"])
            cycle_amb = conn.execute(
                "SELECT COUNT(*) FROM cycle_recovery_checkpoints "
                "WHERE checkpoint_stage = 'CYCLE_AMBIGUOUS' OR is_ambiguous = 1").fetchone()[0]
            submit_amb = conn.execute(
                "SELECT COUNT(*) FROM cycle_recovery_checkpoints "
                "WHERE next_trade_submission_status = 'AMBIGUOUS'").fetchone()[0]
            checkpoints = {
                "by_stage": [{"stage": k, "count": v} for k, v in sorted(by_stage.items())],
                "cycle_ambiguous": int(cycle_amb),
                "submit_ambiguous": int(submit_amb),
            }
        except sqlite3.Error:
            checkpoints = None
        conn.execute("COMMIT")
    except sqlite3.Error:
        return None, None
    finally:
        conn.close()
    return effects, checkpoints


def _locked_len(lock: object, read: Callable[[], Any]) -> int:
    if not _acquire(lock):
        raise _Unavailable("lock")
    try:
        value = read()
    finally:
        lock.release()  # type: ignore[union-attr]
    if value is None:
        return 0  # creata alla prima scrittura: assente = vuota (semantica del core)
    if not isinstance(value, dict | set):
        raise _Unavailable("type")
    return len(value)


def _sim_settlements(rc: object) -> int | None:
    service = vars(rc).get("betfair_service")
    service_vars = vars(service) if service is not None else {}
    broker = service_vars.get("simulation_broker")
    if service_vars.get("simulation_mode") is not True or broker is None:
        return None
    return _locked_len(vars(broker).get("_lock"), lambda: vars(broker)["state"].settlements)


def _settlement(rc: object) -> dict[str, Any]:
    settlement_lock = vars(rc).get("_settlement_lock")
    admission_lock = vars(rc).get("_risk_admission_lock")
    effects, checkpoints = _settlement_db(rc)
    try:
        sim = _sim_settlements(rc)
    except _Unavailable:
        sim = None
    values = {"effects": effects, "checkpoints": checkpoints, "sim_settlements": sim}
    return {
        "unrecorded_settlements": _locked_len(
            settlement_lock, lambda: vars(rc).get("_unrecorded_settlements")),
        "held_batches": _locked_len(admission_lock, lambda: vars(rc).get("_held_batch_ids")),
        **values,
        "unavailable": [k for k, v in values.items() if v is None],
    }


# ---------------------------------------------------------------- GET /v1/config (MCP-01j)

# Allowlist ESPLICITA delle impostazioni operative esposte, identica a
# `semantics.config.keys` del contratto (verificato dai test). Solo `RoserpinaConfig`
# (`rc.config`): credenziali Betfair, Telegram, certificati e impostazioni persistite non sono
# mai lette da qui.
CONFIG_KEYS: Final = {
    "target_profit_cycle_pct": "DECIMAL", "max_single_bet_pct": "DECIMAL",
    "max_total_exposure_pct": "DECIMAL", "max_event_exposure_pct": "DECIMAL",
    "max_daily_loss": "DECIMAL", "max_drawdown_hard_stop_pct": "DECIMAL",
    "max_open_exposure": "DECIMAL", "max_order_exposure": "DECIMAL",
    "max_market_exposure": "DECIMAL", "max_event_exposure_abs": "DECIMAL",
    "max_drawdown_abs": "DECIMAL", "max_session_loss": "DECIMAL",
    "max_exposure_stop": "DECIMAL", "max_recovery_chase_abs": "DECIMAL",
    "auto_reset_drawdown_pct": "DECIMAL", "defense_drawdown_pct": "DECIMAL",
    "lockdown_drawdown_pct": "DECIMAL", "expansion_profit_pct": "DECIMAL",
    "expansion_multiplier": "DECIMAL", "defense_multiplier": "DECIMAL",
    "risk_profile": "ENUM", "table_count": "INT", "max_recovery_tables": "INT",
    "allow_recovery": "BOOL", "anti_duplication_enabled": "BOOL",
    "commission_pct": "DECIMAL", "min_stake": "DECIMAL", "max_stake_abs": "DECIMAL",
    "book_warning": "DECIMAL", "book_block": "DECIMAL", "liquidity_guard_enabled": "BOOL",
    "liquidity_multiplier": "DECIMAL", "min_liquidity_absolute": "DECIMAL",
    "liquidity_warning_only": "BOOL", "min_price": "DECIMAL", "max_win": "DECIMAL",
    "max_win_warning_only": "BOOL", "max_stake_pct": "DECIMAL", "profit_epsilon": "DECIMAL",
    "profit_epsilon_enabled": "BOOL", "auto_green_delay_enabled": "BOOL",
    "auto_green_delay_sec": "DECIMAL",
}
# Campi Optional del core: None = non configurato (NOT_SET). Per gli altri None è INVALID.
OPTIONAL_CONFIG_KEYS: Final = frozenset({
    "target_profit_cycle_pct", "max_single_bet_pct", "max_total_exposure_pct",
    "max_event_exposure_pct", "max_daily_loss", "max_drawdown_hard_stop_pct",
    "max_open_exposure", "max_order_exposure", "max_market_exposure", "max_event_exposure_abs",
    "max_drawdown_abs", "max_session_loss", "max_exposure_stop", "max_recovery_chase_abs",
    "max_stake_abs", "max_win",
})
RISK_PROFILES: Final = frozenset({"CONSERVATIVE", "BALANCED", "AGGRESSIVE"})
# Seconda barriera oltre a `core.redaction.is_sensitive_key`: stessi frammenti del contratto.
SENSITIVE_NAME_FRAGMENTS: Final = (
    "token", "secret", "password", "passwd", "auth", "bearer", "key", "session", "api",
    "cert", "cookie", "ssoid", "private", "credential", "hash", "pin",
)
FIELD_NAME_RE: Final = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
MAX_UNLISTED: Final = 64
INT_LIMIT: Final = 1_000_000_000


def _setting_value(kind: str, raw: object) -> object:
    """Valore nel formato del contratto; ``_Unavailable`` se illeggibile (mai un default)."""
    if kind == "DECIMAL":
        return decimal_str(raw)  # bool, NaN/inf, fuori scala => _Unavailable
    if kind == "BOOL":
        if type(raw) is not bool:
            raise _Unavailable("bool")
        return raw
    if kind == "INT":
        if type(raw) is not int or not -INT_LIMIT <= raw <= INT_LIMIT:
            raise _Unavailable("int")
        return raw
    name = getattr(raw, "value", raw)  # ENUM: RiskProfile del core
    if not isinstance(name, str) or name not in RISK_PROFILES:
        raise _Unavailable("enum")
    return name


def _setting(key: str, kind: str, raw: object) -> dict[str, Any]:
    if raw is None or raw is _MISSING:
        state = "NOT_SET" if raw is None and key in OPTIONAL_CONFIG_KEYS else "INVALID"
        return {"key": key, "kind": kind, "state": state, "value": None}
    try:
        value = _setting_value(kind, raw)
    except _Unavailable:
        return {"key": key, "kind": kind, "state": "INVALID", "value": None}
    return {"key": key, "kind": kind, "state": "SET", "value": value}


def _sensitive_name(name: str) -> bool:
    from core.redaction import is_sensitive_key

    lowered = name.lower()
    return is_sensitive_key(name) or any(f in lowered for f in SENSITIVE_NAME_FRAGMENTS)


def config_view(rc: object, *, clock: Callable[[], float] = time.time) -> dict[str, Any]:
    """Impostazioni operative di ``rc.config`` dall'allowlist del contratto.

    Un solo riferimento letto: ``reload_config`` sostituisce l'oggetto intero, quindi tutte le
    impostazioni appartengono alla stessa versione. Un campo del core fuori allowlist compare
    solo per NOME in ``unlisted``; un nome sensibile o non canonico (o oltre ``MAX_UNLISTED``)
    è soltanto contato in ``redacted``. Nessun valore fuori allowlist esce mai.
    """
    body: dict[str, Any] = {
        "source": "core", "observed_at": clock(), "consistency": "SINGLE_REFERENCE_READ",
        "settings": None, "unlisted": [], "redacted": 0, "unavailable": ["settings"],
    }
    try:
        fields = dict(vars(_attr(rc, "config")))  # copia del dizionario: una sola lettura
    except Exception:  # noqa: BLE001 - config assente o non un oggetto con attributi
        return body
    unlisted: list[str] = []
    redacted = 0
    for name in sorted(fields):
        if name in CONFIG_KEYS or name.startswith("_"):
            continue
        if (not FIELD_NAME_RE.fullmatch(name) or _sensitive_name(name)
                or len(unlisted) >= MAX_UNLISTED):
            redacted += 1
        else:
            unlisted.append(name)
    body.update(
        settings=[_setting(k, kind, fields.get(k, _MISSING)) for k, kind in CONFIG_KEYS.items()],
        unlisted=unlisted, redacted=redacted, unavailable=[],
    )
    return body


# ---------------------------------------------------------------- MCP-01l: mercati e book
#
# Fonte: `MarketTracker._markets`, l'ultimo book ricevuto per ogni mercato (stream o polling), copiato con
# UNA sola operazione (`dict(...)`): il tracker sostituisce il book intero a ogni aggiornamento, quindi ogni
# book letto è coerente in sé. Nessuna chiamata di rete, nessun publish, nessuna scrittura. Lo stato del
# mercato è il dato del book, non il giudizio di negoziabilità del core (F-23). L'età del book è nota solo
# tramite `lastMatchTime`: il tracker non registra l'istante di ricezione.

MARKET_ID_RE: Final = re.compile(r"[0-9]{1,4}\.[0-9]{1,15}")
BOOK_STATUSES: Final = frozenset({"OPEN", "SUSPENDED", "CLOSED", "INACTIVE"})
RUNNER_STATUSES: Final = frozenset({"ACTIVE", "REMOVED", "WINNER", "LOSER", "PLACED", "HIDDEN"})
MAX_MARKETS: Final = 256
MAX_RUNNERS: Final = 64
MAX_LEVELS: Final = 3


def _tracked_books(rc: object) -> dict[str, Any]:
    tracker = _attr(rc, "market_tracker")
    books = vars(tracker).get("_markets")
    if not isinstance(books, dict):
        raise _Unavailable("market_tracker")
    return dict(books)  # una sola copia: ogni valore è il book intero sostituito dal tracker


def _book_dec(value: object) -> str | None:
    """Numero del book (int/float Betfair) come stringa decimale; `decimal_str` rifiuta stringhe e bool."""
    if value is None:
        return None
    try:
        text = decimal_str(value)
    except _Unavailable:
        return None
    return None if text.startswith("-") else text


def _book_header(book: dict[str, Any]) -> dict[str, Any]:
    status = book.get("status")
    inplay = book.get("inplay")
    return {
        "book_status": status if status in BOOK_STATUSES else "UNKNOWN",
        "inplay": inplay if type(inplay) is bool else None,
        "total_matched": _book_dec(book.get("totalMatched")),
        "last_match_time": _epoch(book.get("lastMatchTime")),
    }


def _levels(raw: object, *, descending: bool) -> list[dict[str, str]]:
    if not isinstance(raw, list):
        raise _Unavailable("levels")
    levels = []
    for item in raw[:MAX_LEVELS]:
        if not isinstance(item, dict):
            raise _Unavailable("levels")
        price, size = _book_dec(item.get("price")), _book_dec(item.get("size"))
        if price is None or size is None:
            raise _Unavailable("levels")
        levels.append({"price": price, "size": size})
    prices = [Decimal(level["price"]) for level in levels]
    if prices != sorted(prices, reverse=descending):
        raise _Unavailable("levels")  # scala fuori ordine: mai riordinata in silenzio
    return levels


def _runner(raw: object) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise _Unavailable("runner")
    selection = _ident_or_none(raw.get("selectionId"))  # Betfair: intero
    if selection is None:
        raise _Unavailable("runner")
    ex = raw.get("ex") if isinstance(raw.get("ex"), dict) else {}
    status = raw.get("status")
    return {
        "selection_id": selection,
        "status": status if status in RUNNER_STATUSES else "UNKNOWN",
        "last_price_traded": _book_dec(raw.get("lastPriceTraded")),
        "total_matched": _book_dec(raw.get("totalMatched")),
        "back": _levels(ex.get("availableToBack", []), descending=True),
        "lay": _levels(ex.get("availableToLay", []), descending=False),
    }


def markets_view(rc: object, *, clock: Callable[[], float] = time.time) -> dict[str, Any]:
    body: dict[str, Any] = {"source": "core", "observed_at": float(clock()),
                            "consistency": "SINGLE_REFERENCE_READ", "markets": None, "omitted": 0,
                            "truncated": False, "unavailable": ["markets"]}
    try:
        books = _tracked_books(rc)
    except Exception:  # noqa: BLE001 - tracker assente: indisponibile, mai una lista vuota
        return body
    ids = sorted(k for k in books if isinstance(k, str) and MARKET_ID_RE.fullmatch(k))
    omitted = len(books) - len(ids)
    markets = []
    for market_id in ids[:MAX_MARKETS]:
        book = books[market_id]
        runners = book.get("runners") if isinstance(book, dict) else None
        if not isinstance(runners, list):
            omitted += 1
            continue
        markets.append({"market_id": market_id, **_book_header(book), "runners": len(runners)})
    body.update(markets=markets, omitted=omitted, truncated=len(ids) > MAX_MARKETS, unavailable=[])
    return body


def market_book_view(rc: object, market_id: str, *,
                     clock: Callable[[], float] = time.time) -> dict[str, Any]:
    body: dict[str, Any] = {"source": "core", "observed_at": float(clock()),
                            "consistency": "SINGLE_REFERENCE_READ", "market_id": market_id,
                            "state": "UNAVAILABLE", "book": None}
    if not isinstance(market_id, str) or not MARKET_ID_RE.fullmatch(market_id):
        raise ValueError("market_id")  # il server valida già il percorso: qui è una seconda barriera
    try:
        book = _tracked_books(rc).get(market_id)
    except Exception:  # noqa: BLE001
        return body
    if book is None:
        body["state"] = "NOT_TRACKED"
        return body
    raw_runners = book.get("runners") if isinstance(book, dict) else None
    if not isinstance(raw_runners, list):
        return body
    runners, omitted = [], 0
    for raw in raw_runners:
        try:
            runner = _runner(raw)
        except _Unavailable:
            omitted += 1
            continue
        if len(runners) >= MAX_RUNNERS or any(r["selection_id"] == runner["selection_id"] for r in runners):
            omitted += 1
            continue
        runners.append(runner)
    body.update(state="OK", book={**_book_header(book), "runners": runners, "omitted_runners": omitted})
    return body
# ------------------------------------------------- MCP-01i: stato della chiusura SL/TP (F-04)
#
# Solo dati PERSISTITI dal core: il record di F-04 `risk.sl_tp_close.<customer_ref>` nella tabella
# `settings` (stato CLAIMED/ROUTED/NOT_EXECUTED/UNCERTAIN, tavolo, posizione, betId, dettaglio), gli ordini
# della posizione nel ledger `orders` e le righe d'hedge del ledger, tutti nella STESSA transazione di sola
# lettura; l'esposizione residua dal tavolo, nella stessa acquisizione del lock dei tavoli di `tables`.
# Costanti e stati vengono da `core.sl_tp_close` (nessuna regola replicata). Le chiusure respinte PRIMA
# della rivendicazione (es. ORDER_STATE_UNCERTAIN) non sono persistite dal core: escono solo sul bus e qui
# non compaiono. Hedge della chiusura: legati dal record (`cashout_request_ids`) alla riga del ledger
# (`payload_json.cashout_request_id`), mai per selezione.

MAX_CLOSES: Final = 64
MAX_CLOSE_BETS: Final = 32
HEDGE_COUNT_KEYS: Final = {"HEDGE_PENDING": "pending", "HEDGE_PLACED": "placed",
                           "HEDGE_UNMATCHED": "unmatched", "HEDGE_AMBIGUOUS": "ambiguous",
                           "HEDGE_FAILED": "failed"}


def _close_module() -> Any:
    try:
        from core import sl_tp_close
    except ImportError:
        raise _SnapshotMissing("UNAVAILABLE", "CORE_PENDING_F04") from None
    return sl_tp_close


def _close_rows(rc: object, prefix: str) -> tuple[list[Any], dict[str, list[Any]], list[Any]]:
    def read(conn: sqlite3.Connection) -> tuple[list[Any], dict[str, list[Any]], list[Any]]:
        records = conn.execute(
            "SELECT key, value FROM settings WHERE substr(key, 1, ?) = ? ORDER BY key LIMIT ?",
            (len(prefix), prefix, MAX_CLOSES + 1)).fetchall()
        positions: dict[str, list[Any]] = {}
        for record in records[:MAX_CLOSES]:
            ref = str(record["key"])[len(prefix):]
            positions[ref] = conn.execute(
                "SELECT id, customer_ref, status, payload_json, response_json, ambiguity_reason, "
                "finalized, created_at, updated_at FROM orders WHERE customer_ref = ? ORDER BY id",
                (ref,)).fetchall()
        hedges = conn.execute(
            "SELECT status, payload_json FROM orders WHERE substr(customer_ref, 1, 6) = 'hedge:'"
        ).fetchall()
        return records, positions, hedges

    return _ro_transaction(rc, read)


def _position_summary(rows: list[Any]) -> dict[str, int]:
    counts = {"orders": 0, "partially_matched": 0, "in_reconciliation": 0, "ambiguous": 0,
              "unreadable": 0}
    for row in rows:
        try:
            order = _order_row(row, set(), None)  # stessa mappa degli stati di GET /v1/orders
        except Exception:  # noqa: BLE001
            counts["unreadable"] += 1
            continue
        counts["orders"] += 1
        counts["partially_matched"] += order["status"] == "PARTIALLY_MATCHED"
        counts["ambiguous"] += order["status"] == "AMBIGUOUS"
        counts["in_reconciliation"] += order["reconciliation"]["in_reconciliation"]
    return counts


def _close_hedges(hedges: list[Any], request_ids: object) -> dict[str, Any] | None:
    """Righe d'hedge della chiusura: quelle con ``cashout_request_id`` registrato nel record di F-04.

    Record senza ``cashout_request_ids`` (forma precedente di F-04): legame non dimostrabile, None.
    ``not_in_ledger``: richieste pubblicate senza una riga d'hedge nel ledger (non ancora scritta o soppressa).
    """
    if not isinstance(request_ids, list) or not all(isinstance(r, str) and r for r in request_ids):
        return None
    wanted = set(request_ids)
    counts: dict[str, Any] = {"requests": len(wanted), **{k: 0 for k in HEDGE_COUNT_KEYS.values()}}
    seen: set[str] = set()
    for row in hedges:
        try:
            payload = json.loads(row["payload_json"] or "{}")
        except ValueError:
            continue
        rid = payload.get("cashout_request_id") if isinstance(payload, dict) else None
        if rid not in wanted:
            continue
        seen.add(rid)
        key = HEDGE_COUNT_KEYS.get(str(row["status"] or "").upper())
        if key:
            counts[key] += 1
    counts["not_in_ledger"] = len(wanted - seen)
    return counts


def _residual(table_refs: dict[int, tuple[str, object]] | None, table_id: object,
              ref: str) -> tuple[str | None, str]:
    if table_refs is None:
        return None, "UNAVAILABLE"
    entry = table_refs.get(table_id) if type(table_id) is int else None
    if entry is None or entry[0] != ref:
        return None, "TABLE_RELEASED"  # il tavolo non porta più questa posizione
    try:
        return decimal_str(_money(entry[1], non_negative=True)), "TABLE"
    except _Unavailable:
        return None, "UNAVAILABLE"


def _close_item(mod: Any, raw: object, ref: str, positions: dict[str, list[Any]], hedges: list[Any],
                table_refs: dict[int, tuple[str, object]] | None) -> dict[str, Any]:
    data = json.loads(raw) if isinstance(raw, str) else None
    if not isinstance(data, dict) or str(data.get("customer_ref") or "") != ref:
        raise _Unavailable("close")
    table_id = data.get("table_id")
    market_id, selection_id = _identifier(data.get("market_id")), _identifier(data.get("selection_id"))
    side = data.get("side")
    if type(table_id) is not int or not 1 <= table_id <= 10_000 or market_id is None \
            or selection_id is None or side not in ("BACK", "LAY"):
        raise _Unavailable("close")
    states = {mod.STATE_CLAIMED, mod.STATE_ROUTED, mod.STATE_NOT_EXECUTED, mod.STATE_UNCERTAIN}
    state = mod.read_state(raw)
    close_state = state if state in states else "UNKNOWN"
    trigger = {mod.SL_TRIGGERED: "STOP_LOSS", mod.TP_TRIGGERED: "TAKE_PROFIT"}.get(
        data.get("trigger"), "UNKNOWN")
    raw_bets = data.get("bet_ids")
    bets = [b for b in (_ident_or_none(x) for x in raw_bets) if b] if isinstance(raw_bets, list) else []
    if not isinstance(raw_bets, list) or len(bets) != len(raw_bets) or len(bets) > MAX_CLOSE_BETS:
        raise _Unavailable("close")
    residual, residual_source = _residual(table_refs, table_id, ref)
    detail = data.get("detail")
    return {
        "table_id": table_id, "market_id": market_id, "selection_id": selection_id, "side": side,
        "trigger": trigger, "close_state": close_state,
        "retryable": close_state in mod.REROUTABLE_STATES,
        "detail": _code_of(detail) if isinstance(detail, str) and detail else None,
        "recorded_at": _epoch(data.get("at")),
        "bet_ids": bets,
        "position": _position_summary(positions.get(ref, [])),
        "residual_exposure": residual, "residual_source": residual_source,
        "hedges": _close_hedges(hedges, data.get("cashout_request_ids")),
    }


def _sl_tp_closes(rc: object, table_refs: dict[int, tuple[str, object]] | None) -> dict[str, Any]:
    try:
        mod = _close_module()
        prefix = mod.CLOSE_KEY_PREFIX
        records, positions, hedges = _close_rows(rc, prefix)
    except _SnapshotMissing as missing:
        return {"state": missing.state, "reason": missing.reason}
    except Exception:  # noqa: BLE001 - mai un dato apparentemente valido
        return {"state": "UNAVAILABLE", "reason": "READ_FAILED"}
    truncated = len(records) > MAX_CLOSES
    closes, omitted = [], 0
    for record in records[:MAX_CLOSES]:
        ref = str(record["key"])[len(prefix):]
        try:
            closes.append(_close_item(mod, record["value"], ref, positions, hedges, table_refs))
        except Exception:  # noqa: BLE001 - record illeggibile: contato, mai nascosto
            omitted += 1
    if omitted or truncated:
        state, reason = "PARTIAL", ("RECORDS_UNREADABLE" if omitted else "TRUNCATED")
    else:
        state, reason = "OK", None
    closes.sort(key=lambda c: (c["table_id"], c["recorded_at"] or 0.0))
    return {"state": state, "reason": reason, "closes": closes, "omitted": omitted,
            "truncated": truncated}


def _table_refs(rc: object) -> dict[int, tuple[str, object]]:
    """Tavolo → (customer_ref, esposizione) dei tavoli occupati; solo uso interno, mai esposto."""
    refs: dict[int, tuple[str, object]] = {}
    for row in _attr(rc, "table_manager").snapshot():
        meta = row.get("meta") if isinstance(row, dict) else None
        ref = meta.get("customer_ref") if isinstance(meta, dict) else None
        if isinstance(ref, str) and ref and row.get("status") in ("ACTIVE", "RECOVERY"):
            refs[row["table_id"]] = (ref, row.get("current_exposure"))
    return refs

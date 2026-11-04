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
        },
    )
    return {
        "source": "core",
        "observed_at": float(clock()),
        "consistency": SECTION_CONSISTENCY,
        **values,
        "unavailable": unavailable,
    }

"""Proiezioni di SOLA LETTURA dello stato del runtime per la Control API.

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
    return {"source": "core", "observed_at": float(clock()), **values, "unavailable": unavailable}


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
    return {"source": "core", "observed_at": float(clock()), **values, "unavailable": unavailable}

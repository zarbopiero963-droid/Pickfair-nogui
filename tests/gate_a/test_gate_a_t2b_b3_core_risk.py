"""Gate A T2b batch b3: core/loss_limits.py + core/risk_stop.py (verdetti C-1 su comportamento ratificato)."""
from __future__ import annotations

import json
import os
from types import SimpleNamespace

from core import loss_limits, risk_stop


# =========================================================
# core/loss_limits.py — perdita di sessione (P37, NOCAP/DD351)
# =========================================================
def test_trg_628_b_0968():
    """ID: TRG-628-B-0968 / PF-B1-R2-SESSION-OPTIONAL

    Limite assente (None) => nessun limite di sessione: nessun blocco anche con
    perdita grande. Nessun cap di default inventato (NOCAP/DD351).
    """
    assert loss_limits.session_loss_reason(None, 100.0, -10_000.0) == ""
    assert loss_limits.session_loss_reason(None, None, 0.0) == ""


def test_trg_628_b_0969():
    """ID: TRG-628-B-0969 / PF-B1-R2-SESSION-INVALID

    Limite configurato ma illeggibile, non finito, bool o <= 0 => rifiuto
    esplicito (fail-closed), anche senza perdite.
    """
    for bad in ("abc", float("nan"), float("inf"), 0, 0.0, -5, True, False, [], {}):
        assert loss_limits.session_loss_reason(bad, 0.0, 0.0) == "max_session_loss_non_valido", bad
    # un limite valido senza perdita non blocca
    assert loss_limits.session_loss_reason("50", 0.0, 0.0) == ""


def test_trg_628_b_0970():
    """ID: TRG-628-B-0970 / PF-B1-R2-SESSION-BREACH

    Perdita = max(0, baseline - realized); baseline assente => perdita zero.
    Il limite raggiunto (>=, con tolleranza) blocca; sotto il limite no;
    un guadagno non produce perdita negativa.
    """
    # perdita 30 su limite 50 => nessun blocco
    assert loss_limits.session_loss_reason(50.0, 100.0, 70.0) == ""
    # perdita 50 su limite 50 => blocco alla soglia esatta
    assert loss_limits.session_loss_reason(50.0, 100.0, 50.0) == "session_loss_breached:loss=50.00:limit=50.0"
    # perdita 60 > 50 => blocco con importo esatto
    assert loss_limits.session_loss_reason(50.0, 100.0, 40.0) == "session_loss_breached:loss=60.00:limit=50.0"
    # guadagno: max(0, ...) => nessuna perdita
    assert loss_limits.session_loss_reason(1.0, 100.0, 500.0) == ""
    # baseline assente (prima di start()) => perdita zero
    assert loss_limits.session_loss_reason(1.0, None, -999.0) == ""


# =========================================================
# core/risk_stop.py — RISK_STOP (OD12/PREV9/EXEC)
# =========================================================
def test_trg_628_b_0976():
    """ID: TRG-628-B-0976 / PF-B1-R2-EXPOSURE-CAP

    None non blocca; cap invalido => max_exposure_stop_non_valido; exposure non
    finita => esposizione_non_valida; soglia raggiunta => exposure_stop_reached.
    """
    assert risk_stop.exposure_stop_reason(None, 1e9) == ""
    for bad in ("x", 0, -1, float("nan"), float("inf"), True):
        assert risk_stop.exposure_stop_reason(bad, 1.0) == "max_exposure_stop_non_valido", bad
    for bad_exp in (float("nan"), float("inf"), None, "abc", True):
        assert risk_stop.exposure_stop_reason(100.0, bad_exp) == "esposizione_non_valida", bad_exp
    assert risk_stop.exposure_stop_reason(100.0, 99.0) == ""
    assert risk_stop.exposure_stop_reason(100.0, 100.0) == "exposure_stop_reached:exposure=100.00:limit=100.0"
    assert risk_stop.exposure_stop_reason(100.0, 150.0) == "exposure_stop_reached:exposure=150.00:limit=100.0"


def test_trg_628_b_0977():
    """ID: TRG-628-B-0977 / PF-B1-R2-STOP-TRIGGER

    Solo un limite raggiunto (exposure_stop_reached / session_loss_breached)
    fa scattare il RISK_STOP; un limite configurato male blocca le entrate ma
    non e' un trigger di chiusura.
    """
    assert risk_stop.is_stop_trigger("exposure_stop_reached:exposure=1:limit=1")
    assert risk_stop.is_stop_trigger("session_loss_breached:loss=1:limit=1")
    for other in ("", None, "max_exposure_stop_non_valido", "esposizione_non_valida",
                  "max_session_loss_non_valido", "daily_loss_breached", "emergency_stop_active",
                  "x_exposure_stop_reached"):
        assert not risk_stop.is_stop_trigger(other), other


class _Db:
    """DB finto su file (db_path in tmp_path) con settings persistenti."""

    def __init__(self, path, log):
        self.db_path = str(path)
        self.settings = {}
        self.log = log

    def get_settings(self):
        return dict(self.settings)

    def save_settings(self, data):
        for key, value in data.items():
            active = None
            if value:
                active = json.loads(value)["active"]
            self.log.append(("db", key, active))
            self.settings[key] = value


def test_trg_628_b_0983(tmp_path, monkeypatch):
    """ID: TRG-628-B-0983 / PF-B1-R2-STOP-RESET-CLEAR

    Reset del RISK_STOP: guard armato (db+marker) prima dello stato inattivo; a
    reset verificato si cancella prima la guardia DB e PER ULTIMO il marker
    guardia. Un fallimento della rimozione del marker guardia lascia il reset
    non committato (False) e restore resta bloccato.
    """
    log = []
    real_write = risk_stop.atomic_write_text
    real_remove = os.remove

    def _write(path, text):
        log.append(("write", os.path.basename(path), json.loads(text)["active"]))
        real_write(path, text)

    def _remove(path):
        log.append(("remove", os.path.basename(path)))
        real_remove(path)

    monkeypatch.setattr(risk_stop, "atomic_write_text", _write)
    monkeypatch.setattr(risk_stop.os, "remove", _remove)

    db = _Db(tmp_path / "pf.db", log)
    assert risk_stop.persist_risk_stop(db, "exposure_stop_reached:x") is True
    assert risk_stop.restore_risk_stop(db) == "exposure_stop_reached:x"
    del log[:]

    assert risk_stop.persist_risk_stop(db, "") is True
    guard_marker = "pf.db.risk_stop_reset_guard.json"
    state_marker = "pf.db.risk_stop_pending.json"
    assert log == [
        ("db", risk_stop.RISK_STOP_RESET_GUARD_KEY, True),
        ("write", guard_marker, True),
        ("db", risk_stop.RISK_STOP_STATE_KEY, False),
        ("write", state_marker, False),
        ("db", risk_stop.RISK_STOP_RESET_GUARD_KEY, None),   # guard DB cancellato
        ("remove", guard_marker),                            # marker guard PER ULTIMO
    ]
    assert not os.path.exists(tmp_path / guard_marker)
    assert risk_stop.restore_risk_stop(db) == ""

    # BLOCK: la rimozione del marker guard fallisce => reset non committato e restore bloccato.
    db2 = _Db(tmp_path / "pf2.db", [])
    assert risk_stop.persist_risk_stop(db2, "session_loss_breached:y") is True

    def _remove_fail(path):
        raise PermissionError("locked")

    monkeypatch.setattr(risk_stop.os, "remove", _remove_fail)
    assert risk_stop.persist_risk_stop(db2, "") is False
    assert risk_stop.restore_risk_stop(db2) == "session_loss_breached:y"


def test_trg_628_b_0984(tmp_path):
    """ID: TRG-628-B-0984 / PF-B1-R2-STOP-RESTORE-GUARD

    restore_risk_stop verifica la guardia di reset PRIMA delle copie ordinarie:
    stato principale inattivo ma guardia attiva (reset iniziato e non
    committato) => resta attivo; guardia illeggibile => fail-closed.
    """
    db = _Db(tmp_path / "pf.db", [])
    inactive = risk_stop._state_payload(active=False, reason="", seq=5)
    db.settings[risk_stop.RISK_STOP_STATE_KEY] = inactive
    with open(tmp_path / "pf.db.risk_stop_pending.json", "w", encoding="utf-8") as fh:
        fh.write(inactive)
    assert risk_stop.restore_risk_stop(db) == ""            # nessuna guardia => riaperto

    db.settings[risk_stop.RISK_STOP_RESET_GUARD_KEY] = risk_stop._state_payload(
        active=True, reason="exposure_stop_reached:g", seq=None)
    assert risk_stop.restore_risk_stop(db) == "exposure_stop_reached:g"

    db.settings[risk_stop.RISK_STOP_RESET_GUARD_KEY] = "{non json"
    assert risk_stop.restore_risk_stop(db) == "risk_stop_reset_guard_illeggibile"


def test_trg_628_b_0985():
    """ID: TRG-628-B-0985 / PF-B1-R2-STOP-RESET-BLOCK

    reset_blocker: nessun RISK_STOP => ""; allo startup richiede SEMPRE reset
    esplicito; altrimenti ricontrolla in ordine emergency, daily breach, limiti
    ed exposure. La conferma operatore non disarma uno stop attivo.
    """
    calls = []

    def _rc(reason, *, emergency=False, daily=False, limits="", exposure=""):
        def _daily(source):
            calls.append(("daily", source))
            return {"breached": daily}

        def _exp(flag):
            calls.append(("exposure", flag))
            return exposure

        return SimpleNamespace(
            _risk_stop_reason=reason,
            _daily_loss_entry_blocked=lambda: emergency,
            _monitor_daily_loss_breach=_daily,
            _limits_block_reason=lambda: limits,
            _exposure_stop=_exp,
        )

    assert risk_stop.reset_blocker(_rc(""), at_start=True) == ""
    assert risk_stop.reset_blocker(_rc(""), at_start=False) == ""
    # startup: mai riapertura implicita, nemmeno con tutto pulito
    assert risk_stop.reset_blocker(_rc("exposure_stop_reached:x"), at_start=True) == \
        "risk_stop_reset_esplicito_richiesto"
    assert calls == []
    assert risk_stop.reset_blocker(_rc("r", emergency=True, daily=True), at_start=False) == "emergency_stop_active"
    assert risk_stop.reset_blocker(_rc("r", daily=True, limits="L"), at_start=False) == "daily_loss_breached"
    assert ("daily", "RISK_STOP_RESET") in calls
    assert risk_stop.reset_blocker(_rc("r", limits="L", exposure="E"), at_start=False) == "L"
    assert risk_stop.reset_blocker(_rc("r", exposure="exposure_stop_reached:z"), at_start=False) == \
        "exposure_stop_reached:z"
    assert ("exposure", False) in calls
    assert risk_stop.reset_blocker(_rc("r"), at_start=False) == ""

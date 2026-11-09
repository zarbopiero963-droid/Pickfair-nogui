"""Verifica indipendente del meccanismo di hard stop persistente (patch PR 1 di Claude B, `core/drawdown_hard_stop.py`).

Scritta contro il comportamento DICHIARATO da Claude B (pfab-b-20261109-roserpina-api-001) e il contratto owner
(PF-ROSERPINA-DRAWDOWN-AND-RECOVERY, FINANCIAL-CONTRACT-FINAL, D2-LOCKDOWN-UNLOCK-FINAL): saltata dove il modulo non
esiste (main). Runtime REALE (harness dell'oracolo). Fault injection sullo stato persistito, persistenza al riavvio,
sblocco owner, nessun chiamante MCP/Control API, cap di esposizione invariati.
"""
from __future__ import annotations

import json
import os
import re

import pytest

pytest.importorskip("core.drawdown_hard_stop")

from core.system_state import RuntimeMode                                  # noqa: E402
from tests.roserpina_oracle import oracle as O                             # noqa: E402
from tests.roserpina_oracle.harness import build, read                     # noqa: E402
from tests.roserpina_oracle.test_drawdown_oracle import _signal            # noqa: E402

KEY = "roserpina.drawdown_hard_stop_state"
# RO-DD-08 corretto in 3f59fa67 (retry del salvataggio, ingressi negati finche' il picco non e' salvato)
DD08_FIX = hasattr(__import__("core.drawdown_hard_stop", fromlist=["DrawdownHardStop"]).DrawdownHardStop, "retry_persist")
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _cap(c):
    return c.rc.absolute_cap_reason_for_orders([])


def _state(c):
    raw = (c.db.get_settings() or {}).get(KEY)
    return json.loads(raw) if raw else None


def _breach(tmp_path, **over):
    c = build(tmp_path, **over)
    read(c, 100)
    read(c, 88)                                                     # 12 >= 10: superamento
    return c


# ------------------------------------------------------------------ persistenza e picco
def test_picco_persistito_come_decimal_e_superamento_memorizzato(tmp_path) -> None:
    c = _breach(tmp_path)
    st = _state(c)
    assert O.D(st["peak"]) == O.D("100") and st["breached"] is True and st["breach"]["kind"] == "max_drawdown_abs"
    assert O.D(str(st["breach"]["loss"])) == O.D("12") and c.rc.mode == RuntimeMode.LOCKDOWN


def test_blocco_sopravvive_a_riavvio_rientro_della_cassa_e_rimozione_del_limite(tmp_path) -> None:
    c = _breach(tmp_path)
    c2 = build(tmp_path)                                           # nuovo processo, stesso DB
    read(c2, 100)                                                  # cassa tornata al picco (settlement in utile)
    assert _cap(c2) != "" and _signal(c2, "1.601") is False
    c3 = build(tmp_path, max_drawdown_abs=None)                    # limite tolto dalla config
    read(c3, 100)
    assert _signal(c3, "1.602") is False, "il blocco memorizzato deve restare anche senza limite configurato"


def test_auto_reset_non_ribasa_il_picco_persistito(tmp_path) -> None:
    c = build(tmp_path, max_drawdown_abs=None)
    read(c, 100)
    read(c, 84)
    assert c.rc._maybe_auto_reset(0.0) is True
    assert O.D(_state(c)["peak"]) == O.D("100")


# ------------------------------------------------------------------ fault injection sullo stato persistito
@pytest.mark.parametrize("raw", ["{rotto", json.dumps({"peak": "abc", "seq": 1, "breached": False}),
                                 json.dumps({"peak": "-5", "seq": 1, "breached": False}),
                                 json.dumps({"peak": "NaN", "seq": 1, "breached": False}),
                                 json.dumps({"peak": "100", "seq": True, "breached": False}),
                                 json.dumps({"peak": "100", "seq": 1, "breached": "no"})])
def test_stato_illeggibile_nessun_ingresso_e_mai_sovrascritto(tmp_path, raw) -> None:
    c0 = build(tmp_path)
    c0.db.save_settings({KEY: raw})
    c = build(tmp_path)                                            # riavvio con stato corrotto
    read(c, 100)
    assert "stato_illeggibile" in _cap(c), _cap(c)
    assert _signal(c, "1.611") is False
    assert (c.db.get_settings() or {}).get(KEY) == raw, "stato corrotto sovrascritto"


def test_lettura_dello_stato_che_solleva_nessun_ingresso(tmp_path, monkeypatch) -> None:
    c = build(tmp_path)
    real = c.db.get_settings

    def boom(*a, **k):
        raise OSError("database locked")
    monkeypatch.setattr(c.db, "get_settings", boom)
    c2 = build(tmp_path)
    monkeypatch.setattr(c2.db, "get_settings", boom)
    try:
        c2.rc._hard_stop().restore()
    except Exception:
        pass
    monkeypatch.setattr(c2.db, "get_settings", real)
    read(c2, 100)
    assert _cap(c2) != "" or c2.rc._hard_stop().unreadable, "lettura fallita trattata come stato vuoto"


def test_scrittura_del_picco_che_fallisce_nessun_ingresso(tmp_path, monkeypatch) -> None:
    c = build(tmp_path)
    real = c.db.save_settings

    def save(values):
        if KEY in (values or {}):
            raise OSError("disk full")
        return real(values)
    monkeypatch.setattr(c.db, "save_settings", save)
    read(c, 100)
    assert "picco_non_persistito" in _cap(c), _cap(c)
    assert _signal(c, "1.621") is False


# ------------------------------------------------------------------ sblocco owner
def _unlock(c, **over):
    kw = dict(authorization="owner:test", reason="verifica", operation_id="op-1", confirm=True)
    kw.update(over)
    return c.rc.owner_unlock_drawdown_hard_stop(**kw)


@pytest.mark.parametrize("over", [{"confirm": False}, {"confirm": "yes"}, {"authorization": ""}, {"reason": ""},
                                  {"operation_id": ""}])
def test_sblocco_rifiutato_senza_tutti_i_requisiti(tmp_path, over) -> None:
    c = _breach(tmp_path)
    read(c, 100)
    assert _unlock(c, **over)["unlocked"] is False and _cap(c) != ""


def test_sblocco_owner_non_ribasa_il_picco_e_operation_id_non_riusabile(tmp_path) -> None:
    c = _breach(tmp_path)
    read(c, 100)                                                   # perdita rientrata
    out = _unlock(c)
    assert out["unlocked"] is True, out
    assert _cap(c) == "" and O.D(_state(c)["peak"]) == O.D("100")
    read(c, 88)                                                    # nuovo superamento => blocco di nuovo
    assert _cap(c) != ""
    read(c, 100)
    assert _unlock(c)["unlocked"] is False, "operation_id riusato"


def test_sblocco_con_perdita_ancora_oltre_il_limite_si_riblocca_alla_lettura_successiva(tmp_path) -> None:
    c = _breach(tmp_path)
    _unlock(c)
    read(c, 88)
    assert _cap(c) != ""


def test_sblocco_con_salvataggio_fallito_resta_bloccato(tmp_path, monkeypatch) -> None:
    c = _breach(tmp_path)
    read(c, 100)
    real = c.db.save_settings

    def save(values):
        if KEY in (values or {}):
            raise OSError("disk full")
        return real(values)
    monkeypatch.setattr(c.db, "save_settings", save)
    assert _unlock(c)["unlocked"] is False and _cap(c) != ""


def test_sblocco_e_rebase_mai_chiamati_da_mcp_o_control_api() -> None:
    hits = []
    for root, _dirs, files in os.walk(REPO):
        if any(p in root for p in (os.sep + "tests", os.sep + ".git", "__pycache__")):
            continue
        for f in files:
            if f.endswith(".py"):
                path = os.path.join(root, f)
                src = open(path, encoding="utf-8", errors="ignore").read()
                if re.search(r"owner_(unlock_drawdown_hard_stop|clear_breach|rebase_peak)\s*\(", src):
                    hits.append(os.path.relpath(path, REPO))
    allowed = {os.path.join("core", "drawdown_hard_stop.py"), os.path.join("core", "runtime_controller.py")}
    assert set(hits) <= allowed, hits


# ------------------------------------------------------------------ esposizione: cap invariati
def test_esposizione_non_e_drawdown_ma_i_cap_di_esposizione_restano(tmp_path) -> None:
    c = build(tmp_path, max_total_exposure_pct=30.0)
    read(c, 100)
    read(c, 70, 30)
    assert _cap(c) == "" and c.rc.mode != RuntimeMode.LOCKDOWN
    assert c.rc.mm.fresh_caps_reason(cash=100, order_exposure=1, total_exposure=31, event_exposure=1) == \
        "supera_max_total_exposure"


def test_sblocco_rifiutato_con_ordini_incerti(tmp_path) -> None:
    c = _breach(tmp_path)
    read(c, 100)
    c.rc._mark_uncertain("K", 1, customer_ref="r")
    out = _unlock(c, operation_id="op-u")
    assert out["unlocked"] is False and out["reason"] == "ordini_incerti_presenti", out
    assert _cap(c) != ""


@pytest.mark.xfail(not DD08_FIX, strict=True, reason="RO-DD-08 (patch 15960bac): picco non salvato senza hard stop configurato => "
                                       "ingressi ammessi e dopo il riavvio il LOCKDOWN cumulativo usa il picco vecchio")
def test_picco_non_salvato_poi_riavvio_il_lockdown_cumulativo_non_si_perde(tmp_path, monkeypatch) -> None:
    """RO-DD-04 + regola 1: il LOCKDOWN usa il picco persistente. Senza hard stop assoluto/percentuale configurato,
    un picco salito (100 -> 120) ma NON salvato, seguito da un riavvio, non deve far ripartire dal vecchio picco:
    cassa 100 dopo il riavvio = 16,7 % dal picco reale 120 >= lockdown 15 % => nessun nuovo ingresso."""
    from core.money_management import DeskMode
    c = build(tmp_path, max_drawdown_abs=None, lockdown_drawdown_pct=15.0)
    read(c, 100)
    real = c.db.save_settings

    def save(values):
        if KEY in (values or {}):
            raise OSError("disk full")
        return real(values)
    monkeypatch.setattr(c.db, "save_settings", save)
    read(c, 120)                                                   # picco reale 120, non salvato
    entered_before_restart = _signal(c, "1.631")
    monkeypatch.undo()
    c2 = build(tmp_path, max_drawdown_abs=None, lockdown_drawdown_pct=15.0)
    read(c2, 100)
    led = O.Ledger()
    led.read(100), led.read(120), led.read(100)
    assert O.stop_reached(led.drawdown_pct(), 15)
    assert entered_before_restart is False or (c2.rc._desk_mode() == DeskMode.LOCKDOWN), (
        "picco non salvato: dopo il riavvio la perdita dal picco reale non e' piu' vista", entered_before_restart,
        _state(c2))


@pytest.mark.xfail(strict=True, reason="RO-DD-09 (rischio residuo, doppio guasto): rialzo del picco non salvato, perdita "
                                       "delle posizioni gia' aperte durante il guasto, riavvio => picco vecchio, LOCKDOWN perso")
def test_ro_dd_09_rialzo_non_salvato_poi_perdita_e_riavvio(tmp_path, monkeypatch) -> None:
    """Con 3f59fa67 gli ingressi sono bloccati mentre il picco non e' salvato (RO-DD-08 chiuso). Resta il doppio guasto:
    cassa 100 -> 120 (salvataggio fallito) -> 100 per settlement di posizioni gia' aperte -> riavvio col disco tornato:
    il picco ricaricato e' 100, il drawdown reale dal picco verificato 120 e' 16,7 % >= lockdown 15 %."""
    from core.money_management import DeskMode
    c = build(tmp_path, max_drawdown_abs=None, lockdown_drawdown_pct=15.0)
    read(c, 100)
    real = c.db.save_settings

    def save(values):
        if KEY in (values or {}):
            raise OSError("disk full")
        return real(values)
    monkeypatch.setattr(c.db, "save_settings", save)
    read(c, 120)
    read(c, 100)
    monkeypatch.undo()
    c2 = build(tmp_path, max_drawdown_abs=None, lockdown_drawdown_pct=15.0)
    read(c2, 100)
    assert c2.rc._desk_mode() == DeskMode.LOCKDOWN and _signal(c2, "1.641") is False, _state(c2)

"""#461 PR28-c (PKG-P24-B, DEC-426-P37/P38): RISK_STOP and exposure stop.

Owner sources (Phase 0, no conflict):
- #426 P37 ([5934306152]): stop at session loss and at total exposure, owner
  values; #426 P38: on stop, block new orders + cancel pertinent unmatched +
  attempt cashout, all three together, at the current gates.
- #426 decision 06/10 ([6026322035]) and docs/mcp_operational_contract.md:
  no fixed EUR 10/10 belongs to STOP/cashout; STOP/RISK_STOP blocks new
  orders, cancels pertinent unmatched and attempts cashout via Pickfair
  authority; PAUSE blocks new orders only; EMERGENCY is the strongest
  barrier; RESUME only after a new full readiness.
- #461 [5932610970]: P38 "PR28 coordinamento"; the physical STOP barrier
  (also on cashout) stays PR37.

RISK_STOP reuses the existing CASHOUT_ALL route (CashoutRouter): only bot
orders, own gates. Router pertinence (manual orders untouched) is already
proven by tests/unit/test_cashout_router.py::test_non_bot_orders_are_not_closed.
"""
from __future__ import annotations

import json

import pytest

from core.system_state import RuntimeMode
from database import Database
from services.setting_service import SettingsService
from tests.acceptance.test_issue437_pr04 import MERCATO, _aperto, headless  # noqa: F401
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti, _tavoli_occupati
from tests.acceptance.test_issue437_pr28b import _perdi, _sessione
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal

pytestmark = [pytest.mark.integration, pytest.mark.safety]


def _stop_eventi(c):
    return c.bus.payloads("RISK_STOP_TRIGGERED")


def _cashout_all(c):
    return [p for p in c.bus.payloads("SIGNAL_REJECTED") + c.bus.payloads("REQ_EXECUTE_CASHOUT")
            if isinstance(p, dict)]


# ==========================================================================
# 1. Exposure stop (owner limit, no default, fail-closed)
# ==========================================================================
def test_block_esposizione_raggiunta_rifiuta_e_scatta_risk_stop(tmp_path):
    c = _catena(tmp_path, max_exposure_stop=0.01)
    tavoli_prima = _tavoli_occupati(c)

    c.rc._on_signal_received(_signal())

    assert c.broker.state.orders == {}
    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert _rifiuti(c)[-1].startswith("exposure_stop_reached")
    assert _tavoli_occupati(c) == tavoli_prima
    assert c.rc.duplication_guard._active == {}
    (evento,) = _stop_eventi(c)
    assert evento["reason"].startswith("exposure_stop_reached")
    assert evento["cashout_attempted"] is True


def test_block_risk_stop_resta_attivo_anche_alzando_il_limite(tmp_path):
    c = _catena(tmp_path, max_exposure_stop=0.01)
    c.rc._on_signal_received(_signal())
    c.rc.config.max_exposure_stop = 1000.0

    c.rc._on_signal_received(_signal(market_id="1.777"))

    assert c.broker.state.orders == {}
    assert _rifiuti(c)[-1].startswith("risk_stop_active:exposure_stop_reached")
    assert len(_stop_eventi(c)) == 1


def test_pass_esposizione_sotto_il_limite(tmp_path):
    c = _catena(tmp_path, max_exposure_stop=1000.0)

    c.rc._on_signal_received(_signal())

    assert len(c.broker.state.orders) == 1, _rifiuti(c)
    assert _stop_eventi(c) == []


def test_pass_esposizione_non_impostata_nessuno_stop(tmp_path):
    c = _catena(tmp_path)
    assert c.rc.config.max_exposure_stop is None

    c.rc._on_signal_received(_signal())

    assert len(c.broker.state.orders) == 1, _rifiuti(c)


@pytest.mark.parametrize("valore", [float("nan"), float("inf"), True, "abc", 0.0, -5.0])
def test_block_limite_esposizione_illeggibile_blocca_senza_chiudere(tmp_path, valore):
    """A misconfigured limit blocks entries; it is not a breach, so no cashout."""
    c = _catena(tmp_path, max_exposure_stop=valore)

    c.rc._on_signal_received(_signal())

    assert c.broker.state.orders == {}
    assert _rifiuti(c)[-1] == "max_exposure_stop_non_valido"
    assert _stop_eventi(c) == []


def test_block_auto_next_bloccato_dal_risk_stop(tmp_path):
    c = _catena(tmp_path)
    c.rc.risk_stop("exposure_stop_reached:test")

    consentito, motivo = c.rc._risk_allows_auto_trade()

    assert consentito is False and motivo == "risk_stop_active:exposure_stop_reached:test"


# ==========================================================================
# 2. P37 session loss -> RISK_STOP; PAUSE / EMERGENCY / RESUME semantics
# ==========================================================================
def test_block_perdita_sessione_scatta_risk_stop_con_cashout(tmp_path):
    c = _sessione(tmp_path, max_session_loss=10.0)

    _perdi(c.rc, 10.0)

    (evento,) = _stop_eventi(c)
    assert evento["reason"].startswith("session_loss_breached")
    assert evento["cashout_attempted"] is True
    assert {"signal_type": "CASHOUT_ALL", "source": "RISK_STOP"}.items() <= next(
        p["signal"] for p in c.bus.payloads("SIGNAL_REJECTED")
        if isinstance(p, dict) and p.get("reason") == "cashout_chain_not_wired").items()


def test_pass_pause_blocca_solo_senza_cancel_ne_cashout(tmp_path):
    c = _catena(tmp_path)

    c.rc.pause()

    assert _stop_eventi(c) == []
    assert c.bus.payloads("REQ_EXECUTE_CASHOUT") == []
    assert c.rc._risk_stop_reason == ""


def test_block_emergency_resta_la_barriera_massima(tmp_path):
    c = _catena(tmp_path)
    c.rc.emergency_stop(reason="test")

    esito = c.rc.risk_stop("exposure_stop_reached:test")

    assert esito["cashout_attempted"] is False      # EMERGENCY already cancelled all
    assert c.rc.reset_risk_stop() == {"risk_stop_reset": False, "reason": "emergency_stop_active"}


def test_block_resume_rifiutato_durante_risk_stop(tmp_path):
    c = _catena(tmp_path)
    c.rc.risk_stop("exposure_stop_reached:test")
    c.rc.mode = RuntimeMode.PAUSED

    esito = c.rc.resume()

    assert esito["resumed"] is False
    assert esito["reason"] == "risk_stop_active:exposure_stop_reached:test"


def test_block_reset_rifiutato_se_esposizione_ancora_al_limite(tmp_path):
    """A blocked/uncertain cashout is not a successful close."""
    c = _catena(tmp_path, max_exposure_stop=1000.0)
    c.rc._on_signal_received(_signal())             # one open position
    c.rc.risk_stop("exposure_stop_reached:test")
    c.rc.config.max_exposure_stop = 0.01

    esito = c.rc.reset_risk_stop()

    assert esito["risk_stop_reset"] is False
    assert esito["reason"].startswith("exposure_stop_reached")


def test_pass_reset_dopo_ricontrollo_pulito_riapre(tmp_path):
    c = _catena(tmp_path, max_exposure_stop=1000.0)
    c.rc.risk_stop("exposure_stop_reached:test")

    assert c.rc.reset_risk_stop() == {"risk_stop_reset": True, "reason": ""}
    c.rc._on_signal_received(_signal())

    assert len(c.broker.state.orders) == 1, _rifiuti(c)
    assert len(c.bus.payloads("RISK_STOP_RESET")) == 1


def _avvia(c):
    c.rc.betfair_service.connect = lambda **_k: {"session": "sim"}
    c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0}
    return c.rc.start(execution_mode="SIMULATION")


def test_block_start_non_riapre_da_solo_lo_stop_esposizione(tmp_path):
    """Fix c2 (owner review): start() never clears a stop from in-memory tables."""
    c = _catena(tmp_path, max_exposure_stop=1000.0)
    c.rc.risk_stop("exposure_stop_reached:test")
    c.rc.settings_service.load_roserpina_config = lambda: c.rc.config

    _avvia(c)

    assert c.rc._risk_stop_reason == "exposure_stop_reached:test"
    assert c.rc.reset_risk_stop() == {"risk_stop_reset": True, "reason": ""}   # explicit owner reset


def test_block_boot_reale_stop_esposizione_con_ordini_aperti(tmp_path):
    """Fix c2 (owner review, real boot): process dies with the stop on disk and a
    bot order open; the new process has empty tables (exposure 0) and start()
    must keep the stop, keep entries blocked and retry CASHOUT_ALL."""
    c = _catena(tmp_path, max_exposure_stop=1000.0)
    c.rc._on_signal_received(_signal())                    # bot order open
    assert len(c.broker.state.orders) == 1
    c.rc.config.max_exposure_stop = 0.01
    c.rc._on_signal_received(_signal(market_id="1.777"))   # -> exposure_stop_reached
    motivo = c.rc._risk_stop_reason
    assert motivo.startswith("exposure_stop_reached")

    riavvio = _catena(tmp_path, max_exposure_stop=1000.0)  # crash + restart
    riavvio.rc.settings_service.load_roserpina_config = lambda: riavvio.rc.config
    assert riavvio.rc.table_manager.total_exposure() == 0.0
    _avvia(riavvio)

    assert riavvio.rc._risk_stop_reason == motivo
    assert json.loads(riavvio.rc.db.get_settings()["risk_stop_state"])["reason"] == motivo
    assert any(isinstance(p, dict) and p.get("signal", {}).get("source") == "RISK_STOP"
               for p in riavvio.bus.payloads("SIGNAL_REJECTED"))    # CASHOUT_ALL retried
    riavvio.rc._on_signal_received(_signal(market_id="1.888"))
    assert riavvio.broker.state.orders == {}
    assert _rifiuti(riavvio)[-1] == f"risk_stop_active:{motivo}"


def test_block_stop_non_salvato_su_db_sopravvive_al_riavvio(tmp_path):
    """Fix c2 (owner 23:28): save_settings fails -> blocked at once, and the
    marker next to the db keeps the stop through construct + start()."""
    c = _catena(tmp_path)

    def _rotto(_valori):
        raise OSError("disk full")

    salva = c.rc.db.save_settings
    c.rc.db.save_settings = _rotto
    esito = c.rc.risk_stop("exposure_stop_reached:test")
    c.rc._on_signal_received(_signal())                    # still the same process
    c.rc.db.save_settings = salva

    assert esito["persisted"] is True                      # via marker
    assert c.broker.state.orders == {}
    assert _rifiuti(c)[-1] == "risk_stop_active:exposure_stop_reached:test"

    riavvio = _catena(tmp_path)                            # crash + restart
    riavvio.rc.settings_service.load_roserpina_config = lambda: riavvio.rc.config
    _avvia(riavvio)

    assert riavvio.rc._risk_stop_reason == "exposure_stop_reached:test"
    assert any(isinstance(p, dict) and p.get("signal", {}).get("source") == "RISK_STOP"
               for p in riavvio.bus.payloads("SIGNAL_REJECTED"))    # CASHOUT_ALL retried
    riavvio.rc._on_signal_received(_signal(market_id="1.888"))
    assert riavvio.broker.state.orders == {}
    assert _rifiuti(riavvio)[-1] == "risk_stop_active:exposure_stop_reached:test"


def _marker(c):
    return str(c.rc.db.db_path) + ".risk_stop_pending.json"


def _scrivi(c, db=None, marker=None):
    if db is not None:
        c.db.save_settings({"risk_stop_state": db})
    if marker is not None:
        with open(_marker(c), "w", encoding="utf-8") as fh:
            fh.write(marker)


def _stato(active, reason, seq):
    return json.dumps({"active": active, "reason": reason, "seq": seq})


def test_block_stop_con_versione_db_ignota_prevale_su_reset_vecchio(tmp_path):
    """Fix c3 (Sol): se lettura+scrittura DB falliscono, il nuovo stop nel
    marker ha versione ignota e non puo' perdere contro un vecchio reset."""
    c = _catena(tmp_path)
    _scrivi(c, db=_stato(False, "", 2))
    get_settings, save_settings = c.rc.db.get_settings, c.rc.db.save_settings
    c.rc.db.get_settings = lambda: (_ for _ in ()).throw(OSError("read failed"))
    c.rc.db.save_settings = lambda _v: (_ for _ in ()).throw(OSError("write failed"))

    esito = c.rc.risk_stop("exposure_stop_reached:after-reset")

    c.rc.db.get_settings, c.rc.db.save_settings = get_settings, save_settings
    with open(_marker(c), encoding="utf-8") as fh:
        marker = json.load(fh)
    assert esito["persisted"] is True
    assert marker["active"] is True and marker["seq"] is None

    riavvio = _catena(tmp_path)
    assert riavvio.rc._risk_stop_reason == "exposure_stop_reached:after-reset"


def test_pass_reset_sostituisce_marker_attivo_a_versione_ignota(tmp_path):
    """Fix c3 (Grok): un reset esplicito puo' riaprire soltanto dopo aver
    reso db+marker riconciliabili come inattivi."""
    seed = _catena(tmp_path)
    _scrivi(
        seed,
        db=_stato(False, "", 2),
        marker=_stato(True, "exposure_stop_reached:unknown", None),
    )
    c = _catena(tmp_path, max_exposure_stop=1000.0)
    assert c.rc._risk_stop_reason == "exposure_stop_reached:unknown"

    assert c.rc.reset_risk_stop() == {"risk_stop_reset": True, "reason": ""}

    with open(_marker(c), encoding="utf-8") as fh:
        marker = json.load(fh)
    assert marker["active"] is False and marker["seq"] >= 3
    assert _catena(tmp_path).rc._risk_stop_reason == ""


def test_block_reset_non_riapre_se_marker_ignoto_non_si_aggiorna(tmp_path, monkeypatch):
    """Fix c3 (Grok): DB reset scritto non basta se un marker active/seq=None
    resta autorevole in modo conservativo."""
    from core import risk_stop as stop_rules

    seed = _catena(tmp_path)
    _scrivi(
        seed,
        db=_stato(False, "", 2),
        marker=_stato(True, "exposure_stop_reached:unknown", None),
    )
    c = _catena(tmp_path, max_exposure_stop=1000.0)
    monkeypatch.setattr(
        stop_rules,
        "atomic_write_text",
        lambda *_a, **_k: (_ for _ in ()).throw(OSError("marker write failed")),
    )

    esito = c.rc.reset_risk_stop()

    assert esito == {"risk_stop_reset": False, "reason": "risk_stop_reset_non_persistito"}
    assert c.rc._risk_stop_reason == "exposure_stop_reached:unknown"
    assert _catena(tmp_path).rc._risk_stop_reason == "exposure_stop_reached:unknown"



def test_block_reset_readback_fallito_ripristina_barriera_durevole(tmp_path):
    """Fix c4 (Sol): write reset riuscite ma readback fallito => reset rifiutato
    e restart ancora bloccato, mai active=False durevole come unico stato."""
    c = _catena(tmp_path, max_exposure_stop=1000.0)
    c.rc.risk_stop("exposure_stop_reached:readback")
    original_get = c.rc.db.get_settings
    calls = 0

    def _get_flaky():
        nonlocal calls
        calls += 1
        if calls == 1:
            return original_get()
        raise OSError("readback failed")

    c.rc.db.get_settings = _get_flaky
    esito = c.rc.reset_risk_stop()
    c.rc.db.get_settings = original_get

    assert esito == {"risk_stop_reset": False, "reason": "risk_stop_reset_non_persistito"}
    assert c.rc._risk_stop_reason == "exposure_stop_reached:readback"
    assert _catena(tmp_path).rc._risk_stop_reason != ""


def test_block_reset_marker_non_armabile_non_scrive_stato_inattivo(tmp_path, monkeypatch):
    """Fix c4: il marker write-ahead deve essere armato prima di active=False."""
    from core import risk_stop as stop_rules

    c = _catena(tmp_path, max_exposure_stop=1000.0)
    c.rc.risk_stop("exposure_stop_reached:marker")
    monkeypatch.setattr(
        stop_rules,
        "atomic_write_text",
        lambda *_a, **_k: (_ for _ in ()).throw(OSError("marker unavailable")),
    )

    esito = c.rc.reset_risk_stop()

    assert esito == {"risk_stop_reset": False, "reason": "risk_stop_reset_non_persistito"}
    assert c.rc._risk_stop_reason == "exposure_stop_reached:marker"
    saved = json.loads(c.rc.db.get_settings()["risk_stop_state"])
    assert saved["active"] is True
    assert _catena(tmp_path).rc._risk_stop_reason != ""

@pytest.mark.parametrize("db, marker, atteso", [
    (_stato(True, "exposure_stop_reached:x", 2), _stato(False, "", 3), ""),          # newer reset wins
    (_stato(False, "", 3), _stato(True, "exposure_stop_reached:x", 2), ""),          # stale copy loses
    (_stato(False, "", 3), _stato(True, "exposure_stop_reached:x", 4), "exposure_stop_reached:x"),
    (_stato(False, "", 3), _stato(True, "exposure_stop_reached:x", 3), "exposure_stop_reached:x"),  # tie
    (_stato(False, "", 3), "{rotto", "risk_stop_state_illeggibile"),                 # unreadable copy
    ("{rotto", _stato(True, "session_loss_breached:x", 1), "session_loss_breached:x"),
    (json.dumps({"reason": ""}), json.dumps({"reason": "session_loss_breached:x"}), "session_loss_breached:x"),
    (_stato(False, "exposure_stop_reached:x", 9), None, "risk_stop_state_illeggibile"),  # inconsistent
])
def test_block_ripristino_versionato_e_conservativo(tmp_path, db, marker, atteso):
    """Fix c2 (owner 23:30): highest verifiable seq wins; tie/unknown/unreadable -> stop."""
    _scrivi(_catena(tmp_path), db=db, marker=marker)

    assert _catena(tmp_path).rc._risk_stop_reason == atteso


def test_pass_reset_esplicito_nuova_versione_non_risorge(tmp_path):
    """Fix c2/c4: un reset valido sincronizza una nuova versione inattiva e
    una vecchia barriera non puo\' risorgere al restart."""
    c = _catena(tmp_path, max_exposure_stop=1000.0)
    salva = c.rc.db.save_settings
    c.rc.db.save_settings = lambda _v: (_ for _ in ()).throw(OSError("disk full"))
    c.rc.risk_stop("exposure_stop_reached:test")           # active copy only in the marker
    c.rc.db.save_settings = salva

    assert c.rc.reset_risk_stop()["risk_stop_reset"] is True
    import os
    assert os.path.exists(_marker(c))                       # marker sincronizzato alla versione di reset
    riavvio = _catena(tmp_path)
    assert riavvio.rc._risk_stop_reason == ""


def test_block_reset_non_persistito_resta_bloccato(tmp_path, monkeypatch):
    """Fix c2 (owner 23:30): a reset that cannot be saved anywhere is refused."""
    from core import risk_stop as stop_rules
    c = _catena(tmp_path, max_exposure_stop=1000.0)
    c.rc.risk_stop("exposure_stop_reached:test")
    c.rc.db.save_settings = lambda _v: (_ for _ in ()).throw(OSError("disk full"))
    monkeypatch.setattr(stop_rules, "atomic_write_text",
                        lambda *_a, **_k: (_ for _ in ()).throw(OSError("disk full")), raising=False)

    esito = c.rc.reset_risk_stop()

    assert esito == {"risk_stop_reset": False, "reason": "risk_stop_reset_non_persistito"}
    assert c.rc._risk_stop_reason == "exposure_stop_reached:test"
    assert _catena(tmp_path).rc._risk_stop_reason == "exposure_stop_reached:test"

def test_block_start_non_riapre_stop_da_perdita_di_sessione(tmp_path):
    """Fix c1 (Sol + Grok): the new session cannot verify the loss that stopped it."""
    c = _sessione(tmp_path, max_session_loss=10.0)
    _perdi(c.rc, 10.0)
    assert c.rc._risk_stop_reason.startswith("session_loss_breached")

    _avvia(c)

    assert c.rc._risk_stop_reason.startswith("session_loss_breached")
    assert c.rc.reset_risk_stop(at_start=True)["reason"] == "risk_stop_reset_esplicito_richiesto"
    assert c.rc.reset_risk_stop() == {"risk_stop_reset": True, "reason": ""}   # explicit owner reset


def test_block_start_ritenta_il_cashout_dello_stop_ripristinato(tmp_path):
    """Fix c1 (Grok): after a restart the restored stop re-attempts CASHOUT_ALL."""
    _catena(tmp_path).rc.risk_stop("session_loss_breached:x")

    riavvio = _catena(tmp_path)
    _avvia(riavvio)

    assert riavvio.rc._risk_stop_reason == "session_loss_breached:x"
    (rifiuto,) = [p for p in riavvio.bus.payloads("SIGNAL_REJECTED") if isinstance(p, dict)]
    assert rifiuto["signal"]["source"] == "RISK_STOP"      # no subscriber wired here


def test_block_reset_concorrente_non_cancella_lo_stop_su_disco(tmp_path, monkeypatch):
    """Fix c1 (Sol + Grok): transition and persistence are atomic under the lock."""
    import threading

    from core import risk_stop as stop_rules
    c = _catena(tmp_path, max_exposure_stop=1000.0)
    c.rc.risk_stop("exposure_stop_reached:a")
    originale, fili = stop_rules.persist_risk_stop, []

    def _persist(db, reason):  # a new trigger lands while the reset is saving ""
        if reason == "" and not fili:
            fili.append(threading.Thread(target=c.rc.risk_stop, args=("exposure_stop_reached:b",)))
            fili[0].start()
            fili[0].join(0.3)
        return originale(db, reason)

    monkeypatch.setattr(stop_rules, "persist_risk_stop", _persist)
    c.rc.reset_risk_stop()
    fili[0].join(5)

    salvato = json.loads(c.rc.db.get_settings()["risk_stop_state"])["reason"]
    assert c.rc._risk_stop_reason == salvato == "exposure_stop_reached:b"


def test_block_start_non_riapre_se_la_posizione_resta_aperta(tmp_path):
    """start() rebuilds the tables: the recheck uses the exposure held before."""
    c = _catena(tmp_path, max_exposure_stop=1000.0)
    c.rc._on_signal_received(_signal())             # one open position
    c.rc.config.max_exposure_stop = 0.01
    c.rc.risk_stop("exposure_stop_reached:test")    # cashout chain not wired
    c.rc.betfair_service.connect = lambda **_k: {"session": "sim"}
    c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0}
    c.rc.settings_service.load_roserpina_config = lambda: c.rc.config

    c.rc.start(execution_mode="SIMULATION")

    assert c.rc._risk_stop_reason == "exposure_stop_reached:test"


def test_pass_cashout_consentito_durante_risk_stop(tmp_path):
    c = _catena(tmp_path)
    c.rc.risk_stop("exposure_stop_reached:test")

    aperto, motivo = c.rc._cashout_gates_still_open({"signal_type": "CASHOUT"})

    assert aperto is True, motivo


# ==========================================================================
# 3. Durability: a restart does not clear the RISK_STOP
# ==========================================================================
def test_block_risk_stop_sopravvive_al_riavvio(tmp_path):
    c = _catena(tmp_path)
    c.rc.risk_stop("exposure_stop_reached:test")

    riavvio = _catena(tmp_path)
    riavvio.rc._on_signal_received(_signal())

    assert riavvio.broker.state.orders == {}
    assert _rifiuti(riavvio)[-1] == "risk_stop_active:exposure_stop_reached:test"


@pytest.mark.parametrize("grezzo", ["{rotto", "null", "[]", json.dumps({"x": 1}), "5"])
def test_block_stato_risk_stop_illeggibile_resta_attivo(tmp_path, grezzo):
    c = _catena(tmp_path)
    c.db.save_settings({"risk_stop_state": grezzo})

    riavvio = _catena(tmp_path)

    assert riavvio.rc._risk_stop_reason == "risk_stop_state_illeggibile"


def test_pass_limite_esposizione_salvato_e_ricaricato(tmp_path):
    svc = SettingsService(Database(str(tmp_path / "settings.db")))
    cfg = svc.load_roserpina_config()
    assert cfg.max_exposure_stop is None
    cfg.max_exposure_stop = 10.0
    svc.save_roserpina_config(cfg)

    assert svc.load_roserpina_config().max_exposure_stop == 10.0


# ==========================================================================
# 4. End to end on the real headless app + SIM broker: cancel + cashout
# ==========================================================================
def test_block_risk_stop_cancella_unmatched_e_chiude_posizione_del_bot(headless):  # noqa: F811
    headless.avvia()
    headless.posizione()                          # BACK 10 @ 2.02 matched
    resting = headless.broker.place_bet(           # pertinent unmatched bot order
        market_id=MERCATO, selection_id=22, side="BACK", price=5.0, size=2.0,
        event_name="Alfa v Beta")
    assert float(resting["instructionReports"][0]["sizeMatched"]) == 0.0

    esito = headless.rt.risk_stop("exposure_stop_reached:e2e")
    headless.svuota_bus()

    assert esito["cashout_attempted"] is True
    assert len(headless.hedge()) == 1, "matched bot position not closed"
    aperti = [o for o in headless.broker.get_current_orders(None)
              if o.get("selectionId") == 22 and float(o.get("sizeRemaining") or 0) > 0]
    assert aperti == [], "pertinent unmatched order not cancelled"
    assert headless.di("RISK_STOP_TRIGGERED")

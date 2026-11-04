"""F-11 (#586, DD-03/DD-04) — il picco dell'equity sopravvive al riavvio; un picco non dimostrabile blocca il rischio.

Decisioni owner (#426): opzione A per la migrazione pre-v9 (c.6145958246) e per la ricostruzione del picco
(c.6146717266): persistenza atomica, nessun reset del picco, nessun valore inventato; se il picco non e' verificabile
nessun nuovo rischio e nessuno sblocco automatico (la ricostruzione dallo storico Betfair richiede la ratifica owner).

Su `main` `ada4076` `RiskDesk.equity_peak` e `RuntimeController._cash_equity_peak` vivevano solo in memoria: al
riavvio ripartivano dal saldo corrente. Riprodotto sul runtime vero (HeadlessApp, SQLite su disco, harness F026):
picco 100 e drawdown 3% prima del riavvio, picco 97 e drawdown 0% dopo. Gli stop sul drawdown (assoluto e %, lockdown
del MM) e l'auto-reset Roserpina ripartivano da zero dopo ogni perdita seguita da un riavvio.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile

import pytest

from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "f026"))

KEY = "risk.equity_peaks_state"


def _settings(c) -> dict:
    return c.db.get_settings() or {}


def _peaks(c) -> dict:
    return json.loads(_settings(c)[KEY])


def _write(c, value) -> None:
    c.db.save_settings({KEY: value if isinstance(value, str) else json.dumps(value)})


# ------------------------------------------------------------------ runtime vero: perdita, riavvio
@pytest.mark.safety
def test_block_picco_e_drawdown_sopravvivono_al_riavvio_runtime_vero() -> None:
    from test_f026_financial import run

    home = tempfile.mkdtemp(prefix="f11_")
    primo = run([{"op": "signal", "m": "1.901", "price": 2.0}, {"op": "settle", "m": "1.901", "win": False},
                 {"op": "wait", "s": 2}], home=home)
    secondo = run([{"op": "wait", "s": 3}], home=home)

    prima, dopo = primo["snaps"][-1], secondo["snaps"][0]
    assert prima["bankroll"] < prima["peak"], f"precondizione: perdita registrata {prima}"
    assert secondo["mode_after_start"] == "ACTIVE", secondo.get("start")
    assert dopo["peak"] == prima["peak"], f"picco azzerato dal riavvio: {prima['peak']} -> {dopo['peak']}"
    assert dopo["cash_peak"] == prima["cash_peak"], "picco di cassa (auto-reset) azzerato dal riavvio"
    assert dopo["drawdown_pct"] == pytest.approx(prima["drawdown_pct"]), "drawdown azzerato dal riavvio"


# ------------------------------------------------------------------ ripristino
def test_ripristino_del_picco_persistito_mai_abbassato(tmp_path) -> None:
    c = _chain(tmp_path)
    _write(c, {"equity_peak": 1000.0, "cash_equity_peak": 1200.0})

    assert c.rc._restore_equity_peaks() is True
    assert c.rc.risk_desk.equity_peak == 1000.0
    assert c.rc._cash_equity_peak == 1200.0
    c.rc.risk_desk.sync_bankroll(900.0)                       # perdita dopo il riavvio
    assert c.rc.risk_desk.equity_peak == 1000.0
    assert c.rc.risk_desk.drawdown_pct() == pytest.approx(10.0)  # scheda F-11: picco 1000, equity 900 => 10 %


def test_ripristino_non_abbassa_un_picco_in_memoria_piu_alto(tmp_path) -> None:
    c = _chain(tmp_path)
    c.rc.risk_desk.sync_bankroll(1500.0)
    _write(c, {"equity_peak": 1000.0, "cash_equity_peak": 0.0})

    assert c.rc._restore_equity_peaks() is True
    assert c.rc.risk_desk.equity_peak == 1500.0


# ------------------------------------------------------------------ picco non dimostrabile => nessun nuovo rischio
@pytest.mark.parametrize("valore", ["{non json", json.dumps({"equity_peak": "abc", "cash_equity_peak": 0}),
                                    json.dumps({"equity_peak": float("nan"), "cash_equity_peak": 0}),
                                    json.dumps({"equity_peak": -5, "cash_equity_peak": 0}),
                                    json.dumps({"equity_peak": 0, "cash_equity_peak": 0}),
                                    json.dumps({"equity_peak": 100.0, "cash_equity_peak": -1}),
                                    json.dumps({"equity_peak": True, "cash_equity_peak": 0}),
                                    json.dumps([1, 2])])
def test_block_picco_corrotto_blocca_e_non_viene_sovrascritto(tmp_path, valore: str) -> None:
    c = _chain(tmp_path)
    _write(c, valore)

    assert c.rc._restore_equity_peaks() is False
    rebuild = c.rc._gate_rebuild_on_equity_peaks({"ok": True, "reason": ""})
    assert rebuild["ok"] is False and rebuild["reason"] == "EQUITY_PEAKS_CORRUPT", rebuild
    c.rc.risk_desk.sync_bankroll(50.0)
    assert c.rc._persist_equity_peaks() is False
    assert _settings(c)[KEY] == valore, "stato corrotto sovrascritto: l'evidenza va conservata"


def test_block_picco_assente_con_storico_di_trading_blocca(tmp_path) -> None:
    """Installazione pre-v9: ha gia' operato (stato Roserpina persistito) ma non ha il picco. Nessun valore
    inventato, nessuno sblocco automatico: ricostruzione da ratificare (opzione A)."""
    c = _chain(tmp_path)
    c.db.save_settings({c.rc._EXPANSION_KEY: json.dumps({"baseline": 100.0, "set_at_ts": 1.0})})

    assert c.rc._restore_equity_peaks() is False
    rebuild = c.rc._gate_rebuild_on_equity_peaks({"ok": True, "reason": ""})
    assert rebuild["ok"] is False and rebuild["reason"] == "EQUITY_PEAKS_MISSING_WITH_HISTORY", rebuild
    c.rc.risk_desk.sync_bankroll(80.0)
    assert c.rc._persist_equity_peaks() is False
    assert KEY not in _settings(c), "picco inventato dal saldo corrente su un'installazione con storico"


def test_block_picco_assente_con_ordini_nel_db_blocca(tmp_path) -> None:
    c = _chain(tmp_path)
    c.db.insert_order({"customer_ref": "LEGACY-1", "correlation_id": "c1", "status": "SUBMITTED",
                       "payload": {}, "created_at": 1.0, "outcome": None})

    assert c.rc._restore_equity_peaks() is False
    assert c.rc._gate_rebuild_on_equity_peaks({"ok": True, "reason": ""})["reason"] == \
        "EQUITY_PEAKS_MISSING_WITH_HISTORY"


def test_block_impostazioni_illeggibili_bloccano(tmp_path) -> None:
    c = _chain(tmp_path)

    def _boom():
        raise RuntimeError("disk I/O error")

    c.db.get_settings = _boom
    assert c.rc._restore_equity_peaks() is False
    assert c.rc._gate_rebuild_on_equity_peaks({"ok": True, "reason": ""})["reason"] == "EQUITY_PEAKS_UNREADABLE"


def test_block_storico_illeggibile_blocca(tmp_path) -> None:
    """Picco assente e tabella ordini illeggibile: non si sa se l'installazione ha gia' operato => blocco."""
    c = _chain(tmp_path)
    real = c.db._execute

    def _execute(sql, *a, **k):
        if "FROM orders" in str(sql):
            raise RuntimeError("database disk image is malformed")
        return real(sql, *a, **k)

    c.db._execute = _execute
    assert c.rc._restore_equity_peaks() is False
    assert c.rc._gate_rebuild_on_equity_peaks({"ok": True, "reason": ""})["reason"] == "EQUITY_PEAKS_UNREADABLE"


@pytest.mark.safety
def test_block_avvio_reale_di_un_installazione_con_storico_senza_picco_parte_in_pausa() -> None:
    """Runtime vero: un'installazione che ha gia' operato e non ha il picco (pre-v9) parte in PAUSED, non ammette
    segnali e non inventa il picco."""
    import glob

    from database import Database
    from test_f026_financial import run

    home = tempfile.mkdtemp(prefix="f11_legacy_")
    run([{"op": "signal", "m": "1.911", "price": 2.0}, {"op": "settle", "m": "1.911", "win": False}], home=home)
    dbs = glob.glob(os.path.join(home, "**", "*.db"), recursive=True)
    assert len(dbs) == 1, dbs
    Database(dbs[0])._execute("DELETE FROM settings WHERE key = ?", (KEY,))      # installazione pre-v9

    secondo = run([{"op": "signal", "m": "1.912", "price": 2.0}], home=home)

    assert secondo["mode_after_start"] == "PAUSED", secondo.get("start")
    assert secondo["rebuild"]["ok"] is False
    assert secondo["rebuild"]["reason"] == "EQUITY_PEAKS_MISSING_WITH_HISTORY", secondo["rebuild"]
    assert not any(o["market_id"] == "1.912" and o["status"] == "SUBMITTED" for o in secondo["snaps"][-1]["orders"])
    assert KEY not in (Database(dbs[0]).get_settings() or {}), "picco inventato all'avvio"


def test_installazione_nuova_senza_storico_non_blocca_e_persiste_dalla_prima_lettura(tmp_path) -> None:
    c = _chain(tmp_path)

    assert c.rc._restore_equity_peaks() is True
    assert c.rc._gate_rebuild_on_equity_peaks({"ok": True, "reason": ""})["ok"] is True
    c.rc.risk_desk.sync_bankroll(250.0)
    assert c.rc._persist_equity_peaks() is True
    assert _peaks(c)["equity_peak"] == 250.0


def test_gate_non_nasconde_un_motivo_precedente(tmp_path) -> None:
    c = _chain(tmp_path)
    _write(c, "{non json")
    c.rc._restore_equity_peaks()

    rebuild = c.rc._gate_rebuild_on_equity_peaks({"ok": False, "reason": "EXCHANGE_ORDERS_UNAVAILABLE"})
    assert rebuild["ok"] is False and rebuild["reason"] == "EXCHANGE_ORDERS_UNAVAILABLE"
    assert rebuild["equity_peaks_unverified"] == "EQUITY_PEAKS_CORRUPT"


# ------------------------------------------------------------------ persistenza
def test_persistenza_solo_dopo_il_ripristino(tmp_path) -> None:
    """Prima di START (es. `get_status` all'apertura) lo stato in memoria non e' quello persistito: scriverlo
    abbasserebbe il picco salvato."""
    c = _chain(tmp_path)
    _write(c, {"equity_peak": 1000.0, "cash_equity_peak": 1000.0})
    c.rc.risk_desk.sync_bankroll(300.0)

    assert c.rc._persist_equity_peaks() is False
    assert _peaks(c)["equity_peak"] == 1000.0


def test_nuovo_picco_del_risk_desk_persistito_dalla_lettura_dei_fondi(tmp_path) -> None:
    c = _chain(tmp_path)
    _write(c, {"equity_peak": 100.0, "cash_equity_peak": 100.0})
    c.rc._restore_equity_peaks()
    c.rc.risk_desk.equity_peak = 100.0                          # harness: bankroll iniziale 1000 in memoria

    c.rc._apply_funds_reading({"available": 140.0, "exposure": 0.0}, source="settlement", read_at=None)

    assert _peaks(c)["equity_peak"] == 140.0


def test_nuovo_picco_di_cassa_persistito_dallo_snapshot_del_conto(tmp_path) -> None:
    c = _chain(tmp_path)
    _write(c, {"equity_peak": 100.0, "cash_equity_peak": 100.0})
    c.rc._restore_equity_peaks()

    c.rc._record_account_snapshot({"available": 140.0, "exposure": 0.0}, source="settlement")

    assert _peaks(c)["cash_equity_peak"] == 140.0


def test_stato_tavoli_e_picchi_nella_stessa_transazione(tmp_path) -> None:
    """Settlement e reset di ciclo scrivono lo stato tavoli in una transazione: i picchi viaggiano con essa
    (l'auto-reset ribasa il picco di cassa insieme al marker del reset)."""
    c = _chain(tmp_path)
    _write(c, {"equity_peak": 100.0, "cash_equity_peak": 100.0})
    c.rc._restore_equity_peaks()
    c.rc._cash_equity_peak = 80.0                               # ribasato dall'auto-reset

    payload, _context = c.rc._prepare_recovery_persist(cycle_reset=True, reset_ts=5.0)
    assert json.loads(payload[KEY])["cash_equity_peak"] == 80.0
    assert json.loads(payload[KEY])["equity_peak"] == 100.0


def test_transazione_dello_stato_tavoli_senza_picchi_se_non_verificati(tmp_path) -> None:
    c = _chain(tmp_path)
    _write(c, "{non json")
    c.rc._restore_equity_peaks()

    payload, _context = c.rc._prepare_recovery_persist()
    assert KEY not in payload


def test_resume_non_scavalca_il_picco_non_verificato(tmp_path) -> None:
    c = _chain(tmp_path)
    c.db.save_settings({c.rc._EXPANSION_KEY: json.dumps({"baseline": 100.0, "set_at_ts": 1.0})})
    c.rc._restore_equity_peaks()
    c.rc._reconciliation_required = True
    c.rc._rebuild_open_positions = lambda: {"ok": True, "reason": "", "rebuilt": [], "uncertain": [],
                                            "closed": [], "unattributed": []}

    esito = c.rc.resume()

    assert esito["resumed"] is False and "EQUITY_PEAKS_MISSING_WITH_HISTORY" in esito["reason"], esito


def test_resume_sblocca_solo_dopo_un_picco_valido_persistito(tmp_path) -> None:
    """Nessuno sblocco automatico: resume() resta bloccato finche' non esiste un picco valido persistito (es. la
    ricostruzione ratificata dall'owner); a quel punto lo ripristina e lascia passare."""
    c = _chain(tmp_path)
    c.db.save_settings({c.rc._EXPANSION_KEY: json.dumps({"baseline": 100.0, "set_at_ts": 1.0})})
    c.rc._restore_equity_peaks()
    c.rc._reconciliation_required = True
    c.rc._rebuild_open_positions = lambda: {"ok": True, "reason": "", "rebuilt": [], "uncertain": [],
                                            "closed": [], "unattributed": []}
    assert c.rc.resume()["resumed"] is False

    _write(c, {"equity_peak": 1200.0, "cash_equity_peak": 1200.0})      # picco ratificato scritto dall'owner
    esito = c.rc.resume()

    assert esito["resumed"] is True, esito
    assert c.rc.risk_desk.equity_peak == 1200.0                         # mai abbassato, qui piu' alto della memoria

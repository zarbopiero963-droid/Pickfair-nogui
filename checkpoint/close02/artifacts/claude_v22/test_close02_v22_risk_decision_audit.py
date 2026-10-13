"""close02 v22 — CL22-AUDIT-DECISION-001: decisioni di rischio ricostruibili e osservabili fuori dai log.

Fonti: L6 AUDIT (#461/6085510337, verbatim) «Ricostruire valore, decisione, origine, versione, correlation_id,
esito e recovery senza esporre segreti»; mandato owner CLOSE03 §3 L6-OBSERVABLE «anomalie, blocchi, …
rilevabili senza affidarsi esclusivamente ai log testuali». RED su v21: il rifiuto del cap era solo un evento
del bus e una riga di log (headless_main._on_signal_rejected); nessuna riga nel DB, nulla in get_status()."""
import json
import sqlite3
from unittest.mock import MagicMock

from controllers.dutching_controller import DutchingController
from tests.close02.test_close02_restart_t81u import OWNER_VALID, _ctl

OWNER = dict(OWNER_VALID, **{"roserpina.max_open_exposure": "1"})
SEGRETO = "tok-SEGRETISSIMO-123"


def _avvia(tmp_path, monkeypatch):
    path = str(tmp_path / "a.sqlite")
    rc = _ctl(path, monkeypatch, owner=OWNER)
    rc.start(execution_mode="SIMULATION")
    monkeypatch.setattr(rc.mm, "calculate", MagicMock(
        side_effect=lambda **kw: MagicMock(approved=True, recommended_stake=5.0, table_id=1, reason="")))
    return rc, path


def _decisioni(path):
    con = sqlite3.connect(path)
    rows = [json.loads(r[0]) for r in con.execute("select event_json from audit_events order by id")]
    return [r for r in rows if r.get("event") == "RISK_DECISION"]


SEGNALE = {"market_id": "1.123", "selection_id": "456", "price": 2.0, "signal_type": "BACK",
           "customer_ref": "sig-audit-1", "raw_text": f"messaggio con {SEGRETO}", "chat_id": -1001}


def test_rifiuto_persistito_con_valore_decisione_origine_versione_ref(tmp_path, monkeypatch):
    rc, path = _avvia(tmp_path, monkeypatch)
    rc._on_signal_received(dict(SEGNALE))
    rec = _decisioni(path)
    assert len(rec) == 1, rec
    r = rec[0]
    assert r["decision"] == "REJECTED" and r["reason"].startswith("max_open_exposure_exceeded")
    assert r["limits"]["max_open_exposure"] == 1.0
    assert r["origin"] == "signal" and r["customer_ref"] == "sig-audit-1"
    assert r["config_version"] is None  # config mai salvata dal percorso versionato: nessuna versione inventata
    assert r["market_id"] == "1.123" and r["side"] == "BACK"


def test_record_senza_segreti_ne_testo_libero(tmp_path, monkeypatch):
    rc, path = _avvia(tmp_path, monkeypatch)
    rc._on_signal_received(dict(SEGNALE))
    con = sqlite3.connect(path)
    dump = json.dumps([r[0] for r in con.execute("select event_json from audit_events")])
    assert SEGRETO not in dump and "raw_text" not in dump


def test_rifiuto_osservabile_in_get_status(tmp_path, monkeypatch):
    rc, _ = _avvia(tmp_path, monkeypatch)
    rc._on_signal_received(dict(SEGNALE))
    st = rc.get_status()["risk_decisions"]
    assert st["rejected_total"] == 1 and st["last_rejection"]["reason"].startswith("max_open_exposure_exceeded")


def test_ammissione_persistita(tmp_path, monkeypatch):
    rc, path = _avvia(tmp_path, monkeypatch)
    monkeypatch.setattr(rc.mm, "calculate", MagicMock(
        side_effect=lambda **kw: MagicMock(approved=True, recommended_stake=0.5, table_id=1, reason="")))
    rc._on_signal_received(dict(SEGNALE, customer_ref="sig-audit-ok"))
    rec = _decisioni(path)
    assert [r["decision"] for r in rec] == ["APPROVED"], rec
    assert rec[0]["customer_ref"] == "sig-audit-ok" and rec[0]["stake"] == 0.5
    assert rc.get_status()["risk_decisions"]["approved_total"] == 1


def test_blocco_corsia_gui_persistito(tmp_path, monkeypatch):
    rc, path = _avvia(tmp_path, monkeypatch)
    rc._risk_stop_reason = "risk_stop_manuale"
    r = DutchingController(rc.bus, rc).manual_bet({"market_id": "1.9", "selection_id": 1, "price": 2.0,
                                                    "stake": 0.5, "bet_type": "BACK", "event_key": "m1"})
    assert not r.get("ok", r.get("success", False))
    rec = _decisioni(path)
    assert rec and rec[-1]["decision"] == "REJECTED" and rec[-1]["origin"] == "gui_manual"
    assert "risk_stop_active" in rec[-1]["reason"]


def test_scrittura_audit_fallita_non_ammette_e_resta_osservabile(tmp_path, monkeypatch):
    rc, _ = _avvia(tmp_path, monkeypatch)

    def boom(_e):
        raise sqlite3.OperationalError("disk I/O error")
    monkeypatch.setattr(rc.db, "insert_audit_event", boom)
    rc._on_signal_received(dict(SEGNALE))
    st = rc.get_status()["risk_decisions"]
    assert st["rejected_total"] == 1 and st["persist_failures"] == 1


def test_versione_della_config_salvata_dal_percorso_versionato(tmp_path, monkeypatch):
    rc, path = _avvia(tmp_path, monkeypatch)
    cfg = rc.settings_service.load_roserpina_config()
    rc.settings_service.save_roserpina_config(cfg)  # salvataggio reale: config_version incrementata
    versione = int(rc.db.get_settings()["roserpina.config_version"])
    rc._on_signal_received(dict(SEGNALE))
    assert _decisioni(path)[-1]["config_version"] == versione >= 1

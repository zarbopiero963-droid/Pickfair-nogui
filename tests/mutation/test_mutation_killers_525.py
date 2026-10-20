"""#525 — test che uccidono le mutazioni sopravvissute al primo run reale.

Il primo run del mutation testing a livello sorgente (31 mutazioni,
`guardrails/mutation_catalog.json`) ha lasciato 7 sopravvissute: per queste
nessun test mirato si accorgeva del comportamento sbagliato. Ogni test qui sotto
fissa il contratto che la mutazione rompe, chiamando il codice reale.
"""
from __future__ import annotations

import pytest

from core.reconciliation_engine import ReconciliationEngine
from core.trading_engine import ERROR_AMBIGUOUS, TradingEngine
from database import Database


# ---------------------------------------------------------------------------
# ambiguous_timeout_not_ambiguous / ambiguous_error_type_ignored
# Un timeout o un errore che il client marca come ambiguo NON prova che
# l'ordine non sia partito: l'esito resta incerto (AMBIGUOUS), mai FAILED.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "exc",
    [TimeoutError(), TimeoutError("read timed out"), RuntimeError("Read timeout on placeOrders")],
    ids=["timeout_error", "timeout_error_msg", "runtime_timeout_text"],
)
def test_un_timeout_non_e_mai_un_fallimento_certo(exc):
    assert TradingEngine._esito_certo_o_gia_classificato(exc) is True


def test_un_errore_marcato_ambiguo_dal_client_resta_ambiguo():
    class ErroreAmbiguo(Exception):
        error_type = ERROR_AMBIGUOUS

    assert TradingEngine._esito_certo_o_gia_classificato(ErroreAmbiguo("esito ignoto")) is True


def test_un_errore_generico_senza_prova_non_viene_classificato_qui():
    # controllo di specificita': il classificatore non dichiara "gia' classificato" tutto
    assert TradingEngine._esito_certo_o_gia_classificato(ValueError("payload")) is False


# ---------------------------------------------------------------------------
# recovery_inflight_not_reloaded
# Al riavvio le righe INFLIGHT (ordine forse partito) devono tornare tra le
# chiavi anti-duplicato, insieme alle SUBMITTED; quelle terminali no.
# ---------------------------------------------------------------------------

def test_al_riavvio_le_righe_inflight_e_submitted_sono_ricaricate(tmp_path):
    db = Database(str(tmp_path / "recovery525.sqlite3"))
    db.insert_order({"customer_ref": "REF-INFLIGHT", "correlation_id": "CID-1", "status": "INFLIGHT"})
    db.insert_order({"customer_ref": "REF-SUBMITTED", "correlation_id": "CID-2", "status": "SUBMITTED"})
    db.insert_order({"customer_ref": "REF-MATCHED", "correlation_id": "CID-3", "status": "MATCHED"})
    db.insert_order({"customer_ref": "REF-FAILED", "correlation_id": "CID-4", "status": "FAILED"})

    refs = db.load_pending_customer_refs()
    assert "REF-INFLIGHT" in refs
    assert "REF-SUBMITTED" in refs
    assert "REF-MATCHED" not in refs and "REF-FAILED" not in refs


# ---------------------------------------------------------------------------
# reconcile_failed_not_terminal
# Un FAILED/REJECTED dall'exchange e' uno stato terminale noto, non UNKNOWN.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("remote", ["FAILED", "REJECTED", "failed"])
def test_failed_dell_exchange_resta_failed_in_riconciliazione(remote):
    engine = ReconciliationEngine.__new__(ReconciliationEngine)
    assert engine._map_remote_status_to_leg_status({"status": remote}) == "FAILED"


def test_stato_remoto_sconosciuto_resta_unknown():
    engine = ReconciliationEngine.__new__(ReconciliationEngine)
    assert engine._map_remote_status_to_leg_status({"status": "QUALCOSA"}) == "UNKNOWN"


# ---------------------------------------------------------------------------
# side_bet_type_unchecked
# Un lato diverso da BACK/LAY non arriva mai al client.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("side", ["SIDEWAYS", "", "back "])
def test_order_manager_rifiuta_un_lato_diverso_da_back_e_lay(side):
    import order_manager

    manager = order_manager.OrderManager.__new__(order_manager.OrderManager)
    payload = {"market_id": "1.234", "selection_id": 11, "bet_type": side, "price": 2.0, "stake": 2.0}
    with pytest.raises(order_manager.ValidationError):
        manager._validate_payload(payload)


def test_order_manager_accetta_back_e_lay():
    import order_manager

    manager = order_manager.OrderManager.__new__(order_manager.OrderManager)
    for side in ("BACK", "LAY"):
        manager._validate_payload(
            {"market_id": "1.234", "selection_id": 11, "bet_type": side, "price": 2.0, "stake": 2.0}
        )


# ---------------------------------------------------------------------------
# exposure_total_ignored
# Il cap di esposizione aperta somma il rischio gia' aperto a quello nuovo.
# ---------------------------------------------------------------------------

def test_il_cap_di_esposizione_conta_anche_l_esposizione_gia_aperta():
    from core import validators

    # 8 gia' aperti + 5 nuovi = 13 > 10: rifiuto, anche se 5 da solo starebbe sotto.
    assert validators.open_exposure_cap_reason(10.0, 8.0, 5.0).startswith("max_open_exposure_exceeded")
    assert validators.open_exposure_cap_reason(10.0, 4.0, 5.0) == ""
    assert validators.open_exposure_cap_reason(None, 1e9, 1e9) == ""  # nessun cap di default


# ---------------------------------------------------------------------------
# dedupe_restart_keys_not_reloaded
# Al riavvio il motore ricarica dal DB le chiavi degli ordini forse in volo.
# ---------------------------------------------------------------------------

def test_al_riavvio_il_motore_ricarica_le_chiavi_in_volo(tmp_path):
    import threading
    from collections import deque

    db = Database(str(tmp_path / "restart525.sqlite3"))
    db.insert_order({"customer_ref": "REF-VIVO", "correlation_id": "CID-VIVO", "status": "INFLIGHT"})

    engine = TradingEngine.__new__(TradingEngine)
    engine._lock = threading.RLock()
    engine._inflight_keys = set()
    engine._seen_correlation_ids = set()
    engine._seen_cid_order = deque()
    engine.db = db

    assert engine._repopulate_inflight_from_db() is True
    assert "REF-VIVO" in engine._inflight_keys
    assert "CID-VIVO" in engine._seen_correlation_ids


# ---------------------------------------------------------------------------
# safety_cashout_price_unchecked
# Il safety layer rifiuta un cashout con quota <= 1 (nessun hedge a prezzo
# impossibile arriva all'exchange).
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("price", [1.0, 0.5])
def test_il_safety_layer_rifiuta_un_cashout_con_quota_non_valida(price):
    from core.safety_layer import MarketSanityError, SafetyLayer

    payload = {"market_id": "1.234", "selection_id": 11, "side": "LAY",
               "stake": 2.0, "price": price, "green_up": 0.5}
    with pytest.raises(MarketSanityError, match="price <= 1"):
        SafetyLayer().validate_cashout_request(payload)


def test_il_safety_layer_accetta_un_cashout_valido():
    from core.safety_layer import SafetyLayer

    payload = {"market_id": "1.234", "selection_id": 11, "side": "LAY",
               "stake": 2.0, "price": 2.5, "green_up": 0.5}
    assert SafetyLayer().validate_cashout_request(payload) is True

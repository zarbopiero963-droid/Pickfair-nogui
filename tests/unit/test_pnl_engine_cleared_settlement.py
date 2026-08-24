"""Test hard del percorso settlement reale del core PnLEngine (PR
`runtime_settlement_wiring`).

Coprono le due modifiche del motore:

1. ``apply_cleared_market_settlement`` — l'ingresso del settlement REALE
   (report cleared orders): payload canonico che il contratto settlement del
   RuntimeController ACCETTA (verificato chiamando il contratto VERO, non una
   copia), commissione market-net di policy con esempio numerico documentato,
   fail-closed su input malformato SENZA stato parziale nell'aggregatore.

2. ``auto_close_enabled`` — l'auto-close mark-to-market a soglia (che NON
   piazza ordini reali) e' DISARMATO di default: nessun RUNTIME_CLOSE_POSITION
   fantasma da soli aggiornamenti di prezzo. Armato esplicitamente, chiude.

Esempi numerici (commissione policy Italia 4.5%, base market-net):
- WIN singolo:  gross 100.00 => commission 4.50, net 95.50
- LOSS singolo: gross -50.00 => commission 0.00, net -50.00
- Secondo leg stesso mercato: dopo +100.00, un -30.00 porta il market-net a
  70.00 => commissione totale dovuta 3.15, gia' pagata 4.50 => delta -1.35
  (rimborso) => net del leg = -30.00 - (-1.35) = -28.65
"""
from __future__ import annotations

import math

import pytest

from core.pnl_engine import PnLEngine
from core.runtime_controller import RuntimeController


class _Bus:
    def __init__(self):
        self.events = []
        self.subscriptions = []

    def subscribe(self, topic, handler):
        self.subscriptions.append((topic, handler))

    def publish(self, topic, payload=None):
        self.events.append((topic, dict(payload or {})))


def _fill(engine, *, event_key="e1", market_id="1.100", selection_id=11,
          price=2.0, stake=10.0, table_id=1, batch_id="b1"):
    engine._on_filled({
        "event_key": event_key,
        "market_id": market_id,
        "selection_id": selection_id,
        "bet_type": "BACK",
        "avg_price_matched": price,
        "matched_size": stake,
        "table_id": table_id,
        "batch_id": batch_id,
    })


def _book(market_id="1.100", selection_id=11, back=1.49, lay=1.50):
    return {
        "marketId": market_id,
        "runners": [
            {
                "selectionId": selection_id,
                "ex": {
                    "availableToBack": [{"price": back}],
                    "availableToLay": [{"price": lay}],
                },
            }
        ],
    }


def _close_events(bus):
    return [p for t, p in bus.events if t == "RUNTIME_CLOSE_POSITION"]


# ===========================================================================
# apply_cleared_market_settlement — PASS
# ===========================================================================

@pytest.mark.unit
def test_cleared_win_payload_canonico_accettato_dal_contratto_reale():
    bus = _Bus()
    engine = PnLEngine(bus=bus, commission_pct=4.5)

    payload = engine.apply_cleared_market_settlement(
        market_id="1.100", gross_pnl=100.0,
        settled_date="2026-08-24T09:00:00Z", reported_commission=4.5,
    )

    assert payload["event_key"] == "cleared:1.100"
    assert payload["gross_pnl"] == pytest.approx(100.0)
    assert payload["commission_amount"] == pytest.approx(4.5)
    assert payload["net_pnl"] == pytest.approx(95.5)
    assert payload["commission_pct"] == pytest.approx(4.5)
    assert payload["settlement_kind"] == "realized_settlement"
    assert payload["settlement_basis"] == "market_net_realized"
    assert payload["settlement_source"] == "betfair_cleared_orders"
    assert payload["betfair_reported_commission"] == pytest.approx(4.5)

    # Il contratto VERO del consumer (staticmethod, nessuna copia) accetta.
    contract = RuntimeController._extract_settlement_contract(payload)
    assert contract["settlement_validation"] == "accepted"
    assert contract["settlement_acceptance"] == "ACCEPT_REALIZED_SETTLEMENT"
    assert contract["settlement_authority"] == "explicit_contract"

    assert _close_events(bus) == [payload]


@pytest.mark.unit
def test_cleared_loss_commissione_zero_accettato():
    bus = _Bus()
    engine = PnLEngine(bus=bus, commission_pct=4.5)

    payload = engine.apply_cleared_market_settlement(
        market_id="1.101", gross_pnl=-50.0,
    )

    assert payload["commission_amount"] == pytest.approx(0.0)
    assert payload["net_pnl"] == pytest.approx(-50.0)
    contract = RuntimeController._extract_settlement_contract(payload)
    assert contract["settlement_validation"] == "accepted"


@pytest.mark.unit
def test_cleared_secondo_leg_market_net_rimborso_accettato():
    """Il ramo negative-rebate del contratto, esercitato end-to-end."""
    bus = _Bus()
    engine = PnLEngine(bus=bus, commission_pct=4.5)

    engine.apply_cleared_market_settlement(market_id="1.300", gross_pnl=100.0)
    payload = engine.apply_cleared_market_settlement(
        market_id="1.300", gross_pnl=-30.0,
    )

    assert payload["commission_amount"] == pytest.approx(-1.35)
    assert payload["net_pnl"] == pytest.approx(-28.65)
    assert payload["market_net_gross"] == pytest.approx(70.0)
    assert payload["market_commission_amount_total"] == pytest.approx(3.15)
    contract = RuntimeController._extract_settlement_contract(payload)
    assert contract["settlement_validation"] == "accepted", contract["reason"]


@pytest.mark.unit
def test_cleared_rimuove_le_posizioni_tracked_del_mercato():
    bus = _Bus()
    engine = PnLEngine(bus=bus, commission_pct=4.5)
    _fill(engine, event_key="e1", market_id="1.100")
    _fill(engine, event_key="e2", market_id="1.999")

    payload = engine.apply_cleared_market_settlement(
        market_id="1.100", gross_pnl=10.0,
    )

    assert payload["cleared_positions"] == ["e1"]
    assert "e1" not in engine._positions
    assert "e1" not in engine._position_ledgers
    assert "e2" in engine._positions  # altro mercato: intatto


# ===========================================================================
# apply_cleared_market_settlement — BLOCK (fail-closed, stato intatto)
# ===========================================================================

@pytest.mark.unit
@pytest.mark.parametrize("bad_market", ["", "   ", None])
def test_cleared_market_id_vuoto_solleva_senza_pubblicare(bad_market):
    bus = _Bus()
    engine = PnLEngine(bus=bus, commission_pct=4.5)
    with pytest.raises(ValueError, match="CLEARED_SETTLEMENT_INVALID_MARKET"):
        engine.apply_cleared_market_settlement(
            market_id=bad_market, gross_pnl=10.0,
        )
    assert _close_events(bus) == []
    assert engine._market_net_realized_aggregator.ledger == {}


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad_gross", [float("nan"), float("inf"), float("-inf"), None, "cento"]
)
def test_cleared_gross_malformato_solleva_senza_stato_parziale(bad_gross):
    bus = _Bus()
    engine = PnLEngine(bus=bus, commission_pct=4.5)
    with pytest.raises(ValueError, match="CLEARED_SETTLEMENT_INVALID_GROSS"):
        engine.apply_cleared_market_settlement(
            market_id="1.100", gross_pnl=bad_gross,
        )
    assert _close_events(bus) == []
    assert engine._market_net_realized_aggregator.ledger == {}


@pytest.mark.unit
def test_cleared_commissione_riportata_malformata_solleva_prima_di_mutare():
    """Ordinamento fail-closed: la validazione della commissione riportata
    avviene PRIMA dell'apply dell'aggregatore — un raise non deve lasciare
    il market-net ledger mezzo-aggiornato."""
    bus = _Bus()
    engine = PnLEngine(bus=bus, commission_pct=4.5)
    with pytest.raises(ValueError, match="CLEARED_SETTLEMENT_INVALID_COMMISSION"):
        engine.apply_cleared_market_settlement(
            market_id="1.100", gross_pnl=10.0, reported_commission="boh",
        )
    assert _close_events(bus) == []
    assert engine._market_net_realized_aggregator.ledger == {}


@pytest.mark.unit
def test_contratto_reale_respinge_payload_legacy_solo_pnl():
    """Anti-vacuita': la stessa funzione contratto usata nei PASS respinge
    davvero un payload non canonico."""
    contract = RuntimeController._extract_settlement_contract({"pnl": 5.0})
    assert contract["settlement_validation"] != "accepted"


# ===========================================================================
# auto_close_enabled — default DISARMATO (BLOCK) / armato chiude (PASS)
# ===========================================================================

@pytest.mark.unit
def test_auto_close_disarmato_di_default():
    assert PnLEngine(bus=_Bus()).auto_close_enabled is False


@pytest.mark.unit
def test_default_nessuna_chiusura_fantasma_da_market_update():
    """BLOCK: col default, un book che supererebbe le soglie (+3%/-5%) NON
    genera RUNTIME_CLOSE_POSITION e NON rimuove la posizione: nessun PnL
    contabile realizzato senza un ordine reale."""
    bus = _Bus()
    engine = PnLEngine(bus=bus, commission_pct=4.5)
    _fill(engine, price=2.0, stake=10.0)

    engine._on_market(_book(back=1.49, lay=1.50))  # BACK 2.0 -> lay 1.5: +5 gross

    assert _close_events(bus) == []
    assert "e1" in engine._positions


@pytest.mark.unit
def test_armato_esplicitamente_la_chiusura_a_soglia_scatta():
    """Guardia bidirezionale del flag: armato, lo stesso book chiude."""
    bus = _Bus()
    engine = PnLEngine(bus=bus, commission_pct=4.5, auto_close_enabled=True)
    _fill(engine, price=2.0, stake=10.0)

    engine._on_market(_book(back=1.49, lay=1.50))

    closes = _close_events(bus)
    assert len(closes) == 1
    assert closes[0]["event_key"] == "e1"
    assert "e1" not in engine._positions


@pytest.mark.unit
def test_cleared_settlement_non_dipende_dal_flag_auto_close():
    """Il settlement REALE passa anche a flag disarmato: un mercato settlato
    da Betfair e' realizzato per definizione."""
    bus = _Bus()
    engine = PnLEngine(bus=bus, commission_pct=4.5, auto_close_enabled=False)
    payload = engine.apply_cleared_market_settlement(
        market_id="1.100", gross_pnl=-25.0,
    )
    assert math.isclose(payload["net_pnl"], -25.0)
    assert len(_close_events(bus)) == 1

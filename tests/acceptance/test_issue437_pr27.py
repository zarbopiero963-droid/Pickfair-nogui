"""#461 PR27 (PKG-P24-A): validazione numerica fail-closed per ordini e rischio.

Contratto della scheda: un valore NaN/Inf/bool (o non numerico, o un id non
intero) su un ingresso d'ordine o di rischio non produce trasporto ne' effetti
collaterali, su tutti gli ingressi che convergono all'autorita' PR26. Il fix
client #488 (finitezza in `BetfairClient.place_bet`) NON si duplica: qui si
prova che l'invalido si ferma PRIMA, all'ingresso dell'engine e del runtime.

Phase 0 sul main f5cb7e6 (riprodotto con classi reali):
- engine: `selection_id=True` piazzava sul runner 1, `stake=True` piazzava 1 EUR,
  `selection_id=5678.9` veniva troncato a 5678; ogni invalido lasciava una riga
  `orders` FAILED e un QUICK_BET_ROUTED prima del rifiuto del client;
- runtime (segnale Telegram/GUI): `selection_id` non numerico o NaN sollevava
  dopo l'acquisizione dell'anti-duplicazione (chiave evento bloccata, nessun
  SIGNAL_REJECTED); `selection_id=True`/`market_id=True` arrivavano al broker;
- rischio: `max_open_exposure` NaN/Inf (anche da config illeggibile, che il
  loader converte apposta in NaN) disattivava il cap A2; un'esposizione di
  tavolo corrotta (totale = inf) diventava 0 nel money management (#449,
  consumer runtime); `max_stake_abs` NaN toglieva il tetto assoluto;
- stake fisso del segnale: True => 1 EUR, NaN => MIN_STAKE, Inf => cap singolo,
  non numerico => sostituito in silenzio dallo stake MM;
- OrderManager: `selection_id=True` => 1, `5678.9` => 5678, `stake=True` => 1.
"""
from __future__ import annotations

import math
import os
import sqlite3
import tempfile
from pathlib import Path

import pytest

from core.money_management import RoserpinaMoneyManagement
from core.system_state import RoserpinaConfig
from core.trading_engine import STATUS_DUPLICATE_BLOCKED, STATUS_FAILED
from database import Database
from order_manager import ValidationError
from tests.acceptance.test_issue437_pr26 import (
    _ACK,
    _SessioneHttp,
    _broker_con_liquidita,
    _client_reale,
    _engine,
    _om,
    _richiesta,
)
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain, _signal
from trading_config import MIN_STAKE

pytestmark = [pytest.mark.integration, pytest.mark.safety]

_NAN = float("nan")
_INF = float("inf")


@pytest.fixture()
def db_path(tmp_path) -> str:
    return str(tmp_path / "pickfair_pr27.db")


def _righe_orders(path: str) -> int:
    with sqlite3.connect(path) as conn:
        return conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]


# ==========================================================================
# 1. Engine: l'invalido si ferma all'ingresso, zero trasporto e zero effetti
# ==========================================================================
_INVALIDI_ENGINE = [
    ("selection_id", True, "INVALID_SELECTION_ID"),
    ("selection_id", False, "INVALID_SELECTION_ID"),
    ("selection_id", 5678.9, "INVALID_SELECTION_ID"),
    ("selection_id", _NAN, "INVALID_SELECTION_ID"),
    ("selection_id", "abc", "INVALID_SELECTION_ID"),
    ("selection_id", -3, "INVALID_SELECTION_ID"),
    ("market_id", True, "INVALID_MARKET_ID"),
    ("market_id", "   ", "INVALID_MARKET_ID"),
    ("price", True, "INVALID_PRICE"),
    ("price", _NAN, "INVALID_PRICE"),
    ("price", _INF, "INVALID_PRICE"),
    ("price", -_INF, "INVALID_PRICE"),
    ("price", "nan", "INVALID_PRICE"),
    ("price", "1e400", "INVALID_PRICE"),
    ("price", 1.0, "INVALID_PRICE"),
    ("stake", True, "INVALID_SIZE"),
    ("stake", False, "INVALID_SIZE"),
    ("stake", _NAN, "INVALID_SIZE"),
    ("stake", _INF, "INVALID_SIZE"),
    ("stake", "inf", "INVALID_SIZE"),
    ("stake", 0.0, "INVALID_SIZE"),
    ("size", True, "INVALID_SIZE"),
]


@pytest.mark.parametrize("campo,valore,codice", _INVALIDI_ENGINE)
def test_block_engine_invalido_zero_trasporto_zero_effetti(db_path, campo, valore, codice):
    sessione = _SessioneHttp()
    db = Database(db_path)
    eng, bus, _rec = _engine(db, _client_reale(sessione))
    richiesta = _richiesta()
    if campo == "size":
        richiesta.pop("stake")
    richiesta[campo] = valore

    esito = eng.submit_quick_bet(richiesta)

    assert esito["status"] == STATUS_FAILED
    assert codice in str(esito.get("error"))
    assert sessione.post_inviate == []
    assert _righe_orders(db_path) == 0
    assert db.is_order_intent_consumed("PF26REF0001") is False
    assert "QUICK_BET_ROUTED" not in bus.nomi()
    assert "QUICK_BET_FAILED" in bus.nomi()


@pytest.mark.parametrize("campo,valore", [
    ("price", "2.0"), ("stake", "5"), ("selection_id", "5678"), ("selection_id", 5678.0),
])
def test_pass_engine_valori_validi_in_forma_testuale_restano_ammessi(db_path, campo, valore):
    sessione = _SessioneHttp()
    db = Database(db_path)
    eng, _bus, _rec = _engine(db, _client_reale(sessione))

    esito = eng.submit_quick_bet(_richiesta(**{campo: valore}))

    assert esito["status"] == _ACK
    assert len(sessione.post_inviate) == 1
    istruzione = sessione.post_inviate[0]["instructions"][0]
    assert istruzione["selectionId"] == 5678
    assert istruzione["limitOrder"]["price"] == 2.0
    assert istruzione["limitOrder"]["size"] == (5.0 if campo != "stake" else 5.0)


def test_block_engine_invalido_non_rilascia_la_chiave_di_un_originale_in_volo(db_path):
    sessione = _SessioneHttp()
    db = Database(db_path)
    eng, _bus, _rec = _engine(db, _client_reale(sessione))

    assert eng.submit_quick_bet(_richiesta())["status"] == _ACK
    assert eng.submit_quick_bet(_richiesta(stake=True))["status"] == STATUS_FAILED
    assert "PF26REF0001" in eng._inflight_keys
    assert eng.submit_quick_bet(_richiesta())["status"] == STATUS_DUPLICATE_BLOCKED
    assert len(sessione.post_inviate) == 1


def test_block_engine_sim_selection_bool_nessun_ordine_nel_broker(db_path):
    sb = _broker_con_liquidita()
    db = Database(db_path)
    eng, _bus, _rec = _engine(db, sb, modo="SIMULATION", simulation_broker=sb)

    esito = eng.submit_quick_bet(_richiesta(selection_id=True))

    assert esito["status"] == STATUS_FAILED
    assert sb.state.orders == {}


# ==========================================================================
# 2. Runtime (segnale Telegram/GUI/headless): rifiuto prima di dedup e tavoli
# ==========================================================================
def _catena(tmp_path, **cfg):
    c = _chain(tmp_path, anti_duplication=True)
    del c.rc.mm.calculate  # money management VERO
    c.rc.risk_desk.bankroll_current = 1000.0
    c.rc.risk_desk.equity_peak = 1000.0
    for chiave, valore in cfg.items():
        setattr(c.rc.config, chiave, valore)
    return c


def _rifiuti(c):
    return [p.get("reason") for p in c.bus.payloads("SIGNAL_REJECTED") if isinstance(p, dict)]


def _tavoli_occupati(c):
    return [t.table_id for t in c.rc.table_manager._tables.values() if t.status != "FREE"]


@pytest.mark.parametrize("campo,valore", [
    ("selection_id", "abc"), ("selection_id", _NAN), ("selection_id", True),
    ("selection_id", 11.9), ("selection_id", 0), ("market_id", True),
    ("price", _NAN), ("price", _INF), ("price", True), ("price", "abc"),
])
def test_block_runtime_segnale_invalido_rifiutato_senza_effetti(tmp_path, campo, valore):
    c = _catena(tmp_path)

    c.rc._on_signal_received(_signal(**{campo: valore}))

    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert c.broker.state.orders == {}
    assert len(_rifiuti(c)) == 1
    assert _tavoli_occupati(c) == []
    assert c.rc.duplication_guard._active == {}


def test_pass_runtime_segnale_valido_raggiunge_il_broker(tmp_path):
    c = _catena(tmp_path)

    c.rc._on_signal_received(_signal())

    assert len(c.bus.payloads("CMD_QUICK_BET")) == 1
    assert len(c.broker.state.orders) == 1
    assert _rifiuti(c) == []


@pytest.mark.parametrize("cap", [_NAN, _INF, -_INF, True, "abc"])
def test_block_cap_max_open_exposure_illeggibile_e_fail_closed(tmp_path, cap):
    c = _catena(tmp_path, max_open_exposure=cap)

    c.rc._on_signal_received(_signal())

    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert c.broker.state.orders == {}
    assert len(_rifiuti(c)) == 1
    assert "max_open_exposure" in _rifiuti(c)[0]
    assert _tavoli_occupati(c) == []


def test_pass_cap_max_open_exposure_valido_resta_applicato(tmp_path):
    ammesso = _catena(tmp_path / "a", max_open_exposure=100.0)
    ammesso.rc._on_signal_received(_signal())
    assert len(ammesso.broker.state.orders) == 1

    bloccato = _catena(tmp_path / "b", max_open_exposure=10.0)
    bloccato.rc._on_signal_received(_signal())
    assert bloccato.broker.state.orders == {}
    assert _rifiuti(bloccato)[0].startswith("max_open_exposure_exceeded")


def test_block_config_illeggibile_dal_loader_disattivava_il_cap(tmp_path):
    """Il loader converte apposta un valore illeggibile in NaN (hard-stop
    configurato ma corrotto): il consumer A2 non deve trattarlo come assente."""
    from services.setting_service import SettingsService

    class _Db:
        def get_settings(self):
            return {"roserpina.max_open_exposure": "trenta euro"}

        def get_all_settings(self):
            return self.get_settings()

    cap = SettingsService(_Db()).load_roserpina_config().max_open_exposure
    assert cap is not None and math.isnan(cap)

    c = _catena(tmp_path, max_open_exposure=cap)
    c.rc._on_signal_received(_signal())
    assert c.broker.state.orders == {}


@pytest.mark.parametrize("valore", [_NAN, _INF])
def test_block_esposizione_tavolo_corrotta_blocca_il_segnale(tmp_path, valore):
    """#449, consumer runtime: total_exposure() restituisce inf su uno stato
    corrotto; il money management lo azzerava e l'ordine passava."""
    c = _catena(tmp_path)
    altro = list(c.rc.table_manager._tables.values())[-1]
    altro.status = "ACTIVE"
    altro.current_event_key = "altro-evento"
    altro.current_exposure = valore

    c.rc._on_signal_received(_signal())

    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert c.broker.state.orders == {}
    assert _tavoli_occupati(c) == [altro.table_id]


# ==========================================================================
# 3. Money management: stake fisso del segnale ed esposizioni
# ==========================================================================
def _mm(**cfg) -> RoserpinaMoneyManagement:
    return RoserpinaMoneyManagement(RoserpinaConfig(**cfg))


def _calcola(mm, *, signal=None, totale=0.0, evento=0.0, loss=0.0):
    return mm.calculate(
        signal={"price": 2.0, **(signal or {})},
        bankroll_current=1000.0,
        equity_peak=1000.0,
        current_total_exposure=totale,
        event_current_exposure=evento,
        table={"table_id": 1, "loss_amount": loss, "in_recovery": loss > 0.0},
    )


@pytest.mark.parametrize("stake", [True, _NAN, _INF, -_INF, "abc", -5.0])
def test_block_stake_fisso_del_segnale_invalido_non_diventa_un_ordine(stake):
    decisione = _calcola(_mm(), signal={"stake": stake})

    assert decisione.approved is False
    assert decisione.reason == "stake_segnale_non_valido"


@pytest.mark.parametrize("stake,atteso", [
    (5.0, 5.0), ("7.5", 7.5), (MIN_STAKE, MIN_STAKE), (180.0, 180.0),
])
def test_pass_stake_fisso_valido_e_esatto(stake, atteso):
    """#451: boundary con lo stake atteso esatto (MIN_STAKE e tetto singolo
    18% di 1000 = 180)."""
    decisione = _calcola(_mm(), signal={"stake": stake})

    assert decisione.approved is True
    assert decisione.recommended_stake == pytest.approx(atteso, abs=1e-12)


@pytest.mark.parametrize("campo", ["totale", "evento"])
@pytest.mark.parametrize("valore", [_NAN, _INF, True])
def test_block_esposizione_non_finita_nel_money_management(campo, valore):
    decisione = _calcola(_mm(), **{campo: valore})

    assert decisione.approved is False
    assert decisione.reason == "esposizione_non_valida"


@pytest.mark.parametrize("valore", [_NAN, _INF, True, "abc"])
def test_block_max_stake_abs_illeggibile_e_fail_closed(valore):
    decisione = _calcola(_mm(max_stake_abs=valore))

    assert decisione.approved is False


def test_pass_max_stake_abs_valido_limita_esattamente():
    decisione = _calcola(_mm(max_stake_abs=20.0))

    assert decisione.approved is True
    assert decisione.recommended_stake == pytest.approx(20.0)


def test_block_cap_recovery_booleano_e_config_corrotta():
    """Un bool non e' un importo: come NaN/Inf (#450) il chase va a zero."""
    corrotto = _mm(max_recovery_chase_abs=True)
    nan = _mm(max_recovery_chase_abs=_NAN)

    assert corrotto._calculate_base_stake(price=2.0, bankroll_current=1000.0, table_loss=100.0) == \
        nan._calculate_base_stake(price=2.0, bankroll_current=1000.0, table_loss=100.0) == 30.0


@pytest.mark.parametrize("salvato", [0, "0", "", None])
def test_pass_cap_recovery_disarmato_da_config_salvata(salvato):
    """#450 end-to-end: 0/vuoto/None dal DB => cap DISARMATO, chase storico."""
    from services.setting_service import SettingsService

    class _Db:
        def get_settings(self):
            return {"roserpina.max_recovery_chase_abs": salvato}

        def get_all_settings(self):
            return self.get_settings()

    cfg = SettingsService(_Db()).load_roserpina_config()
    mm = RoserpinaMoneyManagement(cfg)

    assert mm._calculate_base_stake(price=2.0, bankroll_current=1000.0, table_loss=100.0) == \
        pytest.approx(30.0 + 100.0)


# ==========================================================================
# 4. OrderManager: niente bool ne' troncamenti prima del client
# ==========================================================================
@pytest.mark.parametrize("campo,valore", [
    ("selection_id", True), ("selection_id", 5678.9), ("selection_id", "5678.9"),
    ("stake", True), ("price", True), ("market_id", True),
])
def test_block_order_manager_bool_e_id_non_interi_prima_del_client(campo, valore):
    class _Client:
        def __init__(self):
            self.invii = 0

        def place_bet(self, **_kw):
            self.invii += 1
            return {"ok": True, "result": {"status": "SUCCESS", "instructionReports": []}}

    client = _Client()
    payload = {"market_id": "1.234", "selection_id": 5678, "bet_type": "BACK",
               "price": 2.0, "stake": 5.0, "customer_ref": "PF27OM01", campo: valore}

    with pytest.raises(ValidationError):
        _om(client).place_order(payload)
    assert client.invii == 0

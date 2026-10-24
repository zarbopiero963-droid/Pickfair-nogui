"""#554: il cap evento del lotto Dutching usa la STESSA misura della bet manuale e dei segnali.

Prima il precheck del lotto misurava solo il tavolo con la chiave del lotto (`find_by_event_key`): restavano
fuori i tavoli da segnale e manuali dello stesso evento reale e quelli dello stesso mercato; un'eccezione
nella misura valeva 0 (fail-open). Ora il lotto e la bet manuale passano da
`RuntimeController.controller_event_exposure`: tavoli attivi dello stesso evento (event_id, poi nome) o
dello stesso mercato o con la stessa chiave, ciascuno UNA volta sola (unione); riserve non ancora a mercato
comprese (sono tavoli attivi), LAY con la liability. Identita' evento assente, misura assente, eccezione o
valore non finito => rifiuto prima di tavolo, chiave e ordine.

Catena reale (`_catena`): DutchingController, RuntimeController, TradingEngine, SimulationBroker, MM vero.
"""
from __future__ import annotations

import threading

import pytest

from tests.acceptance.test_issue437_pr27 import _catena
from tests.acceptance.test_issue523_dutching_manual_bet import _batch, _busy_tables, _dutching, _manual, _queueing
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal

pytestmark = [pytest.mark.integration, pytest.mark.safety]

ALPHA = "Alpha v Beta"           # evento di `_signal()` e `_manual()` (mercato 1.234)


def _ordini(c):
    return len(c.bus.payloads("CMD_QUICK_BET"))


def _rifiuto_evento(res, errore="Esposizione evento oltre limite"):
    assert res["ok"] is False and res["error"] == errore, res


# ----------------------------------------------------------------------------- 1. segnale + Dutching
def test_lotto_conta_il_tavolo_da_segnale_dello_stesso_evento(tmp_path):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal())                      # tavolo da segnale su ALPHA, mercato 1.234
    esposizione_segnale = c.rc._event_current_exposure(_signal())
    assert esposizione_segnale > 0
    c.rc.config.max_event_exposure_pct = (esposizione_segnale + 5.0) / 10.0   # da solo il lotto (10) sta sotto
    res = _dutching(c).submit_dutching(_batch(event_name=ALPHA, total_stake=10.0))
    _rifiuto_evento(res)                                     # prima: ok (misurava solo la chiave del lotto)
    assert res["event_current_exposure"] == pytest.approx(esposizione_segnale)
    assert _ordini(c) == 1                                   # solo il segnale


def test_preflight_del_lotto_usa_la_stessa_misura(tmp_path):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal())
    c.rc.config.max_event_exposure_pct = (c.rc._event_current_exposure(_signal()) + 5.0) / 10.0
    _rifiuto_evento(_dutching(c).submit_dutching(_batch(event_name=ALPHA, total_stake=10.0), preflight=True))


# ----------------------------------------------------------------------------- 2. piu' mercati
def test_lotto_conta_i_tavoli_dello_stesso_evento_su_altri_mercati(tmp_path):
    c = _catena(tmp_path, max_event_exposure_pct=5.0)        # cap evento 50
    dc = _dutching(c)
    assert dc.manual_bet(_manual(market_id="1.500", selection_id=1, stake=45.0))["ok"] is True
    res = dc.submit_dutching(_batch(event_name=ALPHA, market_id="1.700", total_stake=10.0))   # 45 + 10 > 50
    _rifiuto_evento(res)
    assert res["event_current_exposure"] == 45.0


def test_lotto_conta_i_tavoli_dello_stesso_mercato_con_nome_diverso(tmp_path):
    c = _catena(tmp_path, max_event_exposure_pct=5.0)
    dc = _dutching(c)
    assert dc.manual_bet(_manual(market_id="1.700", selection_id=1, stake=45.0, event_name="Gamma - Delta"))["ok"]
    _rifiuto_evento(dc.submit_dutching(_batch(market_id="1.700", total_stake=10.0)))


def test_unione_senza_doppi_conteggi(tmp_path):
    """Un tavolo che e' dello stesso evento E dello stesso mercato conta una volta; due tavoli diversi (altro
    mercato stesso evento, stesso mercato altro nome) contano entrambi."""
    c = _catena(tmp_path, max_event_exposure_pct=20.0)       # cap 200: nessun rifiuto, si legge la misura
    dc = _dutching(c)
    assert dc.manual_bet(_manual(market_id="1.700", selection_id=1, stake=40.0, event_name="Gamma v Delta"))["ok"]
    assert dc.manual_bet(_manual(market_id="1.500", selection_id=1, stake=30.0, event_name="Gamma v Delta"))["ok"]
    assert dc.manual_bet(_manual(market_id="1.700", selection_id=2, stake=20.0, event_name="Gamma - Delta"))["ok"]
    pre = dc.submit_dutching(_batch(total_stake=10.0), preflight=True)
    assert pre["ok"] is True, pre
    assert dc._entry_event_exposure("k", _batch(), "1.700") == 90.0      # 40 + 30 + 20, ciascuno una volta


# ----------------------------------------------------------------------------- 3. concorrenza
def test_manuale_e_lotto_concorrenti_ne_passa_uno_solo(tmp_path):
    """Guardia di regressione (passa anche sul codice di prima: il manuale vedeva gia' il lotto per mercato):
    con la misura unica, sotto il lock d'ammissione, dei due ingressi concorrenti ne passa sempre uno solo."""
    for giro in range(10):
        c = _catena(tmp_path / str(giro), max_event_exposure_pct=5.0)     # cap 50: 45 + 10 non ci stanno
        dc = _dutching(c)
        partenza = threading.Barrier(2)
        esiti = {}

        def manuale():
            partenza.wait()
            esiti["manuale"] = dc.manual_bet(_manual(market_id="1.700", selection_id=1, stake=45.0,
                                                     event_name="Gamma v Delta"))["ok"]

        def lotto():
            partenza.wait()
            esiti["lotto"] = dc.submit_dutching(_batch(total_stake=10.0))["ok"]
        threads = [threading.Thread(target=f) for f in (manuale, lotto)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sorted(esiti.values()) == [False, True], (giro, esiti)


# ----------------------------------------------------------------------------- 4. LAY
def test_lotto_conta_la_liability_lay_degli_altri_tavoli(tmp_path):
    c = _catena(tmp_path, max_event_exposure_pct=5.0)        # cap 50
    dc = _dutching(c)
    # LAY 20 a 3.0: liability 40 (lo stake da solo, 20, lascerebbe passare il lotto)
    assert dc.manual_bet(_manual(market_id="1.500", selection_id=1, stake=20.0, price=3.0, bet_type="LAY",
                                 event_name="Gamma v Delta"))["ok"] is True
    res = dc.submit_dutching(_batch(total_stake=12.0))       # 40 + 12 > 50
    _rifiuto_evento(res)
    assert res["event_current_exposure"] == 40.0


def test_lotto_lay_conta_la_propria_liability(tmp_path):
    """Lotto LAY 10 su quote 2.0/3.0: gambe 6 e 4, liability 6 + 8 = 14. Con cap 12 il lotto BACK (10)
    passa, quello LAY no: conta la liability, non lo stake."""
    c = _catena(tmp_path, max_event_exposure_pct=1.2)        # cap 12
    res = _dutching(c).submit_dutching(_batch(sides=("LAY", "LAY"), total_stake=10.0))
    _rifiuto_evento(res)
    assert res["batch_exposure"] == 14.0
    assert _ordini(c) == 0
    assert _dutching(c).submit_dutching(_batch(total_stake=10.0))["ok"] is True   # controllo positivo BACK


# ----------------------------------------------------------------------------- 5. fondi insufficienti
def test_cap_evento_rispettato_ma_fondi_freschi_insufficienti_rifiuta(tmp_path):
    c = _catena(tmp_path)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 3.0, "exposure": 0.0, "total": 3.0,
                                                      "simulated": True}
    res = _dutching(c).submit_dutching(_batch(event_name=ALPHA, total_stake=10.0))
    assert res["ok"] is False and str(res.get("reason", "")).startswith("fondi_insufficienti"), res
    assert _ordini(c) == 0 and _busy_tables(c) == []


# ----------------------------------------------------------------------------- 6. dati evento mancanti
@pytest.mark.parametrize("identita", [{"event_name": ""}, {"event_name": "   "}, {"event_name": None}])
def test_lotto_senza_identita_evento_rifiutato(tmp_path, identita):
    c = _catena(tmp_path)
    res = _dutching(c).submit_dutching(_batch(**identita))
    _rifiuto_evento(res, "Esposizione evento non verificabile")
    assert _ordini(c) == 0 and _busy_tables(c) == []


def test_lotto_con_solo_event_id_e_valutabile(tmp_path):
    c = _catena(tmp_path)
    res = _dutching(c).submit_dutching(_batch(event_name="", event_id="31234567"))
    assert res["ok"] is True, res


# ----------------------------------------------------------------------------- 7. eccezione nella misura
def test_eccezione_nella_misura_evento_rifiuta_il_lotto(tmp_path, monkeypatch):
    c = _catena(tmp_path)

    def guasta(*_a, **_k):
        raise RuntimeError("tavoli illeggibili")
    monkeypatch.setattr(c.rc, "_event_current_exposure", guasta)
    res = _dutching(c).submit_dutching(_batch(total_stake=10.0))
    _rifiuto_evento(res)                                     # prima: 0.0 => ok (fail-open)
    assert res["event_current_exposure"] is None
    assert _ordini(c) == 0 and _busy_tables(c) == []


@pytest.mark.parametrize("valore", [float("nan"), float("inf"), -1.0, "abc"])
def test_misura_non_finita_o_negativa_rifiuta(tmp_path, monkeypatch, valore):
    c = _catena(tmp_path)
    monkeypatch.setattr(c.rc, "controller_event_exposure", lambda *_a, **_k: valore)
    _rifiuto_evento(_dutching(c).submit_dutching(_batch(total_stake=10.0)))
    _rifiuto_evento(_dutching(c).manual_bet(_manual(stake=4.0)))
    assert _ordini(c) == 0


def test_runtime_senza_misura_evento_rifiuta_lotto_e_manuale(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    monkeypatch.setattr(type(c.rc), "controller_event_exposure", None)
    _rifiuto_evento(_dutching(c).submit_dutching(_batch(total_stake=10.0)))
    _rifiuto_evento(_dutching(c).manual_bet(_manual(stake=4.0)))    # prima: solo il tavolo con la chiave
    assert _ordini(c) == 0


# ----------------------------------------------------------------------------- 8. riserve pendenti
def test_riserva_non_ancora_a_mercato_conta_nel_cap_del_lotto(tmp_path):
    c = _catena(tmp_path, max_event_exposure_pct=5.0)
    trattenuti, _ = _queueing(c)                              # l'ordine manuale resta accodato: tavolo riservato
    assert _dutching(c).manual_bet(_manual(market_id="1.500", selection_id=1, stake=45.0,
                                           event_name="Gamma v Delta"))["ok"] is True
    assert len(trattenuti) == 1 and c.broker.state.orders == {}
    _rifiuto_evento(_dutching(c).submit_dutching(_batch(total_stake=10.0)))


# ----------------------------------------------------------------------------- 9. controlli positivi
def test_lotto_sotto_il_cap_passa(tmp_path):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal())
    res = _dutching(c).submit_dutching(_batch(event_name=ALPHA, total_stake=10.0))
    assert res["ok"] is True, res
    assert _ordini(c) == 3                                   # segnale + due gambe


def test_lotto_su_altro_evento_non_conta_il_segnale(tmp_path):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal())
    c.rc.config.max_event_exposure_pct = 1.1                 # cap 11: il lotto (10) ci sta, segnale+lotto no
    res = _dutching(c).submit_dutching(_batch(total_stake=10.0))
    assert res["ok"] is True, res


# ----------------------------------------------------------------------------- 10. nessun ordine dopo il rifiuto
def test_dopo_il_rifiuto_nessun_ordine_tavolo_o_chiave(tmp_path):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal())
    occupati = _busy_tables(c)
    chiavi = dict(c.rc.duplication_guard._active)
    c.rc.config.max_event_exposure_pct = (c.rc._event_current_exposure(_signal()) + 5.0) / 10.0
    _rifiuto_evento(_dutching(c).submit_dutching(_batch(event_name=ALPHA, total_stake=10.0)))
    assert _ordini(c) == 1 and len(c.broker.state.orders) == 1
    assert [t.table_id for t in _busy_tables(c)] == [t.table_id for t in occupati]
    assert c.rc.duplication_guard._active.keys() == chiavi.keys()


# ----------------------------------------------------------------------------- review indipendente sul diff #554
def test_lotti_con_solo_event_id_su_mercati_diversi_contano_insieme(tmp_path):
    """P1: il tavolo del lotto non salvava `event_id` nel meta: un secondo lotto (o una bet manuale) dello stesso
    evento, identificato solo dall'id, su un altro mercato non lo vedeva (10 + 10 > 15 passava)."""
    c = _catena(tmp_path, max_event_exposure_pct=1.5)        # cap 15
    dc = _dutching(c)
    assert dc.submit_dutching(_batch(event_name="", event_id="E9", market_id="1.700", total_stake=10.0))["ok"]
    res = dc.submit_dutching(_batch(event_name="", event_id="E9", market_id="1.800", total_stake=10.0))
    _rifiuto_evento(res)
    assert res["event_current_exposure"] == 10.0
    _rifiuto_evento(dc.manual_bet(_manual(event_name="", event_id="E9", market_id="1.900", stake=10.0)))


@pytest.mark.parametrize("event_id", ["   ", 0, True])
@pytest.mark.parametrize("ingresso", ["lotto", "manuale"])
def test_event_id_che_la_misura_ignora_non_vale_come_identita(tmp_path, event_id, ingresso):
    """P2: il gate accettava un `event_id` (spazi, 0, bool) che la misura del runtime scarta: l'ingresso
    risultava verificabile ma veniva misurato solo per mercato. Ora il gate decide come la misura."""
    c = _catena(tmp_path)
    dc = _dutching(c)
    if ingresso == "lotto":
        res = dc.submit_dutching(_batch(event_name="", event_id=event_id))
    else:
        res = dc.manual_bet(_manual(event_name="", event_id=event_id))
    _rifiuto_evento(res, "Esposizione evento non verificabile")
    assert _ordini(c) == 0


def test_tavolo_con_la_stessa_chiave_senza_identita_ne_mercato_conta(tmp_path):
    """Un tavolo ripristinato con la sola chiave (meta e mercato assenti) resta nella misura dell'ingresso con
    quella chiave; senza la chiave non e' riconoscibile."""
    c = _catena(tmp_path)
    tm = c.rc.table_manager
    tavolo = tm.allocate(event_key="chiave-lotto", allow_recovery=False)
    tm.activate(table_id=tavolo.table_id, event_key="chiave-lotto", exposure=25.0, market_id="", selection_id=None,
                meta={})
    voce = {"market_id": "1.700", "event_name": "Gamma v Delta"}
    assert c.rc.controller_event_exposure(voce) == 0.0
    assert c.rc.controller_event_exposure(voce, event_key="chiave-lotto") == 25.0


def test_il_bus_dutching_conserva_event_id():
    """Il normalizzatore del RiskMiddleware conservava solo `event_name`: un lotto identificato dall'id veniva
    sempre rifiutato come non verificabile. L'id assente resta assente (nessun id inventato)."""
    from core.risk_middleware import RiskMiddleware

    class _Bus:
        def __init__(self):
            self.pubblicati = []

        def subscribe(self, *_a, **_k):
            pass

        def publish(self, topic, payload=None):
            self.pubblicati.append((topic, payload))

    richiesta = {"market_id": "1.234", "event_name": "", "event_id": "E9", "market_name": "Match Odds",
                 "bet_type": "BACK", "total_stake": 10.0,
                 "results": [{"selectionId": 1, "price": 2.0, "side": "BACK"},
                             {"selectionId": 2, "price": 3.0, "side": "BACK"}]}
    for evento, atteso in (("E9", "E9"), (None, None)):
        bus = _Bus()
        RiskMiddleware(bus, topics=("REQ_PLACE_DUTCHING",))._handle_dutching(dict(richiesta, event_id=evento))
        comandi = [p for t, p in bus.pubblicati if t == "CMD_PLACE_DUTCHING"]
        assert len(comandi) == 1 and comandi[0].get("event_id") == atteso, bus.pubblicati


# ----------------------------------------------------------------------------- review indipendente su b800cd4
def test_event_id_di_tipo_diverso_resta_lo_stesso_evento(tmp_path):
    """P3: l'id salvato grezzo sul tavolo (123.0) e quello di un ingresso successivo ("123") non coincidevano e,
    con l'id presente su entrambi, i nomi non si confrontavano piu': 10 + 10 passava con cap 15."""
    c = _catena(tmp_path, max_event_exposure_pct=1.5)        # cap 15
    dc = _dutching(c)
    assert dc.submit_dutching(_batch(event_id=123.0, market_id="1.700", total_stake=10.0))["ok"]
    _rifiuto_evento(dc.submit_dutching(_batch(event_id="123", market_id="1.800", total_stake=10.0)))
    _rifiuto_evento(dc.manual_bet(_manual(event_name="Gamma v Delta", event_id=123, market_id="1.900",
                                          stake=10.0)))


@pytest.mark.parametrize("event_id", [123.5, float("nan"), float("inf"), [], {}])
def test_event_id_non_canonico_non_vale_come_identita(tmp_path, event_id):
    c = _catena(tmp_path)
    _rifiuto_evento(_dutching(c).submit_dutching(_batch(event_name="", event_id=event_id)),
                    "Esposizione evento non verificabile")


def test_il_bus_non_trasforma_un_nome_assente_in_none():
    """P3 (preesistente sul main): `str(None)` dava il nome evento "None", che il gate prendeva per un'identita'."""
    from core.risk_middleware import RiskMiddleware

    class _Bus:
        def __init__(self):
            self.pubblicati = []

        def subscribe(self, *_a, **_k):
            pass

        def publish(self, topic, payload=None):
            self.pubblicati.append((topic, payload))

    bus = _Bus()
    RiskMiddleware(bus, topics=("REQ_PLACE_DUTCHING",))._handle_dutching(
        {"market_id": "1.2", "event_name": None, "total_stake": 10.0, "bet_type": "BACK",
         "results": [{"selectionId": 1, "price": 2.0, "side": "BACK"}]})
    comando = [p for t, p in bus.pubblicati if t == "CMD_PLACE_DUTCHING"][0]
    assert comando["event_name"] == ""

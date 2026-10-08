"""Il client Betfair rifiuta quota e size non finite PRIMA dell'invio.

F11 della #453, DECISIONE-426 P27 (#426). `BetfairClient.place_bet` controllava
solo `price <= 1.0` e `size <= 0.0`: NaN e infinito passano `float()`, e
`nan <= 1.0` e' falso. Riprodotto sul `main` `a86bfaa` col client reale e la
sessione HTTP come contatore (il trasporto finto risponde SUCCESS a tutto):

    price=nan   -> nessuna eccezione  POST=1  corpo: "price": NaN
    price=inf   -> nessuna eccezione  POST=1  corpo: "price": Infinity
    size=nan    -> nessuna eccezione  POST=1  corpo: "size": NaN

Stesso esito da `OrderRouter.place`, il seam da cui la catena del cashout
arriva al client in produzione (il SafetyLayer li' controlla solo `price <= 1`
e `stake <= 0`), e da `BetfairService.place_order` e `OrderManager.place_order`
(che controlla solo `price <= 1.0`), oggi senza chiamanti in produzione.
Betfair avrebbe rifiutato un corpo che non e' JSON valido,
ma a fermarlo deve essere il client: `_validate_replace_params` quelle quote le
rifiutava gia'. Ora una quota o una size non finite danno
`RuntimeError("INVALID_PRICE")` / `RuntimeError("INVALID_SIZE")`, zero POST,
come gli altri argomenti non validi.
"""

import json
import math

import pytest

from betfair_client import BetfairClient


class _Risposta:
    status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return [{"jsonrpc": "2.0", "id": 1, "result": {
            "status": "SUCCESS",
            "instructionReports": [
                {"status": "SUCCESS", "betId": "B1", "sizeMatched": 0.0},
            ],
        }}]


class _SessioneContaInvii:
    """Il confine del trasporto: conta le POST e conserva il corpo grezzo."""

    def __init__(self):
        self.grezzi = []

    def post(self, url, **kwargs):
        self.grezzi.append(kwargs["data"])
        return _Risposta()

    def istruzioni(self):
        return [
            istruzione
            for grezzo in self.grezzi
            for istruzione in json.loads(grezzo)[0]["params"]["instructions"]
        ]


def _client():
    sessione = _SessioneContaInvii()
    client = BetfairClient(
        username="u", app_key="a", cert_pem="c.pem", key_pem="k.pem",
        session=sessione,
    )
    client.session_token = "TOK"
    return client, sessione


_ORDINE = {"market_id": "1.234", "selection_id": 5678, "side": "BACK",
           "price": 2.0, "size": 5.0}

_QUOTE_NON_FINITE = [
    float("nan"), float("inf"), float("-inf"),
    "nan", "NaN", "inf", "Infinity", "-Infinity",
]
_SIZE_NON_FINITE = [float("nan"), float("inf"), "nan", "Infinity"]


@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.parametrize("quota,attesa", [
    (1.01, 1.01), (2.0, 2.0), ("2.5", 2.5), (1000.0, 1000.0),
])
def test_pass_quota_valida_arriva_sul_wire(quota, attesa):
    client, sessione = _client()

    esito = client.place_bet(**{**_ORDINE, "price": quota})

    assert esito["ok"] is True
    assert len(sessione.grezzi) == 1
    istruzione = sessione.istruzioni()[0]
    assert istruzione["limitOrder"]["price"] == attesa
    assert istruzione["limitOrder"]["size"] == 5.0
    assert "NaN" not in sessione.grezzi[0]
    assert "Infinity" not in sessione.grezzi[0]


@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.parametrize("quota", _QUOTE_NON_FINITE, ids=repr)
def test_block_quota_non_finita_nessun_invio(quota):
    client, sessione = _client()

    with pytest.raises(RuntimeError, match="^INVALID_PRICE$"):
        client.place_bet(**{**_ORDINE, "price": quota})

    assert sessione.grezzi == []


@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.parametrize("size", _SIZE_NON_FINITE, ids=repr)
def test_block_size_non_finita_nessun_invio(size):
    # Stesso difetto, stesso blocco di validazione: una size NaN o infinita
    # partiva nel corpo come `"size": NaN`.
    client, sessione = _client()

    with pytest.raises(RuntimeError, match="^INVALID_SIZE$"):
        client.place_bet(**{**_ORDINE, "size": size})

    assert sessione.grezzi == []


@pytest.mark.unit
@pytest.mark.safety
def test_edge_gli_errori_pre_invio_esistenti_restano_invariati():
    # I rifiuti di prima restano identici, e l'ordine dei controlli non cambia:
    # con piu' argomenti invalidi vince ancora il primo della lista.
    client, sessione = _client()

    for quota in (1.0, 0.0, -2.0, "x", None):
        with pytest.raises(RuntimeError, match="^INVALID_PRICE$"):
            client.place_bet(**{**_ORDINE, "price": quota})
    for size in (0.0, -1.0, "x", None):
        with pytest.raises(RuntimeError, match="^INVALID_SIZE$"):
            client.place_bet(**{**_ORDINE, "size": size})
    with pytest.raises(RuntimeError, match="^INVALID_MARKET_ID$"):
        client.place_bet(**{**_ORDINE, "market_id": "", "price": float("nan")})

    assert sessione.grezzi == []


@pytest.mark.unit
@pytest.mark.safety
def test_con_quota_e_size_non_finite_vince_la_quota():
    # Il controllo nuovo sta dentro quello di prima: la quota si valida prima
    # della size, come sempre.
    client, sessione = _client()

    with pytest.raises(RuntimeError, match="^INVALID_PRICE$"):
        client.place_bet(**{**_ORDINE, "price": float("nan"), "size": float("nan")})

    assert sessione.grezzi == []


@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.parametrize("campo,valore,codice", [
    ("price", float("nan"), "INVALID_PRICE"),
    ("price", float("inf"), "INVALID_PRICE"),
    ("stake", float("nan"), "INVALID_SIZE"),
])
def test_block_dal_seam_del_cashout_nessun_invio(campo, valore, codice):
    # `OrderRouter.place` e' il seam da cui passa l'hedge del cashout
    # (`cashout_wiring`): inoltra quota e stake cosi' come sono, e lascia
    # propagare le eccezioni del broker (fail-closed). Il SafetyLayer, sulla
    # richiesta di cashout, controlla solo `price <= 1` e `stake <= 0`.
    from core.order_router import OrderRouter

    client, sessione = _client()

    class _Servizio:
        def get_client(self):
            return client

    with pytest.raises(RuntimeError, match=f"^{codice}$"):
        OrderRouter(_Servizio()).place({
            "market_id": "1.234", "selection_id": 5678, "bet_type": "LAY",
            "price": 2.0, "stake": 5.0, campo: valore,
        })

    assert sessione.grezzi == []


@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.parametrize("campo,valore,codice", [
    ("price", float("nan"), "INVALID_PRICE"),
    ("stake", float("inf"), "INVALID_SIZE"),
])
def test_block_dalla_facciata_del_servizio_nessun_invio(campo, valore, codice):
    from services.betfair_service import BetfairService

    client, sessione = _client()
    servizio = BetfairService.__new__(BetfairService)
    servizio._session_invalid = False
    servizio.client = client

    with pytest.raises(RuntimeError, match=f"^{codice}$"):
        servizio.place_order({"market_id": "1.234", "selection_id": 5678,
                              "bet_type": "BACK", "price": 2.0, "stake": 5.0,
                              campo: valore})

    assert sessione.grezzi == []


@pytest.mark.unit
@pytest.mark.safety
def test_block_da_order_manager_nessun_invio():
    # Qui conta che non parta nulla. Dalla PR26 (#461, F14)
    # `OrderManager._validate_payload` controlla anche la finitezza: la quota
    # NaN si ferma prima della saga e del client con `ValidationError`, e un
    # `INVALID_PRICE` del client resterebbe comunque un rifiuto certo
    # (PERMANENT), non AMBIGUOUS. Oggi nessun codice di produzione costruisce
    # un OrderManager.
    from order_manager import OrderManager, ValidationError

    client, sessione = _client()

    class _Bus:
        def publish(self, *args, **kwargs):
            return None

    class _Db:
        def __init__(self):
            self.saghe = {}

        def create_order_saga(self, *, customer_ref, **kwargs):
            self.saghe[customer_ref] = {"customer_ref": customer_ref, **kwargs}

        def get_order_saga(self, customer_ref):
            return self.saghe.get(customer_ref)

        def get_order_saga_by_logical_key(self, logical_key):
            return None

        def update_order_saga(self, *, customer_ref, **kwargs):
            self.saghe.get(customer_ref, {}).update(kwargs)

    om = OrderManager(db=_Db(), bus=_Bus(), client_getter=lambda: client,
                      sleep_fn=lambda _: None)

    with pytest.raises(ValidationError):
        om.place_order({"market_id": "1.234", "selection_id": 5678,
                        "bet_type": "BACK", "price": float("nan"),
                        "stake": 5.0, "customer_ref": "REF-F11"})

    assert sessione.grezzi == []
    assert om.db.saghe == {}


@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.parametrize("variazione", [
    {"price": float("nan")}, {"price": float("inf")}, {"size": float("nan")},
], ids=repr)
def test_per_l_engine_il_rifiuto_e_un_fallimento_certo(variazione):
    # Il TradingEngine tratta un'eccezione del client LIVE come AMBIGUA (lock
    # tenuto, riconciliazione) salvo prova che nulla sia partito. Il rifiuto di
    # una quota o size non finite deve essere una di quelle prove: un codice
    # dell'elenco pre-invio, sollevato dal client reale.
    from core.trading_engine import TradingEngine

    client, sessione = _client()

    with pytest.raises(RuntimeError) as errore:
        client.place_bet(**{**_ORDINE, **variazione})

    assert sessione.grezzi == []
    assert str(errore.value) in TradingEngine._ERRORI_PRIMA_DELL_INVIO
    assert TradingEngine._esito_certo_o_gia_classificato(errore.value, client) is True


@pytest.mark.unit
@pytest.mark.safety
def test_replace_e_place_rifiutano_le_stesse_quote():
    # Prima di F11 il replace le rifiutava e il place no: la stessa quota
    # passava da un percorso e non dall'altro.
    # La griglia contiene solo valori che `float()` accetta e che non sono
    # finiti: e' esattamente la classe che `<= 1.0` lasciava passare.
    assert all(not math.isfinite(float(quota)) for quota in _QUOTE_NON_FINITE)
    for quota in _QUOTE_NON_FINITE:
        with pytest.raises(RuntimeError, match="^INVALID_PRICE$"):
            BetfairClient._validate_replace_params("1.234", "B1", quota)
        client, sessione = _client()
        with pytest.raises(RuntimeError, match="^INVALID_PRICE$"):
            client.place_bet(**{**_ORDINE, "price": quota})
        assert sessione.grezzi == []

"""Il client Betfair rifiuta un lato fuori da BACK/LAY PRIMA dell'invio.

DECISIONE-426 P13 (#426). Prima di questa correzione `BetfairClient.place_bet`
costruiva l'istruzione con `safe_side`, che trasforma in BACK qualsiasi valore
che non sia BACK/LAY, `None` compreso: un refuso, un lato mancante o un valore
di un altro dominio partivano come scommessa BACK, con `ok=True`. Riprodotto
sul `main` `ad9ce64` col client reale e la sessione HTTP come contatore:

    side='SELL'   -> ok=True   POST=1  side sul wire=['BACK']
    side=None     -> ok=True   POST=1  side sul wire=['BACK']
    side='   '    -> ok=True   POST=1  side sul wire=['BACK']

e identico da `OrderRouter.place` (il seam del cashout) e da
`BetfairService.place_order`. Ora il lato si VALIDA come gli altri argomenti:
fuori da BACK/LAY dopo strip/upper => `RuntimeError("INVALID_SIDE")`, zero POST.

`_safe_side` resta invariato: lo usa `calculate_cashout`, fuori dalla decisione.
"""

import json

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
    """Il confine del trasporto: conta le POST e ne conserva il corpo."""

    def __init__(self):
        self.corpi = []

    def post(self, url, **kwargs):
        self.corpi.append(json.loads(kwargs["data"]))
        return _Risposta()


def _client():
    sessione = _SessioneContaInvii()
    client = BetfairClient(
        username="u", app_key="a", cert_pem="c.pem", key_pem="k.pem",
        session=sessione,
    )
    client.session_token = "TOK"
    return client, sessione


def _lati_sul_wire(sessione):
    return [
        istruzione["side"]
        for corpo in sessione.corpi
        for istruzione in corpo[0]["params"]["instructions"]
    ]


_ORDINE = {"market_id": "1.234", "selection_id": 5678, "price": 2.0, "size": 5.0}


@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.parametrize("lato,atteso", [
    ("BACK", "BACK"),
    ("LAY", "LAY"),
    ("back", "BACK"),
    (" lay ", "LAY"),
    ("Lay\t", "LAY"),
])
def test_pass_lato_valido_arriva_sul_wire_normalizzato(lato, atteso):
    client, sessione = _client()

    esito = client.place_bet(side=lato, **_ORDINE)

    assert esito["ok"] is True
    assert len(sessione.corpi) == 1
    assert _lati_sul_wire(sessione) == [atteso]


@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.parametrize("lato", [
    "SELL", "L", "LAYY", "BAC", "LAY BACK",  # refusi e valori di un altro dominio
    "", "   ", None,                          # lato mancante
])
def test_block_lato_invalido_o_mancante_nessun_invio(lato):
    client, sessione = _client()

    with pytest.raises(RuntimeError, match="^INVALID_SIDE$"):
        client.place_bet(side=lato, **_ORDINE)

    assert sessione.corpi == []


@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.parametrize("lato", [1, True, b"LAY", ["LAY"], object()])
def test_malformed_lato_non_testuale_nessun_invio(lato):
    # `str(b"LAY")` e' "b'LAY'" e `str(["LAY"])` e' "['LAY']": nessuna forma
    # non testuale diventa un lato per coincidenza.
    client, sessione = _client()

    with pytest.raises(RuntimeError, match="^INVALID_SIDE$"):
        client.place_bet(side=lato, **_ORDINE)

    assert sessione.corpi == []


@pytest.mark.unit
@pytest.mark.safety
def test_edge_gli_errori_pre_invio_esistenti_restano_invariati():
    # L'ordine dei controlli non cambia: con piu' argomenti invalidi vince
    # ancora il primo della lista, e un lato valido non maschera gli altri.
    client, sessione = _client()

    with pytest.raises(RuntimeError, match="^INVALID_MARKET_ID$"):
        client.place_bet(market_id="", selection_id=5678, side="SELL",
                         price=2.0, size=5.0)
    with pytest.raises(RuntimeError, match="^INVALID_SIZE$"):
        client.place_bet(market_id="1.234", selection_id=5678, side="LAY",
                         price=2.0, size=0)

    assert sessione.corpi == []


@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.parametrize("bet_type", ["SELL", None])
def test_block_dal_seam_del_cashout_nessun_invio(bet_type):
    # `OrderRouter.place` e' il seam da cui passa il cashout headless: passa
    # `payload["bet_type"]` cosi' com'e', e lascia propagare le eccezioni del
    # broker (fail-closed).
    from core.order_router import OrderRouter

    client, sessione = _client()

    class _Servizio:
        def get_client(self):
            return client

    with pytest.raises(RuntimeError, match="^INVALID_SIDE$"):
        OrderRouter(_Servizio()).place({
            "market_id": "1.234", "selection_id": 5678, "bet_type": bet_type,
            "price": 2.0, "stake": 5.0,
        })

    assert sessione.corpi == []


@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.parametrize("lato", [{"bet_type": "SELL"}, {}])
def test_block_dalla_facciata_del_servizio_nessun_invio(lato):
    from services.betfair_service import BetfairService

    client, sessione = _client()
    servizio = BetfairService.__new__(BetfairService)
    servizio._session_invalid = False
    servizio.client = client

    with pytest.raises(RuntimeError, match="^INVALID_SIDE$"):
        servizio.place_order({"market_id": "1.234", "selection_id": 5678,
                              "price": 2.0, "stake": 5.0, **lato})

    assert sessione.corpi == []


def _codici_pre_invio_del_client():
    """I codici che il client reale solleva senza aver spedito nulla.

    Ricavati eseguendo `place_bet` con ciascun argomento invalido, non
    ricopiati: se il client ne aggiunge o ne toglie uno, l'elenco cambia qui.
    """
    invalidi = [
        {"market_id": ""},
        {"selection_id": "x"}, {"selection_id": 0},
        {"price": "x"}, {"price": 1.0},
        {"size": "x"}, {"size": 0},
        {"side": "SELL"}, {"side": None},
    ]
    codici = set()
    for variazione in invalidi:
        client, sessione = _client()
        argomenti = {**_ORDINE, "side": "BACK", **variazione}
        with pytest.raises(RuntimeError) as errore:
            client.place_bet(**argomenti)
        assert sessione.corpi == [], variazione
        codici.add(str(errore.value))
    return codici


@pytest.mark.unit
@pytest.mark.safety
def test_l_elenco_pre_invio_dell_engine_coincide_con_quello_del_client():
    # Il TradingEngine tratta come prova che "nulla e' partito" solo i codici
    # che il client reale solleva prima di costruire la richiesta. Se i due
    # elenchi divergono, un errore pre-invio del client finirebbe AMBIGUO (lock
    # tenuto su un ordine mai spedito), o un codice che il client non solleva
    # piu' prima dell'invio varrebbe ancora come prova.
    from core.trading_engine import TradingEngine

    assert _codici_pre_invio_del_client() == set(TradingEngine._ERRORI_PRIMA_DELL_INVIO)


@pytest.mark.unit
@pytest.mark.safety
def test_invalid_side_vale_come_prova_solo_se_viene_dal_client_reale():
    from core.trading_engine import TradingEngine

    client, _ = _client()

    class _AltroClient:
        def place_bet(self, **kwargs):
            return None

    assert TradingEngine._esito_certo_o_gia_classificato(
        RuntimeError("INVALID_SIDE"), client) is True
    assert TradingEngine._esito_certo_o_gia_classificato(
        RuntimeError("INVALID_SIDE"), _AltroClient()) is False

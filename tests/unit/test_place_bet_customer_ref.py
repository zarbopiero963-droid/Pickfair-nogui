"""PR-C — `place_bet` invia `customerRef` a Betfair (idempotenza dedup 60s).

Fino a PR-C il `customer_ref` (chiave di idempotenza gia' generata a monte da
OrderManager, `uuid4().hex`) NON raggiungeva mai il wire: `placeOrders` partiva
senza `customerRef`, e la doppia-bet su retry era evitata SOLO da `single_shot`
(mai re-inviare) + reconciliation per `bet_id`. Ora `customer_ref` viene
propagato fino a `betfair_client.place_bet` e iniettato come top-level
`customerRef` della `placeOrders`, cosi' Betfair deduplica un'eventuale
ri-sottomissione entro la sua finestra di 60s (rete di sicurezza in piu';
`single_shot` resta invariato).

Invarianti di sicurezza pinnate qui (PASS+BLOCK):
- ref Betfair-valido (charset `[A-Za-z0-9-._+*:;~]`, <=32) => inviato come
  `customerRef` (red-first: pre-fix la chiave non c'e');
- NESSUN ref / ref NON valido => `customerRef` OMESSO, mai inviato mangled e mai
  un ordine bloccato: il payload resta byte-identico a quello legacy (fail-closed
  sul REF, non sull'ordine — un ref sporco non deve rompere il piazzamento reale);
- `single_shot` resta True (posture di retry invariata);
- il `customer_ref` viene THREADATO fino al client dai percorsi vivi
  (OrderManager._raw_place_bet fallback; BetfairService.place_order;
  TradingEngine._kwargs_per_place_bet, aggiunto dalla PR02/#461 — fino ad
  allora il ramo LIVE dell'engine faceva lo splat diretto del payload e
  sollevava TypeError prima di spedire).
"""

from __future__ import annotations

import pytest


class _DummySession:
    def post(self, *a, **k):  # pragma: no cover - non deve mai essere colpita
        raise AssertionError("la sessione reale non deve essere usata nei test")


def _client():
    from betfair_client import BetfairClient

    return BetfairClient(
        username="u", app_key="a", cert_pem="c", key_pem="k", session=_DummySession()
    )


def _capture(client):
    cap = {}

    def fake_post(url, method, params, single_shot=False):
        cap["method"] = method
        cap["params"] = params
        cap["single_shot"] = single_shot
        return {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS"}]}

    client._post_jsonrpc = fake_post
    return cap


def _place(client, **over):
    kw = dict(market_id="1.123", selection_id=47, side="BACK", price=2.0, size=5.0)
    kw.update(over)
    return client.place_bet(**kw)


# ---------------------------------------------------------------------------
# CORE — iniezione di customerRef nella placeOrders
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.safety
def test_valid_customer_ref_sent_as_customerRef():
    c = _client()
    cap = _capture(c)
    ref = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4"  # 32 hex, shape di uuid4().hex
    _place(c, customer_ref=ref)
    assert cap["params"]["customerRef"] == ref
    # posture di retry invariata: placeOrders resta single-shot.
    assert cap["single_shot"] is True
    # non tocca instructions/marketId.
    assert cap["params"]["marketId"] == "1.123"


@pytest.mark.unit
@pytest.mark.safety
def test_no_customer_ref_payload_identical_to_legacy():
    # Nessun ref => NESSUNA chiave customerRef (payload byte-identico a oggi).
    c = _client()
    cap = _capture(c)
    _place(c)  # customer_ref non passato
    assert "customerRef" not in cap["params"]


@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.parametrize(
    "bad_ref",
    [
        "",                                   # vuoto
        "a" * 33,                             # > 32 char
        "has space",                          # spazio non ammesso
        "slash/here",                         # '/' non ammesso
        "quote'here",                         # '\'' non ammesso
        "unicodeéref",                   # non-ASCII
        "trailingnl\n",                       # newline finale (fullmatch, non match)
        "mid\nline",                          # newline interno
    ],
)
def test_invalid_customer_ref_is_omitted_not_mangled(bad_ref):
    # Fail-closed sul REF (mai sull'ordine): un ref non-Betfair-valido viene
    # OMESSO (mai inviato troncato/ripulito => niente falso-dedup, niente rifiuto
    # Betfair, niente ordine bloccato). Payload identico al legacy.
    c = _client()
    cap = _capture(c)
    _place(c, customer_ref=bad_ref)
    assert "customerRef" not in cap["params"]


@pytest.mark.unit
@pytest.mark.safety
def test_valid_customer_ref_charset_boundary():
    # Tutti i caratteri ammessi da Betfair passano invariati.
    c = _client()
    cap = _capture(c)
    ref = "Aa0-._+*:;~"
    _place(c, customer_ref=ref)
    assert cap["params"]["customerRef"] == ref


@pytest.mark.unit
@pytest.mark.safety
def test_customer_ref_is_request_level_not_per_instruction():
    # PR-C invia il ref a **livello di richiesta** (`customerRef` top-level della
    # placeOrders = de-dup 60s), NON come `customerOrderRef` **per-istruzione**
    # (il campo che Betfair riporta sull'ordine in listCurrentOrders). I due campi
    # Betfair sono distinti: il customerOrderRef resta NON inviato, quindi vuoto
    # sugli ordini remoti (popolarlo è un follow-up dedicato). BLOCK: se qualcuno
    # spostasse il ref dentro l'instruction come `customerOrderRef`, cambierebbe la
    # semantica di de-dup e la docstring I1 (database.get_bot_active_orders)
    # diventerebbe falsa senza che nessun test se ne accorga.
    c = _client()
    cap = _capture(c)
    _place(c, customer_ref="a" * 32)
    # top-level (chiave di de-dup della richiesta):
    assert cap["params"]["customerRef"] == "a" * 32
    # mai iniettato per-istruzione:
    instr = cap["params"]["instructions"][0]
    assert "customerOrderRef" not in instr
    assert "customerRef" not in instr


# ---------------------------------------------------------------------------
# THREADING — il customer_ref raggiunge place_bet dai percorsi vivi
# ---------------------------------------------------------------------------
@pytest.mark.unit
@pytest.mark.safety
def test_order_manager_fallback_threads_customer_ref():
    # Client "reale-like": accetta i 5 core + customer_ref ma RIFIUTA gli
    # audit-kwargs (come BetfairClient.place_bet post-PR-C). Pre-fix il fallback
    # di _raw_place_bet era a 5 arg e DROPPAVA customer_ref.
    from order_manager import OrderManager

    seen = {}

    class StrictClient:
        def place_bet(self, *, market_id, selection_id, side, price, size, customer_ref=""):
            seen["customer_ref"] = customer_ref
            return {"ok": True, "result": {}}

    om = OrderManager()
    payload = {
        "market_id": "1.123",
        "selection_id": 47,
        "bet_type": "BACK",
        "price": 2.0,
        "stake": 5.0,
        "customer_ref": "REF12345",
        "event_key": "E1",
    }
    om._raw_place_bet(StrictClient(), payload)
    assert seen["customer_ref"] == "REF12345"


@pytest.mark.unit
@pytest.mark.safety
def test_betfair_service_place_order_forwards_customer_ref():
    from services.betfair_service import BetfairService

    seen = {}

    class FakeClient:
        def place_bet(self, **kw):
            seen.update(kw)
            return {"ok": True}

    svc = BetfairService(settings_service=None)
    svc._session_invalid = False
    svc._session_invalid_reason = ""
    svc.client = FakeClient()

    svc.place_order({
        "market_id": "1.123",
        "selection_id": 47,
        "bet_type": "BACK",
        "price": 2.0,
        "stake": 5.0,
        "customer_ref": "REF98765",
    })
    assert seen.get("customer_ref") == "REF98765"


# ---------------------------------------------------------------------------
# ROUND 2 — rilievi review #452 (Fable/Fugu/GPT), order path denaro reale
# ---------------------------------------------------------------------------
def _om_payload(**over):
    p = {
        "market_id": "1.123",
        "selection_id": 47,
        "bet_type": "BACK",
        "price": 2.0,
        "stake": 5.0,
        "customer_ref": "REF12345",
    }
    p.update(over)
    return p


@pytest.mark.unit
@pytest.mark.safety
def test_raw_place_bet_no_keyerror_on_missing_customer_ref():
    # B1 (Fable/Fugu): payload SENZA customer_ref non deve sollevare KeyError sul
    # percorso denaro reale (accesso diretto payload["customer_ref"] era fragile).
    from order_manager import OrderManager

    seen = {}

    class StrictClient:
        def place_bet(self, *, market_id, selection_id, side, price, size, customer_ref=""):
            seen["customer_ref"] = customer_ref
            return {"ok": True}

    om = OrderManager()
    payload = _om_payload()
    payload.pop("customer_ref")  # chiave assente
    om._raw_place_bet(StrictClient(), payload)  # non deve sollevare
    assert seen["customer_ref"] == ""  # default sicuro


@pytest.mark.unit
@pytest.mark.safety
def test_raw_place_bet_calls_place_bet_exactly_once_on_body_typeerror():
    # B2 (Fable/GPT/Fugu, CRITICO denaro reale): un TypeError sollevato DENTRO
    # place_bet (dopo l'invio HTTP) NON deve MAI far ri-chiamare place_bet =>
    # doppia bet reale. Pre-fix il fallback annidato ri-chiamava place_bet.
    from order_manager import OrderManager

    calls = []

    class BodyTypeErrorClient:
        # Firma reale (5 core + customer_ref); il CORPO solleva TypeError DOPO
        # l'ipotetico invio: place_bet DEVE essere invocato una sola volta.
        def place_bet(self, *, market_id, selection_id, side, price, size, customer_ref=""):
            calls.append(1)
            raise TypeError("errore nel corpo dopo l'invio HTTP")

    om = OrderManager()
    with pytest.raises(TypeError):
        om._raw_place_bet(BodyTypeErrorClient(), _om_payload())
    assert len(calls) == 1, f"place_bet invocato {len(calls)} volte (rischio doppia bet)"


@pytest.mark.unit
@pytest.mark.safety
def test_raw_place_bet_sim_client_receives_audit_kwargs():
    # Il client "sim-like" (accetta tutti gli audit-kwargs) continua a riceverli
    # (nessuna perdita di metadata dopo il passaggio a signature-filtering).
    from order_manager import OrderManager

    seen = {}

    class SimLikeClient:
        def place_bet(self, *, market_id, selection_id, side, price, size,
                      customer_ref="", event_key="", table_id=None, batch_id="",
                      event_name="", market_name="", runner_name=""):
            seen.update(customer_ref=customer_ref, event_key=event_key)
            return {"ok": True}

    om = OrderManager()
    om._raw_place_bet(SimLikeClient(), _om_payload(event_key="E42"))
    assert seen["customer_ref"] == "REF12345"
    assert seen["event_key"] == "E42"


@pytest.mark.unit
@pytest.mark.safety
def test_place_bet_duplicate_transaction_is_order_unknown():
    # B3 (Fable/GPT): un customerRef duplicato entro 60s => Betfair risponde
    # DUPLICATE_TRANSACTION (errore), NON un successo idempotente. Poiche' la
    # PRIMA bet e' viva, l'esito e' order_unknown (=> reconciliation), MAI un
    # hard-FAILED (che marcherebbe come fallita una bet reale piazzata).
    c = _client()

    def fake_post(url, method, params, single_shot=False):
        return {"status": "FAILURE", "errorCode": "DUPLICATE_TRANSACTION",
                "instructionReports": []}

    c._post_jsonrpc = fake_post
    res = _place(c, customer_ref="a" * 32)
    assert res["ok"] is False
    assert res["order_unknown"] is True


@pytest.mark.unit
@pytest.mark.safety
def test_customer_ref_stable_across_normalize_retries():
    # Fugu: il customerRef aiuta solo se lo STESSO ref e' riusato sui retry.
    # _normalize_payload assegna il ref una volta ed e' idempotente => i retry di
    # _call_broker_with_retry (stesso payload) riusano lo stesso ref (Betfair
    # deduplica). Un ref rigenerato NON verrebbe deduplicato.
    from order_manager import OrderManager

    om = OrderManager()
    base = _om_payload()
    base.pop("customer_ref")  # forza la generazione
    n1 = om._normalize_payload(dict(base))
    ref = n1["customer_ref"]
    assert ref  # generato
    n2 = om._normalize_payload(dict(n1))  # ri-normalizzazione (come un retry)
    assert n2["customer_ref"] == ref  # STABILE


# ---------------------------------------------------------------------------
# PR02/#461 — rami dell'adattatore dell'engine che il test di accettazione
# (tests/acceptance/test_issue437_pr02.py) non attraversa: li' si prova il
# percorso vivo con un client a firma stretta, qui la funzione in isolamento.
# ---------------------------------------------------------------------------
def _payload_engine():
    return {
        "market_id": "1.23456789", "selection_id": 47999, "bet_type": "LAY",
        "price": 3.0, "stake": 7.5, "customer_ref": "PFREF1",
        "correlation_id": "CORR-9", "event_key": "EV-1", "table_id": 3,
    }


@pytest.mark.unit
@pytest.mark.safety
def test_adattatore_engine_mappa_e_taglia_per_firma_stretta():
    """Client a firma fissa: mappa `bet_type`/`stake` e butta il resto."""
    from core.trading_engine import TradingEngine

    def place_bet(*, market_id, selection_id, side, price, size, customer_ref=""):
        ...

    kw = TradingEngine._kwargs_per_place_bet(place_bet, _payload_engine())
    assert kw == {
        "market_id": "1.23456789", "selection_id": 47999, "side": "LAY",
        "price": 3.0, "size": 7.5, "customer_ref": "PFREF1",
    }
    assert "correlation_id" not in kw, "chiave di servizio spedita al wire"
    assert "bet_type" not in kw and "stake" not in kw, "nomi di dominio non mappati"


@pytest.mark.unit
def test_adattatore_engine_conserva_gli_audit_kwargs_per_il_sim():
    """Broker con **kwargs (il sim): i metadata di audit non si perdono."""
    from core.trading_engine import TradingEngine

    def place_bet(**kwargs):
        ...

    kw = TradingEngine._kwargs_per_place_bet(place_bet, _payload_engine())
    assert kw["event_key"] == "EV-1" and kw["table_id"] == 3
    assert kw["side"] == "LAY" and kw["size"] == 7.5


@pytest.mark.unit
@pytest.mark.safety
def test_adattatore_engine_firma_non_ispezionabile_tiene_core_e_customer_ref():
    """Firma illeggibile => contratto minimo, MA con `customer_ref`.

    Inoltrare tutto "tanto il broker validera'" e' proprio il TypeError che
    questo adattatore esiste per evitare. Scartare anche `customer_ref`
    sarebbe pero' peggio: e' la chiave di de-dup Betfair (60s), e proprio un
    client avvolto/nativo e' il caso in cui un retry rischia la doppia bet
    reale. Rilievo convergente di GPT-5.6 Sol e Fugu Ultra sulla #478: la
    prima versione di questo test fissava il comportamento sbagliato.
    """
    from core.trading_engine import TradingEngine

    # Serve un callable la cui firma `inspect` NON sappia leggere, su OGNI
    # versione. La prima versione usava `time.time`: su 3.11 e 3.12 solleva,
    # ma su 3.13 `inspect.signature(time.time)` restituisce `()` e il test
    # avrebbe esercitato l'altro ramo restando rosso per la ragione sbagliata.
    # Rilievo di Claude Fable 5.1 sulla #478, misurato su 3.11/3.12/3.13. Un
    # `__signature__` che non e' una Signature fa sollevare `inspect`
    # (TypeError su 3.11, ValueError da 3.12): entrambi sono il ramo voluto.
    import inspect as _inspect

    class _ClientNativo:
        __signature__ = "firma illeggibile"

        def __call__(self, **_kw):
            return None

    nativo = _ClientNativo()
    with pytest.raises((TypeError, ValueError)):
        _inspect.signature(nativo)          # precondizione: ramo non ispezionabile
    kw = TradingEngine._kwargs_per_place_bet(nativo, _payload_engine())
    assert set(kw) == {"market_id", "selection_id", "side", "price", "size",
                       "customer_ref"}
    assert kw["side"] == "LAY" and kw["size"] == 7.5
    assert kw["customer_ref"] == "PFREF1", "chiave de-dup persa sul ramo peggiore"


@pytest.mark.unit
def test_adattatore_engine_accetta_anche_i_nomi_gia_del_client():
    """Payload gia' in forma client (`side`/`size`): nessuna doppia mappatura."""
    from core.trading_engine import TradingEngine

    def place_bet(*, market_id, selection_id, side, price, size, customer_ref=""):
        ...

    kw = TradingEngine._kwargs_per_place_bet(
        place_bet,
        {"market_id": "1.1", "selection_id": 1, "side": "BACK", "price": 2.0,
         "size": 5.0, "customer_ref": "R1"},
    )
    assert kw["side"] == "BACK" and kw["size"] == 5.0


@pytest.mark.unit
@pytest.mark.safety
@pytest.mark.parametrize("vuoto", [None, ""])
def test_adattatore_engine_bet_type_vuoto_non_copre_un_side_valido(vuoto):
    """Un `bet_type` vuoto e' ASSENZA, non un valore: deve vincere `side`.

    Rilievo P2 di Codex sulla #478. Con `payload.get("bet_type", side)` una
    chiave `bet_type` presente ma vuota copriva un `side="LAY"` valido; il
    client riceveva `None`/`""` e `safe_side` lo convertiva in BACK: dove il
    risk gate non e' cablato partiva la scommessa OPPOSTA. Dalla
    DECISIONE-426 P13 il client lo rifiuta (`INVALID_SIDE`): senza questa
    semantica l'ordine LAY valido non partirebbe. Stessa semantica gia' usata
    da `BetfairService.place_order` (`bet_type or side`).
    """
    from core.trading_engine import TradingEngine

    def place_bet(*, market_id, selection_id, side, price, size, customer_ref=""):
        ...

    payload = {"market_id": "1.1", "selection_id": 1, "bet_type": vuoto,
               "side": "LAY", "price": 2.0, "stake": 5.0, "customer_ref": "R1"}
    kw = TradingEngine._kwargs_per_place_bet(place_bet, payload)
    assert kw["side"] == "LAY", f"bet_type={vuoto!r} ha coperto side='LAY': {kw['side']!r}"

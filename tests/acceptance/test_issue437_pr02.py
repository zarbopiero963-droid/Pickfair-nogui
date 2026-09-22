"""PR02 (#461/#437) — il percorso LIVE del TradingEngine deve parlare la lingua
del client vero.

FALSO SUCCESSO RIPRODOTTO in Phase 0 sul main `3dfa8b3`. I doppi usati dai test
dell'engine dichiarano ``place_bet(self, **payload)``: ingoiano QUALSIASI
keyword. Il ``BetfairClient`` vero ha firma keyword-only fissa::

    place_bet(*, market_id, selection_id, side, price, size, customer_ref="")

Con il doppio la submission LIVE "riesce" e i kwargs che arrivano sono
``bet_type`` / ``stake`` / ``correlation_id``; con la firma vera la stessa
chiamata solleva ``TypeError`` e il trasporto riceve **zero** invii::

    --- doppio dei test (**payload) ---
        esito   : {'bet_id': 'BET-1'}
        invii al trasporto: 1
        kwargs arrivati   : {'market_id': ..., 'bet_type': 'BACK', 'stake': 10.0,
                             'customer_ref': 'PF-ABC123', 'correlation_id': 'CORR-1'}
    --- client con FIRMA VERA ---
        [CB] Failure (1/3): place_bet() got an unexpected keyword argument 'bet_type'
        ECCEZIONE: TypeError: ... unexpected keyword argument 'bet_type'
        invii al trasporto: 0

Non e' un difetto di laboratorio: in produzione entrambi gli entrypoint passano
``client_getter=self.betfair_service.get_client``, che in LIVE restituisce il
``BetfairClient`` — privo di ``place_order``, quindi la submission cade proprio
sul fallback ``place_bet(**payload)``. Il ramo LIVE non puo' piazzare nulla.

``order_manager._raw_place_bet`` e ``OrderRouter.place`` la mappatura la fanno
gia' (``bet_type``->``side``, ``stake``->``size``, filtro per firma). Il
TradingEngine no, in due punti. Qui si pinna il contratto.
"""
from __future__ import annotations

import inspect
import time
from typing import Any, Dict, List

import pytest

import betfair_client
from core.trading_engine import TradingEngine, _ExecutionContext

# Firma autorevole: NON ricopiata: letta dal client reale. Se il client cambia
# firma e questo doppio non la segue, il test lo dice invece di restare verde.
FIRMA_CLIENT_REALE = inspect.signature(betfair_client.BetfairClient.place_bet)


class ClientLiveStretto:
    """Doppio con la firma ESATTA di ``BetfairClient.place_bet``.

    Niente ``**kwargs``: e' il punto. Un kwarg fuori contratto deve esplodere
    qui come esploderebbe in produzione, non essere assorbito in silenzio.
    """

    def __init__(self, *, risposta: Any = None, errore: Exception | None = None,
                 errore_dopo_invio: Exception | None = None) -> None:
        self.risposta = {"ok": True, "bet_id": "BET-1"} if risposta is None else risposta
        self.errore = errore
        self.errore_dopo_invio = errore_dopo_invio
        self.invii = 0
        self.ricevuti: List[Dict[str, Any]] = []

    def place_bet(self, *, market_id, selection_id, side, price, size, customer_ref=""):
        if self.errore is not None:          # errore PRIMA del contatto col trasporto
            raise self.errore
        self.invii += 1                      # <- il trasporto ha inviato
        self.ricevuti.append({
            "market_id": market_id, "selection_id": selection_id, "side": side,
            "price": price, "size": size, "customer_ref": customer_ref,
        })
        if self.errore_dopo_invio is not None:   # guasto nel post-processing
            raise self.errore_dopo_invio
        return self.risposta


class _RuntimeLive:
    def get_effective_execution_mode(self) -> str:
        return "LIVE"

    def is_live_allowed(self) -> bool:
        return True

    is_emergency_stopped = False
    betfair_service = None


class _Nulla:
    """Stub inerte per le dipendenze che questo percorso non tocca."""

    def __getattr__(self, _nome):
        return lambda *a, **k: None


def _engine(client: Any, *, via_getter: bool = True) -> TradingEngine:
    eng = TradingEngine(
        bus=_Nulla(), db=_Nulla(),
        client_getter=(lambda: client) if via_getter else (lambda: None),
        executor=_Nulla(),
    )
    eng.runtime_controller = _RuntimeLive()
    eng.simulation_broker = None
    eng.order_manager = None
    return eng


def _ctx(customer_ref: str = "PFABC123") -> _ExecutionContext:
    return _ExecutionContext(correlation_id="CORR-1", customer_ref=customer_ref,
                             created_at=time.time())


def _richiesta(**extra: Any) -> Dict[str, Any]:
    base = {"market_id": "1.23456789", "selection_id": 47999,
            "bet_type": "BACK", "price": 2.5, "stake": 10.0}
    base.update(extra)
    return base


# --------------------------------------------------------------------------
# PASS — il contratto che il percorso LIVE deve rispettare
# --------------------------------------------------------------------------
def test_block_il_percorso_live_chiama_il_client_con_la_sua_firma() -> None:
    """Mappatura `bet_type`->`side`, `stake`->`size`, un solo invio.

    RED prima della patch: l'engine fa `place_bet(**payload)` e il payload
    porta `bet_type`/`stake`/`correlation_id`, che la firma vera rifiuta.
    """
    client = ClientLiveStretto()
    esito = _engine(client)._submit_to_order_path(_ctx(), _richiesta())

    assert client.invii == 1, f"invii al trasporto: {client.invii}, atteso 1"
    arrivati = client.ricevuti[0]
    assert arrivati["side"] == "BACK", f"`bet_type` non mappato su `side`: {arrivati}"
    assert arrivati["size"] == 10.0, f"`stake` non mappato su `size`: {arrivati}"
    assert arrivati["market_id"] == "1.23456789"
    assert arrivati["selection_id"] == 47999
    assert arrivati["price"] == 2.5
    assert esito == {"ok": True, "bet_id": "BET-1"}


def test_block_il_customer_ref_arriva_al_client_dal_percorso_engine() -> None:
    """`customer_ref` e' la chiave di de-dup Betfair (#452/#PR-C): deve passare."""
    client = ClientLiveStretto()
    _engine(client)._submit_to_order_path(_ctx("PFDEDUP42"), _richiesta())
    assert client.ricevuti[0]["customer_ref"] == "PFDEDUP42"


def test_block_nessun_kwarg_fuori_contratto_raggiunge_il_client() -> None:
    """Chiavi di servizio (`correlation_id`, audit) non devono arrivare al wire.

    E' il difetto in forma generale: il payload dell'engine e' piu' largo della
    firma del client, e cio' che avanza va tagliato, non spedito.
    """
    client = ClientLiveStretto()
    _engine(client)._submit_to_order_path(
        _ctx(), _richiesta(event_key="EV-1", batch_id="B-1", table_id=7)
    )
    ammessi = set(FIRMA_CLIENT_REALE.parameters) - {"self"}
    ricevuti = set(client.ricevuti[0])
    assert ricevuti <= ammessi, f"kwargs fuori contratto: {sorted(ricevuti - ammessi)}"


# --------------------------------------------------------------------------
# BLOCK — il doppio non deve poter divergere dal client vero
# --------------------------------------------------------------------------
def test_block_il_doppio_ha_la_stessa_firma_del_client_reale() -> None:
    """Se il doppio potesse divergere, il falso successo tornerebbe.

    E' esattamente com'e' nato: i doppi `place_bet(**payload)` assorbivano
    qualunque keyword e tenevano verde un percorso che in produzione non
    poteva funzionare.
    """
    doppio = inspect.signature(ClientLiveStretto.place_bet)
    attesi = [p for n, p in FIRMA_CLIENT_REALE.parameters.items() if n != "self"]
    ottenuti = [p for n, p in doppio.parameters.items() if n != "self"]
    assert [p.name for p in ottenuti] == [p.name for p in attesi], (
        f"il doppio e' divergente dal client reale: {doppio} vs {FIRMA_CLIENT_REALE}"
    )
    assert not any(p.kind is inspect.Parameter.VAR_KEYWORD for p in ottenuti), (
        "il doppio ha **kwargs: assorbirebbe kwargs fuori contratto e il test "
        "resterebbe verde su un percorso rotto"
    )


# --------------------------------------------------------------------------
# TypeError DOPO l'invio — un solo invio, esito ambiguo, nessun retry
# --------------------------------------------------------------------------
def test_block_typeerror_dopo_invio_non_produce_un_secondo_invio() -> None:
    """Il guasto nel post-processing non deve far ri-partire l'ordine.

    Il trasporto ha gia' spedito: ripetere significherebbe una seconda bet con
    denaro reale. Questo test prova ESATTAMENTE questo — un solo invio — e non
    di piu'.

    Nota onesta, da un rilievo P1 di Codex sulla #478: il ciclo di vita
    AMBIGUO **non** e' garantito oggi per un'eccezione post-invio che non sia
    un timeout. `_handle_submit_exception` classifica `ERROR_PERMANENT` tutto
    cio' che non e' `TimeoutError` o non ha "timeout" nel messaggio, quindi
    l'ordine viene finalizzato FAILED e il lock sul customer_ref viene
    rilasciato: un retry successivo potrebbe ripetere un ordine live dall'esito
    ignoto. Non e' una regressione di questa PR — su `main` vale identico per
    `BetfairService.place_order`, che il mapping ce l'ha sempre avuto — ed e'
    il mandato della scheda PR30 (pacchetto P25, «FSM AMBIGUOUS e replace senza
    retry cieco»). Qui si pinna cio' che si prova; la promessa di ambiguita'
    non si scrive finche' non e' vera.
    """
    client = ClientLiveStretto(
        errore_dopo_invio=TypeError("boom nel post-processing della risposta")
    )
    with pytest.raises(TypeError):
        _engine(client)._submit_to_order_path(_ctx(), _richiesta())
    assert client.invii == 1, (
        f"invii al trasporto: {client.invii}. Piu' di 1 = ordine duplicato con "
        f"denaro reale; 0 = l'invio non e' mai partito e il caso non e' quello atteso"
    )


# --------------------------------------------------------------------------
# MALFORMED — ref assente o non valido non blocca l'ordine
# --------------------------------------------------------------------------
@pytest.mark.parametrize("ref", ["", "   ", "ref con spazi", "x" * 40, None])
def test_ref_assente_o_non_valido_non_blocca_il_piazzamento(ref) -> None:
    """Fail-closed sul REF, non sull'ORDINE.

    Un ref sporco e' un problema di de-dup, non una ragione per non piazzare:
    il client lo valida e lo omette. Qui si pinna che l'engine non trasformi
    un ref invalido in un ordine mancato.
    """
    client = ClientLiveStretto()
    ctx = _ExecutionContext(correlation_id="CORR-1",
                            customer_ref=ref if ref is not None else "",
                            created_at=time.time())
    esito = _engine(client)._submit_to_order_path(ctx, _richiesta())
    assert client.invii == 1, "un ref non valido non deve impedire il piazzamento"
    assert esito == {"ok": True, "bet_id": "BET-1"}


# --------------------------------------------------------------------------
# EDGE — DUPLICATE_TRANSACTION non e' un fallimento definitivo
# --------------------------------------------------------------------------
def test_duplicate_transaction_resta_order_unknown_non_failed() -> None:
    """Betfair ha deduplicato: la PRIMA bet e' VIVA.

    Marcarla FAILED sarebbe un falso-negativo su una bet reale. L'engine deve
    restituire l'esito con `order_unknown`, non sollevare ne' dichiarare
    fallimento.
    """
    duplicato = {"ok": False, "error": "DUPLICATE_TRANSACTION",
                 "classification": "PERMANENT", "order_unknown": True}
    client = ClientLiveStretto(risposta=duplicato)
    esito = _engine(client)._submit_to_order_path(_ctx(), _richiesta())
    assert client.invii == 1
    assert esito.get("order_unknown") is True, (
        f"DUPLICATE_TRANSACTION degradato a fallimento definitivo: {esito}"
    )
    assert esito.get("ok") is False


# ==========================================================================
# Rilievi della review #478, riprodotti prima di correggerli.
# ==========================================================================
class _RuntimeSimulazione:
    """Mode SIMULATION perche' il gate live ha negato. Live NON permesso."""

    def get_effective_execution_mode(self) -> str:
        return "SIMULATION"

    def is_live_allowed(self) -> bool:
        return False

    is_emergency_stopped = False
    betfair_service = None


def test_block_in_simulazione_il_fallback_non_usa_un_client_live() -> None:
    """P1 di Codex sulla #478, ed e' una REGRESSIONE di questa PR.

    In SIMULATION senza sim broker ne' order_manager, il fallback in coda
    prende comunque il client dal getter. Su `main` il payload in forma di
    dominio sollevava `TypeError` e faceva da rete per caso; con l'adattatore
    la chiamata e' valida e una bet REALE parte in SIMULATION, con
    `is_live_allowed()` falso e scavalcando breaker e gestione sessione.

    Misurato su `3dfa8b3` vs il primo head della PR::

        origin/main : TypeError            -> invii al client live: 0
        con patch   : {'ok': True, ...}    -> invii al client live: 1

    Fail-closed: in SIMULATION questo fallback puo' usare SOLO un client
    dimostrabilmente di simulazione.
    """
    client = ClientLiveStretto()
    eng = _engine(client)
    eng.runtime_controller = _RuntimeSimulazione()
    eng.simulation_broker = None
    eng.order_manager = None

    with pytest.raises(RuntimeError, match="FALLBACK_NON_PROTETTO"):
        eng._submit_to_order_path(_ctx(), _richiesta())
    assert client.invii == 0, (
        f"bet REALE partita in SIMULATION: {client.invii} invii al client live"
    )


def test_block_il_fallback_in_simulazione_resta_aperto_al_vero_sim_broker() -> None:
    """Il fail-closed non deve rompere il caso legittimo.

    Se il client risolto E' il broker di simulazione dell'engine, il
    piazzamento in SIMULATION e' corretto e deve passare.
    """
    sim = ClientLiveStretto()
    eng = _engine(sim)
    eng.runtime_controller = _RuntimeSimulazione()
    eng.simulation_broker = sim          # identita' dimostrabile
    eng.order_manager = None

    # `simulation_broker.execute` non esiste su questo doppio: il ramo
    # SIMULATION cade nel fallback, che ora deve riconoscere il sim.
    esito = eng._submit_to_order_path(_ctx(), _richiesta())
    assert sim.invii == 1
    assert esito == {"ok": True, "bet_id": "BET-1"}


def test_block_firma_non_ispezionabile_conserva_il_customer_ref() -> None:
    """Rilievo convergente di GPT-5.6 Sol e Fugu Ultra sulla #478.

    Scartare `customer_ref` quando la firma non e' leggibile toglie la chiave
    di de-dup Betfair (60s) proprio dove serve: un client avvolto o nativo.
    Perdere quella chiave significa rischiare la doppia bet reale su un
    retry — cioe' il difetto che la #452 aveva chiuso.
    """
    from core.trading_engine import TradingEngine

    import time as _time  # built-in C: `inspect.signature` solleva ValueError
    kw = TradingEngine._kwargs_per_place_bet(_time.time, _richiesta(customer_ref="PFDEDUP"))
    assert "customer_ref" in kw, f"chiave de-dup persa: {sorted(kw)}"
    assert kw["customer_ref"] == "PFDEDUP"


def test_block_un_payload_incompleto_non_diventa_un_ordine_piazzato() -> None:
    """Rilievo di Claude Fable 5.1 sulla #478, trattato dove la prova sta.

    Fable chiedeva il fail-closed (KeyError) sui cinque core dentro
    l'adattatore, perche' con `payload.get(...)` un campo assente diventa
    `None` e "raggiunge il trasporto". Misurato: il client lo rifiuta gia',
    con errori piu' precisi di un KeyError generico::

        market_id=None    -> RuntimeError: INVALID_MARKET_ID
        selection_id=None -> RuntimeError: INVALID_SELECTION_ID
        price=None        -> RuntimeError: INVALID_PRICE
        size=None         -> RuntimeError: INVALID_SIZE

    Alzare il controllo nell'adattatore degraderebbe quei messaggi e
    romperebbe 12 test di ciclo di vita dell'engine che usano payload
    parziali di proposito. Qui si pinna cio' che conta davvero: un ordine
    incompleto non diventa una bet piazzata.
    """
    from betfair_client import BetfairClient

    client = BetfairClient.__new__(BetfairClient)   # nessuna sessione: non deve servire
    for mancante in ("market_id", "selection_id", "price", "stake"):
        payload = _richiesta()
        payload.pop(mancante)
        kw = TradingEngine_kwargs(payload)
        with pytest.raises(RuntimeError) as err:
            BetfairClient.place_bet(client, **kw)
        assert "INVALID" in str(err.value), (
            f"campo `{mancante}` assente: atteso un rifiuto INVALID_*, "
            f"ottenuto {err.value!r}"
        )


def TradingEngine_kwargs(payload):
    """Scorciatoia leggibile sull'adattatore reale dell'engine."""
    from core.trading_engine import TradingEngine

    def place_bet(*, market_id, selection_id, side, price, size, customer_ref=""):
        ...

    return TradingEngine._kwargs_per_place_bet(place_bet, payload)


@pytest.mark.parametrize("modo", ["LIVE", "MODO_IGNOTO", "simulation_x"])
def test_block_ogni_modalita_dichiarata_chiude_il_fallback_al_client_non_sim(modo) -> None:
    """Secondo P1 di Codex sulla #478: bastava una modalita' non riconosciuta.

    Il primo guard controllava solo l'uguaglianza con `"SIMULATION"`, quindi un
    runtime con modalita' malformata — o LIVE che non ha risolto il client nel
    ramo protetto — passava comunque dal fallback e piazzava con un client
    live, scavalcando `is_live_allowed`, il controllo di sessione e il circuit
    breaker.

    Modalita' NON dichiarata resta permessa di proposito: e' il percorso
    normale dell'armatura di test, e in produzione non si verifica perche'
    `headless_main.py:299` e `mini_gui.py:444` cablano il runtime subito dopo
    aver costruito l'engine. Renderla fail-closed costerebbe 35 test della
    suite chaos senza chiudere un rischio reale: misurato, non supposto.
    """
    class _RuntimeModo:
        def get_effective_execution_mode(self): return modo
        def is_live_allowed(self): return False
        is_emergency_stopped = False
        betfair_service = None

    client = ClientLiveStretto()
    eng = _engine(client)
    eng.runtime_controller = _RuntimeModo()
    eng.simulation_broker = None
    eng.order_manager = None

    with pytest.raises(RuntimeError):
        eng._submit_to_order_path(_ctx(), _richiesta())
    assert client.invii == 0, (
        f"modalita' `{modo}`: {client.invii} invii a un client non di simulazione"
    )

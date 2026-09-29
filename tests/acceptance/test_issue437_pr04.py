"""PR04 (#461/#437, pacchetto P03 / H28 della #453) — il cashout nella GUI
passa dalla stessa catena dell'headless.

FALSO SUCCESSO RIPRODOTTO in Phase 0 sul main `7b0fc5b`, con la GUI VERA
(``MiniPickfairGUI(test_mode=True, force_simulation=True)``) e l'headless VERO
(``HeadlessApp.build``), Database reale in una cartella temporanea, EventBus
reale, SimulationBroker reale dietro BetfairService. Stesso book, stessa
posizione (BACK 10 @ 2.02 sul runner 11), stesso ``CASHOUT_ALL``::

    [gui] sottoscrittori REQ_EXECUTE_CASHOUT=0 CMD_EXECUTE_CASHOUT=0 CASHOUT_FAILED=0
    [gui] dopo CASHOUT_ALL: ordini=1 hedge LAY=0 available=990.000 exposure=10.000
    [gui]   SIGNAL_REJECTED: reason=cashout_chain_not_wired
    [headless] dopo CASHOUT_ALL: ordini=2 hedge LAY=1 available=1000.100 exposure=0.100
    [headless]   REQ_EXECUTE_CASHOUT -> CMD_EXECUTE_CASHOUT -> CASHOUT_SUCCESS matched=10.1

Intanto i test della scheda (bridge, executor, guardrail di cablaggio) erano
verdi: 91 passed. Il guardrail affermava proprio il gap.

Qui ogni prova parte dall'ingresso reale del processo: il testo del messaggio
passa dal parser vero (``TelegramListener.parse_signal``) e dal
``TelegramService`` dell'app, come arriva dalla chat. Stub solo ai confini
esterni: Telegram start/stop e il sender (niente rete), il widget della tab
Telegram (come il guardrail di cablaggio) e, dove dichiarato, la risposta del
broker per simulare un esito che il SIM da solo non produce.

Calcolo indipendente del green-up: BACK 10 @ 2.02, abbinato sul miglior back
di un book 2.02 / 2.04; poi il mercato scende a 1.98 / 2.00. Per chiudere un
BACK si banca al miglior ``availableToLay`` (2.00), come sull'exchange Betfair
(F6, #453). Stake di chiusura 10 x 2.02 / 2.0 = 10,10. Esito del runner 11:
vince +10,20 - 10,10 = +0,10; perde -10 + 10,10 = +0,10. Green-up 0,10 in
entrambi i casi. La GUI deve fare esattamente cio' che fa l'headless.

Fino a F6 il router e il motore SIM usavano il lato opposto del book: la
posizione si apriva su un book 2.00 / 2.02 e la chiusura bancava a 2.00, il
miglior back. Gli stessi numeri valgono ora dal lato giusto.
"""
from __future__ import annotations

import threading
import types
from typing import Any, Dict, List

import pytest

from core.safety_layer import SafetyLayer
from telegram_listener import TelegramListener

MERCATO = "1.500"
EVENTO = "Alfa v Beta"
ALTRO_MERCATO = "1.600"
ALTRO_EVENTO = "Gamma v Delta"

STAKE_CHIUSURA = round(10.0 * 2.02 / 2.0, 2)  # 10.10
_SE_VINCE = 10.0 * (2.02 - 1.0) - STAKE_CHIUSURA * (2.0 - 1.0)  # +0.10
_SE_PERDE = -10.0 + STAKE_CHIUSURA  # +0.10
GREEN_UP = round((_SE_VINCE + _SE_PERDE) / 2.0, 2)  # 0.10


# --------------------------------------------------------------------------
# Le applicazioni vere
# --------------------------------------------------------------------------
class _TabTelegramSenzaWidget:
    """Stub widget-only della tab Telegram, lo stesso del guardrail di
    cablaggio: in CI ``tkinter`` c'e' e ``customtkinter`` no, e il fallback
    costruirebbe un ``tk.Frame`` vero sopra i tab finti del test_mode. La tab
    non tocca bus ne' servizi."""

    def __init__(self, parent_frame, app):
        self.parent = parent_frame
        self.app = app


class _Sender:
    """Confine esterno: il sender Telegram registra invece di inviare."""

    def __init__(self):
        self.messaggi: List[tuple] = []

    def queue_default_message(self, text, message_type=None):
        self.messaggi.append((message_type, text))


class _App:
    """GUI o headless veri, su un Database reale nella cartella del test."""

    def __init__(self, tipo: str, cartella, monkeypatch):
        from database import Database

        percorso = str(cartella / f"{tipo}.db")
        self.tipo = tipo
        if tipo == "gui":
            import mini_gui

            monkeypatch.setattr(mini_gui, "Database", lambda: Database(percorso))
            monkeypatch.setattr(mini_gui, "TelegramTabUI", _TabTelegramSenzaWidget)
            self.app = mini_gui.MiniPickfairGUI(test_mode=True, force_simulation=True)
        else:
            import headless_main

            monkeypatch.setattr(headless_main, "Database", lambda: Database(percorso))
            self.app = headless_main.HeadlessApp()
            self.app.build(start_services=False)
        svc = self.app.telegram_service
        svc.start = lambda *a, **k: {"started": False, "stub": True}
        svc.stop = lambda *a, **k: None
        self.sender = _Sender()
        svc.get_sender = lambda: self.sender

        # Sonda SENZA sottoscriversi: un sottoscrittore in piu' su
        # REQ_EXECUTE_CASHOUT farebbe credere al runtime che la catena ci sia
        # (_cashout_chain_wired conta i sottoscrittori) e falserebbe la misura.
        self.eventi: List[tuple] = []
        pubblica = self.app.bus.publish

        def _registra(topic, payload=None, *a, **k):
            self.eventi.append((topic, payload))
            return pubblica(topic, payload, *a, **k)

        self.app.bus.publish = _registra

    # -- accessi ------------------------------------------------------------
    @property
    def rt(self):
        return self.app.runtime

    @property
    def broker(self):
        return self.app.betfair_service.get_simulation_broker()

    def di(self, topic: str) -> List[Any]:
        return [p for t, p in self.eventi if t == topic]

    def log(self) -> List[str]:
        return list(getattr(self.app.log_text, "lines", []))

    def hedge(self, market_id: str = MERCATO) -> List[dict]:
        return [
            o for o in self.broker.get_current_orders(None)
            if o.get("marketId") == market_id and str(o.get("side")).upper() == "LAY"
        ]

    # -- azioni -------------------------------------------------------------
    def avvia(self) -> None:
        esito = self.rt.start(execution_mode="SIMULATION")
        assert esito.get("started") is True
        assert self.rt.simulation_mode is True

    def book(self, book: dict) -> None:
        """Ingresso reale dei book in SIM: tracker -> service -> broker."""
        self.rt.market_tracker.on_market_book(book)

    def posizione(self, market_id: str = MERCATO, evento: str = EVENTO) -> None:
        """BACK 10 @ 2.02 del bot, abbinato sul miglior back di un book
        2.02 / 2.04; poi il mercato scende al book corrente di ``_aperto``
        (1.98 / 2.00). Il broker SIM registra la puntata nel ledger del bot
        (simulation_bets), la stessa identita' che il router usa per chiudere."""
        self.book(_aperto(market_id, back=2.02, lay=2.04))
        esito = self.broker.place_bet(
            market_id=market_id, selection_id=11, side="BACK", price=2.02,
            size=10.0, event_name=evento,
        )
        assert esito.get("status") == "SUCCESS"
        assert float(esito["instructionReports"][0]["sizeMatched"]) == 10.0
        self.book(_aperto(market_id))
        self.svuota_bus()

    def messaggio(self, testo: str) -> None:
        """Il messaggio della chat entra dal parser vero e dal TelegramService
        dell'app, cioe' dall'ingresso reale del processo."""
        segnale = TelegramListener(api_id=1, api_hash="x").parse_signal(testo)
        assert segnale is not None, f"il parser non riconosce {testo!r}"
        self.app.telegram_service._handle_signal(segnale)

    def svuota_bus(self, timeout: float = 10.0) -> None:
        fatto = threading.Event()

        def _join():
            self.app.bus._queue.join()
            fatto.set()

        threading.Thread(target=_join, daemon=True).start()
        assert fatto.wait(timeout), "EventBus non drenato entro il timeout"

    def audit(self) -> List[dict]:
        import json

        righe = self.app.db._execute(
            "SELECT event_json FROM audit_events ORDER BY id", fetch=True, commit=False
        )
        out = []
        for riga in righe or []:
            try:
                out.append(json.loads(dict(riga)["event_json"]))
            except Exception:
                continue
        return out

    def chiudi(self) -> None:
        try:
            self.rt.stop()
        except Exception:
            pass
        if self.tipo == "gui":
            self.app.shutdown.shutdown()
        else:
            self.app.stop()


def _aperto(market_id: str, status: str = "OPEN", back: float = 1.98, lay: float = 2.0) -> dict:
    """Book corrente: sul runner 11 si punta a ``back`` e si banca a ``lay``."""
    return {
        "marketId": market_id,
        "status": status,
        "runners": [
            {
                "selectionId": 11,
                "status": "ACTIVE",
                "ex": {
                    "availableToBack": [{"price": back, "size": 100.0}],
                    "availableToLay": [{"price": lay, "size": 100.0}],
                },
            },
            {
                "selectionId": 22,
                "status": "ACTIVE",
                "ex": {
                    "availableToBack": [{"price": 1.9, "size": 100.0}],
                    "availableToLay": [{"price": 1.92, "size": 100.0}],
                },
            },
        ],
    }


@pytest.fixture
def gui(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    istanza = _App("gui", tmp_path, monkeypatch)
    yield istanza
    istanza.chiudi()


@pytest.fixture
def headless(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    istanza = _App("headless", tmp_path, monkeypatch)
    yield istanza
    istanza.chiudi()


def _sottoscrittori(app) -> Dict[str, list]:
    return {k: list(v) for k, v in app.bus._subscribers.items()}


# --------------------------------------------------------------------------
# CABLAGGIO — la stessa catena nei due entrypoint
# --------------------------------------------------------------------------
def test_gui_e_headless_costruiscono_la_stessa_catena(gui, headless):
    """H28: bridge -> executor -> SafetyLayer -> OrderRouter -> residuo, un
    solo consumer per topic in ENTRAMBI gli entrypoint (due consumer di
    REQ_EXECUTE_CASHOUT sarebbero due hedge reali per un comando)."""
    from cashout_executor import CashoutExecutor
    from cashout_request_bridge import CashoutRequestBridge
    from cashout_residual_handler import CashoutResidualHandler
    from core.order_router import OrderRouter

    for app in (gui, headless):
        a = app.app
        assert isinstance(a.order_router, OrderRouter), app.tipo
        assert a.order_router.service is a.betfair_service, app.tipo
        assert isinstance(a.cashout_executor, CashoutExecutor), app.tipo
        assert a.cashout_executor.order_router is a.order_router, app.tipo
        assert isinstance(a.cashout_executor.safety_layer, SafetyLayer), (
            f"{app.tipo}: executor senza SafetyLayer reale"
        )
        assert isinstance(a.cashout_request_bridge, CashoutRequestBridge), app.tipo
        assert isinstance(a.cashout_residual_handler, CashoutResidualHandler), app.tipo

        subs = _sottoscrittori(a)
        req = subs.get("REQ_EXECUTE_CASHOUT", [])
        cmd = subs.get("CMD_EXECUTE_CASHOUT", [])
        assert len(req) == 1 and req[0].__self__ is a.cashout_request_bridge, (
            f"{app.tipo}: REQ_EXECUTE_CASHOUT deve avere il solo bridge, trovati {req}"
        )
        assert len(cmd) == 1 and cmd[0].__self__ is a.cashout_executor, (
            f"{app.tipo}: CMD_EXECUTE_CASHOUT deve avere il solo executor, trovati {cmd}"
        )
        residuo = [
            cb for cb in subs.get("CASHOUT_FAILED", [])
            if getattr(cb, "__self__", None) is a.cashout_residual_handler
        ]
        assert len(residuo) == 1, f"{app.tipo}: gestore del residuo non sottoscritto"


# --------------------------------------------------------------------------
# PASS — dalla chat alla posizione chiusa, uguale all'headless
# --------------------------------------------------------------------------
def test_cashout_dalla_chat_nella_gui_chiude_come_headless(gui, headless):
    """Stesso messaggio, stessa posizione: la GUI piazza lo stesso hedge
    dell'headless, pubblica CASHOUT_SUCCESS solo dopo l'abbinamento, lo mostra
    nel Log e aggiorna Storico Bet e Risk Desk."""
    esiti = {}
    for app in (gui, headless):
        app.avvia()
        app.posizione()
        app.eventi.clear()
        app.messaggio("CASHOUT ALL")
        app.svuota_bus()

        hedge = app.hedge()
        assert len(hedge) == 1, f"{app.tipo}: attesa UNA chiusura, trovate {hedge}"
        assert hedge[0]["selectionId"] == 11
        assert hedge[0]["sizeMatched"] == pytest.approx(STAKE_CHIUSURA)
        assert hedge[0]["priceSize"]["price"] == pytest.approx(2.0)

        successi = app.di("CASHOUT_SUCCESS")
        assert len(successi) == 1, f"{app.tipo}: {successi}"
        assert successi[0]["matched"] == pytest.approx(STAKE_CHIUSURA)
        assert successi[0]["green_up"] == pytest.approx(GREEN_UP)
        assert successi[0]["sim"] is True
        assert app.di("CASHOUT_FAILED") == []
        assert app.di("SIGNAL_REJECTED") == []
        fondi = app.broker.get_account_funds()
        esiti[app.tipo] = (
            hedge[0]["sizeMatched"], hedge[0]["priceSize"]["price"],
            round(fondi["available"], 6), round(fondi["exposure"], 6),
        )

    assert esiti["gui"] == esiti["headless"], esiti

    # Visibile nella GUI: esito nel Log e refresh di Storico Bet e Risk Desk.
    bet_id = gui.di("CASHOUT_SUCCESS")[0]["bet_id"]
    righe = gui.log()
    assert any(r.startswith("CASHOUT_SUCCESS ->") and bet_id in r for r in righe), righe
    for tab in ("bet_history", "risk_desk"):
        assert any(r.startswith(f"Refresh event for {tab} ->") and bet_id in r for r in righe), (
            f"{tab} non aggiornato dopo il cashout: {righe}"
        )
    # Lo Storico Bet legge simulation_bets: l'hedge c'e'.
    storico = gui.app.db.get_recent_simulation_bets(limit=10)
    assert any(b.get("bet_id") == bet_id and str(b.get("side")).upper() == "LAY" for b in storico)


def test_cashout_singolo_dalla_gui_chiude_solo_la_sua_partita(gui):
    """Target giusto: «CASHOUT 🆚 Alfa v Beta» chiude la partita nominata e
    lascia aperta l'altra."""
    gui.avvia()
    gui.posizione(MERCATO, EVENTO)
    gui.posizione(ALTRO_MERCATO, ALTRO_EVENTO)
    gui.eventi.clear()

    gui.messaggio(f"CASHOUT 🆚 {EVENTO}")
    gui.svuota_bus()

    assert len(gui.hedge(MERCATO)) == 1
    assert gui.hedge(ALTRO_MERCATO) == []
    cmd = gui.di("CMD_EXECUTE_CASHOUT")
    assert [c["market_id"] for c in cmd] == [MERCATO]


# --------------------------------------------------------------------------
# BLOCK — STOP, emergenza, doppio comando: zero invii o uno solo
# --------------------------------------------------------------------------
def test_runtime_fermo_nessun_invio_e_rifiuto_visibile(gui):
    gui.avvia()
    gui.posizione()
    broker = gui.broker  # STOP scollega il service: si guarda lo stesso broker
    ordini_prima = broker.get_current_orders(None)
    gui.rt.stop()
    gui.eventi.clear()

    gui.messaggio("CASHOUT ALL")
    gui.svuota_bus()

    assert gui.di("REQ_EXECUTE_CASHOUT") == []
    assert gui.di("CMD_EXECUTE_CASHOUT") == []
    assert broker.get_current_orders(None) == ordini_prima
    rifiuti = gui.di("SIGNAL_REJECTED")
    assert len(rifiuti) == 1 and rifiuti[0]["reason"].startswith("runtime_non_attivo")
    assert any(r.startswith("SIGNAL_REJECTED ->") and "runtime_non_attivo" in r for r in gui.log())


def test_emergency_stop_nessun_invio(gui):
    gui.avvia()
    gui.posizione()
    gui.rt.emergency_stop(reason="test_pr04")
    gui.svuota_bus()
    ordini_prima = gui.broker.get_current_orders(None)
    gui.eventi.clear()

    gui.messaggio("CASHOUT ALL")
    gui.svuota_bus()

    assert gui.di("REQ_EXECUTE_CASHOUT") == []
    assert gui.di("CMD_EXECUTE_CASHOUT") == []
    assert gui.broker.get_current_orders(None) == ordini_prima
    rifiuti = gui.di("SIGNAL_REJECTED")
    assert len(rifiuti) == 1 and rifiuti[0]["reason"].startswith("emergency_stop_active")


def test_doppio_comando_un_solo_hedge(gui):
    """Il «doppio click»: due comandi uguali uno dietro l'altro, prima che il
    bus li abbia smaltiti. Una sola chiusura, un solo CMD."""
    gui.avvia()
    gui.posizione()
    gui.eventi.clear()

    gui.messaggio("CASHOUT ALL")
    gui.messaggio("CASHOUT ALL")
    gui.svuota_bus()

    assert len(gui.hedge()) == 1
    assert len(gui.di("CMD_EXECUTE_CASHOUT")) == 1
    assert len(gui.di("CASHOUT_SUCCESS")) == 1


def test_doppia_richiesta_di_chiusura_un_solo_cmd(gui):
    """La stessa richiesta di chiusura due volte in fila (due route
    concorrenti o un replay): sul bus della GUI la deduplica del bridge ne
    lascia passare una. Col solo comando in chat l'esito dipende da quale
    difesa scatta prima (dedup del bridge o posizione gia' mista per il
    router): qui si isola il bridge."""
    gui.avvia()
    gui.posizione()
    richiesta = {
        "market_id": MERCATO, "selection_id": 11, "side": "LAY",
        "stake": STAKE_CHIUSURA, "price": 2.0, "green_up": GREEN_UP,
        "source": "TELEGRAM",
    }
    gui.eventi.clear()

    gui.app.bus.publish("REQ_EXECUTE_CASHOUT", dict(richiesta))
    gui.app.bus.publish("REQ_EXECUTE_CASHOUT", dict(richiesta))
    gui.svuota_bus()

    assert len(gui.di("CMD_EXECUTE_CASHOUT")) == 1
    assert len(gui.hedge()) == 1
    assert len(gui.di("CASHOUT_SUCCESS")) == 1


# --------------------------------------------------------------------------
# ERRORE — visibile nella GUI, mai un SUCCESS anticipato
# --------------------------------------------------------------------------
def test_errore_del_bridge_visibile_nel_log_senza_success(gui):
    """Il publisher reale dell'auto-close (``_trigger_auto_close_table``)
    manda una richiesta senza lato, quota e green-up (difetto noto, #320-C).
    Prima nella GUI cadeva nel vuoto; ora il bridge la rifiuta: CASHOUT_FAILED
    visibile nel Log, residuo registrato, nessun ordine, nessun SUCCESS."""
    gui.avvia()
    gui.posizione()
    ordini_prima = gui.broker.get_current_orders(None)
    gui.eventi.clear()

    tavolo = types.SimpleNamespace(
        table_id=1, market_id=MERCATO, selection_id=11, current_exposure=10.0
    )
    gui.rt._trigger_auto_close_table(tavolo, "STOP_LOSS_REACHED", -5.0)
    gui.svuota_bus()

    falliti = gui.di("CASHOUT_FAILED")
    assert len(falliti) == 1
    assert falliti[0]["status"] == "REJECTED"
    assert falliti[0]["reason"] == "side_invalido"
    assert gui.di("CMD_EXECUTE_CASHOUT") == []
    assert gui.di("CASHOUT_SUCCESS") == []
    assert gui.broker.get_current_orders(None) == ordini_prima
    assert any(r.startswith("CASHOUT_FAILED ->") and "side_invalido" in r for r in gui.log())
    residui = [e for e in gui.audit() if e.get("type") == "CASHOUT_RESIDUAL"]
    assert residui and residui[-1]["status"] == "REJECTED"
    assert any("CASHOUT REJECTED" in testo for _, testo in gui.sender.messaggi)


def test_safety_layer_della_gui_rifiuta_lo_schema(gui):
    """Il SafetyLayer della GUI e' vivo: un comando con ``selection_id``
    stringa supera le invarianti hard dell'executor ma non lo schema del
    layer. Senza layer il broker SIM lo convertirebbe e piazzerebbe."""
    gui.avvia()
    gui.posizione()
    ordini_prima = gui.broker.get_current_orders(None)
    gui.eventi.clear()

    gui.app.bus.publish("CMD_EXECUTE_CASHOUT", {
        "market_id": MERCATO, "selection_id": "11", "side": "LAY",
        "price": 2.0, "stake": STAKE_CHIUSURA, "green_up": GREEN_UP,
        "source": "TEST",
    })
    gui.svuota_bus()

    falliti = gui.di("CASHOUT_FAILED")
    assert len(falliti) == 1
    assert falliti[0]["status"] == "REJECTED"
    assert falliti[0]["reason"].startswith("validation:") and "selection_id" in falliti[0]["reason"]
    assert gui.broker.get_current_orders(None) == ordini_prima
    assert gui.di("CASHOUT_SUCCESS") == []


def test_esito_ignoto_nessun_cancel_riconciliazione_e_notifica(gui, monkeypatch):
    """Ordine ignoto: il broker risponde come ``BetfairClient`` su timeout
    (``ok=False, order_unknown=True``). Il SIM da solo non lo produce, quindi
    la risposta e' simulata al confine. Esito: AMBIGUOUS, nessun cancel,
    record di riconciliazione, notifica CRITICAL, nessun SUCCESS."""
    gui.avvia()
    gui.posizione()
    cancellazioni: List[tuple] = []
    monkeypatch.setattr(gui.broker, "place_bet", lambda **kw: {"ok": False, "order_unknown": True})
    monkeypatch.setattr(gui.broker, "cancel_orders", lambda *a, **k: cancellazioni.append((a, k)))
    gui.eventi.clear()

    gui.messaggio("CASHOUT ALL")
    gui.svuota_bus()

    falliti = gui.di("CASHOUT_FAILED")
    assert len(falliti) == 1 and falliti[0]["status"] == "AMBIGUOUS"
    assert gui.di("CASHOUT_SUCCESS") == []
    assert cancellazioni == []
    riconciliazioni = [e for e in gui.audit() if e.get("type") == "CASHOUT_RECONCILIATION"]
    assert len(riconciliazioni) == 1 and riconciliazioni[0]["cancel_attempted"] is False
    assert any("[CRITICAL]" in testo and "AMBIGUOUS" in testo for _, testo in gui.sender.messaggi)
    assert any(r.startswith("CASHOUT_FAILED ->") and "AMBIGUOUS" in r for r in gui.log())


def test_hedge_non_abbinato_un_solo_cancel(gui, monkeypatch):
    """Unmatched: fra la lettura del router e l'invio la liquidita' sparisce.
    Il book senza offerte entra dall'ingresso reale subito prima del
    piazzamento; il broker vero lascia il LAY a riposo. Esito: UNMATCHED, un
    solo cancel del resting, residuo registrato, nessun SUCCESS."""
    gui.avvia()
    gui.posizione()
    vuoto = _aperto(MERCATO)
    for runner in vuoto["runners"]:
        runner["ex"] = {"availableToBack": [], "availableToLay": []}
    piazza = gui.broker.place_bet
    invii: List[dict] = []

    def _liquidita_sparita(**kw):
        gui.book(vuoto)
        invii.append(kw)
        return piazza(**kw)

    monkeypatch.setattr(gui.broker, "place_bet", _liquidita_sparita)
    gui.eventi.clear()

    gui.messaggio("CASHOUT ALL")
    gui.svuota_bus()

    assert len(invii) == 1
    falliti = gui.di("CASHOUT_FAILED")
    assert len(falliti) == 1 and falliti[0]["status"] == "UNMATCHED"
    assert gui.di("CASHOUT_SUCCESS") == []
    residui = [e for e in gui.audit() if e.get("type") == "CASHOUT_RESIDUAL"]
    assert len(residui) == 1
    assert residui[0]["cancel_attempted"] is True and residui[0]["cancel_ok"] is True
    # Il resting non c'e' piu': resta solo il BACK originale, abbinato.
    aperti = [o for o in gui.broker.get_current_orders(None) if float(o.get("sizeRemaining") or 0) > 0]
    assert aperti == []
    assert any(r.startswith("CASHOUT_FAILED ->") and "UNMATCHED" in r for r in gui.log())


def test_mercato_sospeso_come_headless(gui, headless):
    """Mercato SUSPENDED: nessuna chiusura alla cieca, identico all'headless
    (la coda fino al resume e' #320-C, D14)."""
    for app in (gui, headless):
        app.avvia()
        app.posizione()
        app.book(_aperto(MERCATO, status="SUSPENDED"))
        ordini_prima = app.broker.get_current_orders(None)
        app.eventi.clear()

        app.messaggio("CASHOUT ALL")
        app.svuota_bus()

        assert app.di("REQ_EXECUTE_CASHOUT") == [], app.tipo
        assert app.broker.get_current_orders(None) == ordini_prima, app.tipo
        assert app.di("CASHOUT_SUCCESS") == [], app.tipo


# --------------------------------------------------------------------------
# TEARDOWN — dalla chiusura reale della finestra, senza fermare prima il runtime
# --------------------------------------------------------------------------
def test_stop_chiude_i_gate_prima_dello_smontaggio(gui, headless):
    """``RuntimeController.stop`` rende il runtime non attivo PRIMA di
    smontare Telegram e Betfair: quando parte lo smontaggio (che per Telegram
    puo' durare secondi) nessun segnale e nessun cashout differito passa piu'
    i gate. Vale per entrambi gli entrypoint."""
    for app in (gui, headless):
        app.avvia()
        visto = []
        app.app.telegram_service.stop = lambda *a, _rt=app.rt, **k: visto.append(
            (_rt.mode.value, _rt._runtime_active())
        )
        app.rt.stop()
        assert visto and visto[0] == ("STOPPED", False), (app.tipo, visto)


def test_stop_disconnette_betfair_anche_se_telegram_solleva(gui, headless):
    """GPT-5.6 Sol e Fugu Ultra sulla #486: se lo stop di Telegram solleva,
    ``stop()`` saltava la disconnessione di Betfair. Ora il servizio Betfair
    si disconnette comunque, il runtime resta fermo e l'errore risale al
    chiamante (gli hook di chiusura lo registrano e proseguono)."""
    for app in (gui, headless):
        app.avvia()
        assert app.app.betfair_service.get_simulation_broker() is not None

        def _telegram_rotto(*a, **k):
            raise RuntimeError("disconnessione Telegram fallita")

        app.app.telegram_service.stop = _telegram_rotto
        with pytest.raises(RuntimeError, match="Telegram"):
            app.rt.stop()

        assert app.app.betfair_service.get_simulation_broker() is None, app.tipo
        assert app.app.betfair_service.get_live_client() is None, app.tipo
        assert app.rt.mode.value == "STOPPED", app.tipo
        assert app.rt._runtime_active() is False, app.tipo


class _ListenerAllaChiusura:
    """Confine Telegram: il listener, mentre si ferma, consegna un ultimo
    messaggio (``testo``) oppure resta appeso finche' sul bus non compare uno
    degli eventi ``attendi`` (una disconnessione lenta che dura quanto serve,
    senza dipendere dai tempi della macchina). Lo stop del ``TelegramService``
    resta quello vero."""

    def __init__(self, app: "_App", testo: str = "", attendi: tuple = (), limite: float = 5.0):
        self.app = app
        self.testo = testo
        self.attendi = attendi
        self.limite = limite
        self.fermate = 0

    def stop(self):
        import time as _time

        self.fermate += 1
        if self.testo:
            self.app.messaggio(self.testo)
        if self.attendi:
            scadenza = _time.monotonic() + self.limite
            while not any(t in self.attendi for t, _ in self.app.eventi):
                assert _time.monotonic() < scadenza, f"nessuno di {self.attendi} entro {self.limite}s"
                _time.sleep(0.02)
        self.app.svuota_bus()
        return {"stopped": True}


def _listener_di_confine(app: "_App", **kw) -> None:
    svc = app.app.telegram_service
    del svc.stop  # di nuovo lo stop vero del servizio, fino al listener
    svc.listener = _ListenerAllaChiusura(app, **kw)


def test_chiusura_finestra_comando_durante_lo_smontaggio_non_parte(gui):
    """GPT-6 Astra sulla #486: dalla PR04 la GUI puo' piazzare un hedge, e
    alla chiusura non fermava il runtime. Un CASHOUT consegnato mentre
    Telegram si disconnette partiva col servizio Betfair ancora collegato:
    gli hook della chiusura smontano Telegram prima di Betfair. Ora la
    chiusura ferma il runtime per primo: rifiutato, zero invii."""
    gui.avvia()
    gui.posizione()
    broker = gui.broker  # la chiusura scollega il service: si guarda lo stesso broker
    ordini_prima = broker.get_current_orders(None)
    _listener_di_confine(gui, testo="CASHOUT ALL")
    gui.eventi.clear()

    gui.app._on_close()
    gui.svuota_bus()

    assert broker.get_current_orders(None) == ordini_prima
    assert gui.di("REQ_EXECUTE_CASHOUT") == []
    assert gui.di("CMD_EXECUTE_CASHOUT") == []
    rifiuti = gui.di("SIGNAL_REJECTED")
    assert len(rifiuti) == 1 and rifiuti[0]["reason"] == "runtime_non_attivo:STOPPED", rifiuti


def test_chiusura_finestra_con_grace_armato_non_parte(gui):
    """Lo scenario di GPT-6 Astra alla lettera: un cashout differito dal
    grace auto-green e' armato quando si chiude la finestra, e scatta durante
    lo smontaggio. Nessun produttore di oggi arma il grace (serve un CASHOUT
    con mercato e selezione): il segnale e' costruito come nei test del grace.
    Esito: annullato con CASHOUT_FAILED visibile, nessun invio."""
    gui.avvia()
    gui.posizione()
    gui.rt.config.auto_green_delay_enabled = True
    gui.rt.config.auto_green_delay_sec = 0.2
    broker = gui.broker
    ordini_prima = broker.get_current_orders(None)
    gui.eventi.clear()

    gui.app.bus.publish("SIGNAL_RECEIVED", {
        "signal_type": "CASHOUT", "market_id": MERCATO, "selection_id": 11,
        "event_name": EVENTO, "source": "TEST",
    })
    gui.svuota_bus()
    assert gui.di("REQ_EXECUTE_CASHOUT") == []  # il grace e' armato, non ancora partito

    # La disconnessione di Telegram dura finche' il grace non e' scattato:
    # si aspetta l'esito sul bus (annullato o instradato), non un tempo fisso.
    _listener_di_confine(gui, attendi=("CASHOUT_FAILED", "REQ_EXECUTE_CASHOUT"))
    gui.app._on_close()
    gui.svuota_bus()

    assert broker.get_current_orders(None) == ordini_prima
    assert gui.di("REQ_EXECUTE_CASHOUT") == []
    falliti = gui.di("CASHOUT_FAILED")
    assert len(falliti) == 1, falliti
    assert falliti[0]["reason"].startswith("grace_aborted:runtime_non_attivo"), falliti


def test_chiusura_finestra_hook_in_ordine_e_tutti_riusciti(gui):
    """Fugu Ultra sulla #486: l'hook runtime_stop ferma anche Telegram e
    Betfair, poi gli hook successivi li richiamano. La stessa chiamata che fa
    ``_on_close``, sui servizi veri: il runtime si ferma per primo e nessun
    hook fallisce, doppio stop e doppia disconnessione compresi.

    Lo stop di Telegram e' quello vero anche quando lo chiama il runtime (il
    fixture lo sostituisce, qui lo si rimette), fino a un listener di confine.
    Dopo il merge della #486 il test copriva il doppio stop col solo metodo
    dell'hook: rafforzato nella PR di F6, come dichiarato nella #486."""
    gui.avvia()
    gui.posizione()
    _listener_di_confine(gui)
    listener = gui.app.telegram_service.listener

    esiti = gui.app.shutdown.shutdown()

    assert [e["name"] for e in esiti] == [
        "runtime_stop", "telegram_stop", "betfair_disconnect", "db_close", "executor_shutdown",
    ]
    assert all(e["ok"] for e in esiti), esiti
    assert gui.rt.mode.value == "STOPPED"
    assert gui.app.betfair_service.get_simulation_broker() is None
    # Il listener si ferma una volta: il secondo stop trova il servizio fermo.
    assert listener.fermate == 1
    assert gui.app.telegram_service.state == "STOPPED"
    assert gui.app.telegram_service.listener is None


def test_chiusura_finestra_esito_tardivo_senza_effetti(gui):
    """La catena non ha thread propri. Dopo la chiusura reale della finestra
    (_on_close: ShutdownManager + destroy, senza fermare prima il runtime a
    mano) un CASHOUT_FAILED tardivo non rompe il worker del bus e non piazza
    nulla."""
    gui.avvia()
    gui.posizione()
    broker = gui.broker  # la chiusura scollega il service: si guarda lo stesso broker
    ordini_prima = broker.get_current_orders(None)
    gui.app._on_close()

    gui.app.bus.publish("CASHOUT_FAILED", {
        "status": "UNMATCHED", "reason": "tardivo", "bet_id": None,
        "market_id": MERCATO, "selection_id": 11, "matched": 0.0,
    })
    gui.svuota_bus()

    assert broker.get_current_orders(None) == ordini_prima
    assert gui.app.bus.subscriber_error_counts() == {}

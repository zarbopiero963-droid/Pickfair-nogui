"""Cablaggio runtime — il guardrail che monta l'applicazione VERA e verifica i fili.

## Perche' questo file esiste

La suite (5000+ verdi) prova che ogni pezzo funziona, ma nessun test montava il
programma che si avvia davvero per chiedere: i pezzi sono COLLEGATI? L'audit di
cablaggio del 2026-08-23 ha trovato funzionalita' complete, testate e verdi che
a runtime sono inerti perche' nessuno le istanzia: il `core/pnl_engine` (unico
publisher di `RUNTIME_CLOSE_POSITION` — gap CHIUSO e promosso in CABLATO dalla
PR `runtime_settlement_wiring`), il `DutchingController` (consumatore di
`CMD_PLACE_DUTCHING`), il `RiskGate` che legge le costanti di `trading_config`
invece della config Roserpina salvata dalla GUI. Tutti con test propri verdi:
il verde dei pezzi non dice niente sul montaggio.

Qui l'applicazione viene costruita DAVVERO — `HeadlessApp.build()` e
`MiniPickfairGUI(test_mode=True)` con Database/EventBus/servizi/runtime REALI,
niente Fake* — e si afferma lo stato dei fili. Due registri:

- **CABLATO** — cio' che oggi e' collegato e non deve scollegarsi mai:
  sottoscrittori del percorso ordine e cashout, stack di osservabilita',
  RiskGate reale, ReconciliationEngine reale nel runtime. Se una modifica
  stacca un filo, il test lo nomina.

- **GAP_NOTI** — cio' che oggi NON e' collegato, voce per voce, con la PR del
  piano che lo colleghera'. Ogni test di gap afferma che il gap C'E' ANCORA:
  quando una PR lo chiude, il suo test FALLISCE apposta e obbliga a spostare
  la voce da GAP_NOTI alle asserzioni CABLATO nello stesso PR. Cosi' il
  registro non puo' mentire in nessuna direzione: un gap non puo' chiudersi
  in silenzio, un filo non puo' staccarsi in silenzio.

## Nota sul riuso del grafo

Le funzioni di grafo (`_moduli`/`_importati`/`_grafo_vivo`) NON sono copiate:
vengono caricate da `tests/percorsi/test_albero_del_bridge_resta_fuori.py`,
che dopo #435 e' la copia CORRETTA (import relativi con livello > 1 risolti da
`importlib`, file non analizzabile => errore, mai `set()` silenzioso). Una
terza copia avrebbe ripropagato il difetto che #435 ha appena chiuso.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib

import pytest

import trading_config
from core.pnl_engine import PnLEngine
from core.reconciliation_engine import ReconciliationEngine
from core.risk_gate import RiskGate

RADICE = pathlib.Path(__file__).resolve().parents[2]


# =========================================================================
# Helper di grafo: caricati dal guard autorevole, non ricopiati.
# =========================================================================
def _carica_guard_percorsi():
    percorso = RADICE / "tests" / "percorsi" / "test_albero_del_bridge_resta_fuori.py"
    spec = importlib.util.spec_from_file_location("_guard_percorsi_bridge", percorso)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


_GUARD = _carica_guard_percorsi()


def _grafo_vivo() -> set[str]:
    moduli = _GUARD._moduli()
    return _GUARD._grafo_vivo(moduli)


def _sorgenti_vive() -> dict[str, str]:
    """Testo sorgente dei soli moduli raggiungibili dagli entrypoint."""
    moduli = _GUARD._moduli()
    vivi = _GUARD._grafo_vivo(moduli)
    return {
        nome: moduli[nome].read_text(encoding="utf-8", errors="replace")
        for nome in vivi
        if nome in moduli
    }


def _sottoscrittori(bus) -> dict[str, int]:
    registro = getattr(bus, "_subscribers", None)
    assert registro is not None, (
        "EventBus senza registro _subscribers: o non e' l'EventBus reale, "
        "o la sua struttura interna e' cambiata e questo guard va aggiornato."
    )
    return {topic: len(callbacks) for topic, callbacks in registro.items() if callbacks}


# =========================================================================
# Fixture: l'applicazione VERA, montata una volta per modulo.
#
# Isolamento (rilievo Fable 5 sulla #438, accolto): il primo taglio faceva
# `os.chdir` in DUE fixture module-scoped che convivono — e siccome
# `Database("pickfair.db")` risolve il path relativo a CONNECT-time (le
# connessioni sono thread-local e lazy), una connessione aperta dopo il
# secondo chdir sarebbe finita nella cartella dell'altra app. Ora:
#   1. la cwd cambia UNA volta sola, in una fixture condivisa, e resta
#      COSTANTE per tutta la vita di entrambe le app (teardown LIFO di
#      pytest: si ripristina dopo che entrambe sono state smontate);
#   2. ogni app riceve comunque il SUO Database con path ASSOLUTO in una
#      sottocartella propria, cosi' nessuna connessione dipende dalla cwd.
# Il Database resta quello REALE: cambia solo dove scrive.
# =========================================================================
@pytest.fixture(scope="module")
def _cartella_modulo(tmp_path_factory):
    cwd = os.getcwd()
    base = tmp_path_factory.mktemp("cablaggio-runtime")
    os.chdir(base)
    try:
        yield base
    finally:
        os.chdir(cwd)


@pytest.fixture(scope="module")
def app_headless(_cartella_modulo):
    import headless_main
    from database import Database

    cartella = _cartella_modulo / "headless"
    cartella.mkdir()
    mp = pytest.MonkeyPatch()
    mp.setattr(
        headless_main, "Database", lambda: Database(str(cartella / "pickfair.db"))
    )
    try:
        app = headless_main.HeadlessApp()
        # start_services=False: si verifica il MONTAGGIO, non si avviano i
        # thread di watchdog/cleanup — il cablaggio e' gia' tutto in build().
        app.build(start_services=False)
        yield app
        app.stop()
    finally:
        mp.undo()


class _TelegramTabUISenzaWidget:
    """Stub widget-only della tab Telegram, come in tutti i test GUI del repo.

    Perche' serve (rosso CI su 36221b9, riprodotto in locale con tkinter
    installato): in CI `tkinter` esiste ma `customtkinter` no, quindi il
    fallback `TelegramTabUI` di mini_gui costruisce un `tk.Frame` REALE sopra
    i tab `object()` del test_mode => `AttributeError: 'object' object has no
    attribute 'tk'`. Nel venv senza tkinter degradava a dummy puri e passava:
    un falso verde d'ambiente. La tab e' SOLO widget — zero cablaggio bus/
    servizi — quindi stubbarla non toglie nulla a cio' che questo guardrail
    afferma: il core (Database, bus, servizi, runtime, engine) resta reale.
    """

    def __init__(self, parent_frame, app):
        self.parent = parent_frame
        self.app = app


@pytest.fixture(scope="module")
def app_gui(_cartella_modulo):
    import mini_gui
    from database import Database

    cartella = _cartella_modulo / "gui"
    cartella.mkdir()
    mp = pytest.MonkeyPatch()
    mp.setattr(
        mini_gui, "Database", lambda: Database(str(cartella / "pickfair.db"))
    )
    mp.setattr(mini_gui, "TelegramTabUI", _TelegramTabUISenzaWidget)
    try:
        gui = mini_gui.MiniPickfairGUI(test_mode=True, force_simulation=True)
        yield gui
        gui.shutdown.shutdown()
    finally:
        mp.undo()


# =========================================================================
# PARTE 1 — CABLATO: i fili che esistono e non devono staccarsi.
# Fotografia verificata su build reale il 2026-08-23; ogni topic qui sotto
# aveva almeno un sottoscrittore vero sul bus dell'app montata.
# =========================================================================
TOPIC_CABLATI_HEADLESS = (
    # ingresso segnali: TelegramService pubblica, RuntimeController consuma
    "SIGNAL_RECEIVED",
    "SIGNAL_APPROVED",
    "SIGNAL_REJECTED",
    # percorso ordine: RuntimeController pubblica, TradingEngine consuma
    "CMD_QUICK_BET",
    # catena cashout (Fase 2.1): router -> bridge -> executor -> residuo
    "REQ_EXECUTE_CASHOUT",
    "CMD_EXECUTE_CASHOUT",
    "CASHOUT_FAILED",
    # ciclo finanziario: RuntimeController consuma; il publisher e' il
    # core.pnl_engine.PnLEngine cablato in RuntimeController.__init__
    # (PR `runtime_settlement_wiring`, promosso dal gap
    # `pnl_engine_mai_istanziato`).
    "RUNTIME_CLOSE_POSITION",
    # mark-to-market: sottoscritto dal PnLEngine cablato (stessa PR). Il
    # publisher arrivera' col market-data poll; l'auto-close resta OFF.
    "MARKET_BOOK_UPDATE",
    # ciclo di vita runtime, consumato dal logger headless
    "RUNTIME_STARTED",
    "RUNTIME_STOPPED",
    "RUNTIME_PAUSED",
    "RUNTIME_RESUMED",
    "RUNTIME_LOCKDOWN",
    "TELEGRAM_STATUS",
)


@pytest.mark.guardrail
@pytest.mark.parametrize("topic", TOPIC_CABLATI_HEADLESS)
def test_headless_ha_il_sottoscrittore(topic, app_headless):
    presenti = _sottoscrittori(app_headless.bus)
    assert presenti.get(topic, 0) >= 1, (
        f"FILO STACCATO: '{topic}' non ha piu' sottoscrittori sul bus "
        f"dell'app headless reale. Un evento pubblicato li' cade nel vuoto."
    )


COMPONENTI_HEADLESS = (
    # osservabilita' (headless_main.build)
    "watchdog_service",
    "alerts_manager",
    "incidents_manager",
    "health_registry",
    "metrics_registry",
    "snapshot_service",
    "diagnostics_service",
    "cleanup_service",
    "retention_manager",
    "runtime_probe",
    # catena di esecuzione cashout (_wire_cashout_execution_chain)
    "order_router",
    "cashout_executor",
    "cashout_request_bridge",
    "cashout_residual_handler",
)


@pytest.mark.guardrail
@pytest.mark.parametrize("nome", COMPONENTI_HEADLESS)
def test_headless_costruisce_il_componente(nome, app_headless):
    componente = getattr(app_headless, nome, None)
    assert componente is not None, (
        f"COMPONENTE NON COSTRUITO: HeadlessApp.build() non produce piu' "
        f"'{nome}'. Le impostazioni/tab che lo governano tornano scatole vuote."
    )


@pytest.mark.guardrail
def test_risk_gate_reale_sul_percorso_ordine(app_headless, app_gui):
    """H-04: senza RiskGate esplicito l'engine ripiega sul segnaposto che
    approva tutto. Vale per ENTRAMBI gli entrypoint."""
    for nome, app in (("headless", app_headless), ("gui", app_gui)):
        engine = app.trading_engine if nome == "headless" else app_gui.trading_engine
        assert engine.risk_gate_wired is True, f"{nome}: risk gate NON cablato (segnaposto fail-open)"
        assert isinstance(engine.risk_middleware, RiskGate), (
            f"{nome}: risk_middleware non e' il RiskGate reale: "
            f"{type(engine.risk_middleware).__name__}"
        )


@pytest.mark.guardrail
def test_reconciliation_engine_reale_nel_runtime(app_headless):
    assert isinstance(app_headless.runtime.reconciliation_engine, ReconciliationEngine), (
        "Il RuntimeController non costruisce piu' il ReconciliationEngine reale: "
        "la riconciliazione all'avvio sparirebbe in silenzio."
    )


@pytest.mark.guardrail
def test_probe_readiness_gate_armato(app_headless, app_gui):
    """Entrambi gli entrypoint agganciano la RuntimeProbe al runtime e armano
    il gate di readiness: e' un filo di safety, non un dettaglio."""
    assert app_headless.runtime.runtime_probe is app_headless.runtime_probe
    assert app_headless.runtime.enforce_probe_readiness_gate is True
    assert app_gui.runtime.runtime_probe is app_gui.runtime_probe
    assert app_gui.runtime.enforce_probe_readiness_gate is True


# =========================================================================
# PARTE 2 — GAP_NOTI: i fili che oggi NON esistono, voce per voce.
#
# OGNI TEST QUI AFFERMA CHE IL GAP E' ANCORA APERTO. Quando la PR indicata
# lo chiude, il test FALLISCE APPOSTA: e' il segnale che la voce va promossa
# nella PARTE 1 (CABLATO) nello stesso PR che chiude il gap. Non cancellare
# la voce: promuoverla. Un gap chiuso senza promozione e' un filo nuovo che
# nessun guardrail sorveglia.
# =========================================================================


@pytest.mark.guardrail
def test_pnl_engine_cablato_publisher_del_ciclo_finanziario(app_headless, app_gui):
    """PROMOSSO da GAP `pnl_engine_mai_istanziato`, chiuso dalla PR
    `runtime_settlement_wiring` (come prescritto dal registro: il gap test e'
    fallito apposta e la voce e' salita qui, in CABLATO).

    Il RuntimeController costruisce in `__init__` il
    `core.pnl_engine.PnLEngine` REALE — publisher unico di
    RUNTIME_CLOSE_POSITION — sul bus dell'app, per ENTRAMBI gli entrypoint
    (headless e GUI, parity by construction). Il sottoscrittore di
    MARKET_BOOK_UPDATE e' asserito dal test dei topic
    (TOPIC_CABLATI_HEADLESS).

    Vincolo di safety che NON deve regredire: sull'app reale l'auto-close
    mark-to-market resta DISARMATO (`auto_close_enabled=False`) — una
    chiusura contabile a soglia senza ordine reale realizzerebbe PnL
    fantasma su una posizione ancora viva su Betfair, in contrasto col
    contratto del runtime («NON chiude automaticamente le posizioni»).
    Il settlement REALE passa dal poller cleared orders, non da questo flag."""
    for nome, app in (("headless", app_headless), ("gui", app_gui)):
        engine = getattr(app.runtime, "pnl_engine", None)
        assert isinstance(engine, PnLEngine), (
            f"{nome}: FILO STACCATO — RuntimeController non costruisce piu' "
            f"il PnLEngine reale (trovato: {type(engine).__name__}). Il "
            "PnL realizzato non entrerebbe piu' nel daily-loss."
        )
        assert engine.bus is app.bus, (
            f"{nome}: il PnLEngine e' su un bus DIVERSO da quello dell'app: "
            "i suoi RUNTIME_CLOSE_POSITION cadrebbero nel vuoto."
        )
        assert engine.auto_close_enabled is False, (
            f"{nome}: auto-close mark-to-market ARMATO di default sull'app "
            "reale: chiusure contabili senza ordine reale — vietato."
        )


@pytest.mark.guardrail
def test_dutching_agganciato_corsia_selettiva_senza_doppi_consumer(
    app_headless, app_gui
):
    """CABLATO (promosso da GAP, piano: PR «dutching agganciato», decisione
    owner 2026-08-23). DutchingController e RiskMiddleware sono costruiti in
    ENTRAMBI gli entrypoint e la corsia dutching del bus e' viva:
    REQ_PLACE_DUTCHING -> RiskMiddleware (dedupe 2s + normalizzazione) ->
    CMD_PLACE_DUTCHING -> DutchingController.submit_dutching (precheck
    Roserpina, tavoli, gambe come CMD_QUICK_BET nel choke dell'engine).
    La corsia e' ARMATA ma DORMIENTE: nessun publisher di
    REQ_PLACE_DUTCHING esiste in produzione — stesso precedente del
    CashoutRequestBridge («dormiente by-design»).

    ANTI-DOPPIO-CONSUMER (il rischio vero di questo cablaggio): il
    middleware e' agganciato SOLO alla corsia dutching. Le altre corsie
    hanno gia' il loro consumer unico (REQ_QUICK_BET -> TradingEngine
    diretto; REQ_EXECUTE_CASHOUT -> CashoutRequestBridge): un middleware
    cablato su tutti i topic inoltrerebbe REQ->CMD in PARALLELO al
    consumer diretto — ordine doppio e cashout doppio strutturali."""
    from controllers.dutching_controller import DutchingController
    from core.risk_middleware import RiskMiddleware

    vivo = _grafo_vivo()
    assert "controllers.dutching_controller" in vivo, (
        "REGRESSIONE: dutching_controller sparito dal grafo vivo."
    )
    assert "core.risk_middleware" in vivo, (
        "REGRESSIONE: risk_middleware sparito dal grafo vivo."
    )

    for nome, app in (("headless", app_headless), ("gui", app_gui)):
        assert isinstance(
            getattr(app, "dutching_controller", None), DutchingController
        ), f"REGRESSIONE ({nome}): DutchingController non costruito."
        assert app.dutching_controller.bus is app.bus
        assert app.dutching_controller.runtime is app.runtime
        assert isinstance(
            getattr(app, "dutching_risk_middleware", None), RiskMiddleware
        ), f"REGRESSIONE ({nome}): RiskMiddleware non costruito."

        presenti = _sottoscrittori(app.bus)
        assert presenti.get("REQ_PLACE_DUTCHING", 0) == 1, (
            f"REGRESSIONE ({nome}): REQ_PLACE_DUTCHING senza il bridge "
            "del middleware (o con doppioni)."
        )
        assert presenti.get("CMD_PLACE_DUTCHING", 0) == 1, (
            f"REGRESSIONE ({nome}): CMD_PLACE_DUTCHING senza l'esecutore "
            "del controller (o con doppioni)."
        )
        # Le corsie preesistenti NON devono guadagnare un secondo consumer:
        # se un conteggio sale oltre l'atteso, qualcuno ha cablato il
        # middleware anche li' e ogni REQ produrrebbe DUE esecuzioni.
        # Attesi di oggi: REQ_QUICK_BET=1 (engine) su entrambi;
        # REQ_EXECUTE_CASHOUT=1 (bridge) su headless, 0 su GUI (il bridge
        # manca dalla GUI: gap «GUI parity», tracciato a parte).
        atteso_cashout = 1 if nome == "headless" else 0
        assert presenti.get("REQ_QUICK_BET", 0) == 1, (
            f"DOPPIO CONSUMER ({nome}): REQ_QUICK_BET deve restare al solo "
            "TradingEngine."
        )
        assert presenti.get("REQ_EXECUTE_CASHOUT", 0) == atteso_cashout, (
            f"DOPPIO CONSUMER ({nome}): REQ_EXECUTE_CASHOUT atteso a "
            f"{atteso_cashout} (bridge dove esiste, mai il middleware in "
            "parallelo)."
        )


@pytest.mark.guardrail
def test_risk_gate_legge_la_config_roserpina_dell_owner(app_headless, app_gui):
    """CABLATO (promosso da GAP, piano: PR «RiskGate ↔ config Roserpina»).

    Il gate del percorso ordine legge la config Roserpina salvata dall'owner
    (vista ``RoserpinaRiskLimits``, snapshot fresco a ogni check), non piu' le
    COSTANTI congelate di trading_config: un ``roserpina.min_price`` salvato
    nella tab arriva al gate al check successivo, SENZA riavvio. A settings
    vergini i default della vista coincidono con le costanti (zero delta).
    Il fail-closed sui campi mancanti/illeggibili e' asserito nei test unit
    (tests/core/test_risk_gate.py, sezione RoserpinaRiskLimits)."""
    from core.risk_gate import RoserpinaRiskLimits

    ordine = {"bet_type": "BACK", "price": 1.30, "stake": 10.0}
    for nome, app in (("headless", app_headless), ("gui", app_gui)):
        gate = app.trading_engine.risk_middleware
        vista = getattr(gate, "config", None)
        assert isinstance(vista, RoserpinaRiskLimits), (
            f"REGRESSIONE ({nome}): il RiskGate non legge la config Roserpina "
            f"dell'owner (config={type(vista).__name__}): i limiti salvati "
            "nella tab non arrivano al percorso ordine."
        )
        assert vista is not trading_config

        # Comportamentale: il valore salvato dall'owner MORDE al check dopo.
        assert gate.check(dict(ordine))["allowed"] is True  # default 1.02
        app.settings_service.save_settings({"roserpina.min_price": 1.50})
        try:
            verdetto = gate.check(dict(ordine))
            assert verdetto["allowed"] is False, (
                f"REGRESSIONE ({nome}): min_price=1.50 salvato dall'owner "
                "non applicato dal gate (ordine a 1.30 permesso)."
            )
            assert verdetto["reason"] == "RISK_PRICE_BELOW_MIN"
        finally:
            # Fixture module-scoped: si ripristina il default per i test dopo.
            app.settings_service.save_settings(
                {"roserpina.min_price": trading_config.MIN_PRICE}
            )
        assert gate.check(dict(ordine))["allowed"] is True


@pytest.mark.guardrail
def test_cablato_betfair_client_risolto_lazy(app_headless):
    """CABLATO (PR «betfair_client lazy»). Il build NON congela piu' il
    client: l'engine lo risolve a ogni submission via client_getter
    (override esplicito se impostato), cosi' in LIVE il ramo col breaker
    di submission e la gestione sessione scaduta non viene scavalcato."""
    engine = app_headless.trading_engine

    # Nessun valore congelato al build: l'attributo resta il default None.
    assert engine.betfair_client is None, (
        "REGRESSIONE: betfair_client ri-congelato al build. A build-time la "
        "connessione non esiste: il valore congelato scavalca breaker e "
        "gestione sessione nel ramo LIVE."
    )

    # Il getter e' quello del BetfairService reale dell'app.
    assert getattr(engine.client_getter, "__self__", None) is (
        app_headless.betfair_service
    ), "REGRESSIONE: client_getter non e' il get_client del BetfairService."

    # Risoluzione lazy: senza override segue il getter del servizio...
    risolutore = getattr(engine, "_resolve_live_client", None)
    assert callable(risolutore), (
        "REGRESSIONE: manca TradingEngine._resolve_live_client — il ramo "
        "LIVE torna a leggere l'attributo congelato."
    )
    assert engine._resolve_live_client() is app_headless.betfair_service.get_client()

    # ...e con override esplicito (test/injection) vince l'override.
    sentinella = object()
    engine.betfair_client = sentinella
    try:
        assert engine._resolve_live_client() is sentinella
    finally:
        engine.betfair_client = None  # fixture module-scoped: ripristina


@pytest.mark.guardrail
def test_cablato_reconciliation_engine_risolto_dal_runtime(app_headless):
    """CABLATO (PR «iniezione ReconciliationEngine», #437 punto 4). Le
    submission ambigue non finiscono piu' nel _NullReconciliationEngine:
    l'engine risolve LIVE il motore del runtime a ogni enqueue — start()
    lo RICOSTRUISCE a ogni avvio, quindi niente riferimenti congelati al
    build (stesso principio della risoluzione lazy del betfair_client)."""
    engine = app_headless.trading_engine

    # Nessuna iniezione congelata al build: l'attributo resta il default.
    assert type(engine.reconciliation_engine).__name__ == (
        "_NullReconciliationEngine"
    ), (
        "REGRESSIONE: motore congelato al build — dopo il primo start() "
        "(che ricostruisce il motore del runtime) andrebbe stantio."
    )

    risolutore = getattr(engine, "_resolve_reconciliation_engine", None)
    assert callable(risolutore), (
        "REGRESSIONE: manca TradingEngine._resolve_reconciliation_engine — "
        "le submission ambigue tornano nel vuoto del segnaposto."
    )

    # Risoluzione live: identita' col motore CORRENTE del runtime...
    originale = app_headless.runtime.reconciliation_engine
    assert engine._resolve_reconciliation_engine() is originale

    # ...che espone davvero l'intake enqueue (non un motore muto).
    assert callable(getattr(originale, "enqueue", None)), (
        "REGRESSIONE: il ReconciliationEngine reale non espone enqueue — "
        "l'iniezione consegnerebbe le ambiguita' a un metodo inesistente."
    )

    # ...e segue la ricostruzione del motore (start() lo rifa' ogni volta).
    class _Rimpiazzo:
        @staticmethod
        def enqueue(**_kw):
            return None

    rimpiazzo = _Rimpiazzo()
    app_headless.runtime.reconciliation_engine = rimpiazzo
    try:
        assert engine._resolve_reconciliation_engine() is rimpiazzo, (
            "REGRESSIONE: risoluzione congelata — dopo un rebuild l'enqueue "
            "andrebbe al motore vecchio."
        )
    finally:
        # Fixture module-scoped: ripristina il motore reale per i test dopo.
        app_headless.runtime.reconciliation_engine = originale


@pytest.mark.guardrail
def test_gap_safety_layer_non_passato_al_cashout_executor(app_headless):
    """GAP (piano: PR «SafetyLayer al CashoutExecutor»). Il parametro esiste
    gia' nel costruttore; headless_main non lo passa, quindi
    validate_cashout_request() non gira mai."""
    assert app_headless.cashout_executor.safety_layer is None, (
        "GAP CHIUSO: il CashoutExecutor riceve un safety_layer. "
        "Promuovi in CABLATO."
    )


@pytest.mark.guardrail
def test_gap_gui_senza_osservabilita_ne_cashout(app_gui):
    """GAP (piano: PR «GUI parity», decisione owner 2026-08-23: parity
    completa). La GUI monta il runtime ma NON watchdog/alert ne' la catena
    cashout: le tab Watchdog e Alert salvano su servizi che nel processo GUI
    non esistono, e un REQ_EXECUTE_CASHOUT cade su un bus senza ascoltatori."""
    mancanti = [
        nome
        for nome in ("watchdog_service", "alerts_manager", "incidents_manager",
                     "order_router", "cashout_executor", "cashout_request_bridge",
                     "cashout_residual_handler")
        if not hasattr(app_gui, nome)
    ]
    assert len(mancanti) == 7, (
        f"GAP IN CHIUSURA: la GUI ora costruisce {7 - len(mancanti)} dei 7 "
        "componenti attesi dalla parity. Completa la parity e promuovi la "
        f"voce in CABLATO (mancano ancora: {mancanti})."
    )
    presenti = _sottoscrittori(app_gui.bus)
    assert presenti.get("REQ_EXECUTE_CASHOUT", 0) == 0, (
        "GAP CHIUSO: la GUI ha un sottoscrittore per REQ_EXECUTE_CASHOUT. "
        "Promuovi in CABLATO."
    )


@pytest.mark.guardrail
def test_gap_ttl_unmatched_senza_interruttore(app_headless):
    """GAP (piano: PR «interruttore TTL unmatched»). Il poller legge
    `config.direct_unmatched_ttl_enabled` via getattr con default False, ma la
    chiave non esiste in RoserpinaConfig ne' nel setting service: la funzione
    e' dormiente senza nessun modo di accenderla."""
    assert not hasattr(app_headless.runtime.config, "direct_unmatched_ttl_enabled"), (
        "GAP CHIUSO: la chiave direct_unmatched_ttl_enabled esiste nella "
        "config. Promuovi in CABLATO asserendo default sicuro (False) e "
        "lettura dal registro impostazioni."
    )

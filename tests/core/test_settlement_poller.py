"""Test hard del poller settlement del RuntimeController (PR
`runtime_settlement_wiring`) e della facade fail-closed
``BetfairService.list_cleared_orders``.

Il poller e' il ciclo di chiusura reale: listClearedOrders(SETTLED,
bet_ids del bot, righe per-bet) => PnLEngine.apply_cleared_market_settlement =>
RUNTIME_CLOSE_POSITION => contratto settlement => realized PnL => daily-loss.
Su questo percorso un errore mascherato da "nessun settlement" e' una perdita
invisibile al kill-switch giornaliero: ogni ramo va provato fail-closed.

Copertura:
- PASS: emissione canonica (parametri fetch inclusi), accettazione dal
  contratto VERO, applicazione al daily-loss fino al breach+emergency stop.
- BLOCK: default OFF, SIM => zero rete, runtime non ACTIVE => zero rete,
  fetch fallito => round abortito e ritentabile, righe malformate scartate
  (mai inventare un profit), dedupe in-memory e durevole (riavvio), stato
  recovery illeggibile => emissione sospesa ma ritentabile, config
  malformata => poller disabilitato, lifecycle thread start/stop pulito.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from core.runtime_controller import RuntimeController
from core.system_state import RoserpinaConfig, RuntimeMode
from services.betfair_service import BetfairService


class _Bus:
    def __init__(self):
        self.events = []

    def subscribe(self, *_args):
        return None

    def publish(self, topic, payload=None):
        self.events.append((topic, dict(payload or {})))


class _DB:
    """Fake db con cycle recovery state e identita' bot configurabili.

    ``recovery``: mappa settlement_key -> stato (dict) oppure Exception da
    sollevare. ``fail_reads=True`` fa fallire OGNI lettura (db illeggibile).
    ``bot_orders``: righe restituite da ``get_bot_active_orders`` (l'allowlist
    d'identita' I1 del bot); ``fail_identity=True`` la rende illeggibile.
    """

    def __init__(self, recovery=None, bot_orders=None):
        self.recovery = dict(recovery or {})
        self.fail_reads = False
        self.fail_identity = False
        self.bot_orders = list(
            bot_orders
            if bot_orders is not None
            else [{"bet_id": "101", "market_id": "1.100", "event_name": "E1"}]
        )

    def get_bot_active_orders(self):
        if self.fail_identity:
            raise RuntimeError("IDENTITY_UNAVAILABLE")
        return list(self.bot_orders)

    def get_cycle_recovery_state(self, key):
        if self.fail_reads:
            raise RuntimeError("DB_UNAVAILABLE")
        item = self.recovery.get(key)
        if isinstance(item, Exception):
            raise item
        return item

    def _execute(self, *_args, **_kwargs):
        return None

    def _fetch_one(self, *_args, **_kwargs):
        return None

    def _fetch_all(self, *_args, **_kwargs):
        return []


class _Settings:
    def __init__(self, extra=None):
        self.extra = dict(extra or {})

    def load_roserpina_config(self):
        cfg = RoserpinaConfig()
        cfg.anti_duplication_enabled = False
        cfg.max_daily_loss = 100.0
        cfg.max_drawdown_hard_stop_pct = 20.0
        cfg.max_open_exposure = 250.0
        return cfg

    def load_market_data_config(self):
        return {"market_data_mode": "poll", "enabled": False, "market_ids": []}

    def get_all_settings(self):
        return dict(self.extra)


class _SettingsRaising(_Settings):
    def get_all_settings(self):
        raise RuntimeError("SETTINGS_UNAVAILABLE")


class _BetfairService:
    def __init__(self, results=None):
        self.calls = []
        self._results = list(results or [])

    def set_simulation_mode(self, _enabled):
        return None

    def list_cleared_orders(self, **kwargs):
        self.calls.append(dict(kwargs))
        if not self._results:
            return []
        item = self._results.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    def get_account_funds(self):
        return {"available": 100.0}

    def status(self):
        return {"connected": True}

    def get_live_client(self):
        return object()

    def get_market_book_snapshot(self, _market_id):
        return None

    def ensure_stream_session_ready(self):
        return True

    def disconnect(self):
        return None

    def cancel_all_orders(self):
        return {"ok": True}


class _Telegram:
    def start(self):
        return {"started": True}

    def stop(self):
        return None

    def status(self):
        return {"connected": True}


_POLL_CFG = {"enabled": True, "poll_sec": 60.0, "lookback_hours": 24.0}


def _controller(*, results=None, recovery=None, settings=None, db=None,
                bot_orders=None, first_sweep_done=True):
    rc = RuntimeController(
        bus=_Bus(),
        db=db if db is not None else _DB(recovery=recovery, bot_orders=bot_orders),
        settings_service=settings or _Settings(),
        betfair_service=_BetfairService(results=results),
        telegram_service=_Telegram(),
    )
    rc.mode = RuntimeMode.ACTIVE
    rc.simulation_mode = False
    rc._settlement_poll_cfg = dict(_POLL_CFG)
    # Default dei test: finestra mobile gia' attiva (il primo sweep completo
    # ha un test dedicato). Lo sweep e' per-generazione: si stampa quella
    # corrente.
    if first_sweep_done:
        rc._settlement_sweep_done_generation = rc._settlement_poll_generation
    return rc


def _closes(rc):
    return [p for t, p in rc.bus.events if t == "RUNTIME_CLOSE_POSITION"]


_ROW = {
    "betId": "101",
    "marketId": "1.100",
    "profit": -40.0,
    "commission": 0.0,
    "settledDate": "2026-08-24T09:00:00Z",
}


# ===========================================================================
# PASS — emissione canonica e wiring fino al daily-loss
# ===========================================================================

@pytest.mark.integration
def test_poll_emette_settlement_canonico_con_parametri_fetch_corretti():
    rc = _controller(results=[[dict(_ROW)]])

    before = datetime.now(timezone.utc)
    rc._poll_cleared_settlements()

    call = rc.betfair_service.calls[0]
    assert call["bet_status"] == "SETTLED"
    assert call["bet_ids"] == ["101"]  # identita' PER-BET del bot (I1)
    assert "group_by" not in call  # righe per-bet, niente rollup
    settled_after = datetime.fromisoformat(call["settled_after"])
    atteso = before - timedelta(hours=24)
    assert abs((settled_after - atteso).total_seconds()) < 300

    closes = _closes(rc)
    assert len(closes) == 1
    payload = closes[0]
    assert payload["event_key"] == "cleared:1.100:101"
    assert payload["gross_pnl"] == pytest.approx(-40.0)
    assert payload["commission_amount"] == pytest.approx(0.0)
    assert payload["net_pnl"] == pytest.approx(-40.0)
    assert payload["settlement_source"] == "betfair_cleared_orders"
    assert payload["settlement_kind"] == "realized_settlement"
    assert payload["settlement_basis"] == "market_net_realized"

    contract = RuntimeController._extract_settlement_contract(payload)
    assert contract["settlement_validation"] == "accepted"
    assert contract["settlement_acceptance"] == "ACCEPT_REALIZED_SETTLEMENT"


@pytest.mark.integration
def test_primo_giro_sweep_completo_poi_finestra_mobile():
    """Rilievo GPT-5.6 su #440: la sola finestra mobile perde i settlement
    maturati durante un downtime piu' lungo del lookback. Primo giro dopo lo
    start: NESSUN settled_after (orizzonte Betfair ~90gg, il dedupe filtra il
    gia' consegnato); giri successivi: finestra mobile. Il flag si arma SOLO
    a fetch riuscito: un primo giro fallito lascia il sweep completo in coda."""
    rc = _controller(
        results=[RuntimeError("NOT_AUTHENTICATED"), [], []],
        first_sweep_done=False,
    )

    rc._poll_cleared_settlements()  # fallisce => flag NON armato
    rc._poll_cleared_settlements()  # sweep completo
    rc._poll_cleared_settlements()  # finestra mobile

    calls = rc.betfair_service.calls
    assert len(calls) == 3
    assert calls[0]["settled_after"] is None
    assert calls[1]["settled_after"] is None
    assert calls[2]["settled_after"] is not None


@pytest.mark.integration
def test_sweep_non_si_arma_se_una_emissione_fallisce():
    """Rilievo Fable su #440: se nel primo giro una riga fallisce in
    emissione (transitorio, es. recovery state illeggibile), lo sweep NON si
    dichiara fatto — il retry avviene ancora a orizzonte completo, mai con
    la finestra mobile che perderebbe i settlement piu' vecchi del lookback."""
    db = _DB()
    db.fail_reads = True  # emissione sospesa: transitorio db
    rc = _controller(
        results=[[dict(_ROW)], [dict(_ROW)], []],
        db=db,
        first_sweep_done=False,
    )

    rc._poll_cleared_settlements()
    assert _closes(rc) == []
    assert rc.betfair_service.calls[0]["settled_after"] is None

    db.fail_reads = False
    rc._poll_cleared_settlements()  # ancora sweep completo, ora emette
    assert len(_closes(rc)) == 1
    assert rc.betfair_service.calls[1]["settled_after"] is None

    rc._poll_cleared_settlements()  # solo ora finestra mobile
    assert rc.betfair_service.calls[2]["settled_after"] is not None


@pytest.mark.integration
def test_sweep_legato_alla_generazione_del_thread():
    """Rilievo GPT-5.6 su #440: un thread VECCHIO sopravvissuto al join non
    puo' bruciare lo sweep della generazione nuova — stampa solo la SUA."""
    rc = _controller(results=[[], []], first_sweep_done=False)
    vecchia_generazione = rc._settlement_poll_generation

    rc._settlement_poll_generation += 1  # e' partita una nuova generazione

    # Il giro del thread VECCHIO completa e stampa la SUA generazione.
    rc._poll_cleared_settlements(generation=vecchia_generazione)
    assert rc._settlement_sweep_done_generation == vecchia_generazione

    # La generazione NUOVA non risulta swept: il suo primo giro e' completo.
    rc._poll_cleared_settlements()
    assert rc.betfair_service.calls[1]["settled_after"] is None


@pytest.mark.integration
def test_identita_per_bet_esclude_le_bet_manuali_anche_sullo_stesso_mercato():
    """Rilievi Fable+GPT-5.6 su #440 (fail-open): un profitto esterno
    dell'account maschererebbe le perdite del bot nel daily-loss. Identita'
    PER-BET: si interrogano solo i betId del bot, e una bet manuale — anche
    sullo STESSO mercato del bot — non viene mai emessa (difesa client-side
    oltre al filtro server-side)."""
    riga_bot = dict(_ROW)
    riga_manuale_stesso_mercato = {
        "betId": "999", "marketId": "1.100", "profit": 500.0, "commission": 22.5,
    }
    rc = _controller(
        results=[[riga_bot, riga_manuale_stesso_mercato]],
        bot_orders=[{"bet_id": "101", "market_id": "1.100", "event_name": "E1"}],
    )

    rc._poll_cleared_settlements()

    assert rc.betfair_service.calls[0]["bet_ids"] == ["101"]
    closes = _closes(rc)
    assert len(closes) == 1
    assert closes[0]["event_key"] == "cleared:1.100:101"  # il +500 manuale NON entra
    assert closes[0]["net_pnl"] == pytest.approx(-40.0)


@pytest.mark.integration
def test_bet_ledger_sim_escluse_dal_poll_live():
    """Gli id del ledger SIM (SIMBET-*) non esistono sull'exchange: esclusi
    dal poll LIVE (rilievo Grok su #440 sul mixing SIM+LIVE)."""
    rc = _controller(
        results=[[dict(_ROW)]],
        bot_orders=[
            {"bet_id": "SIMBET-abc123", "market_id": "1.500", "event_name": "S"},
            {"bet_id": "101", "market_id": "1.100", "event_name": "E1"},
        ],
    )
    rc._poll_cleared_settlements()
    assert rc.betfair_service.calls[0]["bet_ids"] == ["101"]  # solo id numerici


@pytest.mark.integration
def test_nessuna_bet_bot_nessuna_chiamata():
    for orders in ([], [{"bet_id": "SIMBET-x", "market_id": "1.9", "event_name": "S"}]):
        rc = _controller(results=[[dict(_ROW)]], bot_orders=orders)
        rc._poll_cleared_settlements()
        assert rc.betfair_service.calls == []
        assert _closes(rc) == []


@pytest.mark.integration
def test_identita_bot_illeggibile_round_abortito():
    """Identita' illeggibile => NIENTE ingestione account-wide di ripiego."""
    db = _DB()
    db.fail_identity = True
    rc = _controller(results=[[dict(_ROW)]], db=db)

    rc._poll_cleared_settlements()
    assert rc.betfair_service.calls == []
    assert _closes(rc) == []

    db.fail_identity = False
    rc._poll_cleared_settlements()
    assert len(_closes(rc)) == 1  # ripristinato il db, si recupera


@pytest.mark.integration
def test_round_serializzati_dal_lock():
    """Rilievo GPT-5.6 su #440: due giri concorrenti (thread vecchio oltre il
    join + thread nuovo) non devono mai sovrapporsi. Col round-lock tenuto,
    un giro concorrente esce subito senza chiamare nulla."""
    rc = _controller(results=[[dict(_ROW)]])
    assert rc._settlement_poll_round_lock.acquire(blocking=False)
    try:
        rc._poll_cleared_settlements()
        assert rc.betfair_service.calls == []
        assert _closes(rc) == []
    finally:
        rc._settlement_poll_round_lock.release()

    rc._poll_cleared_settlements()
    assert len(_closes(rc)) == 1


@pytest.mark.integration
def test_loop_usa_il_suo_stop_event_non_l_attributo():
    """Rilievo GPT-5.6 su #440: il loop del thread legge l'Event PASSATO come
    argomento — se un restart rimpiazza l'attributo, il thread vecchio resta
    fermabile dal SUO event e non adotta quello nuovo."""
    import threading as _threading

    rc = _controller(results=[[]])
    rc.mode = RuntimeMode.STOPPED  # ogni giro e' no-op: zero rete
    evento_locale = _threading.Event()

    thread = _threading.Thread(
        target=rc._settlement_poll_loop, args=(evento_locale,), daemon=True
    )
    thread.start()
    # Rimpiazzo l'attributo (il vecchio bug lo faceva adottare al loop).
    rc._settlement_poll_stop = _threading.Event()

    evento_locale.set()
    thread.join(timeout=5.0)
    assert not thread.is_alive()


@pytest.mark.integration
def test_duplicato_gia_realizzato_nel_motore_marcato_senza_doppio_publish():
    """Guardia idempotente del motore vista dal poller (rilievo Fugu su
    #440): mercato gia' realizzato (es. processo con set in-memory perso ma
    motore vivo) => il poller marca visto e NON pubblica una seconda volta."""
    rc = _controller(results=[[dict(_ROW)]])
    rc.pnl_engine.apply_cleared_market_settlement(
        market_id="1.100", gross_pnl=-40.0, settlement_ref="101",
    )
    pubblicati_prima = len(_closes(rc))

    rc._poll_cleared_settlements()

    assert len(_closes(rc)) == pubblicati_prima  # nessun secondo publish
    assert "cleared:1.100:101" in rc._settlement_emitted_keys


@pytest.mark.integration
def test_settlement_emesso_arriva_al_daily_loss_fino_al_breach():
    """Il filo intero: perdita reale => realized applicato => monitor
    daily-loss => al superamento del limite (100) hard-stop di emergenza.
    Prima perdita -40: contata, nessun breach. Seconda -70: cumulata -110
    => breach => emergency stop DUREVOLE."""
    rc = _controller(results=[[dict(_ROW)]])

    rc._poll_cleared_settlements()
    payload_1 = _closes(rc)[0]
    rc._on_close_position(payload_1)

    assert rc.risk_desk.realized_pnl == pytest.approx(-40.0)
    stato = dict(rc._daily_loss_monitor_state)
    assert stato["daily_loss_amount"] == pytest.approx(40.0)
    assert bool(stato["breached"]) is False
    assert rc._emergency_stopped is False

    payload_2 = rc.pnl_engine.apply_cleared_market_settlement(
        market_id="1.200", gross_pnl=-70.0,
    )
    rc._on_close_position(payload_2)

    assert rc.risk_desk.realized_pnl == pytest.approx(-110.0)
    stato = dict(rc._daily_loss_monitor_state)
    assert stato["daily_loss_amount"] == pytest.approx(110.0)
    assert bool(stato["breached"]) is True
    assert rc._emergency_stopped is True


# ===========================================================================
# BLOCK — fail-closed su ogni ramo
# ===========================================================================

class _SettingsLimite50(_Settings):
    def load_roserpina_config(self):
        cfg = super().load_roserpina_config()
        cfg.max_daily_loss = 50.0
        return cfg


@pytest.mark.integration
def test_gambe_dutching_winners_first_niente_kill_switch_transitorio():
    """Rilievi GPT-5.6/Fable su #440 (giri 3-5): con piu' gambe per mercato
    (dutching), processare i perdenti prima della vincente sfonderebbe
    TRANSITORIAMENTE il daily-loss e scatterebbe l'emergency stop su un
    mercato che netta positivo. Il mercato e' un gruppo ATOMICO winners-first
    ANCHE con settledDate divergenti tra le gambe (settlement parziali,
    millisecondi diversi). Counterfactual REALE: limite giornaliero 50 e
    gamba perdente -80 — processata per prima avrebbe sfondato il limite da
    sola e attivato l'emergency stop; winners-first il cumulato non scende
    mai sotto il market-net +20 (realized finale 19.10)."""
    righe_loser_first_date_divergenti = [
        {"betId": "201", "marketId": "1.600", "profit": -80.0,
         "settledDate": "2026-08-24T09:00:00.001Z"},  # la perdente ha data ANTERIORE
        {"betId": "202", "marketId": "1.600", "profit": 100.0,
         "settledDate": "2026-08-24T09:00:00.500Z"},
    ]
    rc = _controller(
        results=[righe_loser_first_date_divergenti],
        settings=_SettingsLimite50(),
        bot_orders=[
            {"bet_id": "201", "market_id": "1.600", "event_name": "E"},
            {"bet_id": "202", "market_id": "1.600", "event_name": "E"},
        ],
    )

    rc._poll_cleared_settlements()

    closes = _closes(rc)
    assert [p["event_key"] for p in closes] == [
        "cleared:1.600:202",  # la vincente PRIMA, anche con data posteriore
        "cleared:1.600:201",
    ]

    # Consegna nello stesso ordine di emissione al consumer VERO: la -80
    # da sola sfonderebbe il limite 50 (emergency stop); winners-first il
    # cumulato tocca al minimo il market-net finale e il kill-switch non
    # scatta mai.
    for payload in closes:
        rc._on_close_position(payload)
        assert rc._emergency_stopped is False

    assert rc.risk_desk.realized_pnl == pytest.approx(19.10)
    stato = dict(rc._daily_loss_monitor_state)
    assert bool(stato["breached"]) is False


@pytest.mark.integration
def test_settlement_parziali_lontani_replay_cronologico_il_dip_storico_scatta():
    """Rilievo Fable su #440 (sesto giro, fondato): gambe dello stesso
    mercato settlate a ORE di distanza sono eventi economici DISTINTI — il
    gruppo-mercato atomico avrebbe mascherato un dip storico reale
    intrecciato con un altro mercato. Coi cluster temporali il replay e'
    cronologico: -80 (09:00, M1) poi -60 (10:00, M2) => cumulato -140 =>
    il breach del limite 100 SCATTA (emergency stop), esattamente come
    sarebbe accaduto in tempo reale; il +100 delle 11:00 arriva dopo, a
    kill-switch gia' correttamente attivato."""
    righe = [
        {"betId": "501", "marketId": "1.700", "profit": -80.0,
         "settledDate": "2026-08-24T09:00:00Z"},
        {"betId": "502", "marketId": "1.700", "profit": 100.0,
         "settledDate": "2026-08-24T11:00:00Z"},  # stesso mercato, 2h dopo
        {"betId": "503", "marketId": "1.800", "profit": -60.0,
         "settledDate": "2026-08-24T10:00:00Z"},  # altro mercato, in mezzo
    ]
    rc = _controller(
        results=[righe],
        bot_orders=[
            {"bet_id": "501", "market_id": "1.700", "event_name": "E"},
            {"bet_id": "502", "market_id": "1.700", "event_name": "E"},
            {"bet_id": "503", "market_id": "1.800", "event_name": "E"},
        ],
    )

    rc._poll_cleared_settlements()

    closes = _closes(rc)
    assert [p["event_key"] for p in closes] == [
        "cleared:1.700:501",  # -80 @ 09:00
        "cleared:1.800:503",  # -60 @ 10:00 (il dip -140 si rigioca fedelmente)
        "cleared:1.700:502",  # +100 @ 11:00
    ]

    rc._on_close_position(closes[0])
    assert rc._emergency_stopped is False
    rc._on_close_position(closes[1])
    assert rc._emergency_stopped is True  # breach storico REALE: scatta
    rc._on_close_position(closes[2])  # il vincente tardivo non lo annulla
    assert rc._emergency_stopped is True


@pytest.mark.integration
def test_perdita_senza_data_in_testa_fail_closed():
    """Rilievi GPT-5.6/Fugu su #440: una PERDITA non databile non va in coda
    (un profitto datato la attenuerebbe: fail-open) — sign-aware: nette
    perdite in TESTA, profitti non databili in coda."""
    righe = [
        {"betId": "602", "marketId": "1.050", "profit": 90.0,
         "settledDate": "2026-08-24T09:00:00Z"},
        {"betId": "601", "marketId": "1.900", "profit": -60.0},  # senza data
    ]
    rc = _controller(
        results=[righe],
        bot_orders=[
            {"bet_id": "601", "market_id": "1.900", "event_name": "E"},
            {"bet_id": "602", "market_id": "1.050", "event_name": "E"},
        ],
    )

    rc._poll_cleared_settlements()

    assert [p["event_key"] for p in _closes(rc)] == [
        "cleared:1.900:601",  # la perdita NON databile per prima (worst case)
        "cleared:1.050:602",
    ]


@pytest.mark.integration
def test_formati_iso_misti_ordinati_cronologicamente_non_lessicograficamente():
    """Rilievo Fugu su #440 (sesto giro, fondato): il confronto tra stringhe
    ISO con formati misti non e' cronologico ('.' < 'Z' avrebbe messo
    ...00.001Z PRIMA di ...00Z). Le date sono parse a datetime reale: la
    vincente delle 09:00:00.001Z (stesso istante di settlement della
    perdente, formato con frazione) resta nello stesso cluster winners-first,
    e il mercato delle 08:59:58+00:00 (offset esplicito, 2s prima oltre il
    gap) viene lavorato per primo."""
    righe = [
        {"betId": "701", "marketId": "1.700", "profit": -30.0,
         "settledDate": "2026-08-24T09:00:00Z"},
        {"betId": "702", "marketId": "1.700", "profit": 50.0,
         "settledDate": "2026-08-24T09:00:00.001Z"},
        {"betId": "703", "marketId": "1.800", "profit": -10.0,
         "settledDate": "2026-08-24T08:59:57+00:00"},
    ]
    rc = _controller(
        results=[righe],
        bot_orders=[
            {"bet_id": "701", "market_id": "1.700", "event_name": "E"},
            {"bet_id": "702", "market_id": "1.700", "event_name": "E"},
            {"bet_id": "703", "market_id": "1.800", "event_name": "E"},
        ],
    )

    rc._poll_cleared_settlements()

    assert [p["event_key"] for p in _closes(rc)] == [
        "cleared:1.800:703",  # 08:59:57, cronologicamente primo
        "cleared:1.700:702",  # cluster 09:00: vincente prima
        "cleared:1.700:701",
    ]


@pytest.mark.integration
def test_gruppo_senza_data_in_coda_non_maschera_un_dip_storico():
    """Rilievi GPT-5.6/Fable su #440 (quinto giro): una riga vincente SENZA
    settledDate non deve essere anteposta a una perdita storica datata —
    anteporla attenuerebbe il dip reale (fail-open). I gruppi non databili
    vanno in coda."""
    righe = [
        {"betId": "402", "marketId": "1.050", "profit": 90.0},  # senza data
        {"betId": "401", "marketId": "1.900", "profit": -60.0,
         "settledDate": "2026-08-24T09:00:00Z"},
    ]
    rc = _controller(
        results=[righe],
        bot_orders=[
            {"bet_id": "401", "market_id": "1.900", "event_name": "E"},
            {"bet_id": "402", "market_id": "1.050", "event_name": "E"},
        ],
    )

    rc._poll_cleared_settlements()

    assert [p["event_key"] for p in _closes(rc)] == [
        "cleared:1.900:401",  # la perdita datata PRIMA
        "cleared:1.050:402",  # il profitto non databile in coda
    ]


@pytest.mark.integration
def test_ordine_cronologico_tra_mercati_un_dip_storico_non_si_nasconde():
    """Rilievi Fable+Fugu su #440 (quarto giro): riordinare TRA mercati
    potrebbe anteporre un vincente successivo a un perdente precedente,
    nascondendo un dip storico reale (fail-open sul kill-switch). Tra
    mercati l'ordine e' CRONOLOGICO (settledDate): il perdente delle 09:00
    si lavora PRIMA del vincente delle 10:00, anche se l'API li restituisce
    invertiti e anche se il marketId del vincente e' lessicograficamente
    minore."""
    righe_api_invertite = [
        {"betId": "302", "marketId": "1.050", "profit": 90.0,
         "settledDate": "2026-08-24T10:00:00Z"},
        {"betId": "301", "marketId": "1.900", "profit": -60.0,
         "settledDate": "2026-08-24T09:00:00Z"},
    ]
    rc = _controller(
        results=[righe_api_invertite],
        bot_orders=[
            {"bet_id": "301", "market_id": "1.900", "event_name": "E"},
            {"bet_id": "302", "market_id": "1.050", "event_name": "E"},
        ],
    )

    rc._poll_cleared_settlements()

    assert [p["event_key"] for p in _closes(rc)] == [
        "cleared:1.900:301",  # il perdente delle 09:00 PRIMA (verita' storica)
        "cleared:1.050:302",
    ]


@pytest.mark.integration
def test_catena_transitiva_di_settlement_parziali_non_si_fonde_in_un_cluster():
    """R7 su #440 (GPT-5.6+Fable+Fugu convergenti, fondato): il gap 2s era
    misurato tra gambe CONSECUTIVE, quindi una catena con drift transitivo
    (09:00:00 -> 09:00:01.9 -> 09:00:03.8) si fondeva in un unico cluster
    winners-first di durata illimitata: la vincente arrivata 3.8s dopo
    avrebbe mascherato il dip storico reale -140 (fail-open sul
    kill-switch). La finestra del cluster si misura dall'ANCORA (prima
    gamba): la vincente oltre i 2s dall'ancora e' un evento DISTINTO,
    rigiocato dopo, e il breach del limite 100 scatta."""
    righe = [
        {"betId": "801", "marketId": "1.910", "profit": -80.0,
         "settledDate": "2026-08-24T09:00:00.000Z"},
        {"betId": "802", "marketId": "1.910", "profit": -60.0,
         "settledDate": "2026-08-24T09:00:01.900Z"},  # entro 2s dall'ancora
        {"betId": "803", "marketId": "1.910", "profit": 150.0,
         "settledDate": "2026-08-24T09:00:03.800Z"},  # 1.9s dal predecessore
    ]
    rc = _controller(
        results=[righe],
        bot_orders=[
            {"bet_id": "801", "market_id": "1.910", "event_name": "E"},
            {"bet_id": "802", "market_id": "1.910", "event_name": "E"},
            {"bet_id": "803", "market_id": "1.910", "event_name": "E"},
        ],
    )

    rc._poll_cleared_settlements()

    closes = _closes(rc)
    assert [p["event_key"] for p in closes] == [
        "cleared:1.910:802",  # cluster 09:00:00..01.9 winners-first (-60 > -80)
        "cleared:1.910:801",
        "cleared:1.910:803",  # oltre la finestra dall'ancora: evento distinto
    ]

    rc._on_close_position(closes[0])
    assert rc._emergency_stopped is False  # -60: sotto il limite 100
    rc._on_close_position(closes[1])
    assert rc._emergency_stopped is True  # -140: il dip storico SCATTA
    rc._on_close_position(closes[2])
    assert rc._emergency_stopped is True  # la vincente tardiva non lo annulla


@pytest.mark.integration
def test_gambe_non_databili_worst_case_per_gamba_niente_netting():
    """R7 su #440 (GPT-5.6, fondato): il gruppo non databile era piazzato in
    base al NETTO — un mercato senza date che netta positivo (+100/-80)
    finiva in coda winners-first e il suo -80, nella cronologia reale,
    poteva aver sfondato il limite insieme a una perdita datata (-40):
    breach storico occultato (fail-open). Senza date la simultaneita' delle
    gambe non e' provabile: worst-case PER GAMBA, senza netting — perdite
    non databili in TESTA, profitti non databili in coda."""
    righe = [
        {"betId": "812", "marketId": "1.920", "profit": 100.0},  # senza data
        {"betId": "811", "marketId": "1.920", "profit": -80.0},  # senza data
        {"betId": "813", "marketId": "1.930", "profit": -40.0,
         "settledDate": "2026-08-24T10:00:00Z"},
    ]
    rc = _controller(
        results=[righe],
        bot_orders=[
            {"bet_id": "811", "market_id": "1.920", "event_name": "E"},
            {"bet_id": "812", "market_id": "1.920", "event_name": "E"},
            {"bet_id": "813", "market_id": "1.930", "event_name": "E"},
        ],
    )

    rc._poll_cleared_settlements()

    closes = _closes(rc)
    assert [p["event_key"] for p in closes] == [
        "cleared:1.920:811",  # perdita non databile in testa (worst case)
        "cleared:1.930:813",  # poi la cronologia datata
        "cleared:1.920:812",  # profitto non databile in coda
    ]

    rc._on_close_position(closes[0])
    assert rc._emergency_stopped is False  # -80: sotto il limite 100
    rc._on_close_position(closes[1])
    assert rc._emergency_stopped is True  # -120: il breach non e' occultato


@pytest.mark.integration
def test_gamba_dutching_non_databile_stop_conservativo_documentato():
    """R7 su #440 (Fable, richiesta di documentazione/test esplicito): una
    gamba di dutching PERDENTE senza settledDate accanto alla vincente
    DATATA dello stesso mercato va in testa worst-case: l'emergency stop
    puo' scattare anche se il mercato netta positivo (+20). E' la
    degradazione CONSERVATIVA scelta e documentata in
    ops/settlement_poller.md: senza data la simultaneita' delle gambe non
    e' provabile, e uno stop spurio (fail-closed) e' accettato contro il
    rischio di un breach mascherato (fail-open). Con le date presenti —
    il caso reale Betfair — vale il cluster winners-first."""
    righe = [
        {"betId": "822", "marketId": "1.940", "profit": 100.0,
         "settledDate": "2026-08-24T09:00:00Z"},
        {"betId": "821", "marketId": "1.940", "profit": -80.0},  # senza data
    ]
    rc = _controller(
        results=[righe],
        settings=_SettingsLimite50(),
        bot_orders=[
            {"bet_id": "821", "market_id": "1.940", "event_name": "E"},
            {"bet_id": "822", "market_id": "1.940", "event_name": "E"},
        ],
    )

    rc._poll_cleared_settlements()

    closes = _closes(rc)
    assert [p["event_key"] for p in closes] == [
        "cleared:1.940:821",  # la perdita non databile in testa
        "cleared:1.940:822",
    ]

    rc._on_close_position(closes[0])
    assert rc._emergency_stopped is True  # stop conservativo: -80 > limite 50


@pytest.mark.integration
def test_poll_in_simulation_mode_nessuna_chiamata():
    rc = _controller(results=[[dict(_ROW)]])
    rc.simulation_mode = True
    rc._poll_cleared_settlements()
    assert rc.betfair_service.calls == []
    assert _closes(rc) == []


@pytest.mark.integration
def test_poll_runtime_non_active_nessuna_chiamata():
    rc = _controller(results=[[dict(_ROW)]])
    rc.mode = RuntimeMode.STOPPED
    rc._poll_cleared_settlements()
    assert rc.betfair_service.calls == []
    assert _closes(rc) == []


@pytest.mark.integration
def test_poll_fetch_fallito_round_abortito_poi_recupera():
    rc = _controller(
        results=[RuntimeError("NOT_AUTHENTICATED"), [dict(_ROW)]]
    )

    rc._poll_cleared_settlements()
    assert _closes(rc) == []  # round abortito, nessun "nessun settlement" finto

    rc._poll_cleared_settlements()
    assert len(_closes(rc)) == 1  # ritentato e recuperato


@pytest.mark.integration
def test_poll_righe_malformate_scartate_mai_inventare_profit():
    righe = [
        {"betId": "102", "marketId": "", "profit": 10.0},
        {"betId": "103", "marketId": "1.3", "profit": "12"},
        {"betId": "104", "marketId": "1.4", "profit": float("nan")},
        {"betId": "106", "marketId": "1.6"},
        {"betId": "107", "marketId": "1.7", "profit": True},
        {"marketId": "1.8", "profit": 5.0},
        "spazzatura",
        {"betId": "108", "marketId": "1.5", "profit": 25.0},
        # incoerenza ledger: il bot ha la bet 109 su 1.9, la riga dice 1.2
        {"betId": "109", "marketId": "1.2", "profit": 7.0},
    ]
    rc = _controller(
        results=[righe],
        bot_orders=[
            {"bet_id": b, "market_id": m, "event_name": "E"}
            for b, m in (
                ("102", "1.2"), ("103", "1.3"), ("104", "1.4"),
                ("106", "1.6"), ("107", "1.7"), ("108", "1.5"),
                ("109", "1.9"),
            )
        ],
    )

    rc._poll_cleared_settlements()

    closes = _closes(rc)
    assert len(closes) == 1
    assert closes[0]["event_key"] == "cleared:1.5:108"
    assert closes[0]["commission_amount"] == pytest.approx(1.125)
    assert closes[0]["net_pnl"] == pytest.approx(23.875)


@pytest.mark.integration
def test_poll_dedupe_in_memory_stesso_mercato_due_giri():
    rc = _controller(results=[[dict(_ROW)], [dict(_ROW)]])
    rc._poll_cleared_settlements()
    rc._poll_cleared_settlements()
    assert len(_closes(rc)) == 1


@pytest.mark.integration
def test_poll_dedupe_durevole_dopo_riavvio_nessuna_riemissione():
    """Riavvio simulato: controller NUOVO (set in-memory vuoto) ma il
    checkpoint durevole del consumer esiste => la stessa settlement_key
    non viene ri-emessa (il PnL non si applica due volte)."""
    rc = _controller(
        results=[[dict(_ROW)]],
        recovery={"cleared:1.100:101": {"exists": True, "processed": True}},
    )
    rc._poll_cleared_settlements()
    assert _closes(rc) == []
    assert "cleared:1.100:101" in rc._settlement_emitted_keys


@pytest.mark.integration
def test_poll_recovery_state_illeggibile_sospende_senza_marcare():
    """DB illeggibile: nessuna emissione (rischio doppio conteggio) ma la
    chiave NON viene marcata vista => quando il db risponde si recupera."""
    db = _DB()
    db.fail_reads = True
    rc = _controller(results=[[dict(_ROW)], [dict(_ROW)]], db=db)

    rc._poll_cleared_settlements()
    assert _closes(rc) == []
    assert "cleared:1.100:101" not in rc._settlement_emitted_keys

    db.fail_reads = False
    rc._poll_cleared_settlements()
    assert len(_closes(rc)) == 1


@pytest.mark.integration
def test_poller_disabilitato_di_default_nessun_thread():
    rc = _controller(settings=_Settings())  # nessuna chiave settlement.*
    rc._start_settlement_poller()
    assert rc._settlement_poll_thread is None
    assert rc.betfair_service.calls == []


@pytest.mark.integration
def test_poller_lifecycle_start_stop_pulito():
    rc = _controller(settings=_Settings({"settlement.poll_enabled": True}))
    rc.mode = RuntimeMode.STOPPED  # il giro nel thread esce subito, zero rete

    rc._start_settlement_poller()
    thread = rc._settlement_poll_thread
    assert thread is not None and thread.is_alive()

    rc._stop_settlement_poller()
    assert rc._settlement_poll_thread is None
    assert not thread.is_alive()
    assert rc.betfair_service.calls == []


@pytest.mark.integration
def test_poller_non_parte_in_simulation_mode_anche_se_abilitato():
    rc = _controller(settings=_Settings({"settlement.poll_enabled": True}))
    rc.simulation_mode = True
    rc._start_settlement_poller()
    assert rc._settlement_poll_thread is None


@pytest.mark.integration
def test_config_malformata_o_illeggibile_poller_disabilitato():
    rc = _controller(settings=_SettingsRaising())
    cfg = rc._load_settlement_poll_config()
    assert cfg["enabled"] is False

    rc2 = _controller(settings=_Settings({
        "settlement.poll_enabled": True,
        "settlement.poll_sec": "abc",
        "settlement.lookback_hours": 24,
    }))
    cfg2 = rc2._load_settlement_poll_config()
    assert cfg2["enabled"] is False  # parse fallito => default fail-closed


@pytest.mark.integration
def test_config_clamp_poll_sec_e_lookback():
    rc = _controller(settings=_Settings({
        "settlement.poll_enabled": True,
        "settlement.poll_sec": 1,
        "settlement.lookback_hours": 999,
    }))
    cfg = rc._load_settlement_poll_config()
    assert cfg["enabled"] is True
    assert cfg["poll_sec"] == pytest.approx(5.0)
    assert cfg["lookback_hours"] == pytest.approx(168.0)


# ===========================================================================
# Facade BetfairService.list_cleared_orders — fail-closed
# ===========================================================================

class _ClearedClient:
    def __init__(self, result=None, error=None):
        self.calls = []
        self._result = result if result is not None else []
        self._error = error

    def list_cleared_orders(self, **kwargs):
        self.calls.append(dict(kwargs))
        if self._error is not None:
            raise self._error
        return self._result


def _service(*, simulation=False, session_invalid=False, client=None):
    svc = BetfairService(object())
    svc.simulation_mode = simulation
    svc._session_invalid = session_invalid
    svc._session_invalid_reason = "SESSION_EXPIRED" if session_invalid else ""
    svc.client = client
    return svc


@pytest.mark.unit
def test_facade_sim_mode_solleva_niente_lista_vuota_finta():
    client = _ClearedClient()
    svc = _service(simulation=True, client=client)
    with pytest.raises(RuntimeError, match="CLEARED_ORDERS_UNAVAILABLE_IN_SIMULATION"):
        svc.list_cleared_orders()
    assert client.calls == []


@pytest.mark.unit
def test_facade_sessione_invalida_solleva():
    svc = _service(session_invalid=True, client=_ClearedClient())
    with pytest.raises(RuntimeError, match="LIVE_BLOCKED_SESSION_INVALID"):
        svc.list_cleared_orders()


@pytest.mark.unit
def test_facade_client_assente_solleva():
    svc = _service(client=None)
    with pytest.raises(RuntimeError, match="NO_LIVE_CLIENT"):
        svc.list_cleared_orders()


@pytest.mark.unit
def test_facade_inoltra_parametri_e_filtra_righe_non_dict():
    client = _ClearedClient(result=[{"marketId": "1.1"}, "x", None, {"marketId": "1.2"}])
    svc = _service(client=client)

    rows = svc.list_cleared_orders(
        bet_status="SETTLED",
        bet_ids=["11", "22"],
        settled_after="2026-08-23T00:00:00+00:00",
    )

    assert client.calls == [{
        "bet_status": "SETTLED",
        "market_ids": None,
        "bet_ids": ["11", "22"],
        "settled_after": "2026-08-23T00:00:00+00:00",
        "settled_before": None,
        "group_by": None,
    }]
    assert rows == [{"marketId": "1.1"}, {"marketId": "1.2"}]


@pytest.mark.unit
def test_facade_session_expired_instrada_recovery_e_ripropaga():
    client = _ClearedClient(error=RuntimeError("ANGX-0003 SESSION_EXPIRED"))
    svc = _service(client=client)
    chiamate = {}
    svc.handle_session_expiry = lambda reason="": chiamate.setdefault("reason", reason)

    with pytest.raises(RuntimeError, match="SESSION_EXPIRED"):
        svc.list_cleared_orders()

    assert "SESSION_EXPIRED" in chiamate["reason"]

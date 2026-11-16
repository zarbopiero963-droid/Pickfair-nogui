"""PR-03b-1 — quattro rilievi verificati da Claude B (preflight #628 c.6919799700) e Claude D (#628 M98).

Mandato owner PF-OWNER-PR03B1-FOUR-FINDINGS-GO-20261116. Base: `main` @ 5daa6a51 (dopo la #631).

- P1 (F-11 / AUD-GA-008): installazione che ha gia' operato ma senza stato del drawdown persistito. Su main il picco
  partiva dalla prima cassa letta (picco storico 100, cassa 80, limite 10 %: picco 80, ingresso ammesso). Ora la
  baseline non e' verificata e nessun ingresso parte finche' l'owner non conferma il picco con la procedura esistente.
- P2 (F-12b): il rifiuto `max_recovery_tables_reached` tratteneva la chiave anti-duplicato fino al TTL.
- Limite % non valido: nessun test fermava il ritorno al fail-open (mutante sopravvissuto alla suite, #628 M98).
- Auto-next oltre il drawdown %: verifica del blocco a livello runtime. NON e' end-to-end: l'auto-next di questi test
  non arriva al SimulationBroker (si osserva la pubblicazione di CMD_QUICK_BET); l'E2E auto-next resta NOT_VERIFIED.

Ogni test RED fallisce su main 5daa6a51 e passa sull'head; i test di guardrail passano gia' su main e fermano i mutanti.
Offline: SimulationBroker, DB temporanei, nessuna credenziale.
"""
from __future__ import annotations

import json
import random
import threading
import time
from decimal import Decimal

import pytest

from core.drawdown_hard_stop import (BASELINE_MISSING_HISTORY_UNREADABLE, BASELINE_MISSING_WITH_HISTORY,
                                     HISTORY_SETTINGS_KEYS, REASON_BASELINE_UNVERIFIED, REASON_PCT_INVALID,
                                     STATE_KEY, DrawdownHardStop)
from core.system_state import RuntimeMode
from database import Database
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti
from tests.d1.test_d1_drawdown_hard_stop import (_CONFIRM, _ORDINE_MANUALE, _SpiaBroker, _lettura, _ordini,
                                                 _start_reale)
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal
from tests.integration.test_runtime_controller_cycle_executor import _canonical_close_payload, _make_controller

pytestmark = [pytest.mark.safety]


# ============================================================================== oracolo indipendente (nessun core/)
def _oracolo(*, storico: bool, picco_storico, cassa, limite_pct, picco_dichiarato=None) -> str:
    """Modello di riferimento in Decimal, scritto qui e senza import dal codice di produzione.

    - installazione con storico e nessuno stato: la prima cassa non prova il massimo => BASELINE finche' l'owner non
      dichiara il picco; dopo la conferma il picco e' max(dichiarato, cassa) (mai inventato, mai sotto la cassa);
    - installazione nuova: il picco e' la prima cassa verificata;
    - perdita % = (picco - cassa) / picco * 100 >= limite => HARD_STOP, altrimenti AMMESSO."""
    cassa = Decimal(str(cassa))
    if storico and picco_dichiarato is None:
        return "BASELINE"
    picco = cassa if not storico else max(Decimal(str(picco_dichiarato)), cassa)
    perdita_pct = (picco - cassa) / picco * Decimal(100)
    return "HARD_STOP" if perdita_pct >= Decimal(str(limite_pct)) else "AMMESSO"


# ============================================================================== P1 — storico senza stato persistito
def _db_con_storico(tmp_path, fonte: str, nome: str = "storico.db") -> Database:
    """DB reale di un'installazione che ha gia' operato (sintetico, nessun dato reale) e SENZA stato del drawdown."""
    db = Database(str(tmp_path / nome))
    if fonte == "ordini":
        adesso = time.time()
        db._execute("INSERT INTO orders (customer_ref, correlation_id, status, finalized, created_at, updated_at) "
                    "VALUES (?, ?, 'COMPLETED', 1, ?, ?)", ("ref-storico", "corr-storico", adesso, adesso))
    elif fonte in HISTORY_SETTINGS_KEYS:
        db.save_settings({fonte: json.dumps({"at_ts": 1.0})})
    elif fonte != "nessuna":
        raise AssertionError(fonte)
    assert STATE_KEY not in (db.get_settings() or {})
    return db


_FONTI = ["ordini", *HISTORY_SETTINGS_KEYS]


@pytest.mark.parametrize("fonte", _FONTI)
def test_p1_history_without_persisted_state_is_not_a_verified_baseline(tmp_path, fonte):
    """RED su main: senza record il picco partiva dalla prima cassa, baseline verificata, nessun blocco."""
    stop = DrawdownHardStop(_db_con_storico(tmp_path, fonte))
    stop.restore()
    assert stop.baseline_verified is False
    assert stop.baseline_missing == BASELINE_MISSING_WITH_HISTORY
    assert stop.baseline_block_reason() == REASON_BASELINE_UNVERIFIED


def test_p1_fresh_installation_keeps_the_first_verified_cash_as_peak(tmp_path):
    """Controllo positivo: nessuno storico => nessun blocco, il primo picco e' la prima cassa verificata."""
    stop = DrawdownHardStop(_db_con_storico(tmp_path, "nessuna"))
    stop.restore()
    stop.observe(80.0)
    assert stop.baseline_verified is True and stop.baseline_missing == ""
    assert stop.peak == Decimal("80") and stop.reason(None, 80.0, pct_cap_raw=10.0) == ""


def test_p1_unreadable_history_blocks_like_a_present_history(tmp_path, monkeypatch):
    """Storico non leggibile: non si puo' dimostrare un'installazione nuova => stesso blocco (fail-closed)."""
    db = _db_con_storico(tmp_path, "nessuna")
    reale = db._execute

    def guasto(sql, *a, **k):
        if "FROM orders" in str(sql) or "FROM order_saga" in str(sql):
            raise OSError("tabella illeggibile")
        return reale(sql, *a, **k)

    monkeypatch.setattr(db, "_execute", guasto)
    stop = DrawdownHardStop(db)
    stop.restore()
    assert stop.baseline_verified is False
    assert stop.baseline_missing == BASELINE_MISSING_HISTORY_UNREADABLE


def test_p1_db_without_queries_cannot_prove_a_fresh_installation(tmp_path):
    """Un DB che non permette di interrogare lo storico non dimostra l'assenza di storico => blocco."""
    class _SoloImpostazioni:
        def __init__(self):
            self.settings = {}

        def get_settings(self):
            return dict(self.settings)

        def save_settings(self, values):
            self.settings.update(values)

    stop = DrawdownHardStop(_SoloImpostazioni())
    stop.restore()
    assert stop.baseline_verified is False and stop.baseline_missing == BASELINE_MISSING_HISTORY_UNREADABLE


def test_p1_detection_only_reads_and_never_writes_the_record(tmp_path):
    """Il rilevamento non scrive nulla: nessun record inventato, l'evidenza (assenza di stato) resta intatta."""
    db = _db_con_storico(tmp_path, "ordini")
    prima = dict(db.get_settings() or {})
    for _ in range(3):
        DrawdownHardStop(db).restore()
    assert dict(db.get_settings() or {}) == prima


def test_p1_block_survives_crash_and_restarts_once_a_peak_was_observed(tmp_path):
    """Crash dopo la prima lettura (record scritto con il picco 80 e baseline non verificata): ogni riavvio successivo,
    anche ripetuto, resta bloccato; il picco 80 salvato non diventa mai una baseline verificata da solo."""
    db = _db_con_storico(tmp_path, "ordini")
    stop = DrawdownHardStop(db)
    stop.restore()
    stop.observe(80.0)
    record = json.loads(db.get_settings()[STATE_KEY])
    assert record["baseline_verified"] is False and record["peak"] == "80.0"
    for _ in range(2):                                    # doppio riavvio idempotente
        riletto = DrawdownHardStop(db)
        riletto.restore()
        assert riletto.baseline_block_reason() == REASON_BASELINE_UNVERIFIED


@pytest.mark.parametrize("ambiente", ["SIM", "LIVE"])
def test_p1_block_holds_for_every_environment_and_account(tmp_path, ambiente):
    """Il blocco non dipende da SIM/LIVE ne' dal conto: la provenienza del record successivo resta quella dichiarata."""
    db = _db_con_storico(tmp_path, "ordini")
    stop = DrawdownHardStop(db)
    stop.set_scope({"env": ambiente, "account": "conto-a"})
    stop.restore()
    stop.observe(80.0)
    assert stop.baseline_block_reason() == REASON_BASELINE_UNVERIFIED
    record = json.loads(db.get_settings()[STATE_KEY])
    assert record["scope"] == {"env": ambiente, "account": "conto-a"} and record["baseline_verified"] is False


def test_p1_only_the_owner_confirmation_with_the_declared_peak_lifts_it_and_the_loss_stays(tmp_path):
    """Caso owner: picco storico 100, cassa 80, limite 10 %. La conferma owner con picco dichiarato 100 rende la baseline
    verificata e la perdita del 20 % e' un hard-stop: nessun ingresso. Conferma incompleta => resta bloccato."""
    db = _db_con_storico(tmp_path, "ordini")
    stop = DrawdownHardStop(db)
    stop.restore()
    stop.observe(80.0)
    assert stop.owner_confirm_baseline(**dict(_CONFIRM, confirm_again=False), cash_equity=80.0,
                                       declared_peak=100.0) is False
    assert stop.baseline_block_reason() == REASON_BASELINE_UNVERIFIED
    assert stop.owner_confirm_baseline(**_CONFIRM, cash_equity=80.0, declared_peak=100.0) is True
    assert stop.baseline_verified is True and stop.baseline_missing == "" and stop.peak == Decimal("100")
    assert stop.reason(None, 80.0, pct_cap_raw=10.0) == "drawdown_hard_stop_active"
    riletto = DrawdownHardStop(db)                         # riavvio dopo la conferma: superamento e picco restano
    riletto.restore()
    assert riletto.breached and riletto.peak == Decimal("100") and riletto.baseline_verified


def test_p1_crash_between_owner_confirmation_and_save_keeps_the_block(tmp_path, monkeypatch):
    """Crash fra conferma e salvataggio (salvataggio fallito): la conferma non vale, al riavvio resta bloccato."""
    db = _db_con_storico(tmp_path, "ordini")
    stop = DrawdownHardStop(db)
    stop.restore()
    stop.observe(80.0)
    monkeypatch.setattr(db, "save_settings", lambda values: (_ for _ in ()).throw(OSError("crash")))
    assert stop.owner_confirm_baseline(**_CONFIRM, cash_equity=80.0, declared_peak=100.0) is False
    assert stop.baseline_verified is False
    monkeypatch.undo()
    riletto = DrawdownHardStop(db)
    riletto.restore()
    assert riletto.baseline_block_reason() == REASON_BASELINE_UNVERIFIED and riletto.peak == Decimal("80")


@pytest.mark.parametrize("prima", ["lettura", "rilevamento"])
def test_p1_concurrent_reading_and_restore_never_persist_a_verified_baseline(tmp_path, monkeypatch, prima):
    """Concorrenza deterministica (barriere, nessuno sleep): una lettura del conto contro il ripristino che rileva lo
    storico. In entrambi gli ordini il record finale ha la baseline NON verificata e il picco non sotto la cassa."""
    import core.drawdown_hard_stop as modulo
    db = _db_con_storico(tmp_path, "ordini")
    stop = DrawdownHardStop(db)
    dentro, via = threading.Event(), threading.Event()
    reale = modulo._trading_history

    def rilevamento_lento(*a, **k):
        dentro.set()
        assert via.wait(10)
        return reale(*a, **k)

    errori = []

    def corri(fn):
        try:
            fn()
        except Exception as exc:                          # pragma: no cover - riportato sotto
            errori.append(exc)

    if prima == "rilevamento":
        monkeypatch.setattr(modulo, "_trading_history", rilevamento_lento)
        a = threading.Thread(target=corri, args=(stop.restore,))
        a.start()
        assert dentro.wait(10)
        b = threading.Thread(target=corri, args=(lambda: stop.observe(80.0),))
        b.start()
        via.set()
        a.join(10)
        b.join(10)
    else:
        # Sequenza reale del runtime: il costruttore rilegge sempre lo stato (`restore`, runtime_controller.py:354-355 e
        # `_hard_stop`), poi arriva una lettura del conto, poi la rilettura dello START (`_reload_drawdown_state`).
        stop.restore(reconcile=False)
        stop.observe(80.0)
        stop.restore()
    assert not errori and stop.baseline_block_reason() == REASON_BASELINE_UNVERIFIED
    riletto = DrawdownHardStop(db)
    riletto.restore()
    assert riletto.baseline_verified is False and riletto.peak == Decimal("80")


@pytest.mark.parametrize("seme", [631, 6311])
def test_p1_random_scenarios_match_the_independent_oracle(tmp_path, seme):
    """Proprieta' con seme fisso: storico si/no, picco storico, cassa, limite, conferma owner con picco dichiarato.
    Verdetto del modulo == verdetto dell'oracolo in Decimal in ogni scenario."""
    rnd = random.Random(seme)
    for i in range(40):
        storico = rnd.random() < 0.6
        picco_storico = rnd.choice([100, 120, 87.5, 250])
        cassa = round(rnd.uniform(40, 130), 2)
        limite = rnd.choice([5, 10, 12.5, 20])
        conferma = storico and rnd.random() < 0.5
        dichiarato = picco_storico if conferma else None
        db = _db_con_storico(tmp_path, "ordini" if storico else "nessuna", nome=f"o{seme}_{i}.db")
        stop = DrawdownHardStop(db)
        stop.restore()
        stop.observe(cassa)
        if conferma:
            assert stop.owner_confirm_baseline(**dict(_CONFIRM, operation_id=f"OP-{seme}-{i}"), cash_equity=cassa,
                                               declared_peak=dichiarato)
        motivo = stop.baseline_block_reason() or stop.reason(None, cassa, pct_cap_raw=limite)
        verdetto = {REASON_BASELINE_UNVERIFIED: "BASELINE", "drawdown_hard_stop_active": "HARD_STOP",
                    "": "AMMESSO"}[motivo]
        assert verdetto == _oracolo(storico=storico, picco_storico=picco_storico, cassa=cassa, limite_pct=limite,
                                    picco_dichiarato=dichiarato), (seme, i, storico, cassa, limite, dichiarato, motivo)


# ---------------------------------------------------------------- P1 end-to-end: app reale in SIM fino al broker
def _app_sim(tmp_path, monkeypatch, *, storico: bool, cassa: float = 80.0):
    from tests.risk_config_fixtures import legacy_test_config
    nome = "storico_e2e.db" if storico else "nuova_e2e.db"
    _db_con_storico(tmp_path, "ordini" if storico else "nessuna", nome=nome)
    app, rc = _start_reale(tmp_path, monkeypatch, cassa, nome=nome)
    monkeypatch.setattr(rc.settings_service, "load_roserpina_config",
                        lambda: legacy_test_config(liquidity_guard_enabled=False, max_drawdown_hard_stop_pct=10.0))
    assert rc.start(execution_mode="SIMULATION").get("started") is True
    return app, rc, _SpiaBroker(app.betfair_service.simulation_broker, rc)


def _chiudi(app):
    app.runtime.betfair_service.disconnect = lambda: None
    app.runtime.stop()


def _registra_rifiuti(rc):
    """Motivi dei rifiuti d'ammissione, registrati in modo sincrono (il bus dell'app puo' consegnare in un altro thread)."""
    rifiuti, reale = [], rc._reject_signal

    def registra(signal, reason):
        rifiuti.append(reason)
        return reale(signal, reason)

    rc._reject_signal = registra
    return rifiuti


def _invia_segnale(app, rc, rifiuti, **campi):
    """Segnale reale nel runtime; ogni comando d'ordine prodotto e' consegnato in modo sincrono al TradingEngine reale
    (come nei test E2E della #631), cosi' la spia vede ogni tentativo che arriva al SimulationBroker."""
    trattenuti, reale = [], rc.bus.publish
    rc.bus.publish = lambda t, p=None: trattenuti.append(p) if t == "CMD_QUICK_BET" else reale(t, p)
    try:
        rc._on_signal_received(_signal(stake=1.0, bet_type="BACK", **campi))
    finally:
        rc.bus.publish = reale
    for comando in trattenuti:
        app.trading_engine.submit_quick_bet(comando)
    return trattenuti


@pytest.mark.real_blackbox
def test_p1_e2e_installation_with_history_sends_nothing_to_the_broker(tmp_path, monkeypatch):
    """App reale (runtime, money management, motore, SimulationBroker) su un DB con storico e senza stato, cassa 80,
    limite 10 %. RED su main: il picco diventava 80 e l'ingresso arrivava al broker. Ora: 0 tentativi al broker,
    motivo esplicito; dopo la conferma owner del picco 100 l'hard-stop e' attivo e ancora 0 tentativi."""
    app, rc, spia = _app_sim(tmp_path, monkeypatch, storico=True)
    rifiuti = _registra_rifiuti(rc)
    try:
        assert _invia_segnale(app, rc, rifiuti) == []
        assert spia.tentativi == [] and rifiuti[-1] == REASON_BASELINE_UNVERIFIED, (spia.tentativi, rifiuti)
        esito = rc.owner_confirm_drawdown_baseline(**_CONFIRM, declared_peak=100.0)
        assert esito["confirmed"] is True, esito
        _invia_segnale(app, rc, rifiuti, market_id="1.235")
        assert spia.tentativi == [], spia.tentativi
        assert rc._hard_stop().breached and rifiuti[-1] in ("drawdown_hard_stop_active", "runtime_non_attivo:LOCKDOWN")
        assert _oracolo(storico=True, picco_storico=100, cassa=80, limite_pct=10, picco_dichiarato=100) == "HARD_STOP"
    finally:
        _chiudi(app)


@pytest.mark.real_blackbox
def test_p1_e2e_fresh_installation_reaches_the_broker(tmp_path, monkeypatch):
    """Controllo positivo della stessa catena: installazione nuova, cassa 80 => l'ingresso arriva al broker (1 tentativo,
    1 ordine accettato)."""
    app, rc, spia = _app_sim(tmp_path, monkeypatch, storico=False)
    rifiuti = _registra_rifiuti(rc)
    try:
        assert len(_invia_segnale(app, rc, rifiuti)) == 1, rifiuti
        assert len(spia.tentativi) == 1 and len(spia.ordini_accettati()) == 1, (spia.tentativi, rifiuti)
        assert _oracolo(storico=False, picco_storico=None, cassa=80, limite_pct=10) == "AMMESSO"
    finally:
        _chiudi(app)


# ============================================================================== P2 — chiave del rifiuto per tavoli
def _tavoli_recovery_occupati(c):
    c.rc.config.allow_recovery = True
    c.rc.config.max_recovery_tables = 0
    for t in c.rc.table_manager._tables.values():          # nessun tavolo libero
        t.status = "ACTIVE"
        t.current_event_key = f"occupato-{t.table_id}"


def _libera_tavoli(c):
    for t in c.rc.table_manager._tables.values():
        t.status = "FREE"
        t.current_event_key = None
    c.rc.config.allow_recovery = False


def test_p2_recovery_tables_rejection_releases_the_key_of_the_rejected_attempt(tmp_path):
    """RED su main: chiave `1.234:11:BACK:default` trattenuta fino al TTL dopo il rifiuto."""
    c = _catena(tmp_path)
    _lettura(c, 1000.0)
    _tavoli_recovery_occupati(c)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert any(r.startswith("max_recovery_tables_reached") for r in _rifiuti(c)), _rifiuti(c)
    assert c.rc.duplication_guard._active == {}
    assert _ordini(c) == 0


def test_p2_a_new_legitimate_signal_is_evaluated_normally_after_the_rejection(tmp_path):
    """Dopo il rifiuto e la liberazione di un tavolo, lo stesso segnale legittimo viene valutato e parte UNA volta."""
    c = _catena(tmp_path)
    _lettura(c, 1000.0)
    _tavoli_recovery_occupati(c)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    _libera_tavoli(c)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 1, _rifiuti(c)
    assert "duplicato_bloccato" not in _rifiuti(c)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))      # riconsegna dopo l'ordine: nessun duplicato
    assert _ordini(c) == 1 and _rifiuti(c)[-1] in ("duplicato_bloccato", "evento_gia_in_posizione")


def test_p2_release_never_touches_the_key_of_an_already_accepted_order(tmp_path):
    """Un ordine gia' ammesso tiene la sua chiave; il rifiuto di un ALTRO segnale per tavoli occupati non la rilascia e
    una riconsegna del primo resta bloccata."""
    c = _catena(tmp_path)
    _lettura(c, 1000.0)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 1
    chiavi = set(c.rc.duplication_guard._active)
    assert len(chiavi) == 1
    c.rc.config.allow_recovery = True
    c.rc.config.max_recovery_tables = 0
    for t in c.rc.table_manager._tables.values():
        if t.status == "FREE":
            t.status = "ACTIVE"
            t.current_event_key = f"occupato-{t.table_id}"
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK", market_id="1.999", selection_id=77))
    assert any(r.startswith("max_recovery_tables_reached") for r in _rifiuti(c)), _rifiuti(c)
    assert set(c.rc.duplication_guard._active) == chiavi
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 1


def test_p2_concurrent_deliveries_while_tables_are_busy_send_nothing_and_leave_no_key(tmp_path):
    """Concorrenza deterministica: quattro consegne dello stesso segnale partono insieme (barriera) mentre i tavoli di
    recupero sono occupati. Nessun ordine, nessuna chiave trattenuta; liberati i tavoli, un solo ordine."""
    c = _catena(tmp_path)
    _lettura(c, 1000.0)
    _tavoli_recovery_occupati(c)
    via = threading.Barrier(4)
    errori = []

    def consegna():
        try:
            via.wait(10)
            c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
        except Exception as exc:                          # pragma: no cover
            errori.append(exc)

    fili = [threading.Thread(target=consegna) for _ in range(4)]
    for f in fili:
        f.start()
    for f in fili:
        f.join(30)
    assert not errori and _ordini(c) == 0
    assert c.rc.duplication_guard._active == {}
    _libera_tavoli(c)
    for _ in range(3):
        c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 1, _rifiuti(c)


# ============================================================================== limite % non valido (guardrail)
_LIMITI_NON_VALIDI = [float("nan"), float("inf"), float("-inf"), -5.0, 0.0, 100.01, 150.0, "abc", "nan", True, False]


@pytest.mark.parametrize("limite", _LIMITI_NON_VALIDI, ids=repr)
def test_invalid_percent_limit_denies_every_entry_path(tmp_path, limite):
    """Un limite % configurato ma non valido NON disattiva la protezione: segnale, manuale/Dutching e auto-next negati
    con il motivo esplicito. Passa su main; ferma il mutante «limite non valido = nessun limite» (#628 M98)."""
    c = _catena(tmp_path, max_drawdown_hard_stop_pct=limite)
    _lettura(c, 100.0)
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    assert _ordini(c) == 0 and _rifiuti(c)[-1] == REASON_PCT_INVALID, _rifiuti(c)
    assert c.rc.absolute_cap_reason_for_orders([dict(_ORDINE_MANUALE)]) == REASON_PCT_INVALID


@pytest.mark.parametrize("limite", _LIMITI_NON_VALIDI, ids=repr)
def test_invalid_percent_limit_denies_auto_next(limite):
    """Auto-next con limite % non valido: nessun comando d'ordine, motivo esplicito (gate `_drawdown_hard_stop_reason`
    del percorso auto-next; `_risk_allows_auto_trade` non controlla il drawdown)."""
    _rc, esito, comandi = _auto_next(sync=150.0, pct=limite)
    assert comandi == [] and esito["auto_trade_status"] != "AUTO_TRADE_SUBMITTED", esito
    assert REASON_PCT_INVALID in json.dumps(esito), esito


@pytest.mark.parametrize("limite", _LIMITI_NON_VALIDI, ids=repr)
def test_invalid_percent_limit_is_rejected_by_the_hard_stop_itself(tmp_path, limite):
    stop = DrawdownHardStop(_db_con_storico(tmp_path, "nessuna"))
    stop.restore()
    stop.observe(100.0)
    assert stop.reason(None, 100.0, pct_cap_raw=limite) == REASON_PCT_INVALID


@pytest.mark.parametrize("cassa,atteso", [(90.0, "drawdown_hard_stop_active"),      # 10 % esatto: scatta
                                          (90.01, ""), (100.0, "")])
def test_valid_percent_limit_threshold_is_inclusive(tmp_path, cassa, atteso):
    stop = DrawdownHardStop(_db_con_storico(tmp_path, "nessuna"))
    stop.restore()
    stop.observe(100.0)
    assert stop.reason(None, cassa, pct_cap_raw=10.0) == atteso
    perdita_pct = (Decimal(100) - Decimal(str(cassa))) / Decimal(100) * Decimal(100)   # oracolo in Decimal, picco 100
    assert (atteso != "") == (perdita_pct >= Decimal(10))


# ============================================================================== auto-next oltre il drawdown %
_PROSSIMO = {"market_id": "1.2", "selection_id": 2, "price": 2.0, "stake": 2.0, "bet_type": "BACK"}


def _auto_next(*, sync: float, letture=(150.0,), pct=10.0, forza_attivo=False):
    """Auto-next a livello runtime: si osserva la pubblicazione di CMD_QUICK_BET, NON il broker (non end-to-end)."""
    rc, bus = _make_controller(responses=[{"available": sync, "exposure": 0.0}] * 3)
    rc.mode = RuntimeMode.ACTIVE
    rc.config.max_drawdown_hard_stop_pct = pct
    for valore in letture:
        rc._apply_funds_reading({"available": valore, "exposure": 0.0, "ok": True}, source="test")
    if forza_attivo:
        rc.mode = RuntimeMode.ACTIVE
    rc._on_close_position(_canonical_close_payload(
        auto_trade_enabled=True, cycle_executor_enabled=True,
        mm_context={"cycle_active": True, "cycle_id": "cyc-pr03b1",
                    "table": {"table_id": 1, "loss_amount": 0.0, "in_recovery": False}, "next_signal": dict(_PROSSIMO)}))
    esito = [e for e in bus.events if e[0] == "AUTO_TRADE_MM_RESULT"][0][1]
    return rc, esito, [e for e in bus.events if e[0] == "CMD_QUICK_BET"]


def test_auto_next_without_drawdown_publishes_the_order():
    """Controllo positivo: senza drawdown l'auto-next pubblica il comando d'ordine."""
    _rc, esito, comandi = _auto_next(sync=150.0)
    assert esito["auto_trade_status"] == "AUTO_TRADE_SUBMITTED" and len(comandi) == 1, esito


def test_auto_next_over_the_percent_drawdown_publishes_nothing_and_locks_down():
    """Settlement che porta la cassa a 130 con picco 150 (13,3 % >= 10 %): hard-stop, LOCKDOWN, 0 comandi."""
    rc, esito, comandi = _auto_next(sync=130.0)
    assert comandi == [] and esito["auto_trade_status"] != "AUTO_TRADE_SUBMITTED", esito
    assert rc._hard_stop().breached and rc.mode is RuntimeMode.LOCKDOWN


def test_auto_next_drawdown_gate_holds_even_if_the_runtime_were_active():
    """Il gate di drawdown dell'auto-next vale da solo: con il runtime forzato ACTIVE dopo il superamento, l'auto-next
    resta negato con il motivo del drawdown e nessun comando parte."""
    rc, esito, comandi = _auto_next(sync=130.0, letture=(150.0, 130.0), forza_attivo=True)
    assert comandi == [], esito
    assert "drawdown_hard_stop_active" in json.dumps(esito), esito


def test_auto_next_command_is_denied_again_at_the_final_send_check():
    """Autorizzazione finale: il ricontrollo al punto d'invio nega un comando auto-next dopo il superamento."""
    rc, _esito, _comandi = _auto_next(sync=130.0, letture=(150.0, 130.0), forza_attivo=True)
    assert rc.drawdown_entry_block_reason({"market_id": "1.2", "selection_id": 2, "price": 2.0, "size": 2.0,
                                          "side": "BACK"}) != ""

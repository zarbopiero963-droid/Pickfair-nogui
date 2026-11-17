"""PR-04a — perdita giornaliera per identita' finanziaria (PF-OWNER-PR04A-SCOPE-MM029-GO-20261116).

Contratto owner PF-OWNER-PR04A-LIVE-GLOBAL-SIM-LOCAL-20261116 (opzione 1, LIVE globale / SIM locale):
- SIM oltre il limite giornaliero: bloccati solo i nuovi ingressi di quell'ambito SIM; nessuna emergenza, nessuna
  cancellazione di ordini LIVE, LIVE non disattivato, nessun LOCKDOWN globale;
- LIVE oltre il limite: Emergency Stop globale come oggi (SIM e LIVE bloccati); passare a SIM non lo scioglie;
- ES manuale sempre globale; dati incerti => fail-closed per l'ambito, escalation globale se non isolabile;
- perdita e stati per identita' completa provider + ambiente + conto + valuta: nessuna compensazione fra ambiti.

Banco: catena reale del runtime (RuntimeController, money management, TradingEngine, SimulationBroker, SQLite reale).
Il LIVE e' EMULATO nello stesso processo (`set_simulation_mode(False)`, lettura del conto `simulated=False` con uno
username); la prova LIVE e' a livello di decisione del runtime, MAI un'esecuzione LIVE end-to-end. Oracolo
indipendente in Decimal per ambito (nessun import dalla logica del core).
"""
from __future__ import annotations

import json
import threading
import types
from decimal import Decimal

import pytest

from core import daily_loss_store, financial_scope
from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode
from tests.acceptance.test_issue437_pr27 import _rifiuti
from tests.d1.test_d1_drawdown_hard_stop import _catena_su, _ordini
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal
from tests.integration.test_runtime_controller_cycle_executor import _canonical_close_payload

pytestmark = [pytest.mark.integration, pytest.mark.safety]

COMM = Decimal("0.045")
CONTO_A = "conto.a@example.invalid"
CONTO_B = "conto.b@example.invalid"


class Oracolo:
    """Perdita realizzata del giorno per ambito (ambiente, conto), in Decimal; blocco se perdita >= limite."""

    def __init__(self, limite):
        self.limite, self.pnl = Decimal(str(limite)), {}

    def settle(self, ambito, lordo):
        lordo = Decimal(str(lordo))
        netto = lordo - (lordo * COMM if lordo > 0 else Decimal(0))
        self.pnl[ambito] = self.pnl.get(ambito, Decimal(0)) + netto

    def blocca(self, ambito):
        return max(Decimal(0), -self.pnl.get(ambito, Decimal(0))) >= self.limite


def _banco(tmp_path, nome="pr04a.db", **cfg):
    c = _catena_su(tmp_path, nome, **cfg)
    c.rc.risk_desk.equity_peak = 0.0
    c.rc.risk_desk.bankroll_start = 0.0
    return c


def _ambiente(c, env, conto=CONTO_A, cassa=1000.0):
    c.rc.set_simulation_mode(env == "SIM")
    c.rc.betfair_service.client = types.SimpleNamespace(username=conto)
    fondi = {"available": cassa, "exposure": 0.0, "ok": True, "simulated": env == "SIM"}
    if env == "SIM":
        fondi["total"] = cassa                       # come il broker simulato (PFC-008: totale coerente)
    c.rc.betfair_service.get_account_funds = lambda: dict(fondi)
    assert c.rc._apply_funds_reading(dict(fondi), source="pr04a")["applied"]


_N = iter(range(100_000))


def _settle(c, lordo, **identita):
    n = next(_N)
    comm = round(lordo * 0.045, 6) if lordo > 0 else 0.0
    c.rc.mode = RuntimeMode.ACTIVE if c.rc.mode is not RuntimeMode.LOCKDOWN else c.rc.mode
    payload = dict(pnl=lordo - comm, net_pnl=lordo - comm, gross_pnl=lordo, commission_amount=comm,
                   event_key=f"evt-pr04a-{n}", batch_id=f"b-pr04a-{n}", correlation_id=f"corr-pr04a-{n}")
    payload.update(identita)
    prima = float(c.rc.risk_desk.realized_pnl)
    c.rc._on_close_position(_canonical_close_payload(**payload))
    assert abs(float(c.rc.risk_desk.realized_pnl) - prima - (lordo - comm)) < 0.01, "settlement non applicato"


def _ingresso(c, market_id):
    prima = _ordini(c)
    c.rc._on_signal_received(_signal(stake=2.0, bet_type="BACK", market_id=market_id))
    return _ordini(c) - prima, (_rifiuti(c) or [None])[-1]


def _sim():
    return financial_scope.runtime_scope(simulated=True, live_account="")


def _live(c, conto=CONTO_A):
    c.rc.betfair_service.client = types.SimpleNamespace(username=conto)
    return c.rc._financial_scope_for(False)


# ============================================================================ identita' finanziaria (unita')
def test_scope_key_porta_provider_ambiente_conto_valuta():
    key = financial_scope.scope_key("betfair", "LIVE", "betfair:abc", "EUR")
    assert financial_scope.parts(key) == ("betfair", "LIVE", "betfair:abc", "EUR")
    assert financial_scope.is_verified(key) and not financial_scope.is_sim(key)
    assert financial_scope.escalates_globally(key)
    usdc = financial_scope.scope_key("betfair", "LIVE", "betfair:abc", "USDC")
    assert usdc != key, "EUR e USDC non devono mai condividere lo stesso ambito"
    assert financial_scope.is_sim(_sim()) and not financial_scope.escalates_globally(_sim())


@pytest.mark.parametrize("key", [
    financial_scope.scope_key("betfair", "LIVE", "", "EUR"),          # conto LIVE non identificato
    financial_scope.scope_key("betfair", "UNTAGGED", "x", "EUR"),     # ambiente sconosciuto
    financial_scope.scope_key("", "SIM", "sim", "EUR"),               # provider mancante
    financial_scope.scope_key("betfair", "SIM", "sim", ""),           # valuta mancante
    "", "solo-testo",
])
def test_ambito_non_verificabile_sale_all_emergenza_globale(key):
    assert not financial_scope.is_verified(key)
    assert financial_scope.escalates_globally(key)


# ============================================================================ test obbligatori owner (sez. 5)
def test_perdita_sim_oltre_soglia_blocca_la_sim_e_non_altera_il_live(tmp_path):
    c = _banco(tmp_path, max_daily_loss=30.0)
    c.rc.live_enabled = True
    cancellazioni = []
    c.rc.emergency_stop = lambda *a, **k: cancellazioni.append(("emergency_stop", a, k))
    _ambiente(c, "SIM")
    _settle(c, -40.0)
    _ambiente(c, "SIM")                              # rilettura del conto dopo il settlement
    nuovi_sim, motivo_sim = _ingresso(c, "1.801")
    assert nuovi_sim == 0 and motivo_sim == "daily_loss_breached:SIM", motivo_sim
    assert not c.rc._emergency_stopped and not c.rc._daily_loss_pending_stop
    assert cancellazioni == [], "una perdita SIM non deve mai produrre l'Emergency Stop (cancellazione LIVE)"
    assert c.rc.live_enabled is True, "il LIVE non va disattivato per una perdita SIM"
    assert c.rc.mode is not RuntimeMode.LOCKDOWN
    _ambiente(c, "LIVE")
    assert c.rc._daily_loss_monitor_state["breached"] is False
    assert c.rc._limits_block_reason() == "", "la perdita SIM non entra nei limiti del LIVE"


def test_perdita_live_oltre_soglia_emergency_stop_globale(tmp_path):
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "LIVE")
    _settle(c, -40.0)
    assert c.rc._emergency_stopped and str(c.rc._emergency_reason).startswith("DAILY_LOSS_BREACH")
    _ambiente(c, "SIM")
    nuovi, _ = _ingresso(c, "1.803")
    assert nuovi == 0, "Emergency Stop globale: anche la SIM resta bloccata (scelta owner esplicita)"
    assert c.rc._emergency_stopped, "passare a SIM non scioglie l'emergenza LIVE"


def test_sim_piu_50_live_meno_40_limite_30_blocca_il_live(tmp_path):
    o, c = Oracolo(30), _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM"); _settle(c, 50.0); o.settle("SIM", 50)
    _ambiente(c, "LIVE"); _settle(c, -40.0); o.settle("LIVE-A", -40)
    assert o.blocca("LIVE-A")
    assert c.rc._emergency_stopped, "il profitto SIM non deve compensare la perdita LIVE"
    nuovi, _ = _ingresso(c, "1.804")
    assert nuovi == 0


def test_live_meno_40_poi_sim_lecita_bloccata_da_es_globale_scelta_owner(tmp_path):
    """Esclusione esplicita owner (sez. 5): NON contaminazione del P&L. La perdita SIM resta zero; e' l'Emergency Stop
    globale a bloccare la SIM."""
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "LIVE"); _settle(c, -40.0)
    _ambiente(c, "SIM")
    assert c.rc._daily_loss_monitor_state["daily_loss_amount"] == 0.0, "nessuna perdita LIVE nel P&L della SIM"
    nuovi, motivo = _ingresso(c, "1.805")
    assert nuovi == 0 and c.rc._emergency_stopped, motivo


def test_emergency_stop_manuale_blocca_entrambi(tmp_path):
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    c.rc.emergency_stop(reason="MANUAL_OWNER")
    assert _ingresso(c, "1.806")[0] == 0
    _ambiente(c, "LIVE")
    assert _ingresso(c, "1.807")[0] == 0
    assert c.rc._emergency_stopped


def test_perdita_live_non_cancellata_cambiando_ambiente(tmp_path):
    """LIVE -20, SIM +50, LIVE -15 (limite 30): il passaggio dalla SIM non cancella la perdita LIVE dimostrata."""
    o, c = Oracolo(30), _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "LIVE"); _settle(c, -20.0); o.settle("LIVE", -20)
    _ambiente(c, "SIM"); _settle(c, 50.0); o.settle("SIM", 50)
    _ambiente(c, "LIVE"); _settle(c, -15.0); o.settle("LIVE", -15)
    assert o.blocca("LIVE") and c.rc._emergency_stopped
    stato_live = c.rc._monitor_daily_loss_breach(source="T", scope=_live(c))
    assert stato_live["daily_loss_amount"] == pytest.approx(35.0)


def test_conto_live_a_non_compensa_conto_live_b(tmp_path):
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "LIVE", conto=CONTO_A)
    _settle(c, 50.0)
    _ambiente(c, "LIVE", conto=CONTO_B)
    _settle(c, -40.0)
    assert c.rc._emergency_stopped, "il profitto del conto A non deve compensare la perdita del conto B"


# ============================================================================ attribuzione per identita' della scommessa
def _riserva(c, market_id):
    nuovi, motivo = _ingresso(c, market_id)
    assert nuovi == 1, motivo
    for tid in range(1, 64):
        t = c.rc.table_manager.get_table(tid)
        if t is not None and t.market_id == market_id and t.current_event_key:
            return t
    raise AssertionError("tavolo della riserva non trovato")


def _settle_tavolo(c, tavolo, lordo):
    _settle(c, lordo, table_id=tavolo.table_id, customer_ref=tavolo.meta["customer_ref"],
            event_key=tavolo.current_event_key, market_id=tavolo.market_id)


def test_riserva_porta_l_ambito_finanziario_della_scommessa(tmp_path):
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo = _riserva(c, "1.810")
    assert tavolo.meta["financial_scope"] == _sim()


def test_settlement_sim_tardivo_attribuito_alla_sim_non_compensa_il_live(tmp_path):
    """Scommessa SIM aperta, passaggio a LIVE, il settlement SIM (+50) arriva dopo: va alla SIM (identita' della
    scommessa); la perdita LIVE di 40 blocca."""
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo = _riserva(c, "1.811")
    _ambiente(c, "LIVE")
    _settle_tavolo(c, tavolo, 50.0)
    assert c.rc._scope_pnl.get(_sim(), 0.0) > 0.0, "il profitto della scommessa SIM va all'ambito SIM"
    assert c.rc._realized_for_scope(_live(c)) == pytest.approx(0.0)
    _settle(c, -40.0)
    assert c.rc._emergency_stopped


def test_perdita_sim_tardiva_dopo_il_passaggio_a_live_non_blocca_il_live(tmp_path):
    """Perdita di una scommessa SIM regolata mentre il runtime e' LIVE: e' della SIM (blocco locale), non del LIVE."""
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo = _riserva(c, "1.812")
    _ambiente(c, "LIVE")
    _settle_tavolo(c, tavolo, -40.0)
    assert not c.rc._emergency_stopped, "una perdita SIM non deve mai produrre l'Emergency Stop"
    assert c.rc._daily_loss_scopes[_sim()]["breached"] is True
    assert c.rc._daily_loss_monitor_state["breached"] is False


def test_profitto_senza_identita_non_accreditato_perdita_senza_identita_all_ambito_corrente(tmp_path):
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    _settle(c, 50.0)
    assert c.rc._scope_pnl == {} and c.rc._unscoped_profit > 0.0
    assert c.rc._realized_for_scope(_sim()) == pytest.approx(0.0), "profitto senza identita' mai accreditato"
    _settle(c, -10.0)
    assert c.rc._scope_pnl == {_sim(): pytest.approx(-10.0)}


def test_perdita_senza_conto_identificato_conta_per_ogni_ambito_e_sale_globale(tmp_path):
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "LIVE")
    c.rc.betfair_service.client = None                      # conto LIVE non identificabile
    c.rc.betfair_service.settings_service = None
    assert not financial_scope.is_verified(c.rc._current_financial_scope())
    _settle(c, -40.0)
    assert c.rc._emergency_stopped, "ambito non verificabile: escalation globale"
    assert c.rc._daily_loss_unattributed["intraday_realized_pnl"] == pytest.approx(-40.0)
    sim = c.rc._new_daily_loss_scope_state(_sim())
    assert sim["daily_loss_amount"] == pytest.approx(40.0) and sim["breached"] is True


# ============================================================================ sessione
def test_perdita_di_sessione_per_ambito(tmp_path):
    c = _banco(tmp_path, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    _ambiente(c, "SIM"); _settle(c, 50.0)
    _ambiente(c, "LIVE"); _settle(c, -40.0)
    assert c.rc._session_loss_block_reason().startswith("session_loss_breached")
    _ambiente(c, "SIM")
    assert c.rc._session_loss_block_reason() == "", "la perdita di sessione LIVE non e' della SIM"


def test_reset_del_ciclo_non_cancella_le_perdite_di_nessun_ambito(tmp_path):
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM"); _settle(c, -25.0)
    _ambiente(c, "LIVE"); _settle(c, -20.0)
    c.rc._reset_recovery_cycle_keeping_losses()
    assert c.rc._monitor_daily_loss_breach(source="T", scope=_sim())["daily_loss_amount"] == pytest.approx(25.0)
    assert c.rc._monitor_daily_loss_breach(source="T", scope=_live(c))["daily_loss_amount"] == pytest.approx(20.0)
    _settle(c, -15.0)
    assert c.rc._emergency_stopped, "LIVE 20 + 15 = 35 >= 30 dopo il reset"


# ============================================================================ riavvio e persistenza
def test_riavvio_conserva_gli_ambiti_e_la_sim_non_blocca_il_live(tmp_path):
    c = _banco(tmp_path, "r.db", max_daily_loss=30.0)
    _ambiente(c, "SIM"); _settle(c, -40.0)
    _ambiente(c, "LIVE"); _settle(c, -10.0)
    record = json.loads(c.db.get_settings()["daily_loss_state"])
    assert record["version"] == 2 and {_sim(), _live(c)} <= set(record["scopes"])     # anche gli ambiti a zero
    r = _banco(tmp_path, "r.db", max_daily_loss=30.0)
    _ambiente(r, "SIM")
    assert r.rc._daily_loss_monitor_state["breached"] is True
    _ambiente(r, "LIVE")
    stato = r.rc._monitor_daily_loss_breach(source="T")
    assert stato["breached"] is False and stato["daily_loss_amount"] == pytest.approx(10.0)
    _settle(r, -25.0)
    assert r.rc._emergency_stopped, "LIVE 10 (prima del riavvio) + 25 = 35 >= 30"


def test_record_legacy_senza_ambito_in_quarantena_per_ogni_ambito(tmp_path):
    c = _banco(tmp_path, "l.db", max_daily_loss=30.0)
    oggi = c.rc._daily_loss_monitor_state["day_utc"]
    c.db.save_settings({"daily_loss_state": json.dumps({
        "day_utc": oggi, "intraday_realized_pnl": -12.0, "breached": False, "breached_at": "", "seq": 3})})
    r = _banco(tmp_path, "l.db", max_daily_loss=30.0)
    for env in ("SIM", "LIVE"):
        _ambiente(r, env)
        assert r.rc._monitor_daily_loss_breach(source="T")["daily_loss_amount"] == pytest.approx(12.0), env
    record = json.loads(r.db.get_settings()["daily_loss_state"])
    assert record["legacy"]["intraday_realized_pnl"] == pytest.approx(-12.0), "quarantena persistita fino al giorno"
    assert record["seq"] > 3


def test_record_legacy_con_profitto_non_accredita_nessun_ambito(tmp_path):
    c = _banco(tmp_path, "lp.db", max_daily_loss=30.0)
    oggi = c.rc._daily_loss_monitor_state["day_utc"]
    c.db.save_settings({"daily_loss_state": json.dumps({
        "day_utc": oggi, "intraday_realized_pnl": 80.0, "breached": False, "breached_at": ""})})
    r = _banco(tmp_path, "lp.db", max_daily_loss=30.0)
    _ambiente(r, "LIVE")
    _settle(r, -35.0)
    assert r.rc._emergency_stopped, "un profitto legacy non attribuibile non compensa la perdita LIVE"


def test_stato_illeggibile_fail_closed_per_ogni_ambito(tmp_path):
    c = _banco(tmp_path, "u.db", max_daily_loss=30.0)
    c.db.save_settings({"daily_loss_state": json.dumps({"version": 2, "scopes": "rotto"})})
    r = _banco(tmp_path, "u.db", max_daily_loss=30.0)
    for env in ("SIM", "LIVE"):
        _ambiente(r, env)
        assert r.rc._monitor_daily_loss_breach(source="T")["breached"] is True, env


def test_crash_durante_la_migrazione_v1_v2_non_perde_la_perdita(tmp_path, monkeypatch):
    """Record v1 nel db; il primo salvataggio v2 fallisce (crash/disco): resta il marker v2. Al riavvio db v1 e
    marker v2 si riconciliano senza perdere ne' la quarantena ne' le perdite d'ambito."""
    c = _banco(tmp_path, "m.db", max_daily_loss=30.0)
    oggi = c.rc._daily_loss_monitor_state["day_utc"]
    c.db.save_settings({"daily_loss_state": json.dumps({
        "day_utc": oggi, "intraday_realized_pnl": -12.0, "breached": False, "breached_at": "", "seq": 1})})
    r = _banco(tmp_path, "m.db", max_daily_loss=30.0)

    def _guasto(_valori):
        raise OSError("disk full")

    r.db.save_settings = _guasto
    _ambiente(r, "SIM"); _settle(r, -10.0)
    marker = json.loads(open(r.db.db_path + ".daily_loss_pending.json", encoding="utf-8").read())
    assert marker["version"] == 2
    r2 = _banco(tmp_path, "m.db", max_daily_loss=30.0)
    _ambiente(r2, "SIM")
    assert r2.rc._monitor_daily_loss_breach(source="T")["daily_loss_amount"] == pytest.approx(22.0)
    _ambiente(r2, "LIVE")
    assert r2.rc._monitor_daily_loss_breach(source="T")["daily_loss_amount"] == pytest.approx(12.0)


def test_reconcile_books_seq_maggiore_e_unione_degli_ambiti():
    a = {"scopes": {"x": (-5.0, False, ""), "y": (-1.0, True, "t")}, "legacy": None, "seq": 4}
    b = {"scopes": {"x": (-8.0, False, ""), "z": (-2.0, False, "")}, "legacy": None, "seq": 3}
    out = daily_loss_store.reconcile_books(a, b)
    assert out["scopes"]["x"] == (-5.0, False, "")          # seq piu' alta
    assert out["scopes"]["y"][1] is True and "z" in out["scopes"]
    b["seq"] = None
    assert daily_loss_store.reconcile_books(a, b)["scopes"]["x"][0] == -8.0   # seq ignota: perdita maggiore


# ============================================================================ concorrenza
def test_settlement_concorrenti_sim_e_live_restano_separati(tmp_path):
    c = _banco(tmp_path, max_daily_loss=1000.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    sim, live = _sim(), _live(c)
    errori = []

    def _lavoro(scope, importo):
        try:
            for _ in range(20):
                c.rc._apply_realized_pnl_without_mutating_bankroll(importo)
                c.rc._attribute_realized_pnl(importo, {"scope": scope, "kind": "scoped"})
        except Exception as exc:  # pragma: no cover - diagnostica
            errori.append(exc)

    fili = [threading.Thread(target=_lavoro, args=(sim, -1.0)), threading.Thread(target=_lavoro, args=(live, -2.0))]
    for f in fili:
        f.start()
    for f in fili:
        f.join()
    assert not errori
    assert c.rc._realized_for_scope(sim) == pytest.approx(-20.0)
    assert c.rc._realized_for_scope(live) == pytest.approx(-40.0)


# ============================================================================ EXPANSION per ambito
def test_baseline_expansion_sim_non_vale_in_live(tmp_path):
    c = _banco(tmp_path)
    _ambiente(c, "SIM", cassa=100.0)
    assert c.rc._start_expansion_cycle("first_start", funds={"available": 100.0, "exposure": 0.0, "ok": True,
                                                             "simulated": True})
    _ambiente(c, "LIVE", cassa=1000.0)
    c.rc._record_account_snapshot({"available": 1000.0, "exposure": 0.0, "ok": True, "simulated": False},
                                  source="t")
    assert c.rc._expansion_growth_pct() is None, "nessuna crescita LIVE calcolata sulla baseline SIM"


def test_baseline_expansion_dello_stesso_ambito_resta_valida(tmp_path):
    c = _banco(tmp_path)
    fondi = {"available": 100.0, "exposure": 0.0, "ok": True, "simulated": False}
    _ambiente(c, "LIVE", cassa=100.0)
    assert c.rc._start_expansion_cycle("first_start", funds=dict(fondi))
    c.rc._record_account_snapshot(dict(fondi, available=120.0), source="t")
    assert c.rc._expansion_growth_pct() == pytest.approx(20.0)
    c.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_B)   # altro conto LIVE
    c.rc._record_account_snapshot(dict(fondi, available=120.0), source="t")
    assert c.rc._expansion_growth_pct() is None, "baseline di un altro conto: nessuna crescita"


def test_baseline_expansion_legacy_senza_ambito_non_espande(tmp_path):
    c = _banco(tmp_path, "e.db")
    c.db.save_settings({c.rc._EXPANSION_KEY: json.dumps({"baseline": 100.0, "reason": "first_start",
                                                         "set_at": "x", "set_at_ts": 1.0})})
    _ambiente(c, "LIVE", cassa=120.0)
    assert c.rc._load_expansion_baseline() and c.rc._expansion_baseline == 100.0
    c.rc._record_account_snapshot({"available": 120.0, "exposure": 0.0, "ok": True, "simulated": False},
                                  source="t")
    assert c.rc._expansion_growth_pct() is None


# ============================================================================ RISK_STOP per ambito
def test_risk_stop_da_perdita_di_sessione_sim_resta_locale(tmp_path):
    c = _banco(tmp_path, "rs.db", max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    instradati = []
    c.rc._route_cashout_signal = lambda segnale: instradati.append(segnale)
    _ambiente(c, "SIM")
    _settle(c, -40.0)
    assert c.rc._risk_stop_reason.endswith("@ambito=" + _sim()), c.rc._risk_stop_reason
    assert [s["signal_type"] for s in instradati] == ["CASHOUT_ALL"], "protezione SIM in modalita' SIM: ammessa"
    assert c.rc._loss_block_reason().startswith(("session_loss_breached", "risk_stop_active"))
    _ambiente(c, "LIVE")
    # (il gate S18 del drawdown di un altro ambito resta, fuori da PR-04a: qui si prova solo il RISK_STOP)
    assert c.rc._risk_stop_applies() is False and c.rc._limits_block_reason() == ""
    assert not c.rc._loss_block_reason().startswith("risk_stop_active"), "il RISK_STOP della SIM non blocca il LIVE"
    assert c.rc.reset_risk_stop()["reason"] == "risk_stop_di_altro_ambito"
    r = _banco(tmp_path, "rs.db", max_session_loss=30.0)                 # riavvio: lo stop SIM resta, locale
    _ambiente(r, "SIM")
    # F1 (owner M134): dopo il riavvio la sessione di rischio SIM riprende con la sua perdita: la SIM resta bloccata
    # (dal superamento di sessione ripristinato e dal RISK_STOP locale)
    assert r.rc._loss_block_reason().startswith(("session_loss_breached", "risk_stop_active"))
    assert r.rc._risk_stop_reason.endswith("@ambito=" + _sim())
    _ambiente(r, "LIVE")
    assert r.rc._risk_stop_applies() is False
    assert not r.rc._loss_block_reason().startswith("risk_stop_active")


def test_risk_stop_sim_non_instrada_cashout_mentre_il_runtime_e_live(tmp_path):
    c = _banco(tmp_path, max_session_loss=30.0)
    instradati = []
    c.rc._route_cashout_signal = lambda segnale: instradati.append(segnale)
    _ambiente(c, "LIVE")
    esito = c.rc.risk_stop("session_loss_breached:x", scope=_sim())
    assert esito["cashout_attempted"] is False and instradati == [], "un CASHOUT_ALL ora andrebbe al LIVE"


def test_risk_stop_live_resta_globale(tmp_path):
    c = _banco(tmp_path, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    c.rc._route_cashout_signal = lambda segnale: None
    _ambiente(c, "LIVE")
    _settle(c, -40.0)
    assert "@ambito=" not in c.rc._risk_stop_reason
    _ambiente(c, "SIM")
    assert c.rc._loss_block_reason().startswith("risk_stop_active"), "RISK_STOP LIVE: globale"


@pytest.mark.parametrize("motivo", ["x@ambito=betfair|LIVE|a|EUR", "x@ambito=rotto", "x@ambito="])
def test_risk_stop_con_ambito_non_sim_o_illeggibile_e_globale(motivo):
    from core.runtime_controller import RuntimeController
    assert RuntimeController._risk_stop_scope(motivo) == ""


def test_recovery_fail_closed_sim_resta_locale_live_resta_globale(tmp_path):
    """Ramo recovery fail-closed (P&L non applicato, runtime STOPPED): superamento proiettato SIM => nessuna
    emergenza; LIVE => Emergency Stop durevole come prima."""
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc._read_cycle_recovery_state = lambda _k: {"status": "RECOVERY_STATE_AMBIGUOUS"}
    c.rc.mode = RuntimeMode.STOPPED
    c.rc._on_close_position(_canonical_close_payload(event_key="evt-rec-sim", batch_id="b-rec-sim",
                                                     correlation_id="c-rec-sim", gross_pnl=-40.0,
                                                     commission_amount=0.0, net_pnl=-40.0, pnl=-40.0))
    assert not c.rc._emergency_stopped
    assert c.rc._daily_loss_scopes[_sim()]["breached"] is True
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.STOPPED
    c.rc._on_close_position(_canonical_close_payload(event_key="evt-rec-live", batch_id="b-rec-live",
                                                     correlation_id="c-rec-live", gross_pnl=-40.0,
                                                     commission_amount=0.0, net_pnl=-40.0, pnl=-40.0))
    assert c.rc._emergency_stopped
    assert c.rc.get_status()["daily_loss_scopes"][_live(c)]["breached"] is True


def test_superamento_sim_non_arma_pending_stop_ne_marker_di_emergenza(tmp_path):
    """Il pending-stop e il marker durevole `DAILY_LOSS_BREACH_PENDING` sono globali: un crash fra la loro scrittura e
    la rimozione riaprirebbe il runtime in emergenza (LIVE compreso). Per un superamento SIM non si armano mai."""
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    marker, pending = [], []
    vero = c.rc._persist_db_emergency_marker
    c.rc._persist_db_emergency_marker = lambda *a, **k: (marker.append(k.get("active")), vero(*a, **k))[1]
    vero_sync = c.rc._sync_bankroll_post_settlement
    c.rc._sync_bankroll_post_settlement = lambda p: (pending.append(c.rc._daily_loss_pending_stop), vero_sync(p))[1]
    _settle(c, -40.0)
    assert c.rc._daily_loss_scopes[_sim()]["breached"] is True
    assert True not in marker and pending == [False]
    _ambiente(c, "LIVE")
    _settle(c, -40.0)
    assert True in marker and pending[-1] is True, "LIVE: pending-stop e marker restano come prima"


def test_risk_stop_sim_poi_superamento_live_sale_al_globale(tmp_path):
    """Sol su 1587e3cf: dopo lo stop di sessione SIM, il superamento della sessione LIVE deve armare il RISK_STOP
    globale persistito e il CASHOUT_ALL (latch per ambito; uno stop locale non assorbe uno globale)."""
    c = _banco(tmp_path, "esc.db", max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    instradati = []
    c.rc._route_cashout_signal = lambda segnale: instradati.append(dict(segnale))
    _ambiente(c, "SIM")
    _settle(c, -40.0)
    assert c.rc._risk_stop_reason.endswith("@ambito=" + _sim())
    _ambiente(c, "LIVE")
    _settle(c, -40.0)                                   # sessione LIVE: -40 (la perdita SIM non conta)
    assert c.rc._risk_stop_reason.startswith("session_loss_breached")
    assert "@ambito=" not in c.rc._risk_stop_reason, "lo stop LIVE deve essere globale"
    assert len(instradati) == 2, instradati
    r = _banco(tmp_path, "esc.db", max_session_loss=30.0)
    assert "@ambito=" not in r.rc._risk_stop_reason, "globale anche dopo il riavvio"
    _ambiente(r, "SIM")
    # F1: la sessione di rischio riprende al riavvio (perdita SIM 40 ancora contata) e lo stop globale resta
    assert r.rc._loss_block_reason().startswith(("session_loss_breached", "risk_stop_active"))
    assert r.rc._risk_stop_reason.startswith("session_loss_breached") and r.rc._risk_stop_applies()


def test_stop_di_esposizione_dopo_uno_stop_sim_e_globale(tmp_path):
    c = _banco(tmp_path, max_session_loss=30.0)
    c.rc._route_cashout_signal = lambda segnale: None
    _ambiente(c, "SIM")
    c.rc.risk_stop("session_loss_breached:x", scope=_sim())
    esito = c.rc.risk_stop("exposure_stop:limit=1")
    assert c.rc._risk_stop_reason == "exposure_stop:limit=1" and esito["reason"] == "exposure_stop:limit=1"
    assert c.rc.risk_stop("session_loss_breached:y", scope=_sim())["reason"] == "exposure_stop:limit=1", \
        "uno stop locale non sostituisce mai uno globale"


# ============================================================================ rilievi Codex su 1587e3cf
def test_perdita_non_attribuibile_si_somma_alle_perdite_dell_ambito_anche_dopo_il_riavvio(tmp_path):
    """Conto A -20, poi -15 senza conto identificabile (limite 50): A deve valere 35, in processo e al riavvio
    (non il minimo fra 20 e la quarantena 15)."""
    c = _banco(tmp_path, "ua.db", max_daily_loss=50.0)
    _ambiente(c, "LIVE")
    conto_a = c.rc._current_financial_scope()
    _settle(c, -20.0)
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None
    _settle(c, -15.0)
    assert c.rc._daily_loss_scopes[conto_a]["daily_loss_amount"] == pytest.approx(35.0)
    r = _banco(tmp_path, "ua.db", max_daily_loss=50.0)
    _ambiente(r, "LIVE")
    assert r.rc._current_financial_scope() == conto_a
    assert r.rc._monitor_daily_loss_breach(source="T")["daily_loss_amount"] == pytest.approx(35.0)


def test_perdita_non_attribuibile_che_porta_un_conto_oltre_il_limite_ferma(tmp_path):
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "LIVE")
    _settle(c, -20.0)
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None
    _settle(c, -15.0)
    assert c.rc._emergency_stopped


def test_barriera_locale_sim_durante_il_sync_del_bankroll(tmp_path):
    """Superamento SIM proiettato: durante l'I/O del sync gli ingressi SIM sono gia' negati (nessun pending globale)."""
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    visti = []
    vero_sync = c.rc._sync_bankroll_post_settlement
    c.rc._sync_bankroll_post_settlement = lambda p: (
        visti.append((c.rc._limits_block_reason(), c.rc._daily_loss_pending_stop)), vero_sync(p))[1]
    _settle(c, -40.0)
    assert visti == [("daily_loss_breached:SIM", False)]
    _ambiente(c, "LIVE")
    assert c.rc._limits_block_reason() == "", "la barriera SIM non tocca il LIVE"


def test_perdita_di_sessione_live_regolata_mentre_il_runtime_e_in_sim(tmp_path):
    c = _banco(tmp_path, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    instradati = []
    c.rc._route_cashout_signal = lambda segnale: instradati.append(dict(segnale))
    _ambiente(c, "LIVE")
    _ambiente(c, "SIM")
    c.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)
    _settle(c, -40.0, event_key="cleared:1.900:77")
    assert c.rc._risk_stop_reason.startswith("session_loss_breached")
    assert "@ambito=" not in c.rc._risk_stop_reason, "perdita di sessione LIVE: RISK_STOP globale"


# ============================================================================ rilievi Sol su d86404ef
def test_crash_dopo_la_marcatura_non_perde_la_perdita_non_attribuibile(tmp_path):
    """Crash subito dopo `_mark_settlement_realized`: ogni ambito deve aver gia' persistito la perdita condivisa."""
    c = _banco(tmp_path, "cr.db", max_daily_loss=50.0)
    _ambiente(c, "LIVE")
    conto_a = c.rc._current_financial_scope()
    _settle(c, -20.0)
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None

    def _crash(_key):
        raise RuntimeError("crash dopo la marcatura")

    c.rc._mark_settlement_realized = _crash
    with pytest.raises(RuntimeError):
        _settle(c, -15.0)
    r = _banco(tmp_path, "cr.db", max_daily_loss=50.0)
    _ambiente(r, "LIVE")
    assert r.rc._current_financial_scope() == conto_a
    assert r.rc._monitor_daily_loss_breach(source="T")["daily_loss_amount"] == pytest.approx(35.0)


def test_perdita_non_attribuibile_valuta_la_sessione_di_ogni_ambito(tmp_path):
    c = _banco(tmp_path, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    instradati = []
    c.rc._route_cashout_signal = lambda segnale: instradati.append(dict(segnale))
    _ambiente(c, "LIVE")
    _settle(c, -20.0)
    c.rc._daily_loss_state_for(c.rc._current_financial_scope())     # ambito A noto al registro
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None
    _settle(c, -15.0)
    assert c.rc._risk_stop_reason.startswith("session_loss_breached"), c.rc._risk_stop_reason
    assert "@ambito=" not in c.rc._risk_stop_reason and instradati


# ============================================================================ rilievi Codex su d86404ef + domanda D
def test_ambito_della_riserva_persistito_e_ricostruito_da_f018(tmp_path):
    c = _banco(tmp_path, "f018.db", max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo = _riserva(c, "1.950")
    righe = c.db._execute("SELECT payload_json FROM orders", fetch=True, commit=False)
    assert any(json.loads(dict(r)["payload_json"]).get("financial_scope") == _sim() for r in righe)
    r = _banco(tmp_path, "f018.db", max_daily_loss=30.0)
    candidati, _ = r.rc._open_order_candidates()
    meta = [x["meta"] for x in candidati if x["customer_ref"] == tavolo.meta["customer_ref"]]
    assert meta and meta[0]["financial_scope"] == _sim()


def test_riserva_senza_ambito_legacy_non_va_all_ambito_corrente(tmp_path):
    """Riserva nota ma senza ambito (ordine precedente a PR-04a ricostruito da F018): la perdita non e' mai attribuita
    per supposizione all'ambito corrente (SIM): conta per ogni ambito, LIVE compreso => escalation globale."""
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo = _riserva(c, "1.951")
    _ambiente(c, "LIVE")
    live = c.rc._current_financial_scope()
    c.rc._daily_loss_state_for(live)
    c.rc.set_simulation_mode(True)                       # settlement mentre il runtime e' in SIM
    meta = dict(tavolo.meta)
    meta.pop("financial_scope")
    c.rc.table_manager.force_unlock(tavolo.table_id)
    c.rc.table_manager.activate(table_id=tavolo.table_id, event_key=tavolo.current_event_key, exposure=2.0,
                                market_id=tavolo.market_id, selection_id=tavolo.selection_id, meta=meta)
    _settle(c, -40.0, table_id=tavolo.table_id, customer_ref=meta["customer_ref"],
            event_key=tavolo.current_event_key, market_id=tavolo.market_id)
    assert c.rc._daily_loss_scopes[live]["daily_loss_amount"] == pytest.approx(40.0)
    assert c.rc._emergency_stopped, "perdita di una riserva senza ambito: conta anche per il LIVE"


def test_dutching_registra_l_ambito_nella_riserva(tmp_path):
    from controllers.dutching_controller import DutchingController
    c = _banco(tmp_path)
    _ambiente(c, "SIM")
    dc = DutchingController(c.rc.bus, c.rc)
    tid = dc._allocate_table("evt-dutch-1", 2.0, {"market_id": "1.952", "batch_id": "lotto-1"})
    assert c.rc.table_manager.get_table(tid).meta["financial_scope"] == _sim()
    _ambiente(c, "LIVE")
    tid2 = dc._allocate_table("evt-dutch-2", 2.0, {"market_id": "1.953", "batch_id": "lotto-2"})
    assert c.rc.table_manager.get_table(tid2).meta["financial_scope"] == _live(c)


def test_settlement_di_lotto_attribuito_tramite_la_riserva(tmp_path):
    """Chiusura di un lotto (stesso batch_id, senza identita' nel payload) mentre il runtime e' passato a LIVE:
    il P&L e' della SIM della riserva."""
    from controllers.dutching_controller import DutchingController
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    dc = DutchingController(c.rc.bus, c.rc)
    dc._allocate_table("evt-dutch-3", 2.0, {"market_id": "1.954", "batch_id": "lotto-3", "type": "dutching_batch"})
    _ambiente(c, "LIVE")
    _settle(c, -40.0, batch_id="lotto-3", table_id=None, event_key="evt-dutch-other")
    assert not c.rc._emergency_stopped
    assert c.rc._daily_loss_scopes[_sim()]["breached"] is True


def test_totale_e_attribuzione_atomici_rispetto_a_un_monitor_concorrente(tmp_path):
    c = _banco(tmp_path, "atom.db", max_daily_loss=100.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo = _riserva(c, "1.955")
    _ambiente(c, "LIVE")
    live = c.rc._current_financial_scope()
    _settle(c, -20.0)
    vero = c.rc._apply_realized_pnl_without_mutating_bankroll
    fili, visti = [], []

    def _monitor():
        visti.append(c.rc._monitor_daily_loss_breach(source="STATUS", scope=live)["intraday_realized_pnl"])

    def _con_monitor_concorrente(pnl):
        vero(pnl)
        f = threading.Thread(target=_monitor)
        f.start()
        f.join(0.3)                                    # finestra fra totale e attribuzione
        fili.append(f)

    c.rc._apply_realized_pnl_without_mutating_bankroll = _con_monitor_concorrente
    _settle_tavolo(c, tavolo, 50.0)                    # profitto SIM tardivo
    for f in fili:
        f.join(5)
    assert visti and visti[0] == pytest.approx(-20.0), "il monitor concorrente ha visto il totale senza ambito"
    record = json.loads(c.db.get_settings()["daily_loss_state"])
    assert record["scopes"][live]["intraday_realized_pnl"] == pytest.approx(-20.0)


def test_perdita_non_attribuibile_arma_la_barriera_sim_prima_del_sync(tmp_path):
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    _settle(c, -20.0)
    visti = []
    vero_sync = c.rc._sync_bankroll_post_settlement
    c.rc._sync_bankroll_post_settlement = lambda p: (visti.append(c.rc._limits_block_reason()), vero_sync(p))[1]
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None
    _settle(c, -15.0, event_key="cleared:1.956:9")       # LIVE senza conto identificabile
    assert visti == ["daily_loss_breached:SIM"]


# ============================================================================ Codex su 8a6c4cbb
def test_superamento_live_a_runtime_fermo_arma_l_emergency_stop_globale(tmp_path):
    """Settlement LIVE tardivo che supera il limite mentre il runtime e' STOPPED: Emergency Stop globale durevole,
    quindi un avvio successivo in SIM (o con un altro conto) non riapre gli ingressi."""
    c = _banco(tmp_path, "st.db", max_daily_loss=30.0)
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.STOPPED
    c.rc._on_close_position(_canonical_close_payload(event_key="cleared:1.960:1", batch_id="b-st", correlation_id="c-st",
                                                     gross_pnl=-40.0, commission_amount=0.0, net_pnl=-40.0, pnl=-40.0))
    assert c.rc._emergency_stopped, "superamento LIVE: Emergency Stop globale anche a runtime fermo"
    r = _banco(tmp_path, "st.db", max_daily_loss=30.0)
    assert r.rc._emergency_stopped, "emergenza durevole dopo il riavvio"


def test_superamento_sim_a_runtime_fermo_resta_locale(tmp_path):
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.STOPPED
    c.rc._on_close_position(_canonical_close_payload(event_key="sim:1.961:1", batch_id="b-st2", correlation_id="c-st2",
                                                     gross_pnl=-40.0, commission_amount=0.0, net_pnl=-40.0, pnl=-40.0))
    assert not c.rc._emergency_stopped
    assert c.rc._daily_loss_scopes[_sim()]["breached"] is True


# ============================================================================ Sol su 8a6c4cbb
def test_riserva_legacy_con_solo_sim_registrata_sale_all_emergenza_globale(tmp_path):
    """Origine non verificabile (riserva senza ambito) con runtime e registro solo SIM: -40 su limite 30 deve salire
    all'Emergency Stop globale, non restare un blocco SIM locale."""
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo = _riserva(c, "1.970")
    meta = dict(tavolo.meta)
    meta.pop("financial_scope")
    c.rc.table_manager.force_unlock(tavolo.table_id)
    c.rc.table_manager.activate(table_id=tavolo.table_id, event_key=tavolo.current_event_key, exposure=2.0,
                                market_id=tavolo.market_id, selection_id=tavolo.selection_id, meta=meta)
    for chiave in [k for k in c.rc._daily_loss_scopes if k != _sim()]:   # registro con la sola SIM
        del c.rc._daily_loss_scopes[chiave]
    _settle(c, -40.0, table_id=tavolo.table_id, customer_ref=meta["customer_ref"],
            event_key=tavolo.current_event_key, market_id=tavolo.market_id)
    assert c.rc._emergency_stopped, "perdita di origine non verificabile: escalation globale"


def test_recovery_fail_closed_proietta_la_perdita_non_attribuibile_su_ogni_ambito(tmp_path):
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "LIVE")
    _settle(c, -20.0)
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None
    c.rc._read_cycle_recovery_state = lambda _k: {"status": "RECOVERY_STATE_AMBIGUOUS"}
    c.rc.mode = RuntimeMode.STOPPED
    c.rc._on_close_position(_canonical_close_payload(event_key="cleared:1.971:1", batch_id="b-rc", correlation_id="c-rc",
                                                     gross_pnl=-15.0, commission_amount=0.0, net_pnl=-15.0, pnl=-15.0))
    assert c.rc._emergency_stopped, "LIVE A 20 + 15 non attribuibile = 35 >= 30: Emergency Stop"


# ============================================================================ Codex su 38979e59
def test_settlement_tardivo_su_tavolo_riassegnato_usa_l_ambito_della_scommessa(tmp_path):
    """Tavolo della riserva SIM riassegnato a una riserva LIVE prima del settlement: il P&L resta della SIM (identita'
    persistita dell'ordine), mai l'ambito del nuovo occupante."""
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo = _riserva(c, "1.980")
    ref, ev = tavolo.meta["customer_ref"], tavolo.current_event_key
    _ambiente(c, "LIVE")
    live = c.rc._current_financial_scope()
    c.rc.table_manager.force_unlock(tavolo.table_id)
    c.rc.table_manager.activate(table_id=tavolo.table_id, event_key="evt-live-altro", exposure=2.0, market_id="1.981",
                                selection_id=11, meta={"customer_ref": "ref-live-altro", "financial_scope": live})
    _settle(c, -40.0, table_id=tavolo.table_id, customer_ref=ref, event_key=ev, market_id="1.980")
    assert not c.rc._emergency_stopped, "perdita della scommessa SIM attribuita al LIVE del nuovo occupante"
    assert c.rc._daily_loss_scopes[_sim()]["breached"] is True


def test_ambito_tornato_a_zero_non_fa_risorgere_un_profitto_del_marker(tmp_path, monkeypatch):
    import core.daily_loss_store as store
    c = _banco(tmp_path, "z.db", max_daily_loss=30.0)
    _ambiente(c, "SIM")
    salva = c.db.save_settings

    def _guasto(_valori):
        raise OSError("disk full")

    c.db.save_settings = _guasto
    _settle(c, 20.0, event_key="sim:1.982:1")                  # profitto SIM solo nel marker
    c.db.save_settings = salva
    vero = store.os.remove
    monkeypatch.setattr(store.os, "remove", lambda p: (_ for _ in ()).throw(PermissionError("locked"))
                        if str(p).endswith(".daily_loss_pending.json") else vero(p))
    netto = c.rc._realized_for_scope(_sim())
    _settle(c, -netto, event_key="sim:1.982:2")                # torna a zero, salvato nel db; marker non rimosso
    r = _banco(tmp_path, "z.db", max_daily_loss=30.0)
    _ambiente(r, "SIM")
    assert r.rc._monitor_daily_loss_breach(source="T")["intraday_realized_pnl"] == pytest.approx(0.0, abs=1e-6)


# ============================================================================ Sol su 38979e59
def test_crash_dopo_la_marcatura_a_runtime_fermo_conserva_l_emergency_stop(tmp_path):
    c = _banco(tmp_path, "cm.db", max_daily_loss=30.0)
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.STOPPED

    def _crash(_key):
        raise RuntimeError("crash dopo la marcatura")

    c.rc._mark_settlement_realized = _crash
    with pytest.raises(RuntimeError):
        c.rc._on_close_position(_canonical_close_payload(
            event_key="cleared:1.990:1", batch_id="b-cm", correlation_id="c-cm",
            gross_pnl=-40.0, commission_amount=0.0, net_pnl=-40.0, pnl=-40.0))
    r = _banco(tmp_path, "cm.db", max_daily_loss=30.0)
    assert r.rc._emergency_stopped, "l'Emergency Stop globale deve essere durevole prima della marcatura"


# ============================================================================ Sol su b02ebcf0
def test_superamento_sim_proiettato_e_durevole_prima_del_sync(tmp_path):
    """Crash durante il sync del bankroll: il superamento SIM proiettato e' gia' persistito (blocco SIM durevole)."""
    c = _banco(tmp_path, "sy.db", max_daily_loss=30.0)
    _ambiente(c, "SIM")

    def _crash(_payload):
        raise RuntimeError("crash durante il sync")

    c.rc._sync_bankroll_post_settlement = _crash
    with pytest.raises(RuntimeError):
        _settle(c, -40.0, event_key="sim:1.995:1")
    r = _banco(tmp_path, "sy.db", max_daily_loss=30.0)
    _ambiente(r, "SIM")
    assert r.rc._monitor_daily_loss_breach(source="T")["breached"] is True
    assert not r.rc._emergency_stopped


def test_quarantena_e_ambiti_salvati_insieme(tmp_path):
    """Crash durante l'aggiornamento degli altri ambiti per una perdita non attribuibile: sul disco non deve mai
    restare la quarantena aggiornata con un ambito ancora vecchio (al riavvio il minimo perderebbe la perdita)."""
    c = _banco(tmp_path, "qa.db", max_daily_loss=50.0)
    _ambiente(c, "LIVE")
    conto_a = c.rc._current_financial_scope()
    _settle(c, -20.0)
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None
    vero = c.rc._monitor_daily_loss_breach

    def _monitor(**kw):
        if kw.get("source") == "RUNTIME_CLOSE_POSITION_UNATTRIBUTED":
            raise RuntimeError("crash fra gli ambiti")
        return vero(**kw)

    c.rc._monitor_daily_loss_breach = _monitor
    with pytest.raises(RuntimeError):
        _settle(c, -15.0)
    record = json.loads(c.db.get_settings()["daily_loss_state"])
    quarantena = (record.get("legacy") or {}).get("intraday_realized_pnl", 0.0)
    if quarantena <= -15.0:
        assert record["scopes"][conto_a]["intraday_realized_pnl"] == pytest.approx(-35.0), record


# ============================================================================ Codex su 3c1ff9e4
def test_settlement_duplicato_non_registra_un_superamento_sim_falso(tmp_path):
    c = _banco(tmp_path, "dup.db", max_daily_loss=30.0)
    _ambiente(c, "SIM")
    payload = _canonical_close_payload(event_key="sim:1.996:1", batch_id="b-dup", correlation_id="c-dup",
                                       gross_pnl=-20.0, commission_amount=0.0, net_pnl=-20.0, pnl=-20.0)
    c.rc.mode = RuntimeMode.ACTIVE
    c.rc._on_close_position(dict(payload))
    c.rc._on_close_position(dict(payload))               # stessa consegna di nuovo
    assert c.rc.risk_desk.realized_pnl == pytest.approx(-20.0)
    assert c.rc._daily_loss_scopes[_sim()]["breached"] is False
    r = _banco(tmp_path, "dup.db", max_daily_loss=30.0)
    _ambiente(r, "SIM")
    assert r.rc._monitor_daily_loss_breach(source="T")["breached"] is False


# ============================================================================ mandato owner P2 (idempotenza durevole)
def _payload_sim(n, importo):
    return _canonical_close_payload(event_key=f"sim:1.997:{n}", batch_id=f"b-idem-{n}", correlation_id=f"c-idem-{n}",
                                    gross_pnl=importo, commission_amount=0.0, net_pnl=importo, pnl=importo)


def test_due_settlement_distinti_con_importi_uguali_contano_entrambi(tmp_path):
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    c.rc._on_close_position(_payload_sim(1, -20.0))
    c.rc._on_close_position(_payload_sim(2, -20.0))
    assert c.rc.risk_desk.realized_pnl == pytest.approx(-40.0)
    assert c.rc._daily_loss_scopes[_sim()]["breached"] is True


def test_doppia_consegna_dopo_riavvio_un_solo_effetto(tmp_path):
    c = _banco(tmp_path, "rr.db", max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    c.rc._on_close_position(_payload_sim(3, -20.0))
    r = _banco(tmp_path, "rr.db", max_daily_loss=30.0)
    _ambiente(r, "SIM")
    r.rc.mode = RuntimeMode.ACTIVE
    r.rc._on_close_position(_payload_sim(3, -20.0))
    stato = r.rc._monitor_daily_loss_breach(source="T")
    assert stato["daily_loss_amount"] == pytest.approx(20.0) and stato["breached"] is False


def test_crash_fra_stato_giornaliero_e_marcatura_poi_riconsegna_un_solo_effetto(tmp_path):
    """Crash dopo il salvataggio dello stato giornaliero e prima della marcatura del settlement: alla riconsegna dopo
    il riavvio la perdita NON si conta due volte (la chiave viaggia nella stessa scrittura dello stato)."""
    c = _banco(tmp_path, "cr2.db", max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE

    def _crash(_key):
        raise RuntimeError("crash prima della marcatura")

    c.rc._mark_settlement_realized = _crash
    with pytest.raises(RuntimeError):
        c.rc._on_close_position(_payload_sim(4, -20.0))
    r = _banco(tmp_path, "cr2.db", max_daily_loss=30.0)
    _ambiente(r, "SIM")
    r.rc.mode = RuntimeMode.ACTIVE
    r.rc._on_close_position(_payload_sim(4, -20.0))
    stato = r.rc._monitor_daily_loss_breach(source="T")
    assert stato["daily_loss_amount"] == pytest.approx(20.0), "perdita contata due volte dopo il crash"
    assert stato["breached"] is False


# ============================================================================ Sol su 3c1ff9e4
def test_hint_della_riserva_richiede_lo_stesso_customer_ref(tmp_path):
    """Tavolo riusato sullo stesso evento da una nuova riserva SIM: un settlement legacy (riserva senza ambito, altro
    customer_ref) non prende l'ambito SIM della nuova riserva: resta non attribuibile (escalation globale)."""
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo = _riserva(c, "1.998")
    for chiave in [k for k in c.rc._daily_loss_scopes if k != _sim()]:
        del c.rc._daily_loss_scopes[chiave]
    _settle(c, -40.0, table_id=tavolo.table_id, customer_ref="ref-legacy-altra-riserva",
            event_key=tavolo.current_event_key, market_id=tavolo.market_id)
    assert c.rc._emergency_stopped, "la perdita legacy ha preso l'ambito SIM del nuovo occupante"


def test_ambito_vuoto_dell_effetto_prevale_sull_hint():
    from core.runtime_controller import RuntimeController
    rc = RuntimeController.__new__(RuntimeController)
    rc.simulation_mode = True
    esito = rc._settlement_attribution(-10.0, {"financial_scope": ""}, {"event_key": "evt-x"}, _sim())
    assert esito["kind"] == "unattributed_loss" and not financial_scope.is_sim(esito["scope"])


def test_crash_dopo_il_salvataggio_del_superamento_live_a_runtime_fermo_riavvio_in_emergenza(tmp_path):
    """Sol su 3c1ff9e4: in STOPPED il superamento LIVE si salva prima dell'Emergency Stop; un crash fra le due
    scritture non deve lasciare il superamento senza emergenza al riavvio."""
    import core.runtime_controller as rcmod
    c = _banco(tmp_path, "sa.db", max_daily_loss=30.0)
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.STOPPED

    def _muore(self, *a, **k):
        raise SystemExit("crash prima dell'Emergency Stop")

    vero = rcmod.RuntimeController._escalate_stopped_live_breach
    rcmod.RuntimeController._escalate_stopped_live_breach = _muore
    try:
        with pytest.raises(SystemExit):
            c.rc._on_close_position(_canonical_close_payload(
                event_key="cleared:1.999:1", batch_id="b-sa", correlation_id="c-sa",
                gross_pnl=-40.0, commission_amount=0.0, net_pnl=-40.0, pnl=-40.0))
    finally:
        rcmod.RuntimeController._escalate_stopped_live_breach = vero
    r = _banco(tmp_path, "sa.db", max_daily_loss=30.0)
    assert r.rc._emergency_stopped, "superamento LIVE persistito senza Emergency Stop dopo il riavvio"


# ============================================================================ criteri D M110 / M110-bis (equivalenti B)
def test_g11_concorrenza_due_distinti_e_una_riconsegna_identica(tmp_path):
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    via = threading.Barrier(3)
    voci = [("sim:1.9301:1", "cc0"), ("sim:1.9301:2", "cc1"), ("sim:1.9301:1", "cc0")]

    def _uno(chiave, corr):
        via.wait(5)
        c.rc._on_close_position(_canonical_close_payload(
            event_key=chiave, batch_id=f"b-{corr}", correlation_id=f"c-{corr}", gross_pnl=-12.0,
            commission_amount=0.0, net_pnl=-12.0, pnl=-12.0))

    fili = [threading.Thread(target=_uno, args=v) for v in voci]
    for f in fili:
        f.start()
    for f in fili:
        f.join(10)
    stato = c.rc._monitor_daily_loss_breach(source="T", scope=_sim())
    assert c.rc.risk_desk.realized_pnl == pytest.approx(-24.0)
    assert stato["daily_loss_amount"] == pytest.approx(24.0) and stato["breached"] is False


@pytest.mark.parametrize("punto", ["_persist_daily_loss_book", "_mark_settlement_realized",
                                   "_commit_settlement_effect"])
def test_g9_crash_nel_salvataggio_poi_riavvio_e_riconsegna_un_solo_effetto(tmp_path, monkeypatch, punto):
    import core.runtime_controller as rcmod
    c = _banco(tmp_path, "g9.db", max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    reale = getattr(rcmod.RuntimeController, punto)

    def _muore(self, *a, **k):
        raise SystemExit(f"crash in {punto}")

    monkeypatch.setattr(rcmod.RuntimeController, punto, _muore)
    try:
        c.rc._on_close_position(_payload_sim(9, -20.0))
    except BaseException:                                   # SystemExit: morte simulata del processo
        pass
    monkeypatch.setattr(rcmod.RuntimeController, punto, reale)
    r = _banco(tmp_path, "g9.db", max_daily_loss=30.0)
    _ambiente(r, "SIM")
    r.rc.mode = RuntimeMode.ACTIVE
    r.rc._on_close_position(_payload_sim(9, -20.0))
    stato = r.rc._monitor_daily_loss_breach(source="T")
    assert stato["daily_loss_amount"] == pytest.approx(20.0) and stato["breached"] is False, punto


def test_hint_per_evento_ignora_una_riserva_di_un_altra_scommessa(tmp_path):
    """Settlement senza table_id (riserva non identificata dal rilascio) con un customer_ref diverso da quello della
    riserva viva sullo stesso evento: l'hint non deve prendere l'ambito SIM di quella riserva."""
    c = _banco(tmp_path, max_daily_loss=100.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo = _riserva(c, "1.9401")
    _ambiente(c, "LIVE")
    _settle(c, -40.0, table_id=None, customer_ref="ref-di-un-altra-scommessa",
            event_key=tavolo.current_event_key, market_id=tavolo.market_id)
    assert c.rc._scope_pnl.get(_sim(), 0.0) == 0.0, "perdita attribuita alla riserva SIM di un'altra scommessa"


# ============================================================================ Sol/Codex su c5dd0bab
def test_chiave_del_settlement_salvata_solo_con_lo_stato_aggiornato_dell_ambito(tmp_path):
    """Settlement SIM tardivo (-20) mentre il runtime e' LIVE; un get_status concorrente durante il sync salva il libro;
    crash; riavvio e riconsegna: la perdita SIM deve valere 20 (mai chiave salvata con lo stato vecchio => 0)."""
    c = _banco(tmp_path, "k1.db", max_daily_loss=30.0)
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.ACTIVE
    vero_sync = c.rc._sync_bankroll_post_settlement

    def _sync_con_status(p):
        c.rc._monitor_daily_loss_breach(source="RUNTIME_STATUS_SNAPSHOT")     # get_status concorrente (ambito LIVE)
        raise SystemExit("crash durante il sync")

    c.rc._sync_bankroll_post_settlement = _sync_con_status
    with pytest.raises(SystemExit):
        c.rc._on_close_position(_payload_sim(61, -20.0))
    r = _banco(tmp_path, "k1.db", max_daily_loss=30.0)
    r.rc.set_simulation_mode(True)
    r.rc.mode = RuntimeMode.ACTIVE
    r.rc._on_close_position(_payload_sim(61, -20.0))
    assert r.rc._monitor_daily_loss_breach(source="T", scope=_sim())["daily_loss_amount"] == pytest.approx(20.0)


def test_stop_sim_locale_al_punto_d_invio_per_un_ordine_gia_in_coda(tmp_path):
    """Un ingresso SIM gia' approvato e in coda non parte dopo il superamento SIM (controllo finale d'invio), senza
    nessuna emergenza globale."""
    c = _banco(tmp_path, max_daily_loss=30.0)
    from tests.d1.test_d1_drawdown_hard_stop import _lettura
    c.rc.set_simulation_mode(True)
    _lettura(c, 1000.0)
    c.rc.mode = RuntimeMode.ACTIVE
    assert c.rc.drawdown_entry_block_reason() == ""
    c.rc._on_close_position(_payload_sim(62, -40.0))
    _lettura(c, 1000.0)
    c.rc.mode = RuntimeMode.ACTIVE
    assert c.rc.drawdown_entry_block_reason() == "daily_loss_breached:SIM"
    assert not c.rc._emergency_stopped


# ============================================================================ M111 (PF-OWNER-PR638-M111-SIM-ENTRY-BYPASS-GO)
@pytest.fixture
def app_headless(tmp_path, monkeypatch):
    from tests.acceptance.test_issue437_pr04 import _App
    from tests.risk_config_fixtures import legacy_test_config
    from core import blackbox_runtime
    monkeypatch.chdir(tmp_path)
    a = _App("headless", tmp_path, monkeypatch)
    if blackbox_runtime.get() is None:
        blackbox_runtime.install("bb/bb.sqlite", entrypoint="test_pr04a_m111")
    monkeypatch.setattr(a.rt.settings_service, "load_roserpina_config",
                        lambda: legacy_test_config(liquidity_guard_enabled=False, max_daily_loss=30.0))
    a.rt.betfair_service.get_account_funds = lambda: {"available": 1000.0, "exposure": 0.0, "ok": True}
    yield a
    a.chiudi()


def _m111_perdita_sim(a, key="sim:1.995:1"):
    a.rt.config.max_daily_loss = 30.0
    a.rt._on_close_position(_canonical_close_payload(
        event_key=key, batch_id="b-" + key, correlation_id="c-" + key, gross_pnl=-40.0, commission_amount=0.0,
        net_pnl=-40.0, pnl=-40.0, commission_pct=4.5))


def _m111_ordini(a):
    return {o.get("betId") for o in a.broker.get_current_orders(None)}


def _m111_ingresso(a, percorso):
    from tests.acceptance.test_issue523_dutching_manual_bet import _batch, _manual
    return (a.app.dutching_controller.submit_dutching(_batch(total_stake=10.0)) if percorso == "dutching"
            else a.app.dutching_controller.manual_bet(_manual(stake=2.0)))


@pytest.mark.parametrize("percorso", ["dutching", "manuale"])
def test_m111_ingresso_sim_oltre_limite_rifiutato_all_ammissione(app_headless, percorso):
    """SIM -40 (limite 30): puntata manuale e Dutching rifiutati in modo esplicito, nessun ordine al broker simulato,
    nessuna emergenza globale. Sotto il limite lo stesso ingresso arriva al broker (controllo positivo)."""
    a = app_headless
    a.avvia()
    a.rt.mode = RuntimeMode.ACTIVE
    prima = _m111_ordini(a)
    assert _m111_ingresso(a, percorso)["ok"] is True
    a.svuota_bus()
    assert _m111_ordini(a) - prima, "ingresso lecito non arrivato al broker SIM"
    _m111_perdita_sim(a)
    a.svuota_bus()
    a.rt.mode = RuntimeMode.ACTIVE
    prima = _m111_ordini(a)
    esito = _m111_ingresso(a, percorso)
    a.svuota_bus()
    assert esito["ok"] is False and "daily_loss_breached:SIM" in esito["error"]
    assert _m111_ordini(a) == prima and a.rt._emergency_stopped is False


@pytest.mark.parametrize("percorso", ["dutching", "manuale"])
def test_m111_blocco_fra_ammissione_e_invio_ferma_ogni_gamba(app_headless, percorso):
    """Ingresso ammesso (comandi trattenuti), poi il limite SIM scatta, poi le gambe arrivano al motore: nessun ordine
    (controllo ripetuto al punto finale d'invio)."""
    a = app_headless
    a.avvia()
    a.rt.mode = RuntimeMode.ACTIVE
    prima = _m111_ordini(a)
    held, reale = [], a.app.bus.publish

    def trattieni(topic, payload=None, *x, **k):
        if topic == "CMD_QUICK_BET":
            held.append(payload)
            return None
        return reale(topic, payload, *x, **k)

    a.app.bus.publish = trattieni
    assert _m111_ingresso(a, percorso)["ok"] is True
    a.app.bus.publish = reale
    _m111_perdita_sim(a)
    for payload in held:
        reale("CMD_QUICK_BET", payload)
    a.svuota_bus()
    assert held and _m111_ordini(a) == prima and a.rt._emergency_stopped is False


def test_m111_cashout_all_resta_disponibile_con_la_sim_bloccata(app_headless):
    a = app_headless
    a.avvia()
    a.posizione()
    _m111_perdita_sim(a)
    a.svuota_bus()
    a.eventi.clear()
    a.messaggio("CASHOUT ALL")
    a.svuota_bus()
    assert a.rt._limits_block_reason() == "daily_loss_breached:SIM"
    assert len(a.hedge()) == 1 and a.rt._emergency_stopped is False


def _fotografa_al_crash(tmp_path, nome, copia):
    """Morte del processo in questo istante: copia coerente del DB (backup SQLite) e dei file accanto (marker). Il
    riavvio parte dalla copia, quindi nessun `finally` successivo del processo «morto» la modifica."""
    import shutil
    import sqlite3
    for f in tmp_path.iterdir():
        if f.name.startswith(nome) and f.name != nome and not f.name.endswith(("-wal", "-shm", "-journal")):
            shutil.copy(f, tmp_path / (copia + f.name[len(nome):]))
    src, dst = sqlite3.connect(str(tmp_path / nome)), sqlite3.connect(str(tmp_path / copia))
    try:
        src.backup(dst)
    finally:
        src.close()
        dst.close()


def test_perdita_non_attribuibile_mai_salvata_a_meta_durante_il_sync(tmp_path):
    """Sol su 385c7598: conto A -20, poi -15 non attribuibile; durante il sync un get_status salva il libro e il
    processo muore. Il libro salvato non contiene mai la quarantena senza la chiave del settlement (stato a meta': al
    riavvio il minimo darebbe A = 20 con la quarantena gia' consumata); dopo riavvio e riconsegna A vale 35."""
    import json
    c = _banco(tmp_path, "uas.db", max_daily_loss=50.0)
    _ambiente(c, "LIVE")
    conto_a = c.rc._current_financial_scope()
    _settle(c, -20.0)
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None
    salvato = {}

    def _sync_con_status(p):
        c.rc._monitor_daily_loss_breach(source="RUNTIME_STATUS_SNAPSHOT")
        salvato.update(json.loads(c.db.get_settings()["daily_loss_state"]))
        _fotografa_al_crash(tmp_path, "uas.db", "uas_crash.db")
        raise SystemExit("crash durante il sync")

    c.rc._sync_bankroll_post_settlement = _sync_con_status
    with pytest.raises(SystemExit):
        c.rc._on_close_position(_canonical_close_payload(
            event_key="cleared:1.77:1", batch_id="b-ua", correlation_id="c-ua", gross_pnl=-15.0,
            commission_amount=0.0, net_pnl=-15.0, pnl=-15.0, commission_pct=4.5))
    assert not (salvato.get("legacy") or {}).get("intraday_realized_pnl"), salvato.get("legacy")
    r = _banco(tmp_path, "uas_crash.db", max_daily_loss=50.0)
    _ambiente(r, "LIVE")
    assert r.rc._current_financial_scope() == conto_a
    r.rc.betfair_service.client = None
    r.rc.betfair_service.settings_service = None
    r.rc._on_close_position(_canonical_close_payload(
        event_key="cleared:1.77:1", batch_id="b-ua", correlation_id="c-ua", gross_pnl=-15.0,
        commission_amount=0.0, net_pnl=-15.0, pnl=-15.0, commission_pct=4.5))
    assert r.rc._monitor_daily_loss_breach(source="T", scope=conto_a)["daily_loss_amount"] == pytest.approx(35.0)


def test_perdita_non_attribuibile_contata_una_sola_volta_dopo_crash_e_riconsegna(tmp_path):
    """Grok su 3389ace9: conto A corrente -20, poi -15 non attribuibile; un get_status su A durante il sync salva A
    gia' aggiornato senza la chiave, crash, riavvio e riconsegna: A deve valere 35 (mai 50, doppio conteggio)."""
    c = _banco(tmp_path, "uad.db", max_daily_loss=100.0)
    _ambiente(c, "LIVE")
    conto_a = c.rc._current_financial_scope()
    _settle(c, -20.0)
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None

    def _sync_con_status(p):
        c.rc._monitor_daily_loss_breach(source="RUNTIME_STATUS_SNAPSHOT", scope=conto_a)
        _fotografa_al_crash(tmp_path, "uad.db", "uad_crash.db")
        raise SystemExit("crash durante il sync")

    def _consegna(rc):
        rc._on_close_position(_canonical_close_payload(
            event_key="cleared:1.78:1", batch_id="b-uad", correlation_id="c-uad", gross_pnl=-15.0,
            commission_amount=0.0, net_pnl=-15.0, pnl=-15.0, commission_pct=4.5))

    c.rc._sync_bankroll_post_settlement = _sync_con_status
    with pytest.raises(SystemExit):
        _consegna(c.rc)
    r = _banco(tmp_path, "uad_crash.db", max_daily_loss=100.0)
    _ambiente(r, "LIVE")
    r.rc.betfair_service.client = None
    r.rc.betfair_service.settings_service = None
    _consegna(r.rc)
    assert r.rc._monitor_daily_loss_breach(source="T", scope=conto_a)["daily_loss_amount"] == pytest.approx(35.0)


def test_eccezione_nel_settlement_non_attribuibile_non_blocca_i_salvataggi_successivi(tmp_path):
    """Eccezione (non crash) durante il sync di una perdita non attribuibile: il libro si completa comunque (ambiti,
    quarantena e chiave insieme), quindi i salvataggi successivi non restano rinviati. A -20, -15 non attribuibile con
    errore nel sync, poi A -10: dopo il riavvio A vale 45 e la riconsegna del -15 non lo cambia."""
    c = _banco(tmp_path, "uae.db", max_daily_loss=100.0)
    _ambiente(c, "LIVE")
    conto_a = c.rc._current_financial_scope()
    _settle(c, -20.0)
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None
    vero_sync = c.rc._sync_bankroll_post_settlement

    def _errore(p):
        raise RuntimeError("sync fallito")

    def _consegna(rc):
        rc._on_close_position(_canonical_close_payload(
            event_key="cleared:1.79:1", batch_id="b-uae", correlation_id="c-uae", gross_pnl=-15.0,
            commission_amount=0.0, net_pnl=-15.0, pnl=-15.0, commission_pct=4.5))

    c.rc._sync_bankroll_post_settlement = _errore
    with pytest.raises(RuntimeError):
        _consegna(c.rc)
    c.rc._sync_bankroll_post_settlement = vero_sync
    _ambiente(c, "LIVE")
    _settle(c, -10.0)
    _fotografa_al_crash(tmp_path, "uae.db", "uae_crash.db")
    r = _banco(tmp_path, "uae_crash.db", max_daily_loss=100.0)
    _ambiente(r, "LIVE")
    assert r.rc._monitor_daily_loss_breach(source="T", scope=conto_a)["daily_loss_amount"] == pytest.approx(45.0)
    r.rc.betfair_service.client = None
    r.rc.betfair_service.settings_service = None
    _consegna(r.rc)
    assert r.rc._monitor_daily_loss_breach(source="T", scope=conto_a)["daily_loss_amount"] == pytest.approx(45.0)


def test_crash_durante_la_propagazione_della_perdita_non_attribuibile(tmp_path):
    """Morte del processo mentre la perdita non attribuibile si propaga agli altri ambiti (copia del DB in quel punto,
    nessun `finally`): la riconsegna dopo il riavvio deve riapplicarla (conto A 35), mai saltarla con il libro vecchio."""
    c = _banco(tmp_path, "uap.db", max_daily_loss=100.0)
    _ambiente(c, "SIM")
    _settle(c, -5.0)
    _ambiente(c, "LIVE")
    conto_a = c.rc._current_financial_scope()
    _settle(c, -20.0)
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None
    vero = c.rc._monitor_daily_loss_breach
    preso = []

    def _monitor(**kw):
        if kw.get("source") == "RUNTIME_CLOSE_POSITION_UNATTRIBUTED" and not preso:
            _fotografa_al_crash(tmp_path, "uap.db", "uap_crash.db")
            preso.append(1)
        return vero(**kw)

    def _consegna(rc):
        rc._on_close_position(_canonical_close_payload(
            event_key="cleared:1.80:1", batch_id="b-uap", correlation_id="c-uap", gross_pnl=-15.0,
            commission_amount=0.0, net_pnl=-15.0, pnl=-15.0, commission_pct=4.5))

    c.rc._monitor_daily_loss_breach = _monitor
    _consegna(c.rc)
    assert preso, "precondizione: propagazione agli altri ambiti eseguita"
    r = _banco(tmp_path, "uap_crash.db", max_daily_loss=100.0)
    _ambiente(r, "LIVE")
    r.rc.betfair_service.client = None
    r.rc.betfair_service.settings_service = None
    _consegna(r.rc)
    assert r.rc._monitor_daily_loss_breach(source="T", scope=conto_a)["daily_loss_amount"] == pytest.approx(35.0)


@pytest.mark.parametrize("errore", [SystemExit, RuntimeError])
def test_interruzione_dopo_superamento_live_non_attribuibile_lascia_l_emergenza_globale(tmp_path, errore):
    """D su 4e1ff71b (M114): conto LIVE A -20, poi -15 non attribuibile (A 35 oltre il limite 30); il sync si
    interrompe; riavvio dallo stato salvato e riconsegna: l'Emergency Stop globale e' attivo e un segnale SIM non parte
    (superamento LIVE = emergenza globale, mai solo un blocco locale)."""
    c = _banco(tmp_path, "uaes.db", max_daily_loss=30.0)
    _ambiente(c, "LIVE")
    conto_a = c.rc._current_financial_scope()
    _settle(c, -20.0)
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None

    def _interrotto(p):
        raise errore("sync interrotto")

    def _consegna(rc):
        rc._on_close_position(_canonical_close_payload(
            event_key="cleared:1.81:1", batch_id="b-uaes", correlation_id="c-uaes", gross_pnl=-15.0,
            commission_amount=0.0, net_pnl=-15.0, pnl=-15.0, commission_pct=4.5))

    c.rc.mode = RuntimeMode.ACTIVE
    c.rc._sync_bankroll_post_settlement = _interrotto
    with pytest.raises(errore):
        _consegna(c.rc)
    r = _banco(tmp_path, "uaes.db", max_daily_loss=30.0)
    _ambiente(r, "LIVE")
    r.rc.betfair_service.client = None
    r.rc.betfair_service.settings_service = None
    _consegna(r.rc)
    assert r.rc._monitor_daily_loss_breach(source="T", scope=conto_a)["breached"] is True
    assert r.rc._emergency_stopped is True
    _ambiente(r, "SIM")
    assert _ingresso(r, "1.882")[0] == 0


def test_completamento_fallito_dopo_interruzione_non_blocca_i_salvataggi_in_silenzio(tmp_path):
    """Sol su 4e1ff71b: sync interrotto E completamento del libro fallito. La perdita non attribuibile non resta in
    attesa per sempre (che rinvierebbe ogni salvataggio successivo in silenzio): il gate segnala lo stato non salvato
    (nessun nuovo ingresso) e un salvataggio successivo torna a scrivere il libro."""
    import json
    c = _banco(tmp_path, "uaf.db", max_daily_loss=100.0)
    _ambiente(c, "LIVE")
    conto_a = c.rc._current_financial_scope()
    _settle(c, -20.0)
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None
    vero_monitor = c.rc._monitor_daily_loss_breach

    def _monitor(**kw):
        if kw.get("source") == "RUNTIME_CLOSE_POSITION_UNATTRIBUTED":
            raise RuntimeError("completamento fallito")
        return vero_monitor(**kw)

    def _errore(p):
        raise RuntimeError("sync fallito")

    c.rc._sync_bankroll_post_settlement = _errore
    c.rc._monitor_daily_loss_breach = _monitor
    with pytest.raises(RuntimeError):
        c.rc._on_close_position(_canonical_close_payload(
            event_key="cleared:1.82:1", batch_id="b-uaf", correlation_id="c-uaf", gross_pnl=-15.0,
            commission_amount=0.0, net_pnl=-15.0, pnl=-15.0, commission_pct=4.5))
    c.rc._monitor_daily_loss_breach = vero_monitor
    _ambiente(c, "LIVE")
    assert c.rc._limits_block_reason() == "daily_loss_state_non_persistito"
    # S2: il salvataggio successivo completa il libro (tutti gli ambiti + quarantena + chiave) e riapre il gate
    c.rc._monitor_daily_loss_breach(source="RUNTIME_STATUS_SNAPSHOT", scope=conto_a)
    salvato = json.loads(c.db.get_settings()["daily_loss_state"])
    assert salvato["scopes"][conto_a]["intraday_realized_pnl"] == pytest.approx(-35.0)
    assert "cleared:1.82:1" in json.dumps(salvato.get("settled") or {}), "chiave non salvata col libro completo"
    assert c.rc._limits_block_reason() != "daily_loss_state_non_persistito"


# ============================================================================ ciclo straordinario 11 (owner, opzione A poi B)
class _LiveSpia:
    """Client LIVE emulato: registra letture e cancellazioni (nessuna rete)."""

    def __init__(self, ordini=None):
        self.letture, self.cancellazioni, self.ordini = 0, [], list(ordini or [])

    def get_current_orders(self, market_ids=None):
        self.letture += 1
        return list(self.ordini)

    def cancel_orders(self, market_id, bet_ids=None, **kw):
        self.cancellazioni.append((market_id, bet_ids))
        self.ordini = [o for o in self.ordini if o.get("marketId") != market_id]
        return {"ok": True, "instructionReports": [{"betId": b} for b in (bet_ids or [])]}


def _settle_cleared(c, n, importo):
    c.rc._on_close_position(_canonical_close_payload(
        event_key=f"cleared:1.95{n}:{n}", batch_id=f"b-c11-{n}", correlation_id=f"c-c11-{n}",
        gross_pnl=importo, commission_amount=0.0, net_pnl=importo, pnl=importo))


def _eventi(c, nome):
    return [p for t, p in getattr(c.bus, "events", []) if t == nome]


def test_c1_emergenza_da_superamento_live_con_runtime_in_sim_agisce_sul_live(tmp_path):
    """Codex P1-1 (C1): superamento LIVE regolato mentre il runtime e' in SIM. L'Emergency Stop deve leggere e
    proteggere gli ordini del conto LIVE, non solo il broker simulato."""
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")
    live = _LiveSpia()
    c.rc.betfair_service.get_live_client = lambda: live
    c.rc.mode = RuntimeMode.ACTIVE
    _settle_cleared(c, 1, -40.0)
    assert c.rc._emergency_stopped is True
    assert live.letture >= 1, "emergenza da esposizione LIVE eseguita solo sulla sede SIM"


def test_c1_conto_live_irraggiungibile_emergenza_non_risolta(tmp_path):
    """C1: se il conto LIVE non e' raggiungibile l'emergenza resta NON risolta (intervento manuale), mai una
    cancellazione dichiarata riuscita."""
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "SIM")

    def _irraggiungibile():
        raise ConnectionError("LIVE non raggiungibile")

    c.rc.betfair_service.get_live_client = _irraggiungibile
    c.rc.mode = RuntimeMode.ACTIVE
    _settle_cleared(c, 2, -40.0)
    esiti = _eventi(c, "EMERGENCY_STOP_TRIGGERED")
    assert c.rc._emergency_stopped is True and esiti
    assert esiti[-1].get("manual_action_required") is True
    assert esiti[-1].get("verification") != "VERIFIED"


@pytest.mark.parametrize("punto", ["_mark_settlement_realized", "_escalate_stopped_live_breach"])
def test_s1_eccezione_dopo_superamento_live_attribuito_conserva_l_emergenza(tmp_path, monkeypatch, punto):
    """Sol-B1 (S1): superamento LIVE attribuito, eccezione fra il salvataggio e l'enforce; riavvio in SIM SENZA
    riconsegna: l'Emergency Stop globale e' attivo (il marker durevole non va perso)."""
    import core.runtime_controller as rcm
    c = _banco(tmp_path, "s1.db", max_daily_loss=30.0)
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.ACTIVE

    def _boom(self, *a, **k):
        raise RuntimeError("eccezione intermedia")

    monkeypatch.setattr(rcm.RuntimeController, punto, _boom)
    with pytest.raises(RuntimeError):
        _settle_cleared(c, 3, -40.0)
    monkeypatch.undo()
    r = _banco(tmp_path, "s1.db", max_daily_loss=30.0)
    _ambiente(r, "SIM")
    assert r.rc._emergency_stopped is True


def test_c3_perdita_di_sessione_live_blocca_gli_ingressi_sim_durante_il_sync(tmp_path):
    """Codex P1-3 (C3): perdita di SESSIONE LIVE (limite 30, nessun limite giornaliero) regolata mentre il runtime e'
    in SIM: durante l'I/O del sync nessun ingresso SIM parte (barriera globale prima del sync)."""
    c = _banco(tmp_path, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0                 # sessione avviata (come il test di sessione esistente)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    vero_sync = c.rc._sync_bankroll_post_settlement
    durante = []

    def _sync(p):
        durante.append(_ingresso(c, "1.961")[0])
        return vero_sync(p)

    c.rc._sync_bankroll_post_settlement = _sync
    _settle_cleared(c, 4, -40.0)
    assert durante == [0], durante


def test_g1_riserva_legacy_senza_ambito_non_va_al_conto_connesso(tmp_path):
    """Grok (G1): settlement `cleared:` con riserva nota ma senza ambito (legacy): mai il conto LIVE connesso al posto
    dell'identita' della prenotazione; perdita in quarantena (conta per tutti), profitto non accreditato."""
    c = _banco(tmp_path, max_daily_loss=30.0)
    _ambiente(c, "LIVE", conto=CONTO_B)
    perdita = c.rc._settlement_attribution(-15.0, {"financial_scope": ""}, {"event_key": "cleared:1.97:1"})
    profitto = c.rc._settlement_attribution(12.0, {"financial_scope": ""}, {"event_key": "cleared:1.97:2"})
    assert perdita == {"scope": financial_scope.UNVERIFIED_SCOPE, "kind": "unattributed_loss"}
    assert profitto == {"scope": financial_scope.UNVERIFIED_SCOPE, "kind": "unscoped_profit"}
    sim = c.rc._settlement_attribution(-15.0, {"financial_scope": ""}, {"event_key": "sim:1.97:3"})
    assert sim["scope"] == _sim()


def test_s2_doppio_guasto_non_lascia_un_libro_parziale(tmp_path):
    """Sol-B2 (S2): perdita non attribuibile con sync interrotto E completamento fallito; poi un get_status salva il
    libro e il processo muore. Dopo riavvio e riconsegna ogni ambito vale il vero: conto A 35 (mai 50, doppio
    conteggio), SIM 20 (mai sottostimata)."""
    c = _banco(tmp_path, "s2.db", max_daily_loss=100.0)
    _ambiente(c, "SIM")
    _settle(c, -5.0, event_key="sim:1.98:1")
    _ambiente(c, "LIVE")
    conto_a = c.rc._current_financial_scope()
    _settle(c, -20.0)
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None
    vero_monitor = c.rc._monitor_daily_loss_breach

    def _monitor(**kw):
        if kw.get("source") == "RUNTIME_CLOSE_POSITION_UNATTRIBUTED":
            raise RuntimeError("completamento fallito")
        return vero_monitor(**kw)

    def _errore(p):
        raise RuntimeError("sync fallito")

    def _consegna(rc):
        rc._on_close_position(_canonical_close_payload(
            event_key="cleared:1.99:1", batch_id="b-s2", correlation_id="c-s2", gross_pnl=-15.0,
            commission_amount=0.0, net_pnl=-15.0, pnl=-15.0, commission_pct=4.5))

    c.rc._sync_bankroll_post_settlement = _errore
    c.rc._monitor_daily_loss_breach = _monitor
    with pytest.raises(RuntimeError):
        _consegna(c.rc)
    c.rc._monitor_daily_loss_breach = vero_monitor
    c.rc._monitor_daily_loss_breach(source="RUNTIME_STATUS_SNAPSHOT", scope=conto_a)
    _fotografa_al_crash(tmp_path, "s2.db", "s2_crash.db")
    r = _banco(tmp_path, "s2_crash.db", max_daily_loss=100.0)
    _ambiente(r, "LIVE")
    r.rc.betfair_service.client = None
    r.rc.betfair_service.settings_service = None
    _consegna(r.rc)
    assert r.rc._monitor_daily_loss_breach(source="T", scope=conto_a)["daily_loss_amount"] == pytest.approx(35.0)
    assert r.rc._monitor_daily_loss_breach(source="T", scope=_sim())["daily_loss_amount"] == pytest.approx(20.0)


def _app_riavviabile(tmp_path, monkeypatch, tag):
    from tests.acceptance.test_issue437_pr04 import _App
    from tests.risk_config_fixtures import legacy_test_config
    from core import blackbox_runtime
    a = _App("headless", tmp_path, monkeypatch)
    if blackbox_runtime.get() is None:
        blackbox_runtime.install("bb/bb.sqlite", entrypoint=tag)
    monkeypatch.setattr(a.rt.settings_service, "load_roserpina_config",
                        lambda: legacy_test_config(liquidity_guard_enabled=False, max_daily_loss=30.0))
    a.rt.betfair_service.get_account_funds = lambda: {"available": 1000.0, "exposure": 0.0, "ok": True}
    a.avvia()
    return a


@pytest.mark.parametrize("percorso", ["manuale", "dutching"])
def test_c2_ingresso_sim_manuale_o_dutching_regolato_dopo_riavvio_resta_sim(tmp_path, monkeypatch, percorso):
    """Codex P1-2 (C2): puntata manuale / lotto Dutching in SIM, riavvio (F018), passaggio a LIVE, settlement
    tardivo -40 (limite 30): la perdita resta SIM (blocco locale), mai quarantena ne' Emergency Stop globale."""
    from tests.acceptance.test_issue523_dutching_manual_bet import _batch, _manual
    monkeypatch.chdir(tmp_path)
    a = _app_riavviabile(tmp_path, monkeypatch, "c2a")
    try:
        a.rt.mode = RuntimeMode.ACTIVE
        esito = (a.app.dutching_controller.manual_bet(_manual(stake=2.0)) if percorso == "manuale"
                 else a.app.dutching_controller.submit_dutching(_batch(total_stake=10.0)))
        a.svuota_bus()
        assert esito["ok"] is True, esito
        tavolo = [t for t in a.rt.table_manager.snapshot() if t.get("current_event_key")][0]
    finally:
        a.chiudi()
    b = _app_riavviabile(tmp_path, monkeypatch, "c2b")
    try:
        ricostruiti = [t for t in b.rt.table_manager.snapshot() if t.get("current_event_key")]
        assert ricostruiti and all((t.get("meta") or {}).get("financial_scope") == _sim() for t in ricostruiti)
        b.rt.config.max_daily_loss = 30.0
        b.rt.set_simulation_mode(False)
        b.rt.betfair_service.client = types.SimpleNamespace(username=CONTO_A)
        b.rt.mode = RuntimeMode.ACTIVE
        b.rt._on_close_position(_canonical_close_payload(
            event_key=tavolo.get("current_event_key"), table_id=tavolo.get("table_id"), market_id=tavolo.get("market_id"),
            batch_id="b-c2", correlation_id="c-c2", gross_pnl=-40.0, commission_amount=0.0, net_pnl=-40.0, pnl=-40.0))
        assert b.rt._emergency_stopped is False
        assert b.rt._daily_loss_state_for(_sim())["daily_loss_amount"] == pytest.approx(40.0)
        assert b.rt._daily_loss_unattributed is None
    finally:
        b.chiudi()


def test_scenario_integrato_due_conti_live_piu_tavoli_sim_emergenza_errore_crash_riavvio(tmp_path):
    """Owner (ciclo 11): due conti LIVE, piu' tavoli aperti, processo temporaneamente in SIM, Emergency Stop, errore
    durante un settlement, crash e riavvio. Oracoli:
    - l'emergenza nata dal LIVE agisce sulla sede LIVE: l'ordine aperto del bot e' cancellato, quello estraneo e'
      segnalato come esposizione da gestire (mai cancellato alla cieca, mai dichiarato risolto);
    - il blocco sopravvive al riavvio: nessun nuovo ordine, ne' LIVE ne' SIM;
    - P&L e perdite restano sul conto giusto (A: perdita 40; B: profitto, nessuna perdita; SIM: 5)."""
    c = _banco(tmp_path, "int.db", max_daily_loss=30.0)
    _ambiente(c, "LIVE", conto=CONTO_A)
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo_a = _riserva(c, "1.881")
    # Il LIVE del banco e' emulato sul SimulationBroker, che salva la request con simulation_mode=True: la riga
    # dell'ordine del conto A si registra come LIVE, come la salverebbe un ordine LIVE reale (venue dell'emergenza).
    for riga in c.rc.db._execute("SELECT id, payload_json FROM orders WHERE customer_ref = ?",
                                 (tavolo_a.meta["customer_ref"],), fetch=True, commit=False):
        dati = json.loads(dict(riga)["payload_json"])
        dati["simulation_mode"] = False
        c.rc.db._execute("UPDATE orders SET payload_json = ? WHERE id = ?", (json.dumps(dati), dict(riga)["id"]))
    # Conto B: il gate S18 del drawdown (#631, fuori perimetro) tiene un solo conto LIVE per processo e rifiuta gli
    # ingressi di un secondo conto; la prenotazione di B (ordine gia' in essere) e' quindi creata direttamente nel
    # gestore dei tavoli, con la sua identita' (ambito B + customer_ref), come la ricostruisce F018.
    c.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_B)
    conto_b = c.rc._financial_scope_for(False)
    c.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)
    tavolo_b = c.rc.table_manager.allocate(event_key="evt-int-b")
    c.rc.table_manager.activate(table_id=tavolo_b.table_id, event_key="evt-int-b", exposure=2.0, market_id="1.882",
                                selection_id=1, meta={"financial_scope": conto_b, "customer_ref": "ref-int-b"})
    tavolo_b = c.rc.table_manager.get_table(tavolo_b.table_id)
    tavolo_sim = c.rc.table_manager.allocate(event_key="evt-int-sim")        # idem per la prenotazione SIM
    c.rc.table_manager.activate(table_id=tavolo_sim.table_id, event_key="evt-int-sim", exposure=2.0, market_id="1.883",
                                selection_id=1, meta={"financial_scope": _sim(), "customer_ref": "ref-int-sim"})
    tavolo_sim = c.rc.table_manager.get_table(tavolo_sim.table_id)
    # Come una prenotazione reale (e come la ricostruisce F018), l'ordine SIM ha la sua riga in `orders` con l'ambito
    # registrato (C2): senza, una riconsegna dopo il rilascio non ne recupera l'identita' e va in quarantena
    # (Codex P1 su b902eca3: mai l'ambito corrente per supposizione).
    import time as _time
    _ora = _time.time()
    c.rc.db._execute("INSERT INTO orders (customer_ref, correlation_id, status, payload_json, response_json, created_at,"
                     " updated_at) VALUES (?,?,?,?,?,?,?)",
                     ("ref-int-sim", "corr-ref-int-sim", "PLACED",
                      json.dumps({"simulation_mode": True, "market_id": "1.883", "financial_scope": _sim()}),
                      json.dumps({"betId": "S-1", "status": "SUCCESS"}), _ora, _ora), commit=True)
    _ambiente(c, "SIM")
    _settle_tavolo(c, tavolo_b, 12.0)                       # profitto del conto B, dalla sua prenotazione
    # processo in SIM, conto LIVE A connesso; sull'exchange: un ordine aperto del bot e uno estraneo
    live = _LiveSpia([
        {"betId": "A-1", "customerOrderRef": tavolo_a.meta["customer_ref"], "marketId": "1.881",
         "status": "EXECUTABLE", "sizeRemaining": 2.0},
        {"betId": "X-9", "customerOrderRef": "", "marketId": "1.999", "status": "EXECUTABLE", "sizeRemaining": 5.0}])
    c.rc.betfair_service.get_live_client = lambda: live
    c.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)
    conto_a = c.rc._financial_scope_for(False)
    _settle_tavolo(c, tavolo_a, -40.0)                      # superamento LIVE A (limite 30) col runtime in SIM
    assert c.rc._emergency_stopped is True
    assert ("1.881", ["A-1"]) in live.cancellazioni, live.cancellazioni
    assert all(m != "1.999" for m, _ in live.cancellazioni), "ordine estraneo cancellato alla cieca"
    esito = _eventi(c, "EMERGENCY_STOP_TRIGGERED")[-1]
    assert any(o.get("bet_id") == "X-9" for o in esito.get("unattributed_open_orders") or []), esito
    assert esito.get("manual_action_required") is True, "esposizione estranea dichiarata risolta"

    # errore durante un settlement (SIM -5), poi il processo muore
    def _errore(p):
        raise RuntimeError("sync fallito")

    c.rc._sync_bankroll_post_settlement = _errore
    with pytest.raises(RuntimeError):
        c.rc._on_close_position(_canonical_close_payload(
            event_key=tavolo_sim.current_event_key, table_id=tavolo_sim.table_id,
            customer_ref=tavolo_sim.meta["customer_ref"], market_id=tavolo_sim.market_id,
            batch_id="b-int-s", correlation_id="c-int-s", gross_pnl=-5.0, commission_amount=0.0,
            net_pnl=-5.0, pnl=-5.0))
    _fotografa_al_crash(tmp_path, "int.db", "int_crash.db")

    r = _banco(tmp_path, "int_crash.db", max_daily_loss=30.0)
    assert r.rc._emergency_stopped is True, "blocco perso al riavvio"
    _ambiente(r, "LIVE", conto=CONTO_A)
    assert _ingresso(r, "1.884")[0] == 0, "ordine LIVE partito dopo il riavvio in emergenza"
    _ambiente(r, "SIM")
    assert _ingresso(r, "1.885")[0] == 0, "ordine SIM partito dopo il riavvio in emergenza"
    r.rc._on_close_position(_canonical_close_payload(            # riconsegna del settlement interrotto
        event_key=tavolo_sim.current_event_key, table_id=tavolo_sim.table_id,
        customer_ref=tavolo_sim.meta["customer_ref"], market_id=tavolo_sim.market_id,
        batch_id="b-int-s", correlation_id="c-int-s", gross_pnl=-5.0, commission_amount=0.0, net_pnl=-5.0, pnl=-5.0))
    stato = lambda k: r.rc._monitor_daily_loss_breach(source="T", scope=k)   # noqa: E731
    assert stato(conto_a)["daily_loss_amount"] == pytest.approx(40.0) and stato(conto_a)["breached"] is True
    assert stato(conto_b)["daily_loss_amount"] == pytest.approx(0.0), "il profitto di B non e' sul conto B"
    assert stato(_sim())["daily_loss_amount"] == pytest.approx(5.0)
    assert r.rc._daily_loss_unattributed is None, "attribuzione persa: P&L finito in quarantena"


def test_c3_perdita_di_sessione_live_blocca_anche_gli_ingressi_live_durante_il_sync(tmp_path):
    """C3, variante LIVE (owner): perdita di sessione LIVE con il runtime in LIVE; nella finestra del sync nemmeno un
    ingresso LIVE parte."""
    c = _banco(tmp_path, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.ACTIVE
    vero_sync = c.rc._sync_bankroll_post_settlement
    durante = []

    def _sync(p):
        durante.append(_ingresso(c, "1.962")[0])
        return vero_sync(p)

    c.rc._sync_bankroll_post_settlement = _sync
    _settle_cleared(c, 5, -40.0)
    assert durante == [0], durante


def test_c3_perdita_di_sessione_del_conto_a_blocca_l_ammissione_del_conto_b_durante_il_sync(tmp_path):
    """C3 (D M116-bis): runtime LIVE sul conto B, perdita di SESSIONE del conto LIVE A (prenotazione di A, -12 su limite
    10): nella finestra del sync l'ammissione non e' libera (`_limits_block_reason` non vuoto)."""
    c = _banco(tmp_path, max_session_loss=10.0)
    c.rc._session_pnl_baseline = 0.0
    _ambiente(c, "LIVE", conto=CONTO_A)
    conto_a = c.rc._financial_scope_for(False)
    _ambiente(c, "LIVE", conto=CONTO_B)
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo = c.rc.table_manager.allocate(event_key="evt-c3-a")
    c.rc.table_manager.activate(table_id=tavolo.table_id, event_key="evt-c3-a", exposure=2.0, market_id="1.963",
                                selection_id=1, meta={"financial_scope": conto_a, "customer_ref": "ref-c3-a"})
    tavolo = c.rc.table_manager.get_table(tavolo.table_id)
    vero_sync = c.rc._sync_bankroll_post_settlement
    durante = []

    def _sync(p):
        durante.append(c.rc._limits_block_reason())
        return vero_sync(p)

    c.rc._sync_bankroll_post_settlement = _sync
    _settle_tavolo(c, tavolo, -12.0)
    assert durante and durante[0], durante


def test_c3_comando_gia_in_coda_non_parte_nella_finestra_del_sync(tmp_path):
    """C3: un ingresso SIM approvato PRIMA del settlement (CMD_QUICK_BET in coda) e consegnato dopo la rilettura del saldo
    ma prima dello stop globale di una perdita di sessione LIVE non arriva al broker (controllo finale d'invio)."""
    c = _banco(tmp_path, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    trattenuti, reale = [], c.bus.publish

    def _trattieni(topic, payload=None, *a, **k):
        if topic == "CMD_QUICK_BET":
            trattenuti.append(payload)
            return None
        return reale(topic, payload, *a, **k)

    c.bus.publish = _trattieni
    c.rc._on_signal_received(_signal(stake=2.0, bet_type="BACK", market_id="1.964"))
    c.bus.publish = reale
    assert trattenuti, "precondizione: ingresso approvato e in coda"
    vero_sync = c.rc._sync_bankroll_post_settlement
    durante = []

    def _sync(p):
        esito = vero_sync(p)              # saldo gia' riletto: resta solo la barriera fino allo stop globale
        prima = _ordini(c)
        for comando in trattenuti:
            reale("CMD_QUICK_BET", comando)
        durante.append(_ordini(c) - prima)
        return esito

    c.rc._sync_bankroll_post_settlement = _sync
    _settle_cleared(c, 6, -40.0)
    assert durante == [0], durante


# ============================================================================ ciclo straordinario 12 (owner: sicurezza ordini LIVE)
def _segna_riga_live(c, customer_ref):
    """Il LIVE del banco e' emulato sul SimulationBroker (request salvata con simulation_mode=True): la riga si registra
    come LIVE, come la salverebbe un ordine LIVE reale."""
    for riga in c.rc.db._execute("SELECT id, payload_json FROM orders WHERE customer_ref = ?", (customer_ref,),
                                 fetch=True, commit=False):
        dati = json.loads(dict(riga)["payload_json"])
        dati["simulation_mode"] = False
        c.rc.db._execute("UPDATE orders SET payload_json = ? WHERE id = ?", (json.dumps(dati), dict(riga)["id"]))


def test_c12_riavvio_in_emergenza_riconcilia_gli_ordini_live_del_bot(tmp_path, monkeypatch):
    """Ciclo 12 (M117): superamento LIVE del conto A col runtime in SIM, errore prima dell'enforce (nessuna cancellazione
    nel processo), crash, riavvio. Dopo il riavvio l'emergenza resta e gli ordini LIVE del bot vengono cancellati con
    verifica sul conto LIVE; quelli estranei sono segnalati come esposizione irrisolta (mai cancellati alla cieca)."""
    import core.runtime_controller as rcm
    c = _banco(tmp_path, "c12.db", max_daily_loss=30.0)
    _ambiente(c, "LIVE", conto=CONTO_A)
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo_a = _riserva(c, "1.981")
    _segna_riga_live(c, tavolo_a.meta["customer_ref"])
    _ambiente(c, "SIM")
    c.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)
    c.rc.mode = RuntimeMode.ACTIVE

    def _boom(self, *a, **k):
        raise RuntimeError("errore dopo il salvataggio")

    monkeypatch.setattr(rcm.RuntimeController, "_mark_settlement_realized", _boom)
    with pytest.raises(RuntimeError):
        _settle_tavolo(c, tavolo_a, -40.0)
    monkeypatch.undo()
    _fotografa_al_crash(tmp_path, "c12.db", "c12_crash.db")

    r = _banco(tmp_path, "c12_crash.db", max_daily_loss=30.0)
    assert r.rc._emergency_stopped is True
    live = _LiveSpia([
        {"betId": "A-1", "customerOrderRef": tavolo_a.meta["customer_ref"], "marketId": "1.981",
         "status": "EXECUTABLE", "sizeRemaining": 2.0},
        {"betId": "X-9", "customerOrderRef": "", "marketId": "1.999", "status": "EXECUTABLE", "sizeRemaining": 5.0}])
    r.rc.betfair_service.get_live_client = lambda: live
    _ambiente(r, "SIM")
    assert _ingresso(r, "1.982")[0] == 0, "ordine partito dopo il riavvio in emergenza"
    assert ("1.981", ["A-1"]) in live.cancellazioni, (live.letture, live.cancellazioni)
    assert all(m != "1.999" for m, _ in live.cancellazioni), "ordine estraneo cancellato alla cieca"
    esiti = _eventi(r, "EMERGENCY_STOP_RECONCILED")
    assert esiti and esiti[-1]["manual_action_required"] is True, "esposizione estranea non segnalata"
    assert any(o.get("bet_id") == "X-9" for o in esiti[-1].get("unattributed_open_orders") or [])
    assert r.rc._emergency_stopped is True


def test_c12_riavvio_in_emergenza_senza_conto_live_resta_irrisolto(tmp_path):
    """Ciclo 12: conto LIVE irraggiungibile alla riconciliazione dopo il riavvio => emergenza mantenuta, esposizione
    dichiarata irrisolta (intervento manuale), nessuna verifica dichiarata riuscita; si ritenta all'evento successivo."""
    c = _banco(tmp_path, "c12b.db", max_daily_loss=30.0)
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.ACTIVE
    c.rc.betfair_service.get_live_client = lambda: _LiveSpia()
    _settle_cleared(c, 8, -40.0)
    assert c.rc._emergency_stopped is True
    r = _banco(tmp_path, "c12b.db", max_daily_loss=30.0)

    def _irraggiungibile():
        raise ConnectionError("LIVE non raggiungibile")

    r.rc.betfair_service.get_live_client = _irraggiungibile
    _ambiente(r, "SIM")
    _ingresso(r, "1.983")
    esiti = _eventi(r, "EMERGENCY_STOP_RECONCILED")
    assert esiti and esiti[-1]["manual_action_required"] is True and esiti[-1].get("verification") != "VERIFIED"
    assert r.rc._emergency_stopped is True
    spia = _LiveSpia()
    r.rc.betfair_service.get_live_client = lambda: spia
    _ingresso(r, "1.984")
    assert spia.letture >= 1, "riconciliazione non ritentata"


def test_c12_stop_di_sessione_live_col_runtime_in_sim_protegge_il_conto_live(tmp_path):
    """Ciclo 12 (N2, Codex+Grok): perdita di sessione LIVE regolata col runtime in SIM: lo stop globale protegge il conto
    LIVE (ordine del bot cancellato con verifica; ordine estraneo segnalato), non solo la sede SIM."""
    c = _banco(tmp_path, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo = _riserva(c, "1.985")
    _segna_riga_live(c, tavolo.meta["customer_ref"])
    _ambiente(c, "SIM")
    c.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)
    c.rc._route_cashout_signal = lambda segnale: None
    live = _LiveSpia([
        {"betId": "L-1", "customerOrderRef": tavolo.meta["customer_ref"], "marketId": "1.985",
         "status": "EXECUTABLE", "sizeRemaining": 2.0},
        {"betId": "X-8", "customerOrderRef": "", "marketId": "1.998", "status": "EXECUTABLE", "sizeRemaining": 5.0}])
    c.rc.betfair_service.get_live_client = lambda: live
    c.rc.mode = RuntimeMode.ACTIVE
    _settle_tavolo(c, tavolo, -40.0)
    assert c.rc._risk_stop_reason.startswith("session_loss_breached") and "@ambito=" not in c.rc._risk_stop_reason
    assert ("1.985", ["L-1"]) in live.cancellazioni, (live.letture, live.cancellazioni)
    assert all(m != "1.998" for m, _ in live.cancellazioni)
    esiti = _eventi(c, "RISK_STOP_LIVE_PROTECTION")
    assert esiti and esiti[-1]["manual_action_required"] is True


def test_c12_perdita_non_attribuibile_arma_la_barriera_di_sessione_cumulativa(tmp_path):
    """Ciclo 12 (N1, Codex): LIVE -20, passaggio a SIM, perdita NON attribuibile -15 (sessione LIVE cumulativa 35,
    limite 30): durante il sync la barriera globale e' armata e l'ammissione e' chiusa."""
    c = _banco(tmp_path, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    _ambiente(c, "LIVE")
    _settle(c, -20.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    c.rc.betfair_service.client = None
    c.rc.betfair_service.settings_service = None
    vero_sync = c.rc._sync_bankroll_post_settlement
    durante = []

    def _sync(p):
        durante.append(c.rc._limits_block_reason())
        return vero_sync(p)

    c.rc._sync_bankroll_post_settlement = _sync
    c.rc._on_close_position(_canonical_close_payload(
        event_key="cleared:1.986:1", batch_id="b-c12n1", correlation_id="c-c12n1", gross_pnl=-15.0,
        commission_amount=0.0, net_pnl=-15.0, pnl=-15.0))
    assert durante == ["session_loss_breached:in_attesa_dello_stop_globale"], durante


# ============================================================================ ciclo 12, estensione (S-a, S-b, R1, R2)
@pytest.mark.parametrize("variante", ["sim_corrente", "conto_live_b_corrente"])
def test_s_a_barriera_di_sessione_armata_prima_che_il_pnl_sia_visibile(tmp_path, variante):
    """S-a (Sol su b643d348): nell'istante in cui il P&L di una perdita di sessione LIVE (limite 30, -40) diventa visibile,
    l'ammissione e il controllo finale d'invio sono gia' chiusi (barriera armata PRIMA dell'applicazione)."""
    import core.runtime_controller as rcm
    c = _banco(tmp_path, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    if variante == "sim_corrente":
        _ambiente(c, "SIM")
        c.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)
    else:
        _ambiente(c, "LIVE", conto=CONTO_B)
    c.rc.mode = RuntimeMode.ACTIVE
    vista = []
    vero = rcm.RuntimeController._apply_and_attribute_realized_pnl

    def _spia(self, *a, **k):
        esito = vero(self, *a, **k)
        vista.append((self._limits_block_reason(), self.drawdown_entry_block_reason()))
        return esito

    rcm.RuntimeController._apply_and_attribute_realized_pnl = _spia
    try:
        if variante == "sim_corrente":
            _settle_cleared(c, 9, -40.0)
        else:
            conto_a = c.rc._financial_scope_for(False).replace(
                financial_scope.parts(c.rc._financial_scope_for(False))[2],
                financial_scope.parts(financial_scope.runtime_scope(simulated=False, live_account=CONTO_A))[2])
            tavolo = c.rc.table_manager.allocate(event_key="evt-sa")
            c.rc.table_manager.activate(table_id=tavolo.table_id, event_key="evt-sa", exposure=2.0, market_id="1.991",
                                        selection_id=1, meta={"financial_scope": conto_a, "customer_ref": "ref-sa"})
            _settle_tavolo(c, c.rc.table_manager.get_table(tavolo.table_id), -40.0)
    finally:
        rcm.RuntimeController._apply_and_attribute_realized_pnl = vero
    assert vista and vista[0][0] and vista[0][1], vista


@pytest.mark.parametrize("punto", ["_sync_bankroll_post_settlement", "_mark_settlement_realized",
                                   "_publish_session_loss_breach"])
@pytest.mark.parametrize("limiti", ["solo_sessione", "combinati"])
def test_s_b_crash_prima_dello_stop_di_sessione_lo_ripristina_al_riavvio(tmp_path, punto, limiti):
    """S-b (Sol su b643d348): perdita di sessione LIVE (-40, limite 30) col runtime in SIM; il processo muore nel punto
    indicato (copia del DB, nessun `finally`). Al riavvio e alla riconsegna: RISK_STOP globale di sessione presente,
    riconciliazione LIVE eseguita o in attesa, nessun doppio conteggio della perdita."""
    import core.runtime_controller as rcm
    cfg = {"max_session_loss": 30.0}
    if limiti == "combinati":
        cfg["max_daily_loss"] = 100.0
    c = _banco(tmp_path, "sb.db", **cfg)
    c.rc._session_pnl_baseline = 0.0
    _ambiente(c, "SIM")
    c.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)
    c.rc.mode = RuntimeMode.ACTIVE
    vero = getattr(rcm.RuntimeController, punto)

    def _muore(self, *a, **k):
        _fotografa_al_crash(tmp_path, "sb.db", "sb_crash.db")
        raise SystemExit("crash")

    setattr(rcm.RuntimeController, punto, _muore)
    try:
        with pytest.raises(SystemExit):
            _settle_cleared(c, 10, -40.0)
    finally:
        setattr(rcm.RuntimeController, punto, vero)
    r = _banco(tmp_path, "sb_crash.db", **cfg)
    r.rc._session_pnl_baseline = 0.0
    spia = _LiveSpia()
    r.rc.betfair_service.get_live_client = lambda: spia
    _ambiente(r, "SIM")
    r.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)
    _settle_cleared(r, 10, -40.0)                               # riconsegna
    assert r.rc._risk_stop_reason.startswith("session_loss_breached"), r.rc._risk_stop_reason
    assert "@ambito=" not in r.rc._risk_stop_reason, "stop di sessione LIVE non globale"
    assert spia.letture >= 1 or r.rc.get_status()["live_exposure"]["reconcile_pending"], "nessuna riconciliazione LIVE"
    assert _ingresso(r, "1.992")[0] == 0
    assert r.rc.risk_desk.realized_pnl >= -40.0 - 1e-6, "perdita contata due volte"


def test_r2_riconsegna_di_un_settlement_gia_contabilizzato_non_riscrive_il_motivo(tmp_path):
    """R2 (owner, D M119): un settlement gia' contabilizzato e riconsegnato non si somma di nuovo alla perdita proiettata:
    nessuna barriera globale nuova e il motivo durevole dell'emergenza non diventa 50 (35+15)."""
    c = _banco(tmp_path, "r2.db", max_daily_loss=30.0)
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.ACTIVE
    c.rc.betfair_service.get_live_client = lambda: _LiveSpia()
    _settle_cleared(c, 11, -20.0)
    _settle_cleared(c, 12, -15.0)                              # 35 >= 30: Emergency Stop
    assert c.rc._emergency_stopped is True
    motivo = (c.db.get_settings() or {}).get("emergency_reason")
    c.rc.reset_emergency()
    c.rc.mode = RuntimeMode.ACTIVE
    _settle_cleared(c, 12, -15.0)                              # riconsegna della stessa chiave
    assert c.rc._daily_loss_pending_stop is False, "barriera globale armata da un duplicato"
    durevole = str((c.db.get_settings() or {}).get("emergency_reason") or "")
    assert "50" not in durevole, (motivo, durevole)


def test_r1_esposizione_live_irrisolta_esplicita_persistente_e_mai_zero_se_sconosciuta(tmp_path):
    """R1 (owner, D M119): dopo un'emergenza con cancellazione inefficace, `get_status` espone l'esposizione LIVE
    NON_RICONCILIATA con gli ordini e l'importo; resta dopo un riavvio e con il runtime in SIM; conto irraggiungibile =>
    SCONOSCIUTA con importo None (mai 0). `total_exposure` invariato nel significato."""
    c = _banco(tmp_path, "r1.db", max_daily_loss=30.0)
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo = _riserva(c, "1.993")
    _segna_riga_live(c, tavolo.meta["customer_ref"])

    class _Ostinato(_LiveSpia):
        def cancel_orders(self, market_id, bet_ids=None, **kw):     # cancellazione accettata ma inefficace
            self.cancellazioni.append((market_id, bet_ids))
            return {"ok": True, "instructionReports": []}

    # LAY non abbinato, stake residuo 2 @ 4.0: liability 2 x (4 - 1) = 6.00 (lo stake 2.00 la sottostimerebbe)
    live = _Ostinato([{"betId": "R-1", "customerOrderRef": tavolo.meta["customer_ref"], "marketId": "1.993",
                       "status": "EXECUTABLE", "sizeRemaining": 2.0, "side": "LAY",
                       "priceSize": {"price": 4.0, "size": 2.0}}])
    c.rc.betfair_service.get_live_client = lambda: live
    _settle_tavolo(c, tavolo, -40.0)
    stato = c.rc.get_status()
    assert "total_exposure" in stato
    vista = stato["live_exposure"]
    assert vista["state"] == "NON_RICONCILIATA" and vista["amount"] == pytest.approx(6.0), vista
    assert vista["amount_display"] == "6.00" and vista["amount_basis"] == "STIMA_CONSERVATIVA_LORDA"
    assert [o.get("bet_id") for o in vista["open_orders"]] == ["R-1"] and vista["manual_action_required"] is True
    r = _banco(tmp_path, "r1.db", max_daily_loss=30.0)
    r.rc.betfair_service.get_live_client = lambda: (_ for _ in ()).throw(ConnectionError("LIVE non raggiungibile"))
    _ambiente(r, "SIM")
    _ingresso(r, "1.994")                                       # ritenta la riconciliazione: conto irraggiungibile
    vista = r.rc.get_status()["live_exposure"]
    assert vista["state"] == "SCONOSCIUTA" and vista["amount"] is None and vista["amount_display"] == "SCONOSCIUTO"
    assert vista["manual_action_required"] is True


def test_r1_testo_unico_per_gui_e_headless():
    from core.runtime_controller import format_live_exposure
    assert format_live_exposure({"state": "NESSUNA"}) == "nessuna"
    testo = format_live_exposure({"state": "SCONOSCIUTA", "amount_display": "SCONOSCIUTO", "reconcile_pending": True})
    assert "SCONOSCIUTO" in testo and "INTERVENTO MANUALE" in testo and "0.00" not in testo
    testo = format_live_exposure({"state": "NON_RICONCILIATA", "amount": 2.0, "amount_display": "2.00",
                                  "open_orders": [{"bet_id": "R-1"}], "unattributed_open_orders": [{"bet_id": "X"}]})
    assert "NON RICONCILIATA: liability 2.00 (stima lorda)" in testo
    assert "1 ordini del bot aperti" in testo and "1 ordini estranei" in testo
    testo = format_live_exposure({"state": "NON_RICONCILIATA", "amount": None, "amount_display": "SCONOSCIUTO",
                                  "matched_positions": [{"bet_id": "M"}]})
    assert "liability SCONOSCIUTO" in testo and "stima" not in testo and "1 posizioni abbinate" in testo
    assert "posizione" in format_live_exposure({"state": "VERIFICATA"})


def test_r1_preflight_headless_mai_pronto_con_esposizione_live_irrisolta(monkeypatch, capsys):
    import headless_main
    app = headless_main.HeadlessApp.__new__(headless_main.HeadlessApp)
    app.runtime = types.SimpleNamespace(
        get_deploy_gate_status=lambda **k: {"allowed": True, "details": {"readiness_payload": {"level": "READY"}}},
        _live_exposure_status=lambda: {"state": "SCONOSCIUTA", "amount_display": "SCONOSCIUTO"})
    monkeypatch.setattr(app, "_parse_args", lambda: {"execution_mode": "LIVE", "live_enabled": True,
                                                     "live_readiness_ok": True}, raising=False)
    codice = app._run_preflight()
    uscita = capsys.readouterr().out
    assert codice == 2 and "SCONOSCIUTO" in uscita and "NON PRONTO" in uscita
    # Runtime che non espone lo stato dell'esposizione LIVE (illeggibile): SCONOSCIUTA, mai "nessuna", mai pronto.
    app.runtime = types.SimpleNamespace(
        get_deploy_gate_status=lambda **k: {"allowed": True, "details": {"readiness_payload": {"level": "READY"}}})
    codice = app._run_preflight()
    uscita = capsys.readouterr().out
    assert codice == 2 and "SCONOSCIUTO" in uscita and "NON PRONTO" in uscita


def test_s_b_interruzione_nel_processo_ripristina_subito_lo_stop_globale(tmp_path, monkeypatch):
    """S-b: errore (non crash) prima della pubblicazione dello stop di sessione LIVE: il processo continua con il
    RISK_STOP globale gia' ripristinato e durevole (nessuna finestra in cui la sola barriera in memoria lo sostituisce)."""
    import core.runtime_controller as rcm
    from core import risk_stop as stop_rules
    c = _banco(tmp_path, "sbi.db", max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    _ambiente(c, "SIM")
    c.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)
    c.rc.mode = RuntimeMode.ACTIVE

    def _errore(self, *a, **k):
        raise RuntimeError("pubblicazione fallita")

    monkeypatch.setattr(rcm.RuntimeController, "_publish_session_loss_breach", _errore)
    with pytest.raises(RuntimeError):
        _settle_cleared(c, 13, -40.0)
    assert c.rc._risk_stop_reason.startswith("session_loss_breached") and "@ambito=" not in c.rc._risk_stop_reason
    assert stop_rules.restore_risk_stop(c.db) == c.rc._risk_stop_reason, "stop non durevole"
    assert c.rc.__dict__.get("_session_loss_pending_global") is False


# ============================================================================ M115 (owner GO): recovery senza riavvio
def _m115_banco(tmp_path, nome, intervallo=0.05, **cfg):
    """Ordine LIVE del bot sul conto A (riga LIVE), runtime poi in SIM; recovery dedicato con intervallo breve."""
    c = _banco(tmp_path, nome, **cfg)
    c.rc._live_recovery_interval_s = intervallo
    _ambiente(c, "LIVE", conto=CONTO_A)
    c.rc.mode = RuntimeMode.ACTIVE
    tavolo = _riserva(c, "1.971")
    _segna_riga_live(c, tavolo.meta["customer_ref"])
    _ambiente(c, "SIM")
    c.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)
    c.rc._route_cashout_signal = lambda segnale: None
    c.rc.mode = RuntimeMode.ACTIVE
    return c, tavolo


def _ordine_bot(tavolo, bet="A-1"):
    return {"betId": bet, "customerOrderRef": tavolo.meta["customer_ref"], "marketId": "1.971",
            "status": "EXECUTABLE", "sizeRemaining": 2.0}


def _attendi(condizione, secondi=3.0):
    import time as _t
    fine = _t.monotonic() + secondi
    while _t.monotonic() < fine:
        if condizione():
            return True
        _t.sleep(0.01)
    return condizione()


def _errore_nel_settlement(monkeypatch, punto, eccezione):
    import core.runtime_controller as rcm
    orig = getattr(rcm.RuntimeController, punto)

    def rotto(self, *a, **k):
        if punto == "_mark_settlement_realized":
            orig(self, *a, **k)
        raise eccezione("errore nel settlement, il processo continua")

    monkeypatch.setattr(rcm.RuntimeController, punto, rotto)


@pytest.mark.parametrize("punto", ["_mark_settlement_realized", "_escalate_stopped_live_breach",
                                   "_sync_bankroll_post_settlement"])
@pytest.mark.parametrize("eccezione", [RuntimeError, SystemExit])
def test_m115_errore_nel_settlement_gestisce_il_live_senza_riavvio_ne_eventi(tmp_path, monkeypatch, punto, eccezione):
    """M115: superamento giornaliero LIVE con errore nel settlement dopo l'applicazione del P&L; il processo continua e
    NON arriva alcun evento successivo. Ingressi bloccati, ordine LIVE del bot cancellato e verificato sul conto LIVE,
    ordine estraneo segnalato (intervento manuale), mai cancellato."""
    c, tavolo = _m115_banco(tmp_path, f"m115_{punto}_{eccezione.__name__}.db", max_daily_loss=30.0)
    live = _LiveSpia([_ordine_bot(tavolo),
                      {"betId": "X-7", "customerOrderRef": "", "marketId": "1.997", "status": "EXECUTABLE",
                       "sizeRemaining": 4.0}])
    c.rc.betfair_service.get_live_client = lambda: live
    with monkeypatch.context() as m:
        _errore_nel_settlement(m, punto, eccezione)
        with pytest.raises(eccezione):
            _settle_tavolo(c, tavolo, -40.0)
    # nessun nuovo evento: lo stato si osserva subito
    assert c.rc._emergency_stopped is True, "stop globale non attivo nello stesso processo"
    assert ("1.971", ["A-1"]) in live.cancellazioni, (live.letture, live.cancellazioni)
    assert all(mk != "1.997" for mk, _ in live.cancellazioni), "ordine estraneo cancellato alla cieca"
    stato = c.rc.get_status()
    vista = stato["live_exposure"]
    assert vista["state"] == "NON_RICONCILIATA" and vista["manual_action_required"] is True
    assert stato["live_exposure_unresolved"] is True and stato["live_exposure_manual_action_required"] is True
    assert _eventi(c, "EMERGENCY_STOP_MANUAL_ACTION_REQUIRED"), "esposizione estranea non segnalata"
    assert _ingresso(c, "1.972")[0] == 0


def test_m115_conto_live_irraggiungibile_recovery_dedicato_senza_eventi(tmp_path, monkeypatch):
    """M115: Betfair non raggiungibile al momento dell'errore => esposizione SCONOSCIUTA (mai zero), intervento manuale;
    il recovery dedicato ritenta da solo (nessun segnale ne' settlement) e, appena il conto risponde, cancella e
    verifica. Poi si ferma (idempotente: una sola cancellazione)."""
    c, tavolo = _m115_banco(tmp_path, "m115_irr.db", max_daily_loss=30.0)
    stato = {"raggiungibile": False}
    live = _LiveSpia([_ordine_bot(tavolo)])

    def cliente():
        if not stato["raggiungibile"]:
            raise ConnectionError("LIVE non raggiungibile")
        return live

    c.rc.betfair_service.get_live_client = cliente
    with monkeypatch.context() as m:
        _errore_nel_settlement(m, "_mark_settlement_realized", RuntimeError)
        with pytest.raises(RuntimeError):
            _settle_tavolo(c, tavolo, -40.0)
    assert c.rc._emergency_stopped is True
    vista = c.rc.get_status()["live_exposure"]
    assert vista["state"] == "SCONOSCIUTA" and vista["amount"] is None and vista["reconcile_pending"] is True
    assert c.rc.get_status()["live_exposure_unresolved"] is True, "esposizione sconosciuta presentata come risolta"
    assert live.cancellazioni == []
    stato["raggiungibile"] = True
    assert _attendi(lambda: ("1.971", ["A-1"]) in live.cancellazioni), "recovery dedicato non ritentato senza eventi"
    assert _attendi(lambda: c.rc.get_status()["live_exposure"]["state"] == "VERIFICATA")
    assert c.rc.get_status()["live_exposure_unresolved"] is False
    assert _attendi(lambda: not RuntimeController._live_recovery_active(c.rc)), "recovery non fermato dopo la verifica"
    assert live.cancellazioni.count(("1.971", ["A-1"])) == 1, live.cancellazioni
    assert c.rc._emergency_stopped is True, "la verifica non riapre gli ingressi (solo reset_emergency)"


def test_m115_cancellazione_fallita_resta_irrisolta_e_ritentata(tmp_path, monkeypatch):
    """M115: la cancellazione non chiude l'ordine (l'exchange lo mostra ancora aperto) => mai chiusura dichiarata:
    esposizione NON RICONCILIATA, intervento manuale, recovery ancora attivo che ritenta."""
    c, tavolo = _m115_banco(tmp_path, "m115_cf.db", max_daily_loss=30.0)

    class _Ostinato(_LiveSpia):
        def cancel_orders(self, market_id, bet_ids=None, **kw):
            self.cancellazioni.append((market_id, bet_ids))
            return {"ok": False, "status": "FAILURE"}

    live = _Ostinato([_ordine_bot(tavolo)])
    c.rc.betfair_service.get_live_client = lambda: live
    with monkeypatch.context() as m:
        _errore_nel_settlement(m, "_sync_bankroll_post_settlement", RuntimeError)
        with pytest.raises(RuntimeError):
            _settle_tavolo(c, tavolo, -40.0)
    vista = c.rc.get_status()["live_exposure"]
    assert vista["state"] == "NON_RICONCILIATA" and vista["verification"] != "VERIFIED"
    assert vista["manual_action_required"] is True
    n = len(live.cancellazioni)
    assert _attendi(lambda: len(live.cancellazioni) > n), "nessun nuovo tentativo del recovery dedicato"
    assert RuntimeController._live_recovery_active(c.rc)
    assert c.rc.get_status()["live_exposure"]["state"] == "NON_RICONCILIATA"
    # chiusura del banco: ferma il recovery (un tentativo gia' in corso puo' riarmare l'attesa: si ripete)
    assert _attendi(lambda: c.rc.__dict__.__setitem__("_emergency_reconcile_pending", False)
                    or not RuntimeController._live_recovery_active(c.rc))


def test_m115_riavvio_durante_la_riconciliazione_riprende_e_verifica(tmp_path, monkeypatch):
    """M115: crash DURANTE la riconciliazione nello stesso processo (cancellazione interrotta). Al riavvio emergenza ed
    esposizione irrisolta sono ancora durevoli; il conto e' irraggiungibile all'avvio, poi il recovery dedicato
    cancella e verifica senza alcun evento."""
    c, tavolo = _m115_banco(tmp_path, "m115_rr.db", max_daily_loss=30.0)

    class _Crash(_LiveSpia):
        def cancel_orders(self, market_id, bet_ids=None, **kw):
            raise SystemExit("crash durante la cancellazione")

    c.rc.betfair_service.get_live_client = lambda: _Crash([_ordine_bot(tavolo)])
    with monkeypatch.context() as m:
        _errore_nel_settlement(m, "_mark_settlement_realized", RuntimeError)
        with pytest.raises(BaseException):
            _settle_tavolo(c, tavolo, -40.0)
    _fotografa_al_crash(tmp_path, "m115_rr.db", "m115_rr_crash.db")
    c.rc._emergency_reconcile_pending = False
    _attendi(lambda: not RuntimeController._live_recovery_active(c.rc))

    import core.runtime_controller as rcm
    monkeypatch.setattr(rcm.RuntimeController, "_LIVE_RECOVERY_INTERVAL_S", 0.05)
    stato = {"raggiungibile": False}
    live = _LiveSpia([_ordine_bot(tavolo)])

    def cliente():
        if not stato["raggiungibile"]:
            raise ConnectionError("LIVE non raggiungibile all'avvio")
        return live

    r = _banco(tmp_path, "m115_rr_crash.db", max_daily_loss=30.0)
    r.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)   # N6: riavvio sul conto dello stop
    r.rc.betfair_service.get_live_client = cliente
    assert r.rc._emergency_stopped is True
    assert r.rc.get_status()["live_exposure"]["state"] in ("SCONOSCIUTA", "NON_RICONCILIATA")
    assert RuntimeController._live_recovery_active(r.rc), "recovery dedicato non avviato dopo il riavvio"
    stato["raggiungibile"] = True
    assert _attendi(lambda: ("1.971", ["A-1"]) in live.cancellazioni)
    assert _attendi(lambda: r.rc.get_status()["live_exposure"]["state"] == "VERIFICATA")
    assert _ingresso(r, "1.973")[0] == 0


def test_m115_stop_di_sessione_interrotto_protegge_il_live_senza_eventi(tmp_path, monkeypatch):
    """M115 + S-b: perdita di sessione LIVE (solo max_session_loss), errore nella pubblicazione dello stop: lo stop
    globale si ripristina dal marker E l'ordine LIVE del bot viene cancellato nello stesso processo, senza eventi."""
    import core.runtime_controller as rcm
    c, tavolo = _m115_banco(tmp_path, "m115_sess.db", max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    live = _LiveSpia([_ordine_bot(tavolo)])
    c.rc.betfair_service.get_live_client = lambda: live

    def pubblica(self, *a, **k):
        raise RuntimeError("errore nella pubblicazione dello stop di sessione")

    with monkeypatch.context() as m:
        m.setattr(rcm.RuntimeController, "_publish_session_loss_breach", pubblica)
        with pytest.raises(RuntimeError):
            _settle_tavolo(c, tavolo, -40.0)
    assert str(c.rc._risk_stop_reason).startswith("session_loss_breached")
    assert ("1.971", ["A-1"]) in live.cancellazioni, (live.letture, live.cancellazioni)
    assert c.rc.get_status()["live_exposure"]["state"] == "VERIFICATA"
    assert _ingresso(c, "1.974")[0] == 0


def test_m115_sweep_live_mai_in_parallelo(tmp_path):
    """M115: mentre uno sweep LIVE e' in corso (bloccato a meta' lettura), una riconciliazione concorrente (recovery
    dedicato, segnale, settlement) non avvia un secondo sweep: nessuna lettura ne' cancellazione duplicata."""
    c, tavolo = _m115_banco(tmp_path, "m115_conc.db", intervallo=60.0, max_daily_loss=30.0)
    entrato, libera = threading.Event(), threading.Event()

    class _Lento(_LiveSpia):
        def get_current_orders(self, market_ids=None):
            if self.letture == 0:
                self.letture += 1
                entrato.set()
                libera.wait(5)
                return list(self.ordini)
            return super().get_current_orders(market_ids)

    live = _Lento([_ordine_bot(tavolo)])
    c.rc.betfair_service.get_live_client = lambda: live
    c.rc._emergency_stopped = True
    c.rc._emergency_reconcile_pending = True
    primo = threading.Thread(target=RuntimeController._reconcile_restored_emergency, args=(c.rc,),
                             kwargs={"trigger": "primo"})
    primo.start()
    assert entrato.wait(5)
    assert RuntimeController._reconcile_restored_emergency(c.rc, trigger="concorrente") is None
    assert live.letture == 1 and live.cancellazioni == [], (live.letture, live.cancellazioni)
    libera.set()
    primo.join(5)
    assert live.cancellazioni == [("1.971", ["A-1"])], live.cancellazioni


def test_c12_crash_vero_prima_del_finally_riavvio_riconcilia_sul_live(tmp_path, monkeypatch):
    """Ciclo 12 + M115: il processo MUORE dopo la marcatura del settlement, prima del `finally` (nessun Emergency Stop
    nel processo). Al riavvio il solo marker durevole (sede LIVE anche col runtime in SIM) ripristina l'emergenza e la
    riconciliazione cancella l'ordine LIVE del bot sul conto LIVE."""
    import core.runtime_controller as rcm
    c, tavolo = _m115_banco(tmp_path, "c12v.db", max_daily_loss=30.0)
    c.rc.betfair_service.get_live_client = lambda: _LiveSpia()
    orig = rcm.RuntimeController._mark_settlement_realized

    def muore(self, *a, **k):
        orig(self, *a, **k)
        _fotografa_al_crash(tmp_path, "c12v.db", "c12v_crash.db")
        raise SystemExit("morte del processo")

    with monkeypatch.context() as m:
        m.setattr(rcm.RuntimeController, "_mark_settlement_realized", muore)
        with pytest.raises(SystemExit):
            _settle_tavolo(c, tavolo, -40.0)
    c.rc._emergency_reconcile_pending = False
    live = _LiveSpia([_ordine_bot(tavolo)])
    monkeypatch.setattr(rcm.RuntimeController, "_LIVE_RECOVERY_INTERVAL_S", 60.0)
    r = _banco(tmp_path, "c12v_crash.db", max_daily_loss=30.0)
    assert r.rc._emergency_stopped is True
    r.rc.betfair_service.get_live_client = lambda: live
    _ambiente(r, "SIM")
    assert _ingresso(r, "1.976")[0] == 0
    assert ("1.971", ["A-1"]) in live.cancellazioni, (live.letture, live.cancellazioni)
    assert r.rc.get_status()["live_exposure"]["state"] == "VERIFICATA"


def test_codex_posizione_live_abbinata_mai_verificata_ne_zero(tmp_path, monkeypatch):
    """Codex P1 su afbd5c89: dopo lo sweep resta solo una posizione LIVE ABBINATA (nessun ordine da cancellare). La
    cancellazione non chiude una posizione abbinata: esposizione NON_RICONCILIATA con intervento manuale, mai
    VERIFICATA ne' importo 0; il recovery resta attivo finche' la posizione c'e'."""
    c, tavolo = _m115_banco(tmp_path, "cx_matched.db", max_daily_loss=30.0)
    abbinata = {"betId": "A-9", "customerOrderRef": tavolo.meta["customer_ref"], "marketId": "1.971",
                "status": "EXECUTION_COMPLETE", "side": "LAY", "sizeMatched": 4.0, "sizeRemaining": 0.0,
                "averagePriceMatched": 3.0}
    live = _LiveSpia([abbinata])
    c.rc.betfair_service.get_live_client = lambda: live
    _settle_tavolo(c, tavolo, -40.0)
    assert c.rc._emergency_stopped is True
    stato = c.rc.get_status()
    vista = stato["live_exposure"]
    assert vista["matched_positions"], vista
    assert vista["state"] != "VERIFICATA" and vista["amount"] != 0.0, vista
    assert vista["manual_action_required"] is True and stato["live_exposure_unresolved"] is True
    assert "nessun ordine LIVE residuo" not in rcmod_format(vista)
    assert RuntimeController._live_recovery_active(c.rc), "recovery fermato con una posizione abbinata aperta"
    c.rc._emergency_reconcile_pending = False
    _attendi(lambda: not RuntimeController._live_recovery_active(c.rc))


def rcmod_format(vista):
    from core.runtime_controller import format_live_exposure
    return format_live_exposure(vista)


def test_codex_riavvio_con_esposizione_irrisolta_ripristina_il_recovery(tmp_path, monkeypatch):
    """Codex P1 su afbd5c89: stop di sessione LIVE completato col runtime in SIM, protezione LIVE con conto
    irraggiungibile (esposizione irrisolta durevole). Dopo il riavvio la riconciliazione e il recovery dedicato
    ripartono dal record durevole e cancellano l'ordine del bot appena il conto risponde, senza eventi."""
    import core.runtime_controller as rcm
    c, tavolo = _m115_banco(tmp_path, "cx_rst.db", intervallo=60.0, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0

    def irraggiungibile():
        raise ConnectionError("LIVE non raggiungibile")

    c.rc.betfair_service.get_live_client = irraggiungibile
    _settle_tavolo(c, tavolo, -40.0)
    assert str(c.rc._risk_stop_reason).startswith("session_loss_breached")
    assert c.rc.get_status()["live_exposure_unresolved"] is True
    c.rc._emergency_reconcile_pending = False
    _attendi(lambda: not RuntimeController._live_recovery_active(c.rc))

    monkeypatch.setattr(rcm.RuntimeController, "_LIVE_RECOVERY_INTERVAL_S", 0.05)
    r = _banco(tmp_path, "cx_rst.db", max_session_loss=30.0)
    r.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)   # N6: riavvio sul conto dello stop
    assert str(r.rc._risk_stop_reason).startswith("session_loss_breached")
    assert r.rc.__dict__.get("_emergency_reconcile_pending") is True, "riconciliazione non ripristinata al riavvio"
    assert RuntimeController._live_recovery_active(r.rc), "recovery dedicato non ripristinato al riavvio"
    live = _LiveSpia([_ordine_bot(tavolo)])
    r.rc.betfair_service.get_live_client = lambda: live
    assert _attendi(lambda: ("1.971", ["A-1"]) in live.cancellazioni), (live.letture, live.cancellazioni)
    assert _attendi(lambda: r.rc.get_status()["live_exposure"]["state"] == "VERIFICATA")


# ============================================================================ R1 liability LIVE (owner GO)
from decimal import Decimal as _D


def _liab(open_orders=(), foreign=(), matched=()):
    from core.runtime_controller import live_liability
    return live_liability(list(open_orders), list(foreign), list(matched))


@pytest.mark.parametrize("side,quota,stake,attesa", [
    ("BACK", 3.0, 10.0, "10.00"), ("LAY", 3.0, 10.0, "20.00"), ("LAY", 1.01, 10.0, "0.10"),
    ("LAY", 1000.0, 2.0, "1998.00"), ("BACK", 1000.0, 2.0, "2.00"), ("LAY", 2.5, 3.33, "5.00"),
    ("lay", 3.3, 0.07, "0.17"),                        # 0.161 per eccesso: arrotondamento conservativo
])
def test_r1_liability_back_lay_quote_diverse(side, quota, stake, attesa):
    out = _liab(open_orders=[{"bet_id": "o", "side": side, "price": quota, "size_remaining": stake}])
    assert out["total"] == attesa and out["unmatched"] == attesa and out["matched"] == "0.00", out
    assert _D(out["total"]) == _D(attesa)
    out = _liab(matched=[{"bet_id": "m", "side": side, "average_price_matched": quota, "size_matched": stake}])
    assert out["total"] == attesa and out["matched"] == attesa and out["unmatched"] == "0.00", out


def test_r1_liability_parziale_conta_residuo_e_abbinato_una_volta():
    """LAY 10 @ 3.0: 4 abbinati a quota media 2.8 (liability 7.20) + 6 residui @ 3.0 (12.00) = 19.20; mai 10 x 2."""
    out = _liab(open_orders=[{"bet_id": "P", "side": "LAY", "price": 3.0, "size_remaining": 6.0}],
                matched=[{"bet_id": "P", "side": "LAY", "average_price_matched": 2.8, "size_matched": 4.0}])
    assert (out["unmatched"], out["matched"], out["total"]) == ("12.00", "7.20", "19.20"), out
    # estraneo parzialmente abbinato: residuo e abbinato nella stessa voce, entrambi contati
    out = _liab(foreign=[{"bet_id": "X", "side": "LAY", "price": 3.0, "size_remaining": 6.0, "size_matched": 4.0,
                          "average_price_matched": 2.8}])
    assert out["total"] == "19.20", out


def test_r1_liability_piu_ordini_stesso_mercato_stima_lorda_senza_compensazioni():
    """BACK 10 @ 2.0 e LAY 10 @ 2.0 sulla stessa selezione: la posizione netta sarebbe ~0, ma il valore e' dichiarato
    stima conservativa LORDA (10 + 10 = 20), mai una compensazione non calcolata sugli esiti."""
    out = _liab(open_orders=[{"bet_id": "B", "market_id": "1.5", "side": "BACK", "price": 2.0, "size_remaining": 10},
                             {"bet_id": "L", "market_id": "1.5", "side": "LAY", "price": 2.0, "size_remaining": 10}])
    assert out["total"] == "20.00" and out["basis"] == "STIMA_CONSERVATIVA_LORDA", out


@pytest.mark.parametrize("voce", [
    {"side": None, "price": 3.0, "size_remaining": 2.0}, {"side": "LAY", "price": None, "size_remaining": 2.0},
    {"side": "LAY", "price": 3.0, "size_remaining": None}, {"side": "LAY", "price": 1.0, "size_remaining": 2.0},
    {"side": "LAY", "price": "nan", "size_remaining": 2.0}, {"side": "BACK", "price": 3.0, "size_remaining": "inf"},
    {"side": "BACK", "price": 3.0, "size_remaining": -1}, {"side": "SELL", "price": 3.0, "size_remaining": 2.0},
    {"side": "LAY", "price": "x", "size_remaining": 2.0},
])
def test_r1_liability_dati_incompleti_sconosciuta_mai_zero(voce):
    ok = {"bet_id": "ok", "side": "BACK", "price": 2.0, "size_remaining": 5.0}
    out = _liab(open_orders=[ok, dict(voce, bet_id="ko")])
    assert out["total"] is None and out["unmatched"] is None and "ko" in out["unknown_items"], out


def test_r1_liability_abbinata_senza_lato_o_quota_e_sconosciuta_in_get_status(tmp_path):
    """Posizione LIVE abbinata senza lato/quota di esecuzione (dato reale mancante): esposizione NON_RICONCILIATA con
    importo SCONOSCIUTO, mai lo stake e mai zero; persistente dopo il riavvio."""
    c, tavolo = _m115_banco(tmp_path, "liab_ko.db", intervallo=60.0, max_daily_loss=30.0)
    live = _LiveSpia([{"betId": "A-9", "customerOrderRef": tavolo.meta["customer_ref"], "marketId": "1.971",
                       "status": "EXECUTION_COMPLETE", "sizeMatched": 4.0, "sizeRemaining": 0.0}])
    c.rc.betfair_service.get_live_client = lambda: live
    _settle_tavolo(c, tavolo, -40.0)
    vista = c.rc.get_status()["live_exposure"]
    assert vista["state"] == "NON_RICONCILIATA" and vista["amount"] is None and vista["amount_display"] == "SCONOSCIUTO"
    c.rc._emergency_reconcile_pending = False
    _attendi(lambda: not RuntimeController._live_recovery_active(c.rc))
    r = _banco(tmp_path, "liab_ko.db", max_daily_loss=30.0)
    vista = r.rc.get_status()["live_exposure"]
    assert vista["amount"] is None and vista["state"] in ("NON_RICONCILIATA", "SCONOSCIUTA")
    r.rc._emergency_reconcile_pending = False


def test_r1_liability_lay_abbinata_in_get_status_gui_e_dopo_riavvio(tmp_path):
    """LAY abbinato 4 @ 3.0 (liability 8.00, non lo stake 4.00): get_status, testo GUI/headless e stato persistente
    concordano; dopo il riavvio lo stesso valore."""
    from core.runtime_controller import format_live_exposure
    c, tavolo = _m115_banco(tmp_path, "liab_ok.db", intervallo=60.0, max_daily_loss=30.0)
    live = _LiveSpia([{"betId": "A-9", "customerOrderRef": tavolo.meta["customer_ref"], "marketId": "1.971",
                       "status": "EXECUTION_COMPLETE", "side": "LAY", "sizeMatched": 4.0, "sizeRemaining": 0.0,
                       "averagePriceMatched": 3.0}])
    c.rc.betfair_service.get_live_client = lambda: live
    _settle_tavolo(c, tavolo, -40.0)
    vista = c.rc.get_status()["live_exposure"]
    assert vista["state"] == "NON_RICONCILIATA" and vista["amount_display"] == "8.00", vista
    assert vista["liability"]["matched"] == "8.00" and vista["liability"]["unmatched"] == "0.00"
    assert "liability 8.00 (stima lorda)" in format_live_exposure(vista)
    c.rc._emergency_reconcile_pending = False
    _attendi(lambda: not RuntimeController._live_recovery_active(c.rc))
    r = _banco(tmp_path, "liab_ok.db", max_daily_loss=30.0)
    r.rc.betfair_service.get_live_client = lambda: (_ for _ in ()).throw(ConnectionError("giu'"))
    vista2 = r.rc.get_status()["live_exposure"]
    assert vista2["amount_display"] in ("8.00", "SCONOSCIUTO") and vista2["state"] != "VERIFICATA", vista2
    assert r.rc.__dict__.get("_emergency_reconcile_pending") is True
    r.rc._emergency_reconcile_pending = False


def test_codex_record_legacy_risolto_con_abbinate_non_e_verificato_al_riavvio(tmp_path):
    """Record durevole scritto dal codice di afbd5c89 (bug Codex): `resolved=True`, VERIFIED, ma con una posizione LIVE
    abbinata. Dopo l'aggiornamento e il riavvio: mai VERIFICATA, liability della posizione, riconciliazione in attesa."""
    import json as _json
    c = _banco(tmp_path, "legacy.db", max_daily_loss=30.0)
    c.rc.db.save_settings({RuntimeController._LIVE_EXPOSURE_KEY: _json.dumps({
        "source": "EMERGENCY_STOP", "verification": "VERIFIED", "resolved": True, "open_orders": [],
        "unattributed_open_orders": [], "matched_positions": [
            {"bet_id": "L-1", "market_id": "1.9", "side": "LAY", "size_matched": 5.0, "average_price_matched": 2.2}],
        "at": "2026-11-17T10:00:00"})})
    r = _banco(tmp_path, "legacy.db", max_daily_loss=30.0)
    assert r.rc.__dict__.get("_emergency_reconcile_pending") is True
    vista = r.rc.get_status()["live_exposure"]
    assert vista["state"] == "NON_RICONCILIATA" and vista["amount_display"] == "6.00", vista
    r.rc._emergency_reconcile_pending = False
    # difesa in profondita': anche senza riconciliazione in attesa la vista non dichiara chiusa una posizione abbinata
    assert r.rc.get_status()["live_exposure"]["state"] == "NON_RICONCILIATA"


# ============================================================================ Codex P1 su b902eca3
def test_codex_b902_marker_di_sessione_non_salvato_non_lascia_settlement_senza_stop(tmp_path, monkeypatch):
    """Codex P1 su b902eca3: la scrittura del marker `session_loss_stop_pending` fallisce (errore transitorio), poi
    il processo muore dopo la marcatura del settlement. Al riavvio la chiave e' salvata: la riconsegna non rifa' nulla.
    Mai uno stato persistente con settlement completato e stop obbligatorio assente (M121)."""
    import core.runtime_controller as rcm
    c, tavolo = _m115_banco(tmp_path, "cx_mk.db", intervallo=60.0, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    c.rc.betfair_service.get_live_client = lambda: _LiveSpia()
    vero_save = c.rc.db.save_settings

    def save_fallisce_solo_il_marker(valori, *a, **k):
        if RuntimeController._SESSION_STOP_MARKER in (valori or {}) and (valori or {}).get(
                RuntimeController._SESSION_STOP_MARKER):
            raise OSError("disco momentaneamente non scrivibile")
        return vero_save(valori, *a, **k)

    orig = rcm.RuntimeController._mark_settlement_realized

    def muore(self, *a, **k):
        orig(self, *a, **k)
        _fotografa_al_crash(tmp_path, "cx_mk.db", "cx_mk_crash.db")
        raise SystemExit("morte del processo")

    monkeypatch.setattr(c.rc.db, "save_settings", save_fallisce_solo_il_marker)
    with monkeypatch.context() as m:
        m.setattr(rcm.RuntimeController, "_mark_settlement_realized", muore)
        with pytest.raises(BaseException):
            _settle_tavolo(c, tavolo, -40.0)
    c.rc._emergency_reconcile_pending = False
    r = _banco(tmp_path, "cx_mk_crash.db", max_session_loss=30.0)
    stop = str(getattr(r.rc, "_risk_stop_reason", "") or "")
    assert r.rc._emergency_stopped or (stop and not RuntimeController._risk_stop_scope(stop)), \
        "settlement durevole senza stop globale obbligatorio"
    r.rc._emergency_reconcile_pending = False


def test_codex_b902_doppio_guasto_settlement_non_applicato_e_ritentabile(tmp_path, monkeypatch):
    """Marker di sessione E RISK_STOP non persistibili: il settlement NON si applica (nessun P&L, nessuna chiave), la
    barriera d'ingresso globale resta armata; la riconsegna, a disco di nuovo scrivibile, applica lo stop una volta."""
    from core import risk_stop as stop_rules
    c, tavolo = _m115_banco(tmp_path, "cx_mk2.db", intervallo=60.0, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    c.rc.betfair_service.get_live_client = lambda: _LiveSpia()
    vero_save = c.rc.db.save_settings
    prima = float(c.rc.risk_desk.realized_pnl)

    def save_marker_ko(valori, *a, **k):
        if (valori or {}).get(RuntimeController._SESSION_STOP_MARKER):
            raise OSError("disco non scrivibile")
        return vero_save(valori, *a, **k)

    with monkeypatch.context() as m:
        m.setattr(c.rc.db, "save_settings", save_marker_ko)
        m.setattr(stop_rules, "persist_risk_stop", lambda db, reason: False)
        with pytest.raises(RuntimeError, match="non durevole"):
            _settle_tavolo(c, tavolo, -40.0)
    assert float(c.rc.risk_desk.realized_pnl) == prima, "P&L applicato senza stop durevole"
    assert c.rc._limits_block_reason(), "barriera d'ingresso globale tolta"
    assert _ingresso(c, "1.979")[0] == 0
    _settle_tavolo(c, tavolo, -40.0)                              # riconsegna, disco tornato scrivibile
    assert float(c.rc.risk_desk.realized_pnl) == pytest.approx(prima - 40.0)
    assert str(c.rc._risk_stop_reason).startswith("session_loss_breached")
    c.rc._emergency_reconcile_pending = False


def test_codex_b902_verifica_live_precedente_non_vale_per_un_nuovo_stop(tmp_path):
    """Codex P1 su b902eca3: sweep LIVE verificato (nessun ordine), reset dell'emergenza, nuovo ordine LIVE, poi stop di
    sessione globale col runtime in LIVE e cashout non eseguito. La verifica dello stop precedente non vale per il nuovo:
    esposizione mai VERIFICATA/zero senza una nuova conferma dell'exchange."""
    c = _banco(tmp_path, "cx_stale.db", max_session_loss=30.0)
    c.rc._live_recovery_interval_s = 60.0
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.ACTIVE
    c.rc.betfair_service.get_live_client = lambda: _LiveSpia()
    c.rc.emergency_stop("prova", live_exposure=True)
    assert c.rc.get_status()["live_exposure"]["state"] == "VERIFICATA"
    c.rc.reset_emergency()
    assert c.rc.get_status()["live_exposure"]["state"] == "NESSUNA", "verifica di uno stop chiuso ancora mostrata"
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.ACTIVE
    c.rc._session_pnl_baseline = float(c.rc.risk_desk.realized_pnl)
    tavolo = _riserva(c, "1.961")
    _segna_riga_live(c, tavolo.meta["customer_ref"])
    c.rc._route_cashout_signal = lambda segnale: None             # cashout non eseguito
    _settle_tavolo(c, tavolo, -40.0)
    assert str(c.rc._risk_stop_reason).startswith("session_loss_breached")
    vista = c.rc.get_status()["live_exposure"]
    assert vista["state"] != "VERIFICATA" and vista["amount"] != 0.0 and vista["manual_action_required"] is True, vista
    c.rc._emergency_reconcile_pending = False


def test_codex_b902_riconsegna_dopo_rilascio_recupera_l_ambito_della_scommessa(tmp_path, monkeypatch):
    """Codex P1 su b902eca3: il rilascio della riserva LIVE e' gia' registrato (effetto con customer_ref,
    realized_applied=0), poi il processo muore prima del P&L. Al riavvio (runtime in SIM) la riconsegna e' un
    `duplicate` senza ambito: la perdita va al conto LIVE della scommessa (dalla request persistita), mai alla SIM."""
    import core.runtime_controller as rcm
    c = _banco(tmp_path, "cx_rel.db", max_daily_loss=30.0)
    _ambiente(c, "LIVE", conto=CONTO_A)
    c.rc.mode = RuntimeMode.ACTIVE
    conto_a = c.rc._current_financial_scope()
    tavolo = _riserva(c, "1.951")
    _segna_riga_live(c, tavolo.meta["customer_ref"])
    payload = _canonical_close_payload(
        pnl=-40.0, net_pnl=-40.0, gross_pnl=-40.0, commission_amount=0.0, event_key=tavolo.current_event_key,
        batch_id="b-cx-rel", correlation_id="c-cx-rel", table_id=tavolo.table_id,
        customer_ref=tavolo.meta["customer_ref"], market_id=tavolo.market_id)

    def muore(self, *a, **k):
        _fotografa_al_crash(tmp_path, "cx_rel.db", "cx_rel_crash.db")
        raise SystemExit("morte prima del P&L")

    with monkeypatch.context() as m:
        m.setattr(rcm.RuntimeController, "_apply_and_attribute_realized_pnl", muore)
        with pytest.raises(SystemExit):
            c.rc._on_close_position(dict(payload))
    r = _banco(tmp_path, "cx_rel_crash.db", max_daily_loss=30.0)
    _ambiente(r, "SIM")
    r.rc.betfair_service.get_live_client = lambda: _LiveSpia()
    r.rc.mode = RuntimeMode.ACTIVE
    r.rc._on_close_position(dict(payload))
    sim = financial_scope.runtime_scope(simulated=True, live_account="")
    scopes = r.rc.__dict__.get("_daily_loss_scopes") or {}
    assert float((scopes.get(sim) or {}).get("daily_loss_amount") or 0.0) == 0.0, "perdita LIVE attribuita alla SIM"
    assert float((scopes.get(conto_a) or {}).get("daily_loss_amount") or 0.0) == pytest.approx(40.0), scopes.keys()
    assert r.rc._emergency_stopped is True, "superamento LIVE senza Emergency Stop globale"
    r.rc._emergency_reconcile_pending = False


def _registra_esito(c, **campi):
    import json as _json
    rec = {"source": "TEST", "verification": "VERIFIED", "resolved": True, "open_orders": [],
           "unattributed_open_orders": [], "matched_positions": [], "at": "2026-11-17T09:00:00"}
    rec.update(campi)
    c.rc.db.save_settings({RuntimeController._LIVE_EXPOSURE_KEY: _json.dumps(rec)})
    c.rc._live_exposure_record = None


def test_codex_b902_esito_verificato_rimasto_non_vale_per_nuovo_stop_live(tmp_path, monkeypatch):
    """Esito VERIFICATO rimasto su disco (pulizia al reset non riuscita o processo precedente): un nuovo stop globale
    LIVE (RISK_STOP col runtime in LIVE; Emergency Stop con sweep non completato) non lo eredita."""
    c = _banco(tmp_path, "cx_rest.db", max_session_loss=30.0)
    c.rc._live_recovery_interval_s = 60.0
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.ACTIVE
    _registra_esito(c)
    c.rc._route_cashout_signal = lambda segnale: None
    c.rc.risk_stop("session_loss_breached:prova", scope=c.rc._current_financial_scope())
    assert c.rc.get_status()["live_exposure"]["state"] == "SCONOSCIUTA"
    c.rc._emergency_reconcile_pending = False
    _attendi(lambda: not RuntimeController._live_recovery_active(c.rc))
    r = _banco(tmp_path, "cx_rest.db", max_session_loss=30.0)     # riavvio: l'esito vecchio resta invalidato su disco
    assert r.rc._risk_stop_reason and r.rc.get_status()["live_exposure"]["state"] != "VERIFICATA"
    r.rc._emergency_reconcile_pending = False
    c2 = _banco(tmp_path, "cx_rest2.db", max_daily_loss=30.0)
    c2.rc._live_recovery_interval_s = 60.0
    _ambiente(c2, "LIVE")
    _registra_esito(c2)

    def sweep_rotto(self, **k):
        raise RuntimeError("sweep interrotto")

    monkeypatch.setattr(RuntimeController, "_emergency_cancel_and_verify", sweep_rotto)
    with pytest.raises(RuntimeError):
        c2.rc.emergency_stop("prova", live_exposure=True)
    assert c2.rc.get_status()["live_exposure"]["state"] == "SCONOSCIUTA"
    c.rc._emergency_reconcile_pending = c2.rc._emergency_reconcile_pending = False


def test_codex_b902_nuovo_stop_conserva_un_esito_irrisolto(tmp_path):
    """Un esito LIVE IRRISOLTO (ordini e importo noti) resta com'e' all'inizio di un nuovo stop: l'esposizione e' ancora
    quella e non diventa una generica «in attesa»."""
    c = _banco(tmp_path, "cx_keep.db", max_session_loss=30.0)
    c.rc._live_recovery_interval_s = 60.0
    _ambiente(c, "LIVE")
    c.rc.mode = RuntimeMode.ACTIVE
    _registra_esito(c, verification="OPEN_ORDERS_REMAIN", resolved=False,
                    open_orders=[{"bet_id": "K-1", "side": "BACK", "price": 2.0, "size_remaining": 3.0}])
    c.rc._route_cashout_signal = lambda segnale: None
    c.rc.risk_stop("session_loss_breached:prova", scope=c.rc._current_financial_scope())
    vista = c.rc.get_status()["live_exposure"]
    assert vista["state"] == "NON_RICONCILIATA" and vista["amount_display"] == "3.00", vista
    c.rc._emergency_reconcile_pending = False


# ============================================================================ M128: N1/N2 (Codex su 440351a1)
def test_m128_n1_ripiego_del_marker_esegue_la_protezione_live(tmp_path, monkeypatch):
    """N1 (regressione di 440351a1): marker di sessione non scrivibile, nessun crash, runtime in SIM, ordine LIVE del bot
    aperto e conto raggiungibile. Il ripiego rende durevole lo stop E ne esegue le azioni di protezione: ordine LIVE del
    bot cancellato e verificato (come senza guasto), mai solo lo stop salvato."""
    c, tavolo = _m115_banco(tmp_path, "n1.db", intervallo=60.0, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    live = _LiveSpia([_ordine_bot(tavolo)])
    c.rc.betfair_service.get_live_client = lambda: live
    vero_save = c.rc.db.save_settings

    def save_marker_ko(valori, *a, **k):
        if (valori or {}).get(RuntimeController._SESSION_STOP_MARKER):
            raise OSError("disco momentaneamente non scrivibile")
        return vero_save(valori, *a, **k)

    monkeypatch.setattr(c.rc.db, "save_settings", save_marker_ko)
    _settle_tavolo(c, tavolo, -40.0)
    assert str(c.rc._risk_stop_reason).startswith("session_loss_breached")
    assert ("1.971", ["A-1"]) in live.cancellazioni, (live.letture, live.cancellazioni)
    assert c.rc.get_status()["live_exposure"]["state"] == "VERIFICATA"
    assert _ingresso(c, "1.978")[0] == 0
    c.rc._emergency_reconcile_pending = False


@pytest.mark.parametrize("marker", ["{illeggibile", "[]", "\"testo\"", "{\"reason\": 5}"])
def test_m128_n2_marker_di_sessione_illeggibile_ripristina_la_barriera_globale(tmp_path, marker):
    """N2: al riavvio il marker dello stop di sessione e' presente ma illeggibile/non valido. Non si puo' provare che lo
    stop non serva: RISK_STOP globale ripristinato (fail-closed), nessun ordine SIM ne' LIVE, riconciliazione in attesa."""
    c = _banco(tmp_path, "n2.db", max_session_loss=30.0)
    c.rc.db.save_settings({RuntimeController._SESSION_STOP_MARKER: marker})
    r = _banco(tmp_path, "n2.db", max_session_loss=30.0)
    stop = str(getattr(r.rc, "_risk_stop_reason", "") or "")
    assert stop and not RuntimeController._risk_stop_scope(stop), stop
    _ambiente(r, "SIM")
    r.rc.mode = RuntimeMode.ACTIVE
    assert _ingresso(r, "1.969")[0] == 0
    r.rc._emergency_reconcile_pending = False


def test_m128_n2_impostazioni_illeggibili_al_ripristino_barriera_globale(tmp_path, monkeypatch):
    """N2, ramo `except`: impostazioni non leggibili durante il ripristino del marker => RISK_STOP globale (in memoria e
    durevole se possibile), mai la sola riconciliazione in attesa."""
    c = _banco(tmp_path, "n2b.db", max_session_loss=30.0)

    def illeggibili():
        raise OSError("db illeggibile")

    monkeypatch.setattr(c.rc.db, "get_settings", illeggibili)
    RuntimeController._restore_session_stop_marker(c.rc)
    stop = str(c.rc._risk_stop_reason or "")
    assert stop.startswith("session_loss_breached") and not RuntimeController._risk_stop_scope(stop)
    c.rc._emergency_reconcile_pending = False



# ============================================================================ M130: N4 e N5
def _settle_cleared_n(c, n, importo):
    _settle_cleared(c, n, importo)


@pytest.mark.parametrize("gia_corrente", [False, True])
def test_m130_n4_limite_attivato_a_meta_giornata_conta_la_perdita_gia_regolata(tmp_path, gia_corrente):
    """N4 (regressione rispetto a main, D M129): nessun max_daily_loss, runtime in SIM; il primo settlement LIVE A -20
    introduce l'ambito (mai corrente o gia' corrente); poi limite 30 e A -15. La perdita del giorno e' 35: superamento,
    Emergency Stop globale. La baseline del nuovo ambito nasce PRIMA del P&L, mai dopo."""
    c = _banco(tmp_path, "n4.db")
    c.rc.config.max_daily_loss = None
    if gia_corrente:
        _ambiente(c, "LIVE", conto=CONTO_A)
    _ambiente(c, "SIM")
    c.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)
    conto_a = c.rc._financial_scope_for(False)
    c.rc.betfair_service.get_live_client = lambda: _LiveSpia()
    c.rc.mode = RuntimeMode.ACTIVE
    _settle_cleared(c, 61 if gia_corrente else 71, -20.0)
    c.rc.config.max_daily_loss = 30.0
    _settle_cleared(c, 62 if gia_corrente else 72, -15.0)
    st = c.rc._monitor_daily_loss_breach(source="T", scope=conto_a)
    assert st["daily_loss_amount"] == pytest.approx(35.0) and st["breached"] is True, st
    assert c.rc._emergency_stopped is True
    c.rc._emergency_reconcile_pending = False


def test_m130_n4_nessuna_perdita_contata_due_volte_ne_baseline_persa_al_riavvio(tmp_path):
    """N4: lo stato nato prima del P&L resta coerente dopo il riavvio (nessun doppio conteggio, nessuna perdita persa)."""
    c = _banco(tmp_path, "n4r.db")
    c.rc.config.max_daily_loss = None
    _ambiente(c, "SIM")
    c.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)
    conto_a = c.rc._financial_scope_for(False)
    c.rc.mode = RuntimeMode.ACTIVE
    _settle_cleared(c, 81, -20.0)
    r = _banco(tmp_path, "n4r.db", max_daily_loss=30.0)
    r.rc.betfair_service.client = types.SimpleNamespace(username=CONTO_A)
    st = r.rc._monitor_daily_loss_breach(source="T", scope=conto_a)
    assert st["daily_loss_amount"] == pytest.approx(20.0) and st["breached"] is False, st


@pytest.mark.parametrize("guasto", ["false", "eccezione"])
def test_m130_n5_marker_conservato_finche_lo_stop_non_e_durevole(tmp_path, monkeypatch, guasto):
    """N5 (Codex su b42f5f9c, D M129-bis): marker di sessione scritto, poi la persistenza del RISK_STOP fallisce
    (`False` senza eccezione, oppure eccezione). Il marker non si cancella senza uno stop riletto durevole: dopo crash e
    riavvio lo stop globale c'e' e nessun ordine parte. Quando il disco torna scrivibile il ripristino lo rende durevole
    e toglie il marker (nessun blocco permanente da marker)."""
    from core import risk_stop as stop_rules
    c, tavolo = _m115_banco(tmp_path, "n5.db", intervallo=60.0, max_session_loss=30.0)
    c.rc._session_pnl_baseline = 0.0
    c.rc.betfair_service.get_live_client = lambda: _LiveSpia()

    def persist_ko(db, reason):
        if guasto == "false":
            return False
        raise OSError("disco non scrivibile")

    with monkeypatch.context() as m:
        m.setattr(stop_rules, "persist_risk_stop", persist_ko)
        try:
            _settle_tavolo(c, tavolo, -40.0)
        except OSError:
            pass
        assert _ingresso(c, "1.976")[0] == 0, "ordine nello stesso processo"
        marker = (c.rc.db.get_settings() or {}).get(RuntimeController._SESSION_STOP_MARKER)
        assert marker, "marker cancellato senza uno stop durevole"
        _fotografa_al_crash(tmp_path, "n5.db", "n5_crash.db")
    c.rc._emergency_reconcile_pending = False
    r = _banco(tmp_path, "n5_crash.db", max_session_loss=30.0)     # disco di nuovo scrivibile
    stop = str(getattr(r.rc, "_risk_stop_reason", "") or "")
    assert stop.startswith("session_loss_breached") and not RuntimeController._risk_stop_scope(stop)
    assert stop_rules.restore_risk_stop(r.rc.db).startswith("session_loss_breached"), "stop non reso durevole"
    assert not (r.rc.db.get_settings() or {}).get(RuntimeController._SESSION_STOP_MARKER), "marker rimasto"
    _ambiente(r, "SIM")
    r.rc.mode = RuntimeMode.ACTIVE
    assert _ingresso(r, "1.977")[0] == 0
    r.rc._emergency_reconcile_pending = False


def test_m130_n5_controllo_sotto_soglia_nessun_blocco_e_nessun_marker(tmp_path):
    """Proprieta' 7: sotto soglia con persistenza regolare nessuno stop, nessun marker residuo, ordini ammessi. Banco
    solo SIM: il gate S18 del drawdown (un solo conto LIVE per processo, preesistente e fuori perimetro) non entra."""
    c = _banco(tmp_path, "n5ok.db", max_session_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    c.rc._session_pnl_baseline = float(c.rc.risk_desk.realized_pnl)
    tavolo = _riserva(c, "1.974")
    _settle_tavolo(c, tavolo, -5.0)
    assert not (c.rc.db.get_settings() or {}).get(RuntimeController._SESSION_STOP_MARKER)
    assert not str(c.rc._risk_stop_reason or "")
    esito = _ingresso(c, "1.975")
    assert esito[0] == 1, esito


def test_m130_n5_ripristino_al_riavvio_con_persistenza_ko_conserva_il_marker(tmp_path, monkeypatch):
    """Riavvio con marker di sessione presente e RISK_STOP non persistibile (`False`): stop globale in memoria, nessun
    ordine, marker CONSERVATO (unica decisione durevole). Al riavvio successivo, disco scrivibile: stop durevole e
    marker tolto (nessun blocco permanente da marker)."""
    import json as _json
    from core import risk_stop as stop_rules
    c = _banco(tmp_path, "n5r.db", max_session_loss=30.0)
    c.rc.db.save_settings({RuntimeController._SESSION_STOP_MARKER: _json.dumps(
        {"reason": "session_loss_breached:loss=40.00:limit=30.0", "scope": "x", "at": "2026-11-17T10:00:00"})})
    with monkeypatch.context() as m:
        m.setattr(stop_rules, "persist_risk_stop", lambda db, reason: False)
        r = _banco(tmp_path, "n5r.db", max_session_loss=30.0)
        assert str(r.rc._risk_stop_reason).startswith("session_loss_breached")
        assert (r.rc.db.get_settings() or {}).get(RuntimeController._SESSION_STOP_MARKER), "marker tolto senza stop durevole"
        r.rc._emergency_reconcile_pending = False
    r2 = _banco(tmp_path, "n5r.db", max_session_loss=30.0)
    assert stop_rules.restore_risk_stop(r2.rc.db).startswith("session_loss_breached")
    assert not (r2.rc.db.get_settings() or {}).get(RuntimeController._SESSION_STOP_MARKER)
    r2.rc._emergency_reconcile_pending = False


def test_m130_libro_giornaliero_scrittura_non_riletta_e_un_fallimento(tmp_path, monkeypatch):
    """Matrice D, ledger «falso»: `save_settings` del libro ritorna senza scrivere. Il salvataggio non vale (rilettura):
    marker di riserva scritto e stato «non salvato» (cancello d'ingresso). Dopo il riavvio la perdita c'e' (dal marker)."""
    from core import daily_loss_store
    c = _banco(tmp_path, "lb.db", max_daily_loss=30.0)
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    sim = _sim()
    vero = c.rc.db.save_settings

    def perso(valori, *a, **k):
        if daily_loss_store.DAILY_LOSS_STATE_KEY in (valori or {}):
            return False                                          # scrittura persa senza eccezione
        return vero(valori, *a, **k)

    with monkeypatch.context() as m:
        m.setattr(c.rc.db, "save_settings", perso)
        _settle(c, -12.0)
        assert daily_loss_store.persist_failed(c.rc._daily_loss_persisted), "scrittura persa creduta salvata"
    r = _banco(tmp_path, "lb.db", max_daily_loss=30.0)
    assert r.rc._monitor_daily_loss_breach(source="T", scope=sim)["daily_loss_amount"] == pytest.approx(12.0)


def test_m130_superamento_live_nel_libro_ricostruisce_l_emergenza_al_riavvio(tmp_path):
    """Matrice D, «es» eccezione/False/corrotto: il libro salvato dice LIVE superato oggi ma l'Emergency Stop non e'
    durevole. Al riavvio, senza riconsegna, Emergency Stop ripristinato con riconciliazione LIVE in attesa; un libro con
    superamento solo SIM non lo fa (nessun blocco globale per un ambito locale)."""
    import json as _json
    from core import daily_loss_store
    c = _banco(tmp_path, "rb.db", max_daily_loss=30.0)
    _ambiente(c, "LIVE", conto=CONTO_A)
    c.rc.mode = RuntimeMode.ACTIVE
    c.rc.betfair_service.get_live_client = lambda: _LiveSpia()
    _settle(c, -40.0)
    assert c.rc._emergency_stopped is True
    c.rc.db.save_settings({"emergency_stopped": "0"})             # Emergency Stop non durevole, libro superato
    c.rc._emergency_reconcile_pending = False
    r = _banco(tmp_path, "rb.db", max_daily_loss=30.0)
    assert r.rc._emergency_stopped is True and r.rc._emergency_reason == "DAILY_LOSS_BREACH_RESTORED_FROM_BOOK"
    assert r.rc.__dict__.get("_emergency_reconcile_pending") is True
    r.rc._emergency_reconcile_pending = False
    s = _banco(tmp_path, "rs.db", max_daily_loss=30.0)
    _ambiente(s, "SIM")
    s.rc.mode = RuntimeMode.ACTIVE
    _settle(s, -40.0)
    assert s.rc._emergency_stopped is False
    t = _banco(tmp_path, "rs.db", max_daily_loss=30.0)
    assert t.rc._emergency_stopped is False, "superamento solo SIM promosso a blocco globale"
    book = _json.loads((t.rc.db.get_settings() or {}).get(daily_loss_store.DAILY_LOSS_STATE_KEY) or "{}")
    assert book, "libro non salvato"


def _reset_confermato(rc, operatore="operatore-test", autorizzazione="AUT-TEST-1"):
    """G1: doppia conferma dell'operatore (richiesta con sfida, poi reset con confirm e confirm_again)."""
    richiesta = rc.request_risk_session_reset(operator=operatore, authorization=autorizzazione, reason="test")
    assert richiesta["requested"] is True, richiesta
    return rc.reset_risk_stop(operator=operatore, authorization=autorizzazione,
                              challenge_id=richiesta["challenge_id"], confirm=True, confirm_again=True)


# ============================================================================ M134/M136: N6, N3, N7, F1
class _ContoLive:
    """Conto LIVE emulato con identita' (username): ordini per conto, letture e cancellazioni registrate."""

    def __init__(self, nome, ordini=None, cancella=True):
        self.username, self.ordini, self.cancella = nome, list(ordini or []), cancella
        self.letture, self.cancellazioni = 0, []

    def get_current_orders(self, market_ids=None):
        self.letture += 1
        return [dict(o) for o in self.ordini]

    def cancel_orders(self, market_id, bet_ids=None, **kw):
        self.cancellazioni.append((market_id, bet_ids))
        if self.cancella:
            self.ordini = [o for o in self.ordini if o.get("marketId") != market_id]
            return {"ok": True, "instructionReports": [{"betId": b} for b in (bet_ids or [])]}
        return {"ok": False, "status": "FAILURE"}


def _connetti(c, conto):
    c.rc.betfair_service.client = conto
    c.rc.betfair_service.get_live_client = lambda: conto


def _n6_stop_su_a(tmp_path, nome):
    """ES da superamento giornaliero del conto LIVE A con un ordine del bot ancora aperto (cancellazione rifiutata)."""
    c, tavolo = _m115_banco(tmp_path, nome, intervallo=60.0, max_daily_loss=30.0)
    conto_a = _ContoLive(CONTO_A, [_ordine_bot(tavolo)], cancella=False)
    _connetti(c, conto_a)
    _settle_tavolo(c, tavolo, -40.0)
    assert c.rc._emergency_stopped is True
    c.rc._emergency_reconcile_pending = False
    _attendi(lambda: not RuntimeController._live_recovery_active(c.rc))
    return c, tavolo, conto_a


def test_m134_n6_il_conto_b_non_certifica_il_conto_a(tmp_path):
    """N6 (owner M134, P1): l'Emergency Stop nato dal conto A e' legato ad A. Dopo il riavvio con il client del conto B
    nessuna lettura ne' cancellazione su B, mai VERIFICATA: esposizione irrisolta, intervento manuale, riconciliazione
    in attesa. Tornato A, la riconciliazione riprende su A e verifica."""
    c, tavolo, conto_a = _n6_stop_su_a(tmp_path, "n6b.db")
    assert c.rc.get_status()["live_exposure"]["state"] == "NON_RICONCILIATA"
    r = _banco(tmp_path, "n6b.db", max_daily_loss=30.0)
    conto_b = _ContoLive(CONTO_B)
    _connetti(r, conto_b)
    esito = RuntimeController._reconcile_restored_emergency(r.rc, trigger="test")
    vista = r.rc.get_status()["live_exposure"]
    assert esito["verification"] == "ACCOUNT_MISMATCH" and conto_b.letture == 0 and conto_b.cancellazioni == []
    assert vista["state"] != "VERIFICATA" and vista["manual_action_required"] is True
    assert vista["account_mismatch"]["required_accounts"] == [_live(c, CONTO_A)]
    assert vista["state"] == "NON_RICONCILIATA" and [o["bet_id"] for o in vista["open_orders"]] == ["A-1"],         "l'esposizione nota del conto A non resta visibile"
    assert r.rc.__dict__.get("_emergency_reconcile_pending") is True and r.rc._emergency_stopped is True
    conto_a.cancella = True
    _connetti(r, conto_a)
    RuntimeController._reconcile_restored_emergency(r.rc, trigger="test")
    assert conto_a.ordini == [] and r.rc.get_status()["live_exposure"]["state"] == "VERIFICATA"
    r.rc._emergency_reconcile_pending = False


def test_m134_n6_cambi_di_conto_ripetuti_a_b_a_b_con_riavvii(tmp_path):
    """N6 (owner M134): riavvii alternati A -> B -> A -> B: con B mai VERIFICATA ne' letture su B; con A (cancellazione
    ancora rifiutata) NON_RICONCILIATA; lo stop e il vincolo restano a ogni riavvio; con A finalmente efficace, VERIFICATA."""
    c, tavolo, conto_a = _n6_stop_su_a(tmp_path, "n6x.db")
    conto_b = _ContoLive(CONTO_B)
    for conto in (conto_b, conto_a, conto_b, conto_a, conto_b):
        r = _banco(tmp_path, "n6x.db", max_daily_loss=30.0)
        _connetti(r, conto)
        RuntimeController._reconcile_restored_emergency(r.rc, trigger="test")
        vista = r.rc.get_status()["live_exposure"]
        assert r.rc._emergency_stopped is True and vista["manual_action_required"] is True
        assert vista["state"] in ("NON_RICONCILIATA", "SCONOSCIUTA") and conto_a.ordini, vista
        r.rc._emergency_reconcile_pending = False
    assert conto_b.letture == 0 and conto_b.cancellazioni == [], "il conto B e' stato interrogato o toccato"
    conto_a.cancella = True
    r = _banco(tmp_path, "n6x.db", max_daily_loss=30.0)
    _connetti(r, conto_a)
    RuntimeController._reconcile_restored_emergency(r.rc, trigger="test")
    assert conto_a.ordini == [] and r.rc.get_status()["live_exposure"]["state"] == "VERIFICATA"
    r.rc._emergency_reconcile_pending = False


def test_m134_n6_vincolo_del_conto_illeggibile_nessuna_certificazione(tmp_path):
    c, tavolo, conto_a = _n6_stop_su_a(tmp_path, "n6i.db")
    c.rc.db.save_settings({RuntimeController._LIVE_ACCOUNTS_KEY: "{corrotto"})
    conto_a.cancella, conto_a.letture = True, 0
    r = _banco(tmp_path, "n6i.db", max_daily_loss=30.0)
    _connetti(r, conto_a)
    esito = RuntimeController._reconcile_restored_emergency(r.rc, trigger="test")
    assert esito["verification"] == "ACCOUNT_MISMATCH" and conto_a.letture == 0
    assert r.rc.get_status()["live_exposure"]["manual_action_required"] is True
    r.rc._emergency_reconcile_pending = False


def test_m134_n6_superamento_di_a_regolato_con_b_connesso_non_tocca_b(tmp_path):
    """Settlement del conto A (ambito della riserva) mentre il client e' connesso a B: l'Emergency Stop si lega ad A,
    non legge ne' cancella su B e resta irrisolto."""
    c, tavolo = _m115_banco(tmp_path, "n6c.db", intervallo=60.0, max_daily_loss=30.0)
    conto_b = _ContoLive(CONTO_B, [{"betId": "B-9", "marketId": "1.999", "status": "EXECUTABLE",
                                    "sizeRemaining": 1.0, "customerOrderRef": "manuale"}])
    _connetti(c, conto_b)
    _settle_tavolo(c, tavolo, -40.0)
    vista = c.rc.get_status()["live_exposure"]
    assert c.rc._emergency_stopped is True and conto_b.letture == 0 and conto_b.cancellazioni == []
    assert vista["state"] != "VERIFICATA" and vista["manual_action_required"] is True
    assert RuntimeController._live_bound_accounts(c.rc) == {_live(c, CONTO_A): "PENDING"}
    c.rc._emergency_reconcile_pending = False


def _n3_estranea(**campi):
    base = {"betId": "EXT1", "marketId": "1.873", "selectionId": 1, "customerOrderRef": "altro-sistema",
            "side": "LAY", "status": "EXECUTION_COMPLETE", "priceSize": {"price": 3.0, "size": 10.0},
            "sizeRemaining": 0.0, "sizeMatched": 10.0, "averagePriceMatched": 3.0}
    base.update(campi)
    return base


def _n3_stop(tmp_path, nome, ordini):
    c, tavolo = _m115_banco(tmp_path, nome, intervallo=60.0, max_daily_loss=30.0)
    conto = _ContoLive(CONTO_A, ordini)
    _connetti(c, conto)
    _settle_tavolo(c, tavolo, -40.0)
    realized = float(c.rc.risk_desk.realized_pnl)
    vista = c.rc.get_status()["live_exposure"]
    c.rc._emergency_reconcile_pending = False
    return c, conto, vista, realized


def test_m136_n3_posizione_estranea_lay_abbinata_liability_20_mai_verificata(tmp_path):
    """N3 (owner M136, P1): LAY estraneo 10 @ 3.00 interamente abbinato => NON_RICONCILIATA, liability 20.00 (Decimal),
    origine ESTRANEA distinta dal bot; non cancellata ne' coperta; il P&L del bot non cambia (solo visibilita')."""
    c, conto, vista, realized = _n3_stop(tmp_path, "n3l.db", [_n3_estranea()])
    assert vista["state"] == "NON_RICONCILIATA" and vista["manual_action_required"] is True
    assert vista["liability"]["matched"] == "20.00" and Decimal(vista["liability"]["total"]) >= Decimal("20.00")
    assert [(m["bet_id"], m["origin"]) for m in vista["matched_positions"]] == [("EXT1", "ESTRANEA")]
    assert all(cancel[1] != ["EXT1"] for cancel in conto.cancellazioni), "posizione estranea cancellata"
    assert float(c.rc.risk_desk.realized_pnl) == realized == pytest.approx(-40.0)
    from core.runtime_controller import format_live_exposure
    assert "1 posizioni abbinate estranee" in format_live_exposure(vista)


def test_m136_n3_estranea_back_abbinata_e_dati_mancanti(tmp_path):
    _c, _conto, vista, _r = _n3_stop(tmp_path, "n3b.db", [_n3_estranea(side="BACK", sizeMatched=5.0,
                                                                         averagePriceMatched=2.0)])
    assert vista["state"] == "NON_RICONCILIATA" and vista["liability"]["matched"] == "5.00"
    _c, _conto, vista, _r = _n3_stop(tmp_path, "n3m.db", [_n3_estranea(averagePriceMatched=None)])
    assert vista["state"] == "NON_RICONCILIATA" and vista["amount"] is None and vista["amount_display"] == "SCONOSCIUTO"
    _c, _conto, vista, _r = _n3_stop(tmp_path, "n3x.db", [_n3_estranea(sizeMatched="illeggibile")])
    assert vista["amount"] is None and vista["manual_action_required"] is True


def test_m136_n3_estranea_parziale_contata_una_volta_e_chiusa_senza_abbinato_ignorata(tmp_path):
    parziale = _n3_estranea(betId="EXT2", status="EXECUTABLE", sizeRemaining=2.0, sizeMatched=3.0,
                            priceSize={"price": 3.0, "size": 5.0})
    vuota = _n3_estranea(betId="EXT3", sizeMatched=0.0)
    _c, _conto, vista, _r = _n3_stop(tmp_path, "n3p.db", [parziale, vuota])
    assert [o["bet_id"] for o in vista["unattributed_open_orders"]] == ["EXT2"]
    assert all(m["bet_id"] not in ("EXT2", "EXT3") for m in vista["matched_positions"]), "doppio conteggio"
    # residuo LAY 2 @ 3 = 4, abbinato LAY 3 @ 3 = 6
    assert vista["liability"]["unmatched"] == "4.00" and vista["liability"]["matched"] == "6.00"


def test_m134_n6_emergency_stop_manuale_legato_al_conto_connesso(tmp_path):
    """N6: Emergency Stop manuale col runtime LIVE sul conto A (nessun superamento, nessun marker): resta legato ad A.
    Dopo il riavvio con il conto B nessuna verifica su B, l'ordine del bot su A resta irrisolto e visibile."""
    c, tavolo = _m115_banco(tmp_path, "n6m.db", intervallo=60.0)
    _ambiente(c, "LIVE", conto=CONTO_A)
    conto_a = _ContoLive(CONTO_A, [_ordine_bot(tavolo)], cancella=False)
    _connetti(c, conto_a)
    c.rc.emergency_stop("manuale")
    assert RuntimeController._live_bound_accounts(c.rc) == {_live(c, CONTO_A): "PENDING"}
    c.rc._emergency_reconcile_pending = False
    _attendi(lambda: not RuntimeController._live_recovery_active(c.rc))
    r = _banco(tmp_path, "n6m.db")
    conto_b = _ContoLive(CONTO_B)
    _connetti(r, conto_b)
    esito = RuntimeController._reconcile_restored_emergency(r.rc, trigger="test")
    assert esito["verification"] == "ACCOUNT_MISMATCH" and conto_b.letture == 0
    assert r.rc.get_status()["live_exposure"]["state"] != "VERIFICATA"
    r.rc._emergency_reconcile_pending = False


def test_m134_n6_tutti_i_conti_vincolati_devono_essere_verificati(tmp_path):
    """N6: con due conti LIVE vincolati (A e B), la verifica del solo A non chiude l'esposizione: resta B in attesa."""
    c, tavolo = _m115_banco(tmp_path, "n6ab.db", intervallo=60.0, max_daily_loss=30.0)
    conto_a = _ContoLive(CONTO_A)
    _connetti(c, conto_a)
    RuntimeController._bind_live_account(c.rc, _live(c, CONTO_B))
    _connetti(c, conto_a)
    _settle_tavolo(c, tavolo, -40.0)
    vista = c.rc.get_status()["live_exposure"]
    assert vista["state"] != "VERIFICATA" and vista["accounts_pending"] == [_live(c, CONTO_B)]
    assert vista["manual_action_required"] is True
    c.rc._emergency_reconcile_pending = False


def test_m136_n7_verifica_in_sola_lettura_non_cancella_l_ordine_del_bot(tmp_path):
    """N7: col runtime LIVE la verifica programmata dopo lo stop di sessione e' in sola lettura: l'ordine del bot resta
    (lo chiude il CASHOUT_ALL), elencato come esposizione irrisolta; nessuna cancellazione dalla verifica."""
    c, tavolo = _m115_banco(tmp_path, "n7b.db", intervallo=60.0, max_session_loss=30.0)
    _ambiente(c, "LIVE", conto=CONTO_A)
    conto = _ContoLive(CONTO_A, [_ordine_bot(tavolo)])
    _connetti(c, conto)
    c.rc.mode = RuntimeMode.ACTIVE
    c.rc._session_pnl_baseline = 0.0
    _settle(c, -40.0)
    assert str(c.rc._risk_stop_reason).startswith("session_loss_breached")
    RuntimeController._reconcile_restored_emergency(c.rc, trigger="test")
    vista = c.rc.get_status()["live_exposure"]
    assert conto.cancellazioni == [] and conto.letture > 0
    assert vista["state"] == "NON_RICONCILIATA" and [o["bet_id"] for o in vista["open_orders"]] == ["A-1"]
    c.rc._emergency_reconcile_pending = False


def test_m136_n7_stop_di_sessione_col_runtime_live_programma_la_verifica_in_sola_lettura(tmp_path):
    """N7 (owner M136): stop di sessione globale col runtime gia' LIVE => verifica programmata (recovery dedicato), in
    sola lettura (il CASHOUT_ALL lavora sul LIVE: nessuna cancellazione delle sue coperture); conto irraggiungibile =>
    ritenta; tornato raggiungibile => verificata. Il reset resta rifiutato col superamento attivo."""
    c = _banco(tmp_path, "n7.db", max_session_loss=30.0)
    c.rc._live_recovery_interval_s = 0.05
    c.rc._route_cashout_signal = lambda segnale: None
    _ambiente(c, "LIVE", conto=CONTO_A)
    stato = {"raggiungibile": False}
    conto = _ContoLive(CONTO_A, [{"betId": "H-1", "marketId": "1.977", "status": "EXECUTABLE", "sizeRemaining": 1.0,
                                  "customerOrderRef": "hedge-del-bot", "side": "BACK", "priceSize": {"price": 2.0}}])

    def cliente():
        if not stato["raggiungibile"]:
            raise ConnectionError("LIVE non raggiungibile")
        return conto

    c.rc.betfair_service.get_live_client = cliente
    c.rc.mode = RuntimeMode.ACTIVE
    c.rc._session_pnl_baseline = 0.0
    _settle(c, -40.0)
    assert str(c.rc._risk_stop_reason).startswith("session_loss_breached")
    assert c.rc.__dict__.get("_emergency_reconcile_pending") is True
    assert RuntimeController._live_recovery_active(c.rc), "nessuna verifica programmata"
    stato["raggiungibile"] = True
    assert _attendi(lambda: c.rc.get_status()["live_exposure"]["state"] != "SCONOSCIUTA")
    assert conto.cancellazioni == [], "la verifica dello stop di sessione LIVE ha cancellato ordini"
    assert c.rc.reset_risk_stop()["risk_stop_reset"] is False or c.rc.get_status()["live_exposure"]["state"] in (
        "NESSUNA", "VERIFICATA")
    assert _attendi(lambda: c.rc.__dict__.__setitem__("_emergency_reconcile_pending", False)
                    or not RuntimeController._live_recovery_active(c.rc))


def _f1_banco(tmp_path, nome, **cfg):
    c = _banco(tmp_path, nome, **cfg)
    c.rc._route_cashout_signal = lambda segnale: None
    _ambiente(c, "SIM")
    c.rc.mode = RuntimeMode.ACTIVE
    return c


def test_m134_f1_dieci_riavvii_con_start_non_azzerano_la_sessione_ne_lo_stop(tmp_path):
    """F1 (owner M134): sessione di rischio aperta con start(), perdita SIM -40 (limite 30) => stop. Dieci riavvii, ogni
    volta con start(): perdita di sessione sempre 40, stop presente, nessun ingresso; la sessione ha sempre lo stesso id."""
    c = _f1_banco(tmp_path, "f1.db", max_session_loss=30.0)
    RuntimeController._open_or_resume_risk_session(c.rc)
    sid = c.rc._risk_session["id"]
    _settle(c, -40.0)
    assert str(c.rc._risk_stop_reason).startswith("session_loss_breached")
    for _ in range(10):
        r = _f1_banco(tmp_path, "f1.db", max_session_loss=30.0)
        RuntimeController._open_or_resume_risk_session(r.rc)          # come start(): riprende, mai azzera
        assert r.rc._risk_session["id"] == sid
        assert r.rc._session_loss_block_reason().startswith("session_loss_breached:loss=40.00")
        assert r.rc._risk_stop_reason and r.rc._loss_block_reason()
        assert r.rc.reset_risk_stop(at_start=True)["risk_stop_reset"] is False
        nuovi, _motivo = _ingresso(r, "1.9%02d" % (10 + _))
        assert nuovi == 0


def test_m134_f1_reset_esplicito_chiude_la_sessione_superata_e_ne_apre_una_nuova(tmp_path):
    """F1: il reset esplicito (ricontrollo completo pulito) chiude la sessione superata, salvata fra le chiuse, e ne apre
    una nuova a zero; dopo un riavvio la nuova sessione resta quella aperta. Sessioni consecutive con profitti."""
    c = _f1_banco(tmp_path, "f1r.db", max_session_loss=30.0)
    RuntimeController._open_or_resume_risk_session(c.rc)
    vecchia = c.rc._risk_session["id"]
    _settle(c, -40.0)
    esito = _reset_confermato(c.rc)
    assert esito["risk_stop_reset"] is True, esito
    assert c.rc._risk_session["id"] != vecchia and [s["id"] for s in c.rc._risk_session["closed"]] == [vecchia]
    assert c.rc._session_loss_block_reason() == ""
    _settle(c, 10.0)
    _settle(c, -20.0)                                    # sessione 2: +9.55 -20 => perdita 10.45, sotto il limite
    assert c.rc._session_loss_block_reason() == "" and not c.rc._risk_stop_reason
    r = _f1_banco(tmp_path, "f1r.db", max_session_loss=30.0)
    assert r.rc._risk_session["id"] == c.rc._risk_session["id"]
    assert r.rc._session_loss_block_reason() == ""
    _settle(r, -25.0)                                    # 10.45 + 25 > 30: la sessione 2 continua dopo il riavvio
    assert str(r.rc._risk_stop_reason).startswith("session_loss_breached")


def test_m134_f1_settlement_tardivo_attribuito_alla_sessione_d_origine(tmp_path):
    """F1: una scommessa della sessione 1 regolata dopo la sua chiusura va alla sessione 1 (mai alla 2); se porta la
    sessione 1 oltre il limite scatta uno stop persistente (locale SIM), senza toccare la perdita della sessione 2.
    La riconsegna dello stesso settlement non conta due volte."""
    c = _f1_banco(tmp_path, "f1t.db", max_session_loss=30.0)
    RuntimeController._open_or_resume_risk_session(c.rc)
    vecchia = c.rc._risk_session["id"]
    tavolo = _riserva(c, "1.931")
    assert tavolo.meta["risk_session_id"] == vecchia
    _settle(c, -20.0)
    assert RuntimeController._close_risk_session_locked(c.rc) is True      # chiusura (sotto il limite)
    nuova = c.rc._risk_session["id"]
    tardivo = _canonical_close_payload(event_key=tavolo.current_event_key, batch_id="b-f1-tardivo",
                                       correlation_id="c-f1-tardivo", table_id=tavolo.table_id,
                                       customer_ref=tavolo.meta["customer_ref"], market_id=tavolo.market_id,
                                       gross_pnl=-15.0, commission_amount=0.0, net_pnl=-15.0, pnl=-15.0)
    c.rc._on_close_position(dict(tardivo))              # tardivo della sessione 1: 20 + 15 > 30
    assert c.rc._session_loss_block_reason() == "", "perdita tardiva attribuita alla sessione nuova"
    assert f"sessione_precedente={vecchia}" in str(c.rc._risk_stop_reason)
    chiusa = next(s for s in c.rc._risk_session["closed"] if s["id"] == vecchia)
    assert chiusa["delta"]["desk"] == pytest.approx(-35.0)
    r = _f1_banco(tmp_path, "f1t.db", max_session_loss=30.0)
    assert r.rc._risk_session["id"] == nuova and f"sessione_precedente={vecchia}" in str(r.rc._risk_stop_reason)
    assert next(s for s in r.rc._risk_session["closed"] if s["id"] == vecchia)["delta"]["desk"] == pytest.approx(-35.0)
    r.rc._on_close_position(dict(tardivo))              # riconsegna IDENTICA dopo il riavvio: nessun doppio conteggio
    assert next(s for s in r.rc._risk_session["closed"] if s["id"] == vecchia)["delta"]["desk"] == pytest.approx(-35.0)
    assert r.rc._session_loss_block_reason() == ""


def test_m134_f1_sessione_non_verificabile_blocco_conservativo(tmp_path):
    c = _f1_banco(tmp_path, "f1u.db", max_session_loss=30.0)
    RuntimeController._open_or_resume_risk_session(c.rc)
    tavolo = _riserva(c, "1.932")
    c.rc._risk_session["id"] = "sessione-diversa"       # la sessione della scommessa non e' ne' corrente ne' chiusa
    _settle_tavolo(c, tavolo, -1.0)
    assert "sessione_di_rischio_non_verificabile" in str(c.rc._risk_stop_reason)


def test_m134_f1_salvataggio_fallito_alla_chiusura_lo_stop_resta(tmp_path, monkeypatch):
    """F1: la sessione superata non si chiude senza prova su disco: libro non scrivibile (False) => reset rifiutato,
    stop e sessione invariati; dopo il riavvio la perdita e' ancora quella."""
    c = _f1_banco(tmp_path, "f1p.db", max_session_loss=30.0)
    RuntimeController._open_or_resume_risk_session(c.rc)
    sid = c.rc._risk_session["id"]
    _settle(c, -40.0)
    vero = c.rc.db.save_settings
    monkeypatch.setattr(c.rc.db, "save_settings", lambda valori, *a, **k: False
                        if daily_loss_store.DAILY_LOSS_STATE_KEY in (valori or {}) else vero(valori, *a, **k))
    esito = _reset_confermato(c.rc)
    assert esito == {"risk_stop_reset": False, "reason": "sessione_di_rischio_non_salvata"}
    assert c.rc._risk_session["id"] == sid and c.rc._risk_stop_reason
    monkeypatch.undo()
    r = _f1_banco(tmp_path, "f1p.db", max_session_loss=30.0)
    assert r.rc._risk_stop_reason and r.rc._session_loss_block_reason().startswith("session_loss_breached:loss=40.00")


def test_m134_f1_libro_illeggibile_stop_di_sessione_persistente(tmp_path):
    c = _f1_banco(tmp_path, "f1c.db", max_session_loss=30.0)
    RuntimeController._open_or_resume_risk_session(c.rc)
    c.rc.db.save_settings({daily_loss_store.DAILY_LOSS_STATE_KEY: "{corrotto"})
    r = _f1_banco(tmp_path, "f1c.db", max_session_loss=30.0)
    assert r.rc._risk_stop_reason == "session_loss_breached:sessione_di_rischio_illeggibile"


def test_m134_f1_chiusura_esplicita_ammessa_solo_senza_superamento(tmp_path):
    c = _f1_banco(tmp_path, "f1e.db", max_session_loss=30.0)
    RuntimeController._open_or_resume_risk_session(c.rc)
    _settle(c, -10.0)
    assert c.rc.close_risk_session()["risk_session_closed"] is True
    assert c.rc._session_loss_block_reason() == ""
    _settle(c, -40.0)
    assert c.rc.close_risk_session() == {"risk_session_closed": False, "reason": "risk_stop_attivo"}
    c.rc._risk_stop_reason = ""                          # anche senza stop, una sessione superata non si chiude
    assert c.rc.close_risk_session()["reason"].startswith("session_loss_breached")


def test_m134_f1_fra_db_e_marker_vale_la_sessione_con_revisione_piu_alta():
    def libro(rev, desk, seq):
        return json.dumps({"version": 2, "scopes": {}, "legacy": None, "seq": seq, "settled": None,
                           "risk_session": {"id": "s1", "opened_at": "", "rev": rev, "closed": [],
                                            "delta": {"desk": desk, "scopes": {}, "unscoped": 0.0}}})
    oggi = "2026-11-17"
    a = daily_loss_store.parse_book(libro(5, -40.0, 1), today_utc=oggi)
    b = daily_loss_store.parse_book(libro(4, -10.0, 2), today_utc=oggi)
    assert daily_loss_store.reconcile_books(a, b)["risk_session"]["delta"]["desk"] == -40.0
    assert daily_loss_store.reconcile_books(b, a)["risk_session"]["delta"]["desk"] == -40.0
    pari = daily_loss_store.parse_book(libro(5, -10.0, 2), today_utc=oggi)
    assert daily_loss_store.reconcile_books(pari, a)["risk_session"]["delta"]["desk"] == -40.0
    with pytest.raises(ValueError):
        daily_loss_store.parse_book(libro("x", -1.0, 1), today_utc=oggi)


def test_m134_f1_cambio_sim_live_e_conto_non_azzera_la_sessione(tmp_path):
    c = _f1_banco(tmp_path, "f1m.db", max_session_loss=30.0)
    RuntimeController._open_or_resume_risk_session(c.rc)
    _ambiente(c, "LIVE", conto=CONTO_A)
    c.rc.betfair_service.get_live_client = lambda: _ContoLive(CONTO_A)
    _settle(c, -40.0)
    assert str(c.rc._risk_stop_reason).startswith("session_loss_breached")
    for env, conto in (("SIM", CONTO_A), ("LIVE", CONTO_B), ("LIVE", CONTO_A)):
        r = _f1_banco(tmp_path, "f1m.db", max_session_loss=30.0)
        _ambiente(r, env, conto=conto)
        RuntimeController._open_or_resume_risk_session(r.rc)
        assert r.rc._risk_stop_reason and r.rc._loss_block_reason()
        assert r.rc._session_loss_reason_for(_live(r, CONTO_A)).startswith("session_loss_breached:loss=40.00")
        r.rc._emergency_reconcile_pending = False
    c.rc._emergency_reconcile_pending = False


# ============================================================================ M139: G1 reset confermato, G2 archivio
from core import risk_session_ledger


def _g_banco(tmp_path, nome, **cfg):
    c = _f1_banco(tmp_path, nome, max_session_loss=30.0, **cfg)
    RuntimeController._open_or_resume_risk_session(c.rc)
    return c


def _registro(c):
    return risk_session_ledger.load(c.rc.db)


def test_m139_g1_reset_con_doppia_conferma_valida_e_audit_completo(tmp_path):
    c = _g_banco(tmp_path, "g1ok.db")
    vecchia = c.rc._risk_session["id"]
    _settle(c, -40.0)
    richiesta = c.rc.request_risk_session_reset(operator="mario", authorization="AUT-7", reason="ripresa")
    assert richiesta["requested"] and richiesta["session_id"] == vecchia and richiesta["exposure_state"]
    esito = c.rc.reset_risk_stop(operator="mario", authorization="AUT-7", challenge_id=richiesta["challenge_id"],
                                 confirm=True, confirm_again=True)
    assert esito == {"risk_stop_reset": True, "reason": ""}
    tipi = [e["kind"] for e in _registro(c)]
    assert tipi == ["RESET_STARTED", "SESSION_CLOSED", "RESET_COMPLETED"], tipi
    avvio = _registro(c)[0]["payload"]
    assert avvio["operator"] == "mario" and avvio["authorization"] == "AUT-7" and avvio["session_id"] == vecchia
    assert avvio["new_session_id"] == c.rc._risk_session["id"] and avvio["delta"]["desk"] == pytest.approx(-40.0)
    assert avvio["session_loss"].startswith("session_loss_breached") and avvio["at"] and avvio["exposure_state"]
    assert not c.rc._risk_stop_reason


@pytest.mark.parametrize("caso", ["senza_richiesta", "senza_seconda_conferma", "operatore_diverso", "scaduta"])
def test_m139_g1_conferma_mancante_errata_o_scaduta_rifiuta(tmp_path, monkeypatch, caso):
    c = _g_banco(tmp_path, f"g1no_{caso}.db")
    sid = c.rc._risk_session["id"]
    _settle(c, -40.0)
    richiesta = c.rc.request_risk_session_reset(operator="mario", authorization="AUT-7", reason="ripresa")
    args = dict(operator="mario", authorization="AUT-7", challenge_id=richiesta["challenge_id"], confirm=True,
                confirm_again=True)
    if caso == "senza_richiesta":
        args["challenge_id"] = "inventata"
    elif caso == "senza_seconda_conferma":
        args["confirm_again"] = False
    elif caso == "operatore_diverso":
        args["operator"] = "altro"
    else:
        monkeypatch.setattr(RuntimeController, "_RESET_CHALLENGE_TTL_S", -1.0)
        richiesta = c.rc.request_risk_session_reset(operator="mario", authorization="AUT-7", reason="ripresa")
        args["challenge_id"] = richiesta["challenge_id"]
    esito = c.rc.reset_risk_stop(**args)
    attesi = {"senza_richiesta": "conferma_non_richiesta_o_diversa", "senza_seconda_conferma": "doppia_conferma_richiesta",
              "operatore_diverso": "operatore_o_autorizzazione_diversi", "scaduta": "conferma_scaduta"}
    assert esito == {"risk_stop_reset": False, "reason": attesi[caso]}
    assert c.rc._risk_session["id"] == sid and c.rc._risk_stop_reason and _registro(c) == []
    assert c.rc.reset_risk_stop()["reason"] == "conferma_operatore_richiesta"     # senza nulla: mai


def test_m139_g1_conferma_monouso_non_riutilizzabile(tmp_path):
    c = _g_banco(tmp_path, "g1dup.db")
    _settle(c, -40.0)
    richiesta = c.rc.request_risk_session_reset(operator="mario", authorization="AUT-7", reason="ripresa")
    args = dict(operator="mario", authorization="AUT-7", challenge_id=richiesta["challenge_id"], confirm=True,
                confirm_again=True)
    assert c.rc.reset_risk_stop(**args)["risk_stop_reset"] is True
    _settle(c, -40.0)                                      # nuova sessione superata
    c.rc.risk_stop("session_loss_breached:test", scope=_sim())
    assert c.rc.reset_risk_stop(**args)["reason"] == "conferma_gia_usata"
    assert c.rc._risk_stop_reason


@pytest.mark.parametrize("guasto", ["eccezione", "falso"])
def test_m139_g1_audit_non_scrivibile_nessun_reset(tmp_path, monkeypatch, guasto):
    c = _g_banco(tmp_path, f"g1au_{guasto}.db")
    sid = c.rc._risk_session["id"]
    _settle(c, -40.0)
    richiesta = c.rc.request_risk_session_reset(operator="mario", authorization="AUT-7", reason="ripresa")

    def rotto(evento):
        if guasto == "eccezione":
            raise OSError("audit giu'")
        return False

    monkeypatch.setattr(c.rc.db, "insert_audit_event", rotto)
    esito = c.rc.reset_risk_stop(operator="mario", authorization="AUT-7", challenge_id=richiesta["challenge_id"],
                                 confirm=True, confirm_again=True)
    assert esito == {"risk_stop_reset": False, "reason": "audit_non_scrivibile"}
    assert c.rc._risk_session["id"] == sid and c.rc._risk_stop_reason
    monkeypatch.undo()
    r = _f1_banco(tmp_path, f"g1au_{guasto}.db", max_session_loss=30.0)
    assert r.rc._risk_session["id"] == sid and r.rc._risk_stop_reason


@pytest.mark.parametrize("punto", ["prima_della_chiusura", "dopo_la_chiusura"])
def test_m139_g1_crash_durante_il_reset_recuperato_all_avvio(tmp_path, monkeypatch, punto):
    c = _g_banco(tmp_path, f"g1cr_{punto}.db")
    sid = c.rc._risk_session["id"]
    _settle(c, -40.0)
    richiesta = c.rc.request_risk_session_reset(operator="mario", authorization="AUT-7", reason="ripresa")
    if punto == "prima_della_chiusura":
        def muore(self, *a, **k):
            raise SystemExit("crash")
        monkeypatch.setattr(RuntimeController, "_open_new_risk_session", muore)
    else:
        vero = risk_session_ledger.append

        def muore_al_completamento(db, kind, payload):
            if kind == "RESET_COMPLETED":
                raise SystemExit("crash")
            return vero(db, kind, payload)
        monkeypatch.setattr(risk_session_ledger, "append", muore_al_completamento)
    with pytest.raises(SystemExit):
        c.rc.reset_risk_stop(operator="mario", authorization="AUT-7", challenge_id=richiesta["challenge_id"],
                             confirm=True, confirm_again=True)
    monkeypatch.undo()
    r = _f1_banco(tmp_path, f"g1cr_{punto}.db", max_session_loss=30.0)
    tipi = [e["kind"] for e in _registro(r)]
    assert r.rc._risk_stop_reason, "lo stop non deve sparire con un reset interrotto"
    if punto == "prima_della_chiusura":
        assert tipi[-1] == "RESET_FAILED" and r.rc._risk_session["id"] == sid
    else:
        assert tipi[-1] == "RESET_COMPLETED" and _registro(r)[-1]["payload"]["recovered"] is True
        assert r.rc._risk_session["id"] != sid


def test_m139_g1_reset_confermato_non_supera_altri_limiti(tmp_path):
    c = _g_banco(tmp_path, "g1lim.db", max_daily_loss=30.0)
    _ambiente(c, "LIVE", conto=CONTO_A)
    _connetti(c, _ContoLive(CONTO_A))
    _settle(c, -40.0)                                      # sessione e giornaliero LIVE superati: Emergency Stop
    RuntimeController._reconcile_restored_emergency(c.rc, trigger="t")
    richiesta = c.rc.request_risk_session_reset(operator="mario", authorization="AUT-7", reason="ripresa")
    esito = c.rc.reset_risk_stop(operator="mario", authorization="AUT-7", challenge_id=richiesta.get("challenge_id"),
                                 confirm=True, confirm_again=True)
    assert esito["risk_stop_reset"] is False and c.rc._emergency_stopped is True
    _ambiente(c, "SIM")
    assert _ingresso(c, "1.9701")[0] == 0
    c.rc._emergency_reconcile_pending = False


def _chiudi_sessioni(c, n):
    for _ in range(n):
        esito = c.rc.close_risk_session()
        assert esito["risk_session_closed"] is True, esito


def test_m139_g2_oltre_venti_sessioni_archivio_completo_e_ricostruito(tmp_path):
    c = _g_banco(tmp_path, "g2arch.db")
    ids = []
    for _ in range(25):
        _settle(c, -1.0)
        ids.append(c.rc._risk_session["id"])
        _chiudi_sessioni(c, 1)
    archiviate = [e["payload"]["id"] for e in _registro(c) if e["kind"] == "SESSION_CLOSED"]
    assert archiviate == ids and len(c.rc._risk_session["closed"]) == RuntimeController._RISK_SESSION_CLOSED_CAP
    assert all(e["payload"]["delta"]["desk"] == pytest.approx(-1.0) and "accounts" in e["payload"]
               and "limit" in e["payload"] and e["payload"]["opened_at"] for e in _registro(c))
    r = _f1_banco(tmp_path, "g2arch.db", max_session_loss=30.0)
    indice = RuntimeController._risk_archive(r.rc)
    assert set(ids) <= set(indice) and indice[ids[0]]["delta"]["desk"] == pytest.approx(-1.0)


def test_m139_g2_tardivo_di_una_sessione_oltre_le_venti_attribuito_e_protetto(tmp_path):
    c = _g_banco(tmp_path, "g2late.db")
    vecchia = c.rc._risk_session["id"]
    tavolo = _riserva(c, "1.9702")
    _settle(c, -20.0)
    _chiudi_sessioni(c, 22)
    assert vecchia not in {s["id"] for s in c.rc._risk_session["closed"]}, "deve essere fuori dalla finestra"
    tardivo = _canonical_close_payload(event_key=tavolo.current_event_key, batch_id="b-g2", correlation_id="c-g2",
                                       table_id=tavolo.table_id, customer_ref=tavolo.meta["customer_ref"],
                                       market_id=tavolo.market_id, gross_pnl=-15.0, commission_amount=0.0,
                                       net_pnl=-15.0, pnl=-15.0)
    c.rc._on_close_position(dict(tardivo))
    assert c.rc._session_loss_block_reason() == "", "tardivo contato nella sessione corrente"
    assert f"sessione_precedente={vecchia}" in str(c.rc._risk_stop_reason)
    tardivi = [e["payload"] for e in _registro(c) if e["kind"] == "LATE_SETTLEMENT"]
    assert [t["session_id"] for t in tardivi] == [vecchia] and not c.rc._risk_session["archive_pending"]
    r = _f1_banco(tmp_path, "g2late.db", max_session_loss=30.0)
    assert RuntimeController._risk_archive(r.rc)[vecchia]["delta"]["desk"] == pytest.approx(-35.0)
    r.rc._on_close_position(dict(tardivo))                 # riconsegna identica dopo il riavvio
    assert RuntimeController._risk_archive(r.rc)[vecchia]["delta"]["desk"] == pytest.approx(-35.0)
    assert len([e for e in _registro(r) if e["kind"] == "LATE_SETTLEMENT"]) == 1


def test_m139_g2_archivio_non_scrivibile_la_sessione_resta_nella_finestra_e_si_ripara(tmp_path, monkeypatch):
    c = _g_banco(tmp_path, "g2fail.db")
    prima = c.rc._risk_session["id"]
    monkeypatch.setattr(c.rc.db, "insert_audit_event", lambda evento: False)
    _chiudi_sessioni(c, 1)
    _chiudi_sessioni(c, RuntimeController._RISK_SESSION_CLOSED_CAP + 2)
    assert prima in {s["id"] for s in c.rc._risk_session["closed"]}, "sessione non archiviata uscita dalla finestra"
    monkeypatch.undo()
    r = _f1_banco(tmp_path, "g2fail.db", max_session_loss=30.0)
    assert prima in {e["payload"]["id"] for e in _registro(r) if e["kind"] == "SESSION_CLOSED"}


def test_m139_g2_tardivo_in_attesa_scritto_al_riavvio_senza_doppioni(tmp_path, monkeypatch):
    c = _g_banco(tmp_path, "g2pend.db")
    vecchia = c.rc._risk_session["id"]
    tavolo = _riserva(c, "1.9703")
    _chiudi_sessioni(c, 1)
    vero = c.rc.db.insert_audit_event
    monkeypatch.setattr(c.rc.db, "insert_audit_event", lambda evento: False)
    c.rc._on_close_position(_canonical_close_payload(
        event_key=tavolo.current_event_key, batch_id="b-g2p", correlation_id="c-g2p", table_id=tavolo.table_id,
        customer_ref=tavolo.meta["customer_ref"], market_id=tavolo.market_id, gross_pnl=-5.0, commission_amount=0.0,
        net_pnl=-5.0, pnl=-5.0))
    assert [p["session_id"] for p in c.rc._risk_session["archive_pending"]] == [vecchia]
    monkeypatch.setattr(c.rc.db, "insert_audit_event", vero)
    r = _f1_banco(tmp_path, "g2pend.db", max_session_loss=30.0)
    assert not r.rc._risk_session["archive_pending"]
    assert len([e for e in _registro(r) if e["kind"] == "LATE_SETTLEMENT"]) == 1
    s = _f1_banco(tmp_path, "g2pend.db", max_session_loss=30.0)
    assert len([e for e in _registro(s) if e["kind"] == "LATE_SETTLEMENT"]) == 1


def test_m139_g2_registro_manomesso_non_verificabile_blocco_conservativo(tmp_path):
    c = _g_banco(tmp_path, "g2tamper.db")
    vecchia = c.rc._risk_session["id"]
    tavolo = _riserva(c, "1.9704")
    _chiudi_sessioni(c, RuntimeController._RISK_SESSION_CLOSED_CAP + 2)
    riga = dict(c.rc.db._execute("SELECT id, event_json FROM audit_events WHERE event_json LIKE ? ORDER BY id ASC "
                                 "LIMIT 1", ("%SESSION_CLOSED%",), fetchone=True))
    evento = json.loads(riga["event_json"])
    evento["payload"]["delta"]["desk"] = 5.0                  # manomissione del P&L archiviato (hash non piu' valido)
    c.rc.db._execute("UPDATE audit_events SET event_json = ? WHERE id = ?", (json.dumps(evento), riga["id"]),
                     commit=True)
    with pytest.raises(risk_session_ledger.LedgerUnverifiable):
        risk_session_ledger.load(c.rc.db)
    r = _f1_banco(tmp_path, "g2tamper.db", max_session_loss=30.0)
    r.rc._on_close_position(_canonical_close_payload(
        event_key=tavolo.current_event_key, batch_id="b-g2t", correlation_id="c-g2t", table_id=tavolo.table_id,
        customer_ref=tavolo.meta["customer_ref"], market_id=tavolo.market_id, gross_pnl=-1.0, commission_amount=0.0,
        net_pnl=-1.0, pnl=-1.0))
    assert "sessione_di_rischio_non_verificabile" in str(r.rc._risk_stop_reason)
    assert vecchia


def _gui_su(rc, testi, conferme):
    import mini_gui
    gui = object.__new__(mini_gui.MiniPickfairGUI)
    gui._test_mode = True
    gui._test_text_answers = dict(testi)
    gui._test_confirm_answers = list(conferme)
    gui.runtime = rc
    gui.uiq = type("Q", (), {"post": staticmethod(lambda fn, *a: fn(*a))})()
    gui.errori, gui.registro = [], []
    gui._safe_show_error = lambda titolo, testo: gui.errori.append((titolo, testo))
    gui._log = lambda testo: gui.registro.append(testo)
    gui._run_runtime_command_async = lambda nome, fn: gui.registro.append((nome, fn()))
    return gui


_TESTI_GUI = {"Operatore autorizzato:": "mario", "Riferimento dell'autorizzazione:": "AUT-GUI",
              "Motivo del reset:": "ripresa"}


@pytest.mark.parametrize("conferme,atteso", [([True, True], True), ([True, False], False), ([False], False),
                                             ([], False)])
def test_m139_g1_gui_doppia_conferma_sul_runtime_reale(tmp_path, conferme, atteso):
    c = _g_banco(tmp_path, f"g1gui_{len(conferme)}_{atteso}.db")
    _settle(c, -40.0)
    gui = _gui_su(c.rc, _TESTI_GUI, conferme)
    gui._runtime_reset_risk_session()
    assert (not c.rc._risk_stop_reason) is atteso
    tipi = [e["kind"] for e in _registro(c)]
    assert tipi == (["RESET_STARTED", "SESSION_CLOSED", "RESET_COMPLETED"] if atteso else []), tipi
    if atteso:
        assert _registro(c)[0]["payload"]["operator"] == "mario"


def test_m139_g1_gui_senza_operatore_o_senza_dialoghi_reali_non_conferma_mai(tmp_path, monkeypatch):
    import mini_gui
    c = _g_banco(tmp_path, "g1gui_vuoto.db")
    _settle(c, -40.0)
    gui = _gui_su(c.rc, {"Operatore autorizzato:": "mario"}, [True, True])
    gui._runtime_reset_risk_session()
    assert c.rc._risk_stop_reason and _registro(c) == []
    gui = _gui_su(c.rc, _TESTI_GUI, [])
    gui._test_mode = False
    monkeypatch.setattr(mini_gui, "_REAL_DIALOGS", False)       # fallback senza tkinter: risponderebbe sempre si'
    assert gui._ask_confirm("x", "y") is False and gui._ask_text("x", "y") is None
    gui._runtime_reset_risk_session()
    assert c.rc._risk_stop_reason and _registro(c) == []


def _headless_su(rc):
    from headless_main import HeadlessApp
    app = HeadlessApp.__new__(HeadlessApp)
    app.runtime = rc
    return app


@pytest.mark.parametrize("conferme,rc_atteso", [((True, True), 0), ((True, False), 4), ((False, False), 4)])
def test_m139_g1_headless_stesso_contratto(tmp_path, conferme, rc_atteso):
    c = _g_banco(tmp_path, f"g1cli_{conferme[0]}_{conferme[1]}.db")
    _settle(c, -40.0)
    app = _headless_su(c.rc)
    rc = app._risk_session_reset_cli({"operator": "mario", "authorization": "AUT-CLI", "reason": "ripresa",
                                      "confirm": conferme[0], "confirm_again": conferme[1]})
    assert rc == rc_atteso and bool(c.rc._risk_stop_reason) is (rc_atteso != 0)
    assert [e["kind"] for e in _registro(c)][:1] == (["RESET_STARTED"] if rc_atteso == 0 else [])


def test_m139_g1_headless_opzioni_dalla_riga_di_comando(monkeypatch):
    from headless_main import HeadlessApp
    app = HeadlessApp.__new__(HeadlessApp)
    monkeypatch.setattr("sys.argv", ["headless_main.py", "--risk-session-reset", "--operator=Mario",
                                     "--authorization=AUT-9", "--reason=Ripresa", "--confirm"])
    letti = app._parse_args()["risk_session_reset"]
    assert letti == {"operator": "Mario", "authorization": "AUT-9", "reason": "Ripresa", "confirm": True,
                     "confirm_again": False}
    monkeypatch.setattr("sys.argv", ["headless_main.py"])
    assert app._parse_args()["risk_session_reset"] is None


def test_m139_g2_indice_archivio_conta_una_chiave_una_volta_e_verifica_la_catena(tmp_path):
    c = _g_banco(tmp_path, "g2idx.db")
    db = c.rc.db
    assert risk_session_ledger.append(db, "SESSION_CLOSED", {"id": "S1", "delta": {"desk": -10.0, "scopes": {},
                                                                                   "unscoped": 0.0}})
    tardivo = {"session_id": "S1", "settlement_key": "k1", "delta": {"desk": -5.0, "scopes": {}, "unscoped": 0.0}}
    assert risk_session_ledger.append(db, "LATE_SETTLEMENT", tardivo)
    assert risk_session_ledger.append(db, "LATE_SETTLEMENT", tardivo)          # doppione (difesa in profondita')
    catena = risk_session_ledger.load(db)
    assert [e["seq"] for e in catena] == [1, 2, 3] and catena[1]["prev"] == catena[0]["hash"]
    assert risk_session_ledger.archive_index(catena)["S1"]["delta"]["desk"] == pytest.approx(-15.0)
    assert risk_session_ledger.append(db, "TIPO_INVENTATO", {}) is None

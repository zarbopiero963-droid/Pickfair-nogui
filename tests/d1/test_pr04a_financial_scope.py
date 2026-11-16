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
    assert r.rc._loss_block_reason().startswith("risk_stop_active")
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
    assert r.rc._loss_block_reason().startswith("risk_stop_active")


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

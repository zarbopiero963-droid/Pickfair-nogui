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
    assert record["version"] == 2 and set(record["scopes"]) == {_sim(), _live(c)}
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

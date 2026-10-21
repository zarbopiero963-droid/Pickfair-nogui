"""Sonde Phase 0 PR29-c (settlement -> tavolo per identita'), su main reale. Non committate."""
import json, threading
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _chain, _signal

def _tab(c):
    return {t["table_id"]: (t["current_event_key"], round(float(t["current_exposure"]), 2), round(float(t["loss_amount"]), 2), t["status"])
            for t in c.rc.table_manager.snapshot() if t["current_event_key"] or float(t["loss_amount"]) > 0}

def _place(c, **sig):
    before = len(c.bus.payloads("CMD_QUICK_BET"))
    c.rc._on_signal_received(_signal(**sig))
    cmds = c.bus.payloads("CMD_QUICK_BET")[before:]
    assert len(cmds) == 1, c.bus.payloads("SIGNAL_REJECTED")
    return cmds[0]

def _bet_id(c, ref):
    for o in c.broker.state.orders.values():
        if o.customer_ref == ref:
            return str(o.bet_id)

def _cleared(market, bet, net):
    gross = net if net <= 0 else round(net / 0.955, 2)
    comm = 0.0 if net <= 0 else round(gross - net, 2)
    return {"event_key": f"cleared:{market}:{bet}", "market_id": market, "table_id": None, "batch_id": "",
            "pnl": net, "gross_pnl": gross, "commission_amount": comm, "net_pnl": net, "commission_pct": 4.5,
            "settlement_basis": "market_net_realized", "settlement_source": "betfair_cleared_orders",
            "settlement_kind": "realized_settlement", "settlement_ref": bet}

def _setup(tmp_path, **kw):
    c = _chain(tmp_path, stake=10.0, anti_duplication=True, **kw)
    c.rc.betfair_service.get_account_funds = lambda: {"available": 1000.0, "exposure": 0.0}
    rows = []
    c.rc.betfair_service.list_cleared_orders = lambda bet_status=None, market_ids=None: list(rows)
    return c, rows

def test_s1_single_bet_cleared(tmp_path):
    c, rows = _setup(tmp_path)
    cmd = _place(c)
    bet = _bet_id(c, cmd["customer_ref"]); rows.append({"betId": bet, "marketId": "1.234"})
    print("\nS1 before", _tab(c), "bet", bet, "guard", sorted(c.rc.duplication_guard._active))
    c.rc._on_close_position(_cleared("1.234", bet, -10.0))
    print("S1 after ", _tab(c), "guard", sorted(c.rc.duplication_guard._active))

def test_s2_two_bets_same_market(tmp_path):
    c, rows = _setup(tmp_path)
    a = _place(c, selection_id=11, selection="Over 2.5")
    b = _place(c, selection_id=12, selection="Under 2.5", received_at="2026-10-08T10:00:05+00:00")
    ba, bb = _bet_id(c, a["customer_ref"]), _bet_id(c, b["customer_ref"])
    rows += [{"betId": ba, "marketId": "1.234"}, {"betId": bb, "marketId": "1.234"}]
    print("\nS2 before", _tab(c))
    c.rc._on_close_position(_cleared("1.234", ba, -10.0))
    print("S2 after A", _tab(c))
    c.rc._on_close_position(_cleared("1.234", bb, 9.0))
    print("S2 after B", _tab(c))

def test_s3_ambiguous_then_settled(tmp_path):
    c, rows = _setup(tmp_path)
    cmd = _place(c)
    bet = _bet_id(c, cmd["customer_ref"]); rows.append({"betId": bet, "marketId": "1.234"})
    c.rc._on_quick_bet_ambiguous({**cmd, "status": "AMBIGUOUS", "outcome": "AMBIGUOUS"})
    print("\nS3 ambiguous", _tab(c), "uncertain", dict(c.rc._uncertain_reservations_map()))
    c.rc._on_close_position(_cleared("1.234", bet, -10.0))
    print("S3 after settle", _tab(c), "uncertain", dict(c.rc._uncertain_reservations_map()))

def test_s4_replay(tmp_path):
    c, rows = _setup(tmp_path)
    cmd = _place(c)
    bet = _bet_id(c, cmd["customer_ref"]); rows.append({"betId": bet, "marketId": "1.234"})
    c.rc._on_close_position(_cleared("1.234", bet, -10.0))
    print("\nS4 after 1", _tab(c), "realized", c.rc._processed_realized_pnl_keys)
    c2 = _place(c, selection_id=12, selection="Under 2.5", received_at="2026-10-08T10:00:05+00:00")
    print("S4 new reservation", _tab(c))
    c.rc._on_close_position(_cleared("1.234", bet, -10.0))
    print("S4 after replay", _tab(c))

def test_s5_table_id_path_stale(tmp_path):
    c, rows = _setup(tmp_path)
    cmd = _place(c)
    tid = cmd["table_id"]
    p = _cleared("1.234", "X", -10.0); p.update(table_id=tid, event_key="old-key")
    c.rc._on_close_position(p)
    print("\nS5 after stale table_id close", _tab(c))
    c.rc._on_close_position(p)
    print("S5 after replay", _tab(c))

def test_s6_concurrent(tmp_path):
    c, rows = _setup(tmp_path)
    cmd = _place(c)
    bet = _bet_id(c, cmd["customer_ref"]); rows.append({"betId": bet, "marketId": "1.234"})
    rel = []
    orig = c.rc.table_manager.release
    c.rc.table_manager.release = lambda *a, **k: (rel.append(a), orig(*a, **k))[1]
    th = [threading.Thread(target=c.rc._on_close_position, args=(_cleared("1.234", bet, -10.0),)) for _ in range(4)]
    [t.start() for t in th]; [t.join() for t in th]
    print("\nS6 releases", rel, _tab(c))

def test_s0_debug(tmp_path, caplog):
    import logging
    caplog.set_level(logging.WARNING)
    c, rows = _setup(tmp_path)
    cmd = _place(c)
    p = _cleared("1.234", "X", -10.0); p.update(table_id=cmd["table_id"])
    try:
        c.rc._on_close_position(p)
    except Exception as e:
        print("\nS0 EXC", type(e).__name__, e)
    print("\nS0 sync", c.rc._last_bankroll_sync_result if hasattr(c.rc, "_last_bankroll_sync_result") else None)
    print("S0 tab", _tab(c))
    for r in caplog.records[-8:]:
        print("S0 LOG", r.levelname, r.getMessage()[:200])

def test_s7_by_market_concurrent_and_replay(tmp_path):
    c, rows = _setup(tmp_path)
    cmd = _place(c)
    bet = _bet_id(c, cmd["customer_ref"]); rows.append({"betId": bet, "marketId": "1.234"})
    print("\nS7 pinned before", c.rc.duplication_guard.is_pinned("1.234:11:BACK:default"))
    rel = []
    orig = c.rc.table_manager.release
    c.rc.table_manager.release = lambda *a, **k: (rel.append(a), orig(*a, **k))[1]
    th = [threading.Thread(target=c.rc._on_close_position, args=(_cleared("1.234", bet, -10.0),)) for _ in range(4)]
    [t.start() for t in th]; [t.join() for t in th]
    print("S7 by-market concurrent releases", rel, _tab(c))
    c.rc._on_close_position(_cleared("1.234", bet, -10.0))
    print("S7 by-market replay", rel, _tab(c), "realized", sorted(c.rc._processed_realized_pnl_keys))

def test_s8_by_market_replay_with_new_reservation_same_selection(tmp_path):
    # Stesso mercato e STESSA selezione: dopo il settlement di A e la scadenza della chiave, nuovo segnale B.
    c, rows = _setup(tmp_path)
    a = _place(c)
    ba = _bet_id(c, a["customer_ref"]); rows.append({"betId": ba, "marketId": "1.234"})
    c.rc._on_close_position(_cleared("1.234", ba, -10.0))
    c.rc.duplication_guard.release("1.234:11:BACK:default")
    try:
        b = _place(c, received_at="2026-10-08T11:00:00+00:00", chat_id=-100124)
    except AssertionError as e:
        print("\nS8 second entry rejected", str(e)[:300]); return
    print("\nS8 new B", _tab(c))
    c.rc._on_close_position(_cleared("1.234", ba, -10.0))
    print("S8 after replay of A", _tab(c))

def test_s9_restart_rebuild_then_settlement(tmp_path):
    # Riavvio: F018 ricostruisce tavolo e chiave APPUNTATA dalla riga ordine viva; poi il settlement della bet.
    c, rows = _setup(tmp_path)
    cmd = _place(c)
    bet = _bet_id(c, cmd["customer_ref"]); rows.append({"betId": bet, "marketId": "1.234"})
    from core.table_manager import TableManager
    from core.duplication_guard import DuplicationGuard
    c.rc.table_manager = TableManager(table_count=len(c.rc.table_manager.snapshot()))
    c.rc.duplication_guard = DuplicationGuard()
    res = c.rc._rebuild_open_positions()
    print("\nS9 rebuild", {k: res[k] for k in ("ok", "rebuilt", "uncertain", "closed")}, _tab(c),
          "pinned", c.rc.duplication_guard.is_pinned("1.234:11:BACK:default"))
    c.rc._on_close_position(_cleared("1.234", bet, -10.0))
    print("S9 after settle", _tab(c), "pinned", c.rc.duplication_guard.is_pinned("1.234:11:BACK:default"),
          "active", sorted(c.rc.duplication_guard._active))

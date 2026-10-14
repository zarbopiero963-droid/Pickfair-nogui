"""Scenari di durata: N cicli ingresso/settlement su un solo runtime (1/10/100/1000), con fino a 3 posizioni aperte,
riconsegne duplicate, esiti seeded e (da 100 cicli) un crash/riavvio a meta'. Stesso formato di generate.py.
Uso: python gen_durability.py <outdir>"""
import json, os, random, sys

BASE = {"roserpina.risk_profile": "BALANCED", "roserpina.target_profit_cycle_pct": 3.0,
        "roserpina.max_single_bet_pct": 18.0, "roserpina.max_total_exposure_pct": 35.0,
        "roserpina.max_event_exposure_pct": 18.0, "roserpina.max_stake_pct": 30.0,
        "roserpina.defense_drawdown_pct": 7.5, "roserpina.lockdown_drawdown_pct": 60.0,
        "roserpina.auto_reset_drawdown_pct": 15.0, "roserpina.allow_recovery": True,
        "roserpina.max_recovery_tables": 2, "roserpina.table_count": 5, "roserpina.commission_pct": 4.5,
        "roserpina.anti_duplication_enabled": True, "roserpina.max_stake_abs": 20.0,
        "roserpina.max_open_exposure": 100.0, "roserpina.min_stake": 2.0, "roserpina.min_price": 1.02,
        "roserpina.max_win": 10000.0, "roserpina.liquidity_guard_enabled": False,
        "roserpina.max_daily_loss": 400.0}


def build(n, seed):
    rnd = random.Random(f"dur:{n}:{seed}")
    markets = [f"9.{n:04d}{i:05d}" for i in range(n)]
    steps, phases, open_m = [], [], []
    books, outcomes = {}, {}
    crash_at = n // 2 if n >= 100 else None
    for i, m in enumerate(markets):
        books[m] = {"price": rnd.choice([1.5, 1.8, 2.0, 2.5, 3.0]), "back_size": 100.0, "lay_size": 100.0}
        outcomes[m] = rnd.random() < 0.5
        side = "LAY" if rnd.random() < 0.2 else "BACK"
        steps.append({"op": "signal", "m": m, "side": side})
        if rnd.random() < 0.1:
            steps.append({"op": "signal", "m": m, "side": side})         # riconsegna duplicata
        open_m.append(m)
        if len(open_m) >= 3:
            x = open_m.pop(0)
            steps.append({"op": "settle", "m": x, "win": outcomes[x]})
        if crash_at is not None and i == crash_at:
            steps.append({"op": "crash"})
            phases.append(steps); steps = []
    for x in open_m:
        steps.append({"op": "settle", "m": x, "win": outcomes[x]})
    steps.append({"op": "status"})
    phases.append(steps)
    return {"id": f"D{n:04d}", "seed": f"dur:{n}:{seed}", "capital": 1000, "profile": "BALANCED", "recovery": True,
            "tables": 5, "capmode": "abs", "sequence": f"durability_{n}", "levels": {}, "settings": dict(BASE),
            "books": books, "phases": phases, "outcomes": outcomes}


if __name__ == "__main__":
    out = sys.argv[1]
    os.makedirs(out, exist_ok=True)
    for n in (1, 10, 100, 1000):
        json.dump(build(n, 1), open(os.path.join(out, f"D{n:04d}.json"), "w"))
    print("ok")

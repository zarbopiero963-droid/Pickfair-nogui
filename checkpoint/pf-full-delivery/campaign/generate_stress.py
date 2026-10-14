"""Famiglia STRESS: serie di perdite (LAY a quota alta perse, BACK perse) per raggiungere difesa, lockdown,
auto-reset e stop giornaliero/sessione. 8 capitali x 3 profili x stop giornaliero(si/no) x recovery(si/no) = 96.
Uso: python generate_stress.py <out.jsonl>"""
import itertools, json, random, sys

from generate import CAPITALS, PROFILES


def build(seed=20261015):
    out = []
    n = 0
    for cap, prof, daily, rec in itertools.product(CAPITALS, PROFILES, (True, False), (True, False)):
        n += 1
        rnd = random.Random(f"{seed}:{n}")
        s = dict(PROFILES[prof])
        s.update({"roserpina.allow_recovery": rec, "roserpina.max_recovery_tables": 2 if rec else 0,
                  "roserpina.table_count": 5, "roserpina.commission_pct": 4.5, "roserpina.anti_duplication_enabled": True,
                  "roserpina.max_stake_abs": 10000.0, "roserpina.max_open_exposure": float(cap),
                  "roserpina.min_stake": 0.1, "roserpina.min_price": 1.02, "roserpina.max_win": 1e7,
                  "roserpina.liquidity_guard_enabled": False})
        if daily:
            s["roserpina.max_daily_loss"] = round(cap * 0.10, 2)
            s["roserpina.max_session_loss"] = round(cap * 0.15, 2)
        markets = [f"7.{n:04d}{j:02d}" for j in range(12)]
        books, outcomes, steps = {}, {}, []
        for j, m in enumerate(markets):
            lay = j % 2 == 0
            books[m] = {"price": rnd.choice([3.0, 4.0, 5.0]) if lay else rnd.choice([1.5, 2.0]), "back_size": 1e6,
                        "lay_size": 1e6, "side": "LAY" if lay else "BACK"}
            outcomes[m] = True if lay else False            # sempre perdente per il bot
        for k in range(0, 12, 2):                           # due ingressi, due settlement, ripetuto
            for m in markets[k:k + 2]:
                steps.append({"op": "signal", "m": m, "side": books[m]["side"]})
            for m in markets[k:k + 2]:
                steps.append({"op": "settle", "m": m, "win": outcomes[m]})
        steps.append({"op": "status"})
        out.append({"id": f"X{n:05d}", "seed": f"{seed}:{n}", "capital": cap, "profile": prof, "recovery": rec,
                    "tables": 5, "capmode": "pct", "sequence": "stress_daily" if daily else "stress",
                    "levels": {}, "settings": s, "books": books, "phases": [steps], "outcomes": outcomes})
    return out


if __name__ == "__main__":
    sc = build()
    with open(sys.argv[1], "w") as fh:
        for x in sc:
            fh.write(json.dumps(x) + "\n")
    print(len(sc))

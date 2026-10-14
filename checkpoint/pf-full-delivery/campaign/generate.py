"""Generatore deterministico della matrice base: 8 capitali x 144 configurazioni = 1152 scenari (+ capitali di frontiera).

144 = profilo(3) x recovery(2) x tavoli(1|5) x modalita' cap(%|assoluti) x sequenza(6). I restanti parametri RSR collegati
al motore variano su due livelli con assegnazione seeded; la copertura pairwise effettiva e' misurata e riportata.
Uso: python generate.py <out.jsonl> [--frontier]
"""
import itertools, json, random, sys

CAPITALS = [10, 50, 100, 250, 500, 1000, 2500, 5000]
PROFILES = {
    "CONSERVATIVE": {"roserpina.risk_profile": "CONSERVATIVE", "roserpina.target_profit_cycle_pct": 1.5,
                     "roserpina.max_single_bet_pct": 5.0, "roserpina.max_total_exposure_pct": 15.0,
                     "roserpina.max_event_exposure_pct": 5.0, "roserpina.max_stake_pct": 10.0,
                     "roserpina.defense_drawdown_pct": 5.0, "roserpina.lockdown_drawdown_pct": 12.0,
                     "roserpina.auto_reset_drawdown_pct": 10.0},
    "BALANCED": {"roserpina.risk_profile": "BALANCED", "roserpina.target_profit_cycle_pct": 3.0,
                 "roserpina.max_single_bet_pct": 18.0, "roserpina.max_total_exposure_pct": 35.0,
                 "roserpina.max_event_exposure_pct": 18.0, "roserpina.max_stake_pct": 30.0,
                 "roserpina.defense_drawdown_pct": 7.5, "roserpina.lockdown_drawdown_pct": 20.0,
                 "roserpina.auto_reset_drawdown_pct": 15.0},
    "AGGRESSIVE": {"roserpina.risk_profile": "AGGRESSIVE", "roserpina.target_profit_cycle_pct": 5.0,
                   "roserpina.max_single_bet_pct": 25.0, "roserpina.max_total_exposure_pct": 50.0,
                   "roserpina.max_event_exposure_pct": 25.0, "roserpina.max_stake_pct": 40.0,
                   "roserpina.defense_drawdown_pct": 10.0, "roserpina.lockdown_drawdown_pct": 30.0,
                   "roserpina.auto_reset_drawdown_pct": 25.0},
}
SEQUENCES = ["win", "loss", "alternate", "longloss", "adversarial", "cashout"]
# parametri a due livelli (livello 0, livello 1); i valori in funzione del capitale sono calcolati per scenario
TWO_LEVEL = {
    "roserpina.min_price": (1.02, 1.5),
    "roserpina.book_block": (110.0, 102.0),
    "roserpina.book_warning": (105.0, 101.0),
    "roserpina.max_win_warning_only": (True, False),
    "roserpina.max_win": ("big", "cap5"),
    "roserpina.liquidity_guard_enabled": (True, False),
    "roserpina.liquidity_warning_only": (True, False),
    "roserpina.min_liquidity_absolute": (50.0, 500.0),
    "roserpina.liquidity_multiplier": (3.0, 10.0),
    "roserpina.profit_epsilon_enabled": (True, False),
    "roserpina.profit_epsilon": (0.5, 0.01),
    "roserpina.max_daily_loss": (None, "cap5"),
    "roserpina.max_session_loss": (None, "cap5"),
    "roserpina.max_exposure_stop": (None, "cap20"),
    "roserpina.max_drawdown_hard_stop_pct": (None, 10.0),
    "roserpina.expansion_profit_pct": (5.0, 2.0),
    "roserpina.expansion_multiplier": (1.10, 1.30),
    "roserpina.defense_multiplier": (0.80, 0.50),
    "roserpina.min_stake": (0.1, 2.0),
    "roserpina.auto_green_delay_enabled": (False, True),
    "roserpina.auto_green_delay_sec": (2.5, 1.0),
}
PRICES = [1.3, 1.8, 2.0, 2.5, 4.0]


def resolve(v, capital):
    if v == "big":
        return 10000.0
    if v == "cap5":
        return round(capital * 0.05, 2)
    if v == "cap20":
        return round(capital * 0.20, 2)
    return v


def sequence_steps(kind, rnd, markets, lay_share):
    sides = {m: ("LAY" if rnd.random() < lay_share else "BACK") for m in markets}
    s0, steps1 = [], []
    first, rest = markets[:4], markets[4:]
    for m in first:
        s0.append({"op": "signal", "m": m, "side": sides[m]})
    s0.append({"op": "signal", "m": first[0], "side": sides[first[0]]})          # riconsegna: deve essere duplicato
    s0.append({"op": "price", "m": first[1], "price": 2.2})
    outcomes = {"win": [True] * 6, "loss": [False] * 6, "alternate": [True, False] * 3,
                "longloss": [False] * 5 + [True], "adversarial": [rnd.random() < 0.5 for _ in range(6)],
                "cashout": [True, False, True, False, True, False]}[kind]
    if kind == "adversarial":
        s0 = [{"op": "mode", "m": first[1], "mode": "reject"}, {"op": "mode", "m": first[2], "mode": "partial", "size": 1.0},
              {"op": "mode", "m": first[3], "mode": "lost"}] + s0
    if kind == "cashout":
        s0.append({"op": "cashout", "m": first[0]})
    s0.append({"op": "settle", "m": first[0], "win": outcomes[0]})
    s0.append({"op": "settle", "m": first[1], "win": outcomes[1]})
    for m in rest:
        s0.append({"op": "signal", "m": m, "side": sides[m]})                     # rientro dopo il settlement
    if kind == "adversarial":
        s0.append({"op": "crash"})
        steps1 = [{"op": "signal", "m": first[2], "side": sides[first[2]]},       # riconsegna dopo il riavvio
                  {"op": "settle", "m": first[2], "win": outcomes[2]}, {"op": "settle", "m": first[3], "win": outcomes[3]}]
        for j, m in enumerate(rest):
            steps1.append({"op": "settle", "m": m, "win": outcomes[4 + j]})
        steps1.append({"op": "status"})
        return [s0, steps1], outcomes
    if kind == "cashout":
        s0.append({"op": "risk_stop"})
    s0.append({"op": "settle", "m": first[2], "win": outcomes[2]})
    s0.append({"op": "settle", "m": first[3], "win": outcomes[3]})
    for j, m in enumerate(rest):
        s0.append({"op": "settle", "m": m, "win": outcomes[4 + j]})
    s0.append({"op": "status"})
    return [s0], outcomes


def build(capitals, seed=20261014):
    rnd = random.Random(seed)
    combos = list(itertools.product(PROFILES, (True, False), (1, 5), ("pct", "abs"), SEQUENCES))
    assert len(combos) == 144
    out = []
    n = 0
    for cap in capitals:
        for prof, rec, tables, capmode, seq in combos:
            n += 1
            srnd = random.Random(f"{seed}:{n}")
            settings = dict(PROFILES[prof])
            settings.update({"roserpina.allow_recovery": rec, "roserpina.max_recovery_tables": 2 if rec else 0,
                             "roserpina.table_count": tables, "roserpina.commission_pct": 4.5,
                             "roserpina.anti_duplication_enabled": True})
            if capmode == "abs":
                settings.update({"roserpina.max_stake_abs": round(cap * 0.02, 2), "roserpina.max_open_exposure": round(cap * 0.10, 2)})
            else:
                settings.update({"roserpina.max_stake_abs": 10000.0, "roserpina.max_open_exposure": float(cap)})
            levels = {}
            for k, vals in TWO_LEVEL.items():
                lv = srnd.randint(0, 1)
                levels[k] = lv
                v = resolve(vals[lv], cap)
                if v is not None:
                    settings[k] = v
            markets = [f"1.{n:05d}{j}" for j in range(6)]
            phases, outcomes = sequence_steps(seq, srnd, markets, lay_share=0.25)
            books = {m: {"price": srnd.choice(PRICES), "back_size": 100.0 if srnd.random() > 0.15 else 5.0,
                         "lay_size": 100.0} for m in markets}
            out.append({"id": f"S{n:05d}", "seed": f"{seed}:{n}", "capital": cap, "profile": prof, "recovery": rec,
                        "tables": tables, "capmode": capmode, "sequence": seq, "levels": levels, "settings": settings,
                        "books": books, "phases": phases, "outcomes": dict(zip(markets, outcomes))})
    return out


def pairwise_coverage(scen):
    keys = list(TWO_LEVEL)
    seen = {}
    for s in scen:
        for a, b in itertools.combinations(keys, 2):
            seen.setdefault((a, b), set()).add((s["levels"][a], s["levels"][b]))
    total = len(seen) * 4
    return sum(len(v) for v in seen.values()), total


if __name__ == "__main__":
    caps = list(CAPITALS)
    if "--frontier" in sys.argv:
        caps += [2.0, 3.4, 6.7]       # frontiera: stake target vicino a min_stake / cap per ordine
    scen = build(caps)
    with open(sys.argv[1], "w") as fh:
        for s in scen:
            fh.write(json.dumps(s) + "\n")
    got, tot = pairwise_coverage(scen)
    print(json.dumps({"scenarios": len(scen), "pairwise_pairs_covered": got, "pairwise_pairs_total": tot}))

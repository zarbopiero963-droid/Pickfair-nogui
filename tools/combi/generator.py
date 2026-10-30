"""F-33 — Generatore combinatorio riproducibile (livelli L2 pairwise e L3 three-way).

Contratto (sesto mandato owner 29/10/2026, piano §3):
- inventario dei parametri in JSON (``tools/combi/inventory.json``), con dominio e valori rappresentativi;
- vincoli di validita' espliciti (coppie di valori vietate) dichiarati nei gruppi;
- costruzione greedy (famiglia IPOG) con ``random.Random(seed)`` per i soli pareggi: stesso seed, stessi casi;
- report di copertura: tuple richieste, coperte, percentuale, casi, seed, vincoli, SHA del repository;
- nessuna enumerazione del prodotto cartesiano.

Nessuna dichiarazione di copertura e' valida senza il report (``coverage_report.json``) prodotto da qui.
Il modulo non importa nulla dal runtime di produzione.
"""
from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import os
import random
import subprocess
import sys
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

HERE = os.path.dirname(os.path.abspath(__file__))
INVENTORY_PATH = os.path.join(HERE, "inventory.json")

Assignment = Dict[str, Any]
Constraint = Callable[[Assignment], bool]  # True = assegnazione (anche parziale) ammessa


@dataclass(frozen=True)
class Parameter:
    name: str
    values: Tuple[Any, ...]

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("parametro senza nome")
        if len(self.values) < 2:
            raise ValueError(f"parametro {self.name!r}: servono almeno 2 valori, trovati {len(self.values)}")
        if len(set(_key(v) for v in self.values)) != len(self.values):
            raise ValueError(f"parametro {self.name!r}: valori duplicati")


@dataclass
class CoverageReport:
    strength: int
    seed: int
    parameters: List[str]
    cases: List[Assignment]
    tuples_required: int
    tuples_covered: int
    tuples_excluded_by_constraints: int
    constraints: List[str]
    repo_sha: str
    inventory_sha256: str
    group: str = ""

    @property
    def coverage_pct(self) -> float:
        if self.tuples_required == 0:
            return 100.0
        return round(100.0 * self.tuples_covered / self.tuples_required, 3)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema": "pf-combi-coverage/1",
            "group": self.group,
            "strength": self.strength,
            "seed": self.seed,
            "parameters": list(self.parameters),
            "n_cases": len(self.cases),
            "tuples_required": self.tuples_required,
            "tuples_covered": self.tuples_covered,
            "tuples_excluded_by_constraints": self.tuples_excluded_by_constraints,
            "coverage_pct": self.coverage_pct,
            "complete": self.tuples_covered == self.tuples_required,
            # nessuna tupla richiesta (vincoli contraddittori o dominio degenere): copertura al 100% VUOTA, da non
            # leggere come successo (rilievo A13 locale)
            "vacuous": self.tuples_required == 0,
            "constraints": list(self.constraints),
            "repo_sha": self.repo_sha,
            "inventory_sha256": self.inventory_sha256,
            "cases": [dict(c) for c in self.cases],
        }


def _key(value: Any) -> str:
    """Chiave stabile e hashabile di un valore (None, bool, numeri, stringhe, liste)."""
    return json.dumps(value, sort_keys=True, default=str)


def _ordered_tuple_index(params: Sequence[Parameter], strength: int) -> List[Tuple[int, ...]]:
    return list(itertools.combinations(range(len(params)), strength))


def _tuple_values(params: Sequence[Parameter], idx: Tuple[int, ...]) -> Iterable[Tuple[Any, ...]]:
    return itertools.product(*(params[i].values for i in idx))


def _complete(partial: Assignment, params: Sequence[Parameter], constraints: Sequence[Constraint],
              rng: Optional[random.Random] = None) -> Optional[Assignment]:
    """Estende ``partial`` a un caso completo ammesso dai vincoli (backtracking); None se impossibile.

    Con ``rng`` l'ordine dei valori provati e' casuale ma riproducibile (stesso seed, stesso completamento).
    """
    missing = [p for p in params if p.name not in partial]
    case = dict(partial)

    def rec(i: int) -> bool:
        if i == len(missing):
            return _allowed(case, constraints)
        p = missing[i]
        values = list(p.values)
        if rng is not None:
            rng.shuffle(values)
        for v in values:
            case[p.name] = v
            if _allowed(case, constraints) and rec(i + 1):
                return True
            del case[p.name]
        return False

    return case if rec(0) else None


def _allowed(assignment: Assignment, constraints: Sequence[Constraint]) -> bool:
    return all(c(assignment) for c in constraints)


def generate(
    params: Sequence[Parameter],
    *,
    strength: int = 2,
    seed: int = 0,
    constraints: Sequence[Constraint] = (),
    constraint_names: Sequence[str] = (),
    group: str = "",
    repo_sha: str = "",
    inventory_sha256: str = "",
) -> CoverageReport:
    """Costruisce una copertura t-way greedy, deterministica a parita' di seed.

    Algoritmo (IPOG semplificato, orizzontale+verticale):
    1. si enumerano le tuple t-way richieste, escludendo quelle che un vincolo vieta gia' da sole;
    2. si parte dai casi del prodotto cartesiano dei primi ``strength`` parametri (ammessi);
    3. per ogni parametro successivo si estende ogni caso col valore che copre piu' tuple nuove
       (pareggi risolti da ``random.Random(seed)``), poi si creano nuovi casi per le tuple residue;
    4. i casi finali sono completati sui parametri mancanti con valori ammessi.
    """
    if strength < 1:
        raise ValueError("strength deve essere >= 1")
    if strength > len(params):
        raise ValueError(f"strength {strength} > numero di parametri {len(params)}")
    names = [p.name for p in params]
    if len(set(names)) != len(names):
        raise ValueError("nomi di parametro duplicati")
    rng = random.Random(seed)

    # 1. tuple richieste (chiavi) — escluse quelle vietate dai vincoli valutati sulla sola tupla
    required: Dict[Tuple[int, ...], set] = {}
    excluded = 0
    for idx in _ordered_tuple_index(params, strength):
        bucket = set()
        for values in _tuple_values(params, idx):
            partial = {params[i].name: v for i, v in zip(idx, values)}
            if _allowed(partial, constraints) and _complete(partial, params, constraints) is not None:
                bucket.add(tuple(_key(v) for v in values))
            else:
                excluded += 1
        required[idx] = bucket
    total_required = sum(len(b) for b in required.values())
    covered: Dict[Tuple[int, ...], set] = {idx: set() for idx in required}

    def mark(case: Assignment) -> int:
        new = 0
        for idx, bucket in required.items():
            if all(params[i].name in case for i in idx):
                k = tuple(_key(case[params[i].name]) for i in idx)
                if k in bucket and k not in covered[idx]:
                    covered[idx].add(k)
                    new += 1
        return new

    def would_cover(case: Assignment) -> int:
        n = 0
        for idx, bucket in required.items():
            if all(params[i].name in case for i in idx):
                k = tuple(_key(case[params[i].name]) for i in idx)
                if k in bucket and k not in covered[idx]:
                    n += 1
        return n

    # 2. casi iniziali: prodotto dei primi `strength` parametri
    cases: List[Assignment] = []
    for values in itertools.product(*(p.values for p in params[:strength])):
        case = {params[i].name: v for i, v in enumerate(values)}
        if _allowed(case, constraints):
            cases.append(case)
            mark(case)

    # 3. estensione orizzontale + verticale
    for pi in range(strength, len(params)):
        p = params[pi]
        for case in cases:
            best_vals: List[Any] = []
            best_gain = -1
            for v in p.values:
                trial = dict(case)
                trial[p.name] = v
                if not _allowed(trial, constraints) or _complete(trial, params, constraints) is None:
                    continue
                gain = would_cover(trial)
                if gain > best_gain:
                    best_gain, best_vals = gain, [v]
                elif gain == best_gain:
                    best_vals.append(v)
            if best_vals:
                case[p.name] = rng.choice(best_vals)
                mark(case)
        # verticale: tuple che coinvolgono p ancora scoperte
        for idx, bucket in required.items():
            if pi not in idx or any(i > pi for i in idx):
                continue
            for k in sorted(bucket - covered[idx]):
                values = {params[i].name: json.loads(kk) for i, kk in zip(idx, k)}
                # prova a inserirla in un caso esistente compatibile (parametri gia' fissati uguali o assenti)
                placed = False
                for case in cases:
                    if all(n not in case or _key(case[n]) == _key(values[n]) for n in values):
                        trial = dict(case)
                        trial.update(values)
                        if _allowed(trial, constraints) and _complete(trial, params, constraints) is not None:
                            case.update(values)
                            mark(case)
                            placed = True
                            break
                if not placed:
                    case = dict(values)
                    if _allowed(case, constraints):
                        cases.append(case)
                        mark(case)

    # 4. completamento dei casi sui parametri mancanti con backtracking riproducibile. Un caso che i vincoli non
    #    lasciano completare viene SCARTATO (non inventato); le sue tuple restano scoperte e il report lo dice.
    completed: List[Assignment] = []
    for case in cases:
        full = _complete(case, params, constraints, rng)
        if full is not None:
            completed.append(full)
    cases = completed
    covered = {idx: set() for idx in required}
    for case in cases:
        mark(case)
    # ordine deterministico dei campi
    cases = [{n: c[n] for n in names} for c in cases]
    total_covered = sum(len(b) for b in covered.values())
    return CoverageReport(
        strength=strength, seed=seed, parameters=names, cases=cases,
        tuples_required=total_required, tuples_covered=total_covered,
        tuples_excluded_by_constraints=excluded, constraints=list(constraint_names),
        repo_sha=repo_sha, inventory_sha256=inventory_sha256, group=group,
    )


def verify(report: CoverageReport, params: Sequence[Parameter], constraints: Sequence[Constraint] = ()) -> Dict[str, Any]:
    """Ricontrollo indipendente del report: ogni tupla richiesta compare in almeno un caso; nessun caso viola i vincoli."""
    by_name = {p.name: p for p in params}
    missing: List[Dict[str, Any]] = []
    for idx in itertools.combinations(report.parameters, report.strength):
        for values in itertools.product(*(by_name[n].values for n in idx)):
            partial = dict(zip(idx, values))
            if not _allowed(partial, constraints) or _complete(partial, params, constraints) is None:
                continue
            if not any(all(_key(c[n]) == _key(partial[n]) for n in idx) for c in report.cases):
                missing.append(partial)
    violations = [c for c in report.cases if not _allowed(c, constraints)]
    return {"missing_tuples": missing, "constraint_violations": violations,
            "ok": not missing and not violations}


# ----------------------------------------------------------------------------- inventario e gruppi

def load_inventory(path: str = INVENTORY_PATH) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def inventory_sha256(path: str = INVENTORY_PATH) -> str:
    with open(path, "rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def repo_sha(root: Optional[str] = None) -> str:
    try:
        out = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root or os.path.dirname(os.path.dirname(HERE)),
                             capture_output=True, text=True, timeout=10, check=False)
        return out.stdout.strip() if out.returncode == 0 else ""
    except Exception:  # noqa: BLE001 - il report resta valido anche senza git
        return ""


def _valid_values(inv: Dict[str, Any], key: str, limit: int = 5) -> Tuple[Any, ...]:
    for p in inv["parameters"]:
        if p["key"] == key:
            vals = list(p["domain"].get("valid") or [])
            if len(vals) < 2:
                raise ValueError(f"{key}: dominio senza valori rappresentativi")
            return tuple(vals[:limit])
    raise KeyError(key)


def _lt(a: str, b: str) -> Constraint:
    def c(x: Assignment) -> bool:
        if a in x and b in x and x[a] is not None and x[b] is not None:
            return float(x[a]) < float(x[b])
        return True
    return c


def _le(a: str, b: str) -> Constraint:
    def c(x: Assignment) -> bool:
        if a in x and b in x and x[a] is not None and x[b] is not None:
            return float(x[a]) <= float(x[b])
        return True
    return c


# Gruppi del piano §3 (L2 pairwise) e terne finanziarie (L3). I vincoli sono quelli che il codice applica o che
# il contratto owner rende necessari: una coppia che li viola non e' un caso da coprire.
GROUPS: Dict[str, Dict[str, Any]] = {
    "a_rischio_roserpina": {
        "strength": 2,
        "keys": ["roserpina.target_profit_cycle_pct", "roserpina.max_single_bet_pct", "roserpina.max_total_exposure_pct",
                 "roserpina.max_event_exposure_pct", "roserpina.max_stake_abs", "roserpina.min_stake",
                 "roserpina.risk_profile", "roserpina.table_count", "roserpina.max_recovery_tables", "roserpina.allow_recovery"],
        "constraints": [("max_recovery_tables<=table_count", _le("roserpina.max_recovery_tables", "roserpina.table_count")),
                        ("max_event_exposure_pct<=max_total_exposure_pct", _le("roserpina.max_event_exposure_pct", "roserpina.max_total_exposure_pct"))],
    },
    "b_protezioni": {
        "strength": 2,
        "keys": ["roserpina.max_daily_loss", "roserpina.max_drawdown_hard_stop_pct", "roserpina.max_drawdown_abs",
                 "roserpina.max_open_exposure", "roserpina.max_session_loss", "roserpina.max_exposure_stop",
                 "roserpina.auto_reset_drawdown_pct", "roserpina.defense_drawdown_pct", "roserpina.lockdown_drawdown_pct"],
        "constraints": [("defense_drawdown_pct<lockdown_drawdown_pct", _lt("roserpina.defense_drawdown_pct", "roserpina.lockdown_drawdown_pct"))],
    },
    "c_esecuzione_sim": {
        "strength": 2,
        # simulation.commission_pct esclusa: in SIM solo 4.5 e' eseguibile (COMMISSION_POLICY_VIOLATION altrimenti)
        "keys": ["simulation.starting_balance", "simulation.partial_fill_enabled",
                 "simulation.consume_liquidity", "settlement.poll_enabled", "settlement.poll_sec"],
        "constraints": [],
    },
    "d_liquidita_book": {
        "strength": 2,
        "keys": ["roserpina.book_warning", "roserpina.book_block", "roserpina.liquidity_guard_enabled",
                 "roserpina.liquidity_multiplier", "roserpina.min_liquidity_absolute", "roserpina.liquidity_warning_only",
                 "roserpina.min_price", "roserpina.max_win", "roserpina.max_win_warning_only"],
        "constraints": [("book_warning<=book_block", _le("roserpina.book_warning", "roserpina.book_block"))],
    },
    # Terne finanziarie critiche (L3)
    "t_stake_cap": {
        "strength": 3,
        "keys": ["roserpina.max_single_bet_pct", "roserpina.max_total_exposure_pct", "roserpina.max_stake_abs"],
        "constraints": [],
    },
    "t_fase_moltiplicatori": {
        "strength": 3,
        "keys": ["roserpina.expansion_profit_pct", "roserpina.expansion_multiplier", "roserpina.defense_multiplier",
                 "roserpina.defense_drawdown_pct"],
        "constraints": [],
    },
}


def build_group(name: str, inv: Dict[str, Any], *, seed: int = 0, strength: Optional[int] = None) -> CoverageReport:
    spec = GROUPS[name]
    params = [Parameter(k, _valid_values(inv, k)) for k in spec["keys"]]
    cons = [c for _n, c in spec["constraints"]]
    cnames = [n for n, _c in spec["constraints"]]
    rep = generate(params, strength=strength or spec["strength"], seed=seed, constraints=cons,
                   constraint_names=cnames, group=name, repo_sha=repo_sha(), inventory_sha256=inventory_sha256())
    check = verify(rep, params, cons)
    if not check["ok"]:
        raise RuntimeError(f"gruppo {name}: report non verificato: {check}")
    return rep


def pytest_params(report: CoverageReport) -> List[Tuple[str, Assignment]]:
    """Casi come parametri pytest: (id leggibile, assegnazione)."""
    out = []
    for i, case in enumerate(report.cases):
        out.append((f"{report.group or 'case'}-{report.strength}way-s{report.seed}-{i:03d}", dict(case)))
    return out


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="F-33 generatore combinatorio riproducibile")
    ap.add_argument("--inventory", default=INVENTORY_PATH)
    ap.add_argument("--group", required=True, choices=sorted(GROUPS) + ["ALL"])
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--strength", type=int, default=None, help="forza t (default: quello del gruppo)")
    ap.add_argument("--out", default="", help="file JSON del report (default: stdout)")
    args = ap.parse_args(argv)
    inv = load_inventory(args.inventory)
    names = sorted(GROUPS) if args.group == "ALL" else [args.group]
    reports = [build_group(n, inv, seed=args.seed, strength=args.strength).to_dict() for n in names]
    payload: Any = reports[0] if len(reports) == 1 else {"schema": "pf-combi-coverage-set/1", "reports": reports}
    text = json.dumps(payload, indent=1, ensure_ascii=False)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(text)
        for r in reports:
            print(f"{r['group']}: t={r['strength']} seed={r['seed']} casi={r['n_cases']} "
                  f"copertura={r['coverage_pct']}% ({r['tuples_covered']}/{r['tuples_required']})")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())

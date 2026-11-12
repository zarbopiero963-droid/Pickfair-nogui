"""PR-T3 / D-631-12 — oracolo R40 di Claude D, versionato (registro #628 M40).

Oracolo temporale indipendente del drawdown con MEMORIA RIDOTTA (letture recenti e voci di riepilogo 1..3), sequenze
lunghe e letture consegnate fuori ordine o molto in ritardo, sul percorso reale del runtime
(`_record_account_snapshot` + `_apply_funds_reading`).

Verita': esiste t_i < t_j con max(cassa nelle letture con t < t_j) - cassa_j >= limite.
Tre esiti ratificati dall'owner per #631:
  - superamento vero          -> hard-stop, oppure storia insufficiente se la memoria non basta a provarlo;
  - superamento possibile ma non determinabile -> storia insufficiente (sospensione precauzionale, NON falso hard-stop);
  - nessun superamento         -> operativo.
Vietati: falso hard-stop (breached senza superamento vero) e perdita silenziosa (superamento vero senza alcun blocco).

Riferimenti: sul ciclo 14 di #631 (f0f521dc) R40 e' ROSSO con 3 perdite silenziose (casi 0, 1628, 2461 del seed
6310150), riportate anche come casi fissi qui sotto. Il seed, il generatore e il numero di casi sono quelli di M40:
non cambiarli senza aggiornare il registro, altrimenti il confronto tra cicli non vale.
Dipende dal drawdown persistente introdotto dalla #631: va eseguito sul main che la contiene.
"""
import json
import random
import time

import pytest

import core.drawdown_hard_stop as dhs
from tests.regression_t3.drawdown_harness import build

CAP = 15.0
SEED = 6310150
N_CASES = 3000

# Casi falliti sul ciclo 14 (M40): (memoria (recenti, riepilogo), letture (t, cassa), ordine di consegna)
CYCLE14_SILENT_LOSSES = {
    "caso0_d631_12_perdita15": ((1, 1), [(1, 90.0), (3, 80.0), (4, 100.0), (5, 100.0), (6, 100.0), (2, 95.0)],
                                [0, 1, 2, 3, 4, 5]),
    "caso1628_perdita27": ((1, 2), [(16, 100.0), (23, 73.0), (24, 98.0), (35, 104.0), (46, 115.0), (53, 106.0),
                                    (55, 120.0)], [1, 2, 3, 4, 6, 0, 5]),
    "caso2461_perdita25": ((3, 1), [(2, 120.0), (9, 95.0), (14, 100.0), (19, 105.0), (26, 120.0), (35, 115.0),
                                    (40, 110.0), (48, 110.0), (51, 120.0), (57, 120.0)],
                           [1, 4, 9, 2, 3, 8, 6, 0, 7, 5]),
}


def _truth(reads):
    best = None
    for _, cash in sorted(reads):
        if best is not None and best - cash >= CAP - 1e-9:
            return True
        best = cash if best is None else max(best, cash)
    return False


def _set_memory(monkeypatch, recent, summary):
    monkeypatch.setattr(dhs, "RECENT_READINGS_LIMIT", recent)
    monkeypatch.setattr(dhs, "SUMMARY_LIMIT", summary)
    monkeypatch.setattr(dhs, "READING_HISTORY_LIMIT", recent + summary)


def _run(tmp_dir, reads, order):
    c = build(tmp_dir)
    c.rc.config.max_drawdown_abs = CAP
    c.rc._reload_drawdown_state()
    t0 = time.monotonic() + 1.0
    for i in order:
        t, v = reads[i]
        f = {"available": v, "exposure": 0.0, "ok": True}
        c.svc.funds = f
        c.rc._record_account_snapshot(f, source="ORACLE", read_at=t0 + t)
        c.rc._apply_funds_reading(f, source="ORACLE", read_at=t0 + t)
    hs = c.rc._drawdown_hard_stop
    return bool(hs.breached), bool(hs.history_insufficient.get("active"))


@pytest.mark.parametrize("name", sorted(CYCLE14_SILENT_LOSSES))
def test_r40_cycle14_silent_losses_are_blocked(name, tmp_path, monkeypatch):
    (recent, summary), reads, order = CYCLE14_SILENT_LOSSES[name]
    assert _truth(reads), "il caso deve contenere un superamento vero"
    _set_memory(monkeypatch, recent, summary)
    breached, insufficient = _run(tmp_path, reads, order)
    assert breached or insufficient, f"{name}: perdita silenziosa (breached=False, storia insufficiente=False)"


def test_r40_reduced_window_random_oracle(tmp_path, monkeypatch):
    rng = random.Random(SEED)
    false_blocks, silent = [], []
    states = {"hard_stop_vero": 0, "insufficiente_con_perdita_vera": 0, "insufficiente_precauzionale": 0,
              "operativo_corretto": 0}
    for case in range(N_CASES):
        recent, summary = rng.randint(1, 3), rng.randint(1, 3)
        if case == 0:   # caso fisso D-631-12
            (recent, summary), reads, order = CYCLE14_SILENT_LOSSES["caso0_d631_12_perdita15"]
        else:
            k = rng.randint(3, 16)
            step = rng.choice([1, 5])
            lo = rng.choice([70, 85, 95])   # ampiezza 50/35/25: bilancia superamento e operativita'
            reads = sorted((t, float(rng.choice(range(lo, 121, step)))) for t in rng.sample(range(1, 60), k))
            order = list(range(k))
            if case % 2:   # mescolamento completo
                rng.shuffle(order)
            else:          # in ordine, ma 1-3 letture consegnate molto in ritardo (dopo tutte le altre)
                late = rng.sample(order, min(k - 1, rng.randint(1, 3)))
                order = [i for i in order if i not in late] + late
        _set_memory(monkeypatch, recent, summary)
        d = tmp_path / f"c{case}"
        d.mkdir()
        breached, insufficient = _run(d, reads, order)
        truth = _truth(reads)
        if breached and truth:
            states["hard_stop_vero"] += 1
        elif insufficient and truth:
            states["insufficiente_con_perdita_vera"] += 1
        elif insufficient and not truth:
            states["insufficiente_precauzionale"] += 1
        elif not breached and not truth:
            states["operativo_corretto"] += 1
        row = {"case": case, "limits": [recent, summary], "reads": reads, "order": order}
        if breached and not truth:
            false_blocks.append(row)
        if truth and not breached and not insufficient:
            silent.append(row)
    report = {"seed": SEED, "states": states, "false_blocks": false_blocks, "silent_losses": silent}
    (tmp_path / "r40_report.json").write_text(json.dumps(report, indent=1))
    print("R40", json.dumps(report["states"]), "| falsi hard-stop:", len(false_blocks),
          "| perdite silenziose:", len(silent))
    assert not false_blocks, f"falsi hard-stop: {false_blocks[:3]}"
    assert not silent, f"perdite silenziose: {silent[:3]}"
    # senza errori, ogni esito ricade in uno dei tre stati ratificati
    assert sum(states.values()) == N_CASES

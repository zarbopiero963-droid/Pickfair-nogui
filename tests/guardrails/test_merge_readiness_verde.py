"""BLOCK — il gate di merge readiness deve pubblicare un giudizio VERO sull'head.

Il difetto, misurato sulla #463
-------------------------------
`PR Merge Readiness` era l'UNICO workflow cronicamente rosso del repo: 24
failure, 2 cancelled, 1 success sulle ultime 27 run di `main`. Non perche'
sbagliasse giudizio — perche' pubblicava la risposta sul commit sbagliato, e
quella giusta la dava troppo presto:

    evento          conclusione   head_sha      branch
    pull_request    failure       04125cbc8e    <branch della PR>   <- si attacca all'head
    workflow_run    failure       16a8030949    main
    workflow_run    failure       16a8030949    main
    workflow_run    success       16a8030949    main                <- il giudizio giusto

La run `pull_request` parte ~20s dopo il push, trova decine di check ancora in
volo e fallisce: ed e' l'UNICA che si attacca all'head della PR. Le
rivalutazioni `workflow_run`/`check_run` girano nel contesto del branch di
default, quindi il loro esito — compreso il `success` finale — finisce su
`main`, dove non lo guarda nessuno e dove lascia solo una scia di rosso.

Il risultato e' il peggiore dei due mondi: la PR mostra per sempre un rosso
vecchio di venti secondi, e il gate che dovrebbe proteggere il merge diventa
rumore che si impara a ignorare. Un gate che si ignora non e' un gate.

Le due meta' del fix, e perche' servono ENTRAMBE:

1. si aspetta che i check dell'head siano SETTLED prima di decidere — cosi' la
   run che si attacca all'head dice qualcosa di vero;
2. si tolgono i trigger che rivalutano nel contesto di `main` — non aiutavano
   la PR e sporcavano `main`.

Solo la 1: la PR resterebbe rossa a ogni push per i 20 secondi iniziali finche'
non ci si mette d'accordo. Solo la 2: la PR sarebbe rossa per sempre, perche'
resterebbe l'unica valutazione prematura.

Fail-closed preservato: se allo scadere del budget i check NON sono settled, il
gate NON passa. L'attesa serve a dare un giudizio vero, non a fabbricare un
verde.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ".github/workflows/pr-merge-readiness.yml"

sys.path.insert(0, str(ROOT / "scripts"))
import pr_flow_automation as flow  # noqa: E402


def _testo() -> str:
    percorso = ROOT / WORKFLOW
    assert percorso.is_file(), f"{WORKFLOW} non esiste"
    return percorso.read_text(encoding="utf-8")


def _senza_commenti(testo: str) -> str:
    """Un divieto che scatta su una parola dentro un commento e' un falso
    positivo, e un test che grida al lupo si smette di leggerlo."""
    return "\n".join(r for r in testo.splitlines() if not r.strip().startswith("#"))


# ---------------------------------------------------------------------------
# BLOCK — niente trigger che rivalutano nel contesto di `main`
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("trigger", ["workflow_run", "check_run"])
def test_block_nessun_trigger_che_pubblica_fuori_dall_head(trigger: str) -> None:
    blocco_on = re.search(r"^on:\n(?:[ \t].*\n|\n)*", _senza_commenti(_testo()), re.MULTILINE)
    # Se il blocco `on:` non si trova, il parser e' rotto: fallire, non passare
    # a vuoto — un guardrail che non trova quel che controlla non e' soddisfatto.
    assert blocco_on, f"{WORKFLOW}: non trovo il blocco `on:`; il workflow ha cambiato forma"

    assert f"{trigger}:" not in blocco_on.group(0), (
        f"{WORKFLOW}: il trigger `{trigger}` e' tornato. Quelle run girano nel "
        f"contesto del branch di DEFAULT: il loro esito si attacca a `main`, non "
        f"all'head della PR, quindi non rende verde la PR e lascia run rosse su "
        f"`main` (24 su 27, misurato sulla #463). Per rivalutare a mano c'e' "
        f"`workflow_dispatch`."
    )


def test_block_resta_il_trigger_che_si_attacca_all_head() -> None:
    """Toglierli TUTTI spegnerebbe il gate: e' l'altro modo di essere verdi."""
    testo = _senza_commenti(_testo())
    assert re.search(r"^  pull_request:\n", testo, re.MULTILINE), (
        f"{WORKFLOW}: sparito il trigger `pull_request`, l'unico il cui esito si "
        f"attacca all'head della PR. Senza, il gate non giudica piu' niente."
    )


# ---------------------------------------------------------------------------
# BLOCK — il workflow deve CONCEDERE un budget d'attesa allo script
# ---------------------------------------------------------------------------
def test_block_il_workflow_concede_un_budget_di_attesa() -> None:
    m = re.search(r"--wait-pending-seconds[= ]+\"?(\d+)", _senza_commenti(_testo()))
    assert m, (
        f"{WORKFLOW}: non passa `--wait-pending-seconds` allo script. Senza "
        f"budget il default e' 0: il gate decide ~20s dopo il push, con i check "
        f"ancora in volo, e la PR resta rossa per sempre."
    )
    assert int(m.group(1)) > 0, (
        f"budget d'attesa = {m.group(1)}: equivale a non aspettare affatto."
    )


# ---------------------------------------------------------------------------
# BLOCK — anti-stallo: il gate non deve MAI aspettare se stesso
# ---------------------------------------------------------------------------
def test_block_il_gate_non_aspetta_se_stesso() -> None:
    pr = {"statusCheckRollup": [
        {"name": "Merge readiness", "status": "IN_PROGRESS", "conclusion": None},
        {"name": "tests", "status": "COMPLETED", "conclusion": "SUCCESS"},
    ]}
    pending = [c.get("name") for c in flow.split_checks(pr)["pending"]]
    assert "Merge readiness" not in pending, (
        "il self-check e' finito tra i pending: ora che il gate ASPETTA i "
        "pending, questo e' uno stallo — aspetterebbe se stesso fino allo "
        "scadere del budget, per poi fallire. Un deadlock travestito da timeout."
    )


# ---------------------------------------------------------------------------
# BLOCK — il comportamento vero di cmd_readiness
# ---------------------------------------------------------------------------
def _args(**kw: Any) -> Any:
    import argparse
    base = dict(repo="o/r", pr="1", ignore_safe_autofix=True, wait_unknown_seconds=0,
                wait_pending_seconds=60, poll_seconds=1, output="", no_fail=False)
    base.update(kw)
    return argparse.Namespace(**base)


def _decisione(pending: list[str], can_merge: bool = False) -> dict[str, Any]:
    return {
        "already_merged": False, "mergeable": "MERGEABLE", "mergeStateStatus": "CLEAN",
        "reasons": [], "blockers": [], "can_merge": can_merge,
        "pending": [{"name": n} for n in pending],
    }


def test_block_aspetta_finche_i_check_sono_in_volo(monkeypatch: Any) -> None:
    """Il cuore: con check pendenti si RIPROVA, non si decide subito."""
    sequenza = [
        _decisione(["tests", "smoke"]),
        _decisione(["smoke"]),
        _decisione([], can_merge=True),
    ]
    viste: list[int] = []

    def finta(repo: str, pr: str, ignore: bool) -> dict[str, Any]:
        viste.append(1)
        return sequenza[min(len(viste) - 1, len(sequenza) - 1)]

    monkeypatch.setattr(flow, "_readiness_decision", finta)
    monkeypatch.setattr(flow.time, "sleep", lambda _s: None)

    rc = flow.cmd_readiness(_args())
    assert len(viste) == 3, (
        f"il gate ha interrogato l'API {len(viste)} volta/e invece di 3: non sta "
        f"aspettando che i check finiscano, sta decidendo sul primo colpo."
    )
    assert rc == 0, "check tutti settled e verdi: il gate doveva passare"


def test_block_fail_closed_se_i_check_non_finiscono_mai(monkeypatch: Any) -> None:
    """Scaduto il budget con check ancora in volo => NON si passa.

    L'attesa serve a dare un giudizio vero, non a fabbricare un verde: se i
    check non finiscono, la PR non e' pronta e il gate lo dice.
    """
    monkeypatch.setattr(flow, "_readiness_decision",
                        lambda *a, **k: _decisione(["tests"], can_merge=False))
    monkeypatch.setattr(flow.time, "sleep", lambda _s: None)

    rc = flow.cmd_readiness(_args(wait_pending_seconds=1, poll_seconds=1))
    assert rc == 1, (
        "budget scaduto con check ancora pendenti e il gate e' passato lo "
        "stesso: e' un falso verde, esattamente il difetto che questo gate "
        "esiste per impedire."
    )


def test_block_senza_budget_non_aspetta(monkeypatch: Any) -> None:
    """Retrocompatibilita': budget 0 = comportamento di prima, una sola lettura."""
    viste: list[int] = []

    def finta(repo: str, pr: str, ignore: bool) -> dict[str, Any]:
        viste.append(1)
        return _decisione(["tests"])

    monkeypatch.setattr(flow, "_readiness_decision", finta)
    monkeypatch.setattr(flow.time, "sleep", lambda _s: None)

    flow.cmd_readiness(_args(wait_pending_seconds=0))
    assert len(viste) == 1, f"con budget 0 ha letto {len(viste)} volte invece di 1"


# ---------------------------------------------------------------------------
# BLOCK — l'attesa deve leggere una chiave che la decisione produce DAVVERO
#
# Lezione generalizzata dal bug `.ignored` della #462: il workflow leggeva con
# jq una chiave che la decisione non produceva, jq restituiva null e il
# controllo non controllava nulla, restando verde. Qui il rischio e' identico
# ma in Python: se `cmd_readiness` aspettasse su una chiave inesistente,
# `decision.get(...)` sarebbe sempre None, il ciclo non girerebbe mai e
# l'attesa sarebbe un no-op SILENZIOSO — il gate tornerebbe a decidere a 20
# secondi dal push, e questo test sarebbe l'unico a poterlo dire.
# ---------------------------------------------------------------------------
def test_block_lattesa_legge_una_chiave_che_la_decisione_produce() -> None:
    import inspect

    sorgente_attesa = inspect.getsource(flow.cmd_readiness)
    chiavi_lette = set(re.findall(r'decision\.get\("([a-z_]+)"\)', sorgente_attesa))
    chiavi_lette |= set(re.findall(r'decision\["([a-z_]+)"\]', sorgente_attesa))

    prodotte = set(re.findall(r'"([a-z_]+)":', inspect.getsource(flow._base_decision_checks)))
    prodotte |= set(re.findall(r'"([a-z_]+)":', inspect.getsource(flow._base_decision_status)))
    prodotte |= set(re.findall(r'"([a-z_]+)":', inspect.getsource(flow._base_decision_result)))
    prodotte |= set(re.findall(r'"([a-z_]+)":', inspect.getsource(flow._base_decision_metadata)))

    # Zero chiavi lette vorrebbe dire che il parser non ha letto niente, non che
    # il codice sia pulito: fallire, non passare a vuoto.
    assert chiavi_lette, (
        "nessuna chiave letta da cmd_readiness: il parser di questo test e' "
        "rotto e non sta verificando niente."
    )

    inesistenti = sorted(chiavi_lette - prodotte)
    assert not inesistenti, (
        f"cmd_readiness legge dalla decisione chiavi che nessuna funzione "
        f"`_base_decision_*` produce: {inesistenti}. Sono sempre None/assenti, "
        f"quindi le condizioni che le usano non scattano mai e l'attesa diventa "
        f"un no-op silenzioso — il gate torna a decidere a 20s dal push, verde "
        f"in apparenza e inutile di fatto. E' lo stesso difetto del `.ignored` "
        f"letto con jq sulla #462."
    )

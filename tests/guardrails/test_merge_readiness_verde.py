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


def _decisione(pending: list[str], can_merge: bool = False,
               checks_seen: int = 40) -> dict[str, Any]:
    # `checks_seen` va messo apposta: una decisione che non lo porta viene
    # trattata come "non lo so ancora" e il gate aspetta il budget intero —
    # che e' il comportamento voluto, ma qui simuliamo check gia' registrati.
    return {
        "already_merged": False, "mergeable": "MERGEABLE", "mergeStateStatus": "CLEAN",
        "reasons": [], "blockers": [], "can_merge": can_merge,
        "pending": [{"name": n} for n in pending], "checks_seen": checks_seen,
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
    assert len(viste) == 4, (
        f"il gate ha interrogato l'API {len(viste)} volta/e invece di 4: non sta "
        f"aspettando che i check finiscano, sta decidendo sul primo colpo. "
        f"(Conteggio legato a INTERVALLI_STABILI_RICHIESTI="
        f"{flow.INTERVALLI_STABILI_RICHIESTI}: se cambia, va ricalcolato.)"
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


# ---------------------------------------------------------------------------
# BLOCK — rilievi di Claude Fable 5 sulla #463, giro 4. Tutti e tre fondati.
# ---------------------------------------------------------------------------
def _pr(rollup: list[dict[str, Any]], stato: str = "CLEAN") -> dict[str, Any]:
    return {"statusCheckRollup": rollup, "mergeStateStatus": stato,
            "mergeable": "MERGEABLE", "mergedAt": None, "isDraft": False,
            "labels": [], "reviewDecision": "", "state": "OPEN"}


def _can_merge(rollup: list[dict[str, Any]]) -> bool:
    pr = _pr(rollup)
    checks = flow.split_checks(pr)
    return bool(flow._merge_readiness_state(pr, checks, [])["can_merge"])


def test_block_rollup_vuoto_non_e_un_verde() -> None:
    """Il falso verde simmetrico: decidere presto e dire PRONTA.

    Rilievo di Fable 5. L'attesa introdotta in questa PR scatta solo se
    `pending` e' non vuoto. Ma nella finestra iniziale dopo il push GitHub puo'
    non aver ancora registrato NESSUN check: `pending` e' vuoto, non si aspetta,
    e con `mergeStateStatus` CLEAN il gate concludeva `can_merge=True` — verde
    con zero check eseguiti.

    Misurato prima del fix:

        rollup VUOTO (finestra iniziale dopo push)   pending=0  🟢 VERDE

    Zero check visti non vuol dire "tutto a posto": vuol dire "non lo so
    ancora". Un gate che confonde le due cose e' peggio di un gate assente,
    perche' quel verde lo si crede.
    """
    assert not _can_merge([]), (
        "rollup VUOTO e il gate dice PRONTA: e' un verde con zero check "
        "eseguiti. Zero check visti significa 'non lo so ancora', mai 'tutto "
        "a posto'."
    )


def test_block_solo_il_self_check_non_e_un_verde() -> None:
    """Variante piu' insidiosa: l'unico check registrato e' il gate stesso.

    Viene escluso (giustamente, o sarebbe stallo), quindi i check REALI visti
    restano zero — ma il rollup non e' vuoto, e un controllo scritto sulla
    lunghezza grezza del rollup ci cascherebbe.
    """
    assert not _can_merge([
        {"name": "Merge readiness", "status": "IN_PROGRESS", "conclusion": None},
    ]), (
        "l'unico check e' il gate stesso, escluso dal conteggio: i check reali "
        "visti sono zero, quindi non si puo' dichiarare PRONTA."
    )


def test_block_un_check_verde_vero_resta_un_verde() -> None:
    """Contro-prova: il fail-closed non deve diventare 'mai verde'.

    Senza questo, la patch che chiude il falso verde passerebbe anche
    bloccando tutto per sempre — che e' l'altro modo di rompere un gate.
    """
    assert _can_merge([
        {"name": "tests", "status": "COMPLETED", "conclusion": "SUCCESS"},
    ]), "un check reale verde e nessun blocker: il gate DEVE poter dire PRONTA"


def test_block_lattesa_copre_anche_il_rollup_ancora_vuoto() -> None:
    """Non basta fallire: bisogna ASPETTARE che i check compaiano.

    Altrimenti il fix del falso verde diventa un rosso garantito nei primi
    secondi — di nuovo il difetto di partenza, solo col segno invertito.
    """
    import inspect

    sorgente = inspect.getsource(flow.cmd_readiness)
    assert "checks_seen" in sorgente, (
        "l'attesa guarda solo `pending`: nella finestra iniziale `pending` e' "
        "vuoto perche' i check non esistono ancora, quindi non si aspetta e si "
        "decide sul vuoto. Serve attendere anche mentre i check reali visti "
        "sono zero."
    )


def test_block_il_nome_del_job_e_quello_che_il_gate_riconosce() -> None:
    """Anti-stallo, seconda meta' (rilievo 3 di Fable 5).

    L'esclusione del self-check passa per il NOME: `SELF_CHECK_NAMES`. La
    `detailsUrl` del rollup e' l'URL del job (`/actions/runs/<id>/job/<id>`) e
    NON contiene il nome del workflow, quindi il ramo URL di `is_self_check`
    non aiuta qui. Se il job venisse rinominato senza aggiornare
    `SELF_CHECK_NAMES`, il gate finirebbe tra i propri `pending` e aspetterebbe
    se stesso per 15 minuti prima di fallire: uno stallo travestito da timeout,
    su OGNI PR.
    """
    m = re.search(r"^    name: (.+)$", _senza_commenti(_testo()), re.MULTILINE)
    assert m, f"{WORKFLOW}: non trovo il `name:` del job; il workflow ha cambiato forma"
    nome = m.group(1).strip().strip('"').strip("'")

    assert nome.lower() in flow.SELF_CHECK_NAMES, (
        f"il job si chiama {nome!r}, che NON e' in SELF_CHECK_NAMES "
        f"({sorted(flow.SELF_CHECK_NAMES)}). Il gate non si riconoscerebbe piu': "
        f"finirebbe tra i propri `pending` e aspetterebbe se stesso fino al "
        f"timeout, su ogni PR. Se rinomini il job, aggiorna SELF_CHECK_NAMES "
        f"nello stesso commit."
    )


def test_block_aspetta_anche_col_rollup_ancora_vuoto(monkeypatch: Any) -> None:
    """La finestra iniziale: nessun check registrato, `pending` vuoto.

    Senza questo ramo il gate deciderebbe sul vuoto. Con il ramo, aspetta che i
    check compaiano e poi giudica.
    """
    sequenza = [
        _decisione([], checks_seen=0),          # GitHub non ha registrato nulla
        _decisione(["tests"], checks_seen=40),  # i check compaiono
        _decisione([], can_merge=True, checks_seen=40),
    ]
    viste: list[int] = []

    def finta(repo: str, pr: str, ignore: bool) -> dict[str, Any]:
        viste.append(1)
        return sequenza[min(len(viste) - 1, len(sequenza) - 1)]

    monkeypatch.setattr(flow, "_readiness_decision", finta)
    monkeypatch.setattr(flow.time, "sleep", lambda _s: None)

    rc = flow.cmd_readiness(_args())
    assert len(viste) == 4, (
        f"interrogata l'API {len(viste)} volta/e invece di 4: col rollup vuoto "
        f"il gate ha deciso subito, invece di aspettare che i check comparissero. "
        f"(Conteggio legato a INTERVALLI_STABILI_RICHIESTI="
        f"{flow.INTERVALLI_STABILI_RICHIESTI}.)"
    )
    assert rc == 0


def test_block_il_verdetto_sullhead_ha_una_corsia_di_concorrenza_propria() -> None:
    """Il gruppo di concorrenza deve distinguersi da quello della versione
    vecchia del workflow, che resta viva su `main` finche' questa PR non e'
    mergiata.

    Su `dae8a26` la run attaccata all'head e' durata 243s — l'attesa funziona —
    ma e' finita `cancelled` 14s dopo l'avvio di una run `workflow_run` nata
    dalla definizione vecchia. Quelle run calcolano il gruppo con l'espressione
    vecchia, che per la stessa PR coincideva col nostro: con
    `cancel-in-progress: true`, cancellavano il verdetto mentre aspettava.

    La supersession NON va indebolita: un nuovo push sulla stessa PR deve
    ancora cancellare la run precedente, quindi il gruppo resta indicizzato
    sulla PR, non sullo SHA.
    """
    testo = _senza_commenti(_testo())
    m = re.search(r"^  group: (.+)$", testo, re.MULTILINE)
    assert m, f"{WORKFLOW}: non trovo il `group:` di concurrency"
    gruppo = m.group(1)

    assert "pr-merge-readiness-head-" in gruppo, (
        f"gruppo di concorrenza {gruppo!r}: coincide con quello della versione "
        f"vecchia del workflow ancora viva su `main`, che cancella questa run "
        f"mentre aspetta i check."
    )
    assert "pull_request.number" in gruppo, (
        f"gruppo {gruppo!r}: non e' piu' indicizzato sulla PR. Se passasse allo "
        f"SHA, un nuovo push non cancellerebbe piu' la run precedente e ogni "
        f"push lascerebbe una run zombie ad aspettare fino al timeout."
    )


def test_block_almeno_un_workflow_gira_su_ogni_pr() -> None:
    """La precondizione che rende sicuro il fail-closed su `checks_seen <= 0`.

    Rilievo di Fugu Ultra sulla #463: se una PR potesse legittimamente non
    generare NESSUN check (solo-docs, workflow tutti con filtri di path), il
    fail-closed la terrebbe rossa per sempre, bruciando l'intero budget
    d'attesa — un rosso garantito simmetrico al falso verde appena tolto.

    In questo repo non e' raggiungibile: oltre venti workflow partono su OGNI
    PR senza filtri di `paths`. Ma «non e' raggiungibile oggi» invecchia male:
    se qualcuno mettesse un filtro ovunque, il gate diventerebbe irraggiungibile
    e nessun altro controllo lo direbbe. Quindi la precondizione e' sorvegliata
    qui, dove si rompe.

    Non e' un test sul gate: e' un test sull'IPOTESI del gate.
    """
    incondizionati = []
    for percorso in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
        testo = percorso.read_text(encoding="utf-8")
        m = re.search(r"^on:\n(?:[ \t].*\n|\n)*", testo, re.MULTILINE)
        if not m:
            continue
        blocco = m.group(0)
        if not re.search(r"^  pull_request(_target)?:", blocco, re.MULTILINE):
            continue
        if not re.search(r"^\s+paths(-ignore)?:", blocco, re.MULTILINE):
            incondizionati.append(percorso.name)

    # Zero workflow letti = glob rotto, non repo pulito: fallire, non passare
    # a vuoto.
    assert incondizionati, (
        "nessun workflow parte su ogni PR senza filtri di `paths`. Allora una "
        "PR puo' non generare alcun check reale, e il fail-closed su "
        "`checks_seen <= 0` la terrebbe rossa fino allo scadere del budget: un "
        "rosso garantito, simmetrico al falso verde che quel fail-closed "
        "esiste per togliere. Se il parco workflow cambia davvero cosi', il "
        "gate va ripensato — non questo test allentato."
    )


def test_block_non_si_decide_mentre_il_rollup_sta_ancora_crescendo(monkeypatch: Any) -> None:
    """Rilievo 1 di Fable 5 sulla full-range: `checks_seen > 0` non basta.

    Se UN check veloce e' gia' verde mentre gli altri non sono ancora
    registrati nel rollup, `pending` e' vuoto e `checks_seen` vale 1: il gate
    usciva dall'attesa e dichiarava PRONTA con la suite ancora da partire.
    Riprodotto:

        1 check verde, 38 non ancora registrati
          pending=0  checks_seen=1  can_merge=True

    Il criterio giusto non e' "ho visto almeno un check" ma "il rollup ha
    smesso di crescere": si aspetta finche' il numero di check osservati
    cambia fra due letture. Si auto-calibra — niente soglia inventata — e
    impone naturalmente almeno un giro di grazia.
    """
    sequenza = [
        _decisione([], checks_seen=1),                     # solo il primo check
        _decisione([], checks_seen=20),                    # ne compaiono altri
        _decisione([], checks_seen=39),                    # e altri ancora
        _decisione([], can_merge=True, checks_seen=39),    # stabile: ora si decide
    ]
    viste: list[int] = []

    def finta(repo: str, pr: str, ignore: bool) -> dict[str, Any]:
        viste.append(1)
        return sequenza[min(len(viste) - 1, len(sequenza) - 1)]

    monkeypatch.setattr(flow, "_readiness_decision", finta)
    monkeypatch.setattr(flow.time, "sleep", lambda _s: None)

    rc = flow.cmd_readiness(_args())
    assert len(viste) == 5, (
        f"interrogata l'API {len(viste)} volta/e invece di 5: il gate ha "
        f"deciso mentre il rollup stava ancora crescendo — un verde con la "
        f"suite non ancora partita. (Conteggio legato a "
        f"INTERVALLI_STABILI_RICHIESTI={flow.INTERVALLI_STABILI_RICHIESTI}.)"
    )
    assert rc == 0


def test_block_un_solo_intervallo_stabile_non_basta(monkeypatch: Any) -> None:
    """Rilievo convergente di Fable 5 e Fugu Ultra sulla full-range.

    Il criterio "il rollup ha smesso di crescere" verificato su UN SOLO
    intervallo (15s) e' debole: se GitHub ritarda la registrazione oltre quel
    singolo intervallo con pochi check gia' verdi, il gate esce e dichiara
    `can_merge=True` con la suite non ancora comparsa — lo stesso falso verde,
    con finestra piu' stretta.

    Qui il conteggio resta fermo a 1 per due letture consecutive (il plateau
    che ingannerebbe il criterio debole) e solo dopo compaiono gli altri
    check. Il gate NON deve decidere sul plateau.

    Limite dichiarato, scritto anche accanto alla costante: nessun N elimina la
    finestra, la stringe soltanto. Eliminarla richiederebbe l'elenco dei check
    attesi, che invecchierebbe a ogni workflow aggiunto o tolto — e un manifest
    stantio produce falsi ROSSI sistematici, peggio del rischio che chiude.
    """
    sequenza = [
        _decisione([], checks_seen=1),                    # un check veloce, solo
        _decisione([], checks_seen=1),                    # ancora fermo a 1: il plateau
        _decisione([], checks_seen=39),                   # ecco il resto della suite
        _decisione([], checks_seen=39),
        _decisione([], can_merge=True, checks_seen=39),
    ]
    viste: list[dict[str, Any]] = []

    def finta(repo: str, pr: str, ignore: bool) -> dict[str, Any]:
        d = sequenza[min(len(viste), len(sequenza) - 1)]
        viste.append(d)
        return d

    monkeypatch.setattr(flow, "_readiness_decision", finta)
    monkeypatch.setattr(flow.time, "sleep", lambda _s: None)

    rc = flow.cmd_readiness(_args())

    # NOTA su come si misura. `viste` registra cio' che viene LETTO, e il ciclo
    # legge sempre una volta in piu' prima di rivalutare la condizione: quindi
    # l'ULTIMA lettura ha checks_seen=39 sia con N=1 sia con N=2, e asserire su
    # quella non distingue niente. (Prima versione di questo test: lo faceva, e
    # il sabotaggio N=1 passava. Trovato sabotando.)
    #
    # Cio' che distingue e' quante volte ha interrogato l'API — cioe' se ha
    # attraversato il plateau — e di conseguenza su quale decisione si e'
    # fermato:
    #
    #     N=1: letture=2  rc=1   (uscito sul plateau)
    #     N=2: letture=5  rc=0   (arrivato alla decisione stabile)
    assert len(viste) == 5, (
        f"il gate ha interrogato l'API {len(viste)} volte invece di 5: e' uscito "
        f"sul plateau iniziale, dichiarando pronta una PR con la suite non "
        f"ancora comparsa. (Conteggio legato a INTERVALLI_STABILI_RICHIESTI="
        f"{flow.INTERVALLI_STABILI_RICHIESTI}: se cambia, va ricalcolato.)"
    )
    assert rc == 0, (
        "il gate non si e' fermato sulla decisione stabile: un solo intervallo "
        "invariato e' bastato a convincerlo che il rollup fosse fermo."
    )


def test_block_non_esce_su_un_conteggio_appena_cresciuto(monkeypatch: Any) -> None:
    """Rilievo di Grok 4.6 su `6d1c46a`, ed era una regressione mia.

    Introducendo il contatore di intervalli stabili avevo tolto dalla
    condizione il confronto col conteggio CORRENTE. Risultato: il contatore si
    aggiornava sulla lettura PRECEDENTE, quindi la condizione d'uscita
    consultava un valore che non sapeva nulla dell'ultimo fetch. Se il
    conteggio cresceva proprio all'ultima lettura, il gate usciva lo stesso:

        letture=4  ultima_usata_checks_seen=55   <- il rollup stava crescendo

    Qui il conteggio resta fermo a 40 e poi salta a 55 all'ultimo fetch: il
    gate deve fermarsi su una lettura STABILE (40), non su quella cresciuta.
    """
    sequenza = [
        _decisione([], checks_seen=40),
        _decisione([], checks_seen=40),
        _decisione([], checks_seen=40),
        _decisione([], can_merge=True, checks_seen=55),   # cresce all'improvviso
    ]
    viste: list[dict[str, Any]] = []

    def finta(repo: str, pr: str, ignore: bool) -> dict[str, Any]:
        d = sequenza[min(len(viste), len(sequenza) - 1)]
        viste.append(d)
        return d

    monkeypatch.setattr(flow, "_readiness_decision", finta)
    monkeypatch.setattr(flow.time, "sleep", lambda _s: None)

    flow.cmd_readiness(_args())

    assert viste[-1]["checks_seen"] == 40, (
        f"il gate si e' fermato su una lettura con checks_seen="
        f"{viste[-1]['checks_seen']}, cioe' su un conteggio APPENA cresciuto: "
        f"il contatore di stabilita' sta guardando letture vecchie invece di "
        f"quella corrente."
    )


def test_block_la_stabilita_si_conta_solo_a_pending_vuoto(monkeypatch: Any) -> None:
    """Rilievo di Fugu Ultra sulla full-range di `1cca781`, fondato.

    Il contatore cresceva anche mentre c'erano check PENDENTI: bastava che il
    conteggio restasse fermo (i check ci sono gia' tutti, stanno solo girando)
    perche' `intervalli_stabili` arrivasse a N. Cosi', nell'istante in cui
    l'ultimo pending diventava verde, il gate usciva — senza aver mai
    osservato un solo intervallo stabile A PENDING VUOTO. Riprodotto:

        letture=3   intervalli stabili a pending vuoto: ZERO

    «Conteggio fermo mentre la suite gira» non e' la stessa cosa di «il rollup
    e' completo»: la seconda si puo' affermare solo guardando la finestra DOPO
    che i pending sono spariti. Costa due poll (30s) su un gate che ne impiega
    ~300: si pagano volentieri per non pubblicare un verde prematuro.
    """
    sequenza = [
        _decisione(["a", "b", "c"], checks_seen=40),
        _decisione(["a"], checks_seen=40),
        _decisione([], checks_seen=40),
        _decisione([], can_merge=True, checks_seen=40),
    ]
    viste: list[dict[str, Any]] = []

    def finta(repo: str, pr: str, ignore: bool) -> dict[str, Any]:
        d = sequenza[min(len(viste), len(sequenza) - 1)]
        viste.append(d)
        return d

    monkeypatch.setattr(flow, "_readiness_decision", finta)
    monkeypatch.setattr(flow.time, "sleep", lambda _s: None)

    rc = flow.cmd_readiness(_args())

    assert len(viste) >= 4, (
        f"il gate e' uscito dopo {len(viste)} letture: e' uscito nell'istante in "
        f"cui l'ultimo pending e' diventato verde, senza osservare nemmeno un "
        f"intervallo stabile a pending vuoto. La stabilita' accumulata mentre "
        f"la suite girava non dice nulla su quel che si registra dopo."
    )
    assert rc == 0


def test_block_checks_seen_e_top_level_nella_decisione_vera() -> None:
    """La verifica chiesta da Claude Fable 5 sulla full-range di `1cca781`.

    Fable lo poneva come domanda: *«il loop legge `decision.get("checks_seen")`
    al top level, ma viene popolato in `_base_decision_checks` (sotto la chiave
    `checks`?). Se la decisione lo annida, il default 0 mantiene
    `checks_seen <= 0` vero per sempre: il gate consuma l'intero budget e
    fallisce SEMPRE anche a suite verde.»*

    Domanda legittima, e la risposta va inchiodata invece che spiegata: il test
    che gia' esisteva confrontava i NOMI delle chiavi, non il LIVELLO a cui
    stanno. Questo costruisce una decisione con il codice reale e guarda dove
    finisce `checks_seen`.

    (Controprova indipendente, dalla produzione: se fosse sempre 0 il gate
    avrebbe consumato tutti i 900s e sarebbe fallito. Ha chiuso `success` in
    314s.)
    """
    pr = _pr([
        {"name": "tests", "status": "COMPLETED", "conclusion": "SUCCESS"},
        {"name": "smoke", "status": "IN_PROGRESS", "conclusion": None},
    ])
    checks = flow.split_checks(pr)
    stato = flow._merge_readiness_state(pr, checks, [])
    decisione = flow._base_decision({"repo": "o/r", "pr_number": "1"}, pr, checks, stato)

    assert "checks_seen" in decisione, (
        f"`checks_seen` NON e' al livello che il loop legge. Chiavi presenti: "
        f"{sorted(decisione)}. Con `decision.get(\"checks_seen\", 0)` il default "
        f"varrebbe 0 per sempre, la condizione `<= 0` resterebbe vera, e il gate "
        f"consumerebbe l'intero budget fallendo anche a suite verde."
    )
    assert decisione["checks_seen"] == 2, (
        f"checks_seen = {decisione['checks_seen']!r} invece di 2: il conteggio "
        f"dei check reali non rispecchia il rollup."
    )


def test_block_timeout_senza_stabilita_non_e_un_verde(monkeypatch: Any) -> None:
    """Rilievo di Claude Fable 5, e contraddiceva una MIA affermazione.

    Avevo scritto — nel commit, nella spec e nel commento accanto al codice —
    «fail-closed: scaduto il budget si giudica lo stato REALE, se i check non
    sono finiti `can_merge` resta falso». Vero solo se `pending` e' non vuoto.
    Se allo scadere del budget il rollup sta ancora CRESCENDO ma `pending` e'
    momentaneamente vuoto, `can_merge` resta vero e il gate pubblica un verde
    su una suite incompleta. Riprodotto con un rollup che cresce a ogni
    lettura, quindi senza mai raggiungere la stabilita':

        budget scaduto SENZA stabilita' confermata  ->  rc=0 (PRONTA)

    Il budget e' generoso (900s contro ~20s di registrazione), quindi il caso e'
    raro: ma «raro» non e' «fail-closed», e la differenza sta proprio nel ramo
    che si imbocca quando le cose vanno male. Un gate che promette fail-closed
    e non lo e' e' peggio di uno che non lo promette.
    """
    contatore = {"n": 0}

    def finta(repo: str, pr: str, ignore: bool) -> dict[str, Any]:
        contatore["n"] += 1
        # il conteggio cambia a ogni lettura: stabilita' mai raggiunta
        return _decisione([], can_merge=True, checks_seen=contatore["n"])

    monkeypatch.setattr(flow, "_readiness_decision", finta)
    monkeypatch.setattr(flow.time, "sleep", lambda _s: None)

    rc = flow.cmd_readiness(_args(wait_pending_seconds=1, poll_seconds=1))

    assert rc == 1, (
        "budget scaduto senza che il rollup si sia mai stabilizzato, e il gate "
        "ha detto PRONTA: e' un fail-OPEN sul ramo timeout, il contrario di "
        "quanto la spec dichiara. Se la stabilita' non e' stata confermata, il "
        "verdetto non puo' essere verde."
    )

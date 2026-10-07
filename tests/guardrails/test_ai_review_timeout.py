"""Un reviewer che non risponde entro il limite di attesa non revisiona.

La richiesta al modello non e' in streaming: la risposta arriva tutta alla
fine, dopo il ragionamento. Grok 4.7 a reasoning `high` ragiona a lungo, e con
100 s per richiesta il workflow chiudeva la connessione prima della risposta:
sulla #478 Grok non ha mai completato, sulla #483 ha completato 1 tentativo
su 8. La DECISIONE-426 P24 lo aveva alzato a 240 s; la decisione owner del 07-10-2026 lo porta a 400 s per ridurre i tentativi pagati e persi sui diff grandi.

Il limite piu' alto va pagato col timeout del job. Tre tentativi, il backoff,
le chiamate a GitHub al loro tetto e l'avvio del runner devono stare dentro
`timeout-minutes`, altrimenti GitHub uccide il job a meta' e il reviewer tace
senza nemmeno il commento d'errore. L'invariante vale per tutti e cinque i
reviewer.
"""
from __future__ import annotations

import re
from pathlib import Path, PurePath, PureWindowsPath
from typing import List, Tuple

import pytest

ROOT = Path(__file__).resolve().parents[2]
GROK = ".github/workflows/pr-review-xai-grok46.yml"
LIMITE_MINIMO_GROK = 400  # secondi per richiesta (owner 07-10-2026)
TENTATIVI_ATTESI = 3
# Avvio del runner e dell'interprete. Le chiamate a GitHub si contano a parte,
# dal file: erano un margine fisso di 120 s, meno delle chiamate da 30 s che
# ogni workflow fa davvero (Codex sulla #484). Sono sette, non sei: una e'
# scritta su piu' righe e la prima regex non la vedeva (Codex sulla #492).
AVVIO_RUNNER = 60


def _relativo(percorso: PurePath, radice: PurePath) -> str:
    """Sempre con `/`: su Windows `str()` darebbe `\\` e `GROK` non combacerebbe."""
    return percorso.relative_to(radice).as_posix()


def _reviewer() -> List[str]:
    trovati = sorted(_relativo(p, ROOT) for p in (ROOT / ".github/workflows").glob("pr-review-*.yml"))
    assert len(trovati) == 5 and GROK in trovati, f"attesi i cinque reviewer, trovati {trovati}"
    return trovati


def _testo(workflow: str) -> str:
    return (ROOT / workflow).read_text(encoding="utf-8")


def _attesa_modello(workflow: str) -> Tuple[int, int, int]:
    """(tentativi, limite per richiesta, backoff totale) dal ciclo che chiama il modello.

    Il corpo del ciclo si legge per indentazione: dentro ci sono due `raise`
    e due `sleep`, e fermarsi al primo `raise` salterebbe il secondo ramo.
    Se il parser non trova qualcosa, il test fallisce: un guardrail che salta
    in silenzio sembra verde e non protegge niente.
    """
    righe = _testo(workflow).splitlines()
    inizio = [i for i, r in enumerate(righe) if re.match(r"\s*for attempt in range\(1, \d+\):\s*$", r)]
    assert len(inizio) == 1, f"{workflow}: atteso un solo ciclo di tentativi, trovati {len(inizio)}"
    testa = righe[inizio[0]]
    corpo_testo = _corpo(righe, inizio[0])
    tentativi = int(re.search(r"range\(1, (\d+)\)", testa).group(1)) - 1
    limiti = re.findall(r"urlopen\(req, timeout=(\d+)\)", corpo_testo)
    assert len(limiti) == 1, f"{workflow}: atteso un solo limite per richiesta nel ciclo, trovati {limiti}"
    attese = set(re.findall(r"time\.sleep\(min\((\d+), (\d+) \*\* attempt\)\)", corpo_testo))
    assert len(attese) == 1, f"{workflow}: backoff non riconosciuto nel ciclo: {attese}"
    tetto, base = map(int, attese.pop())
    # Il backoff scatta anche dopo l'ultimo tentativo, prima che il ciclo esca.
    return tentativi, int(limiti[0]), sum(min(tetto, base ** a) for a in range(1, tentativi + 1))


def _corpo(righe: List[str], i: int) -> str:
    """Le righe rientrate sotto la riga `i`: il blocco di un `for` o di un `def`."""
    rientro = len(righe[i]) - len(righe[i].lstrip())
    corpo = []
    for riga in righe[i + 1:]:
        if riga.strip() and len(riga) - len(riga.lstrip()) <= rientro:
            break
        corpo.append(riga)
    return "\n".join(corpo)


def _attesa_github(workflow: str) -> int:
    """Secondi per le chiamate a GitHub al loro tetto: punti di chiamata x timeout."""
    testo = _testo(workflow)
    righe = testo.splitlines()
    definizioni = [i for i, r in enumerate(righe) if re.match(r"\s*def gh_request\(", r)]
    assert len(definizioni) == 1, f"{workflow}: atteso un solo gh_request, trovati {len(definizioni)}"
    limiti = re.findall(r"urlopen\(req, timeout=(\d+)\)", _corpo(righe, definizioni[0]))
    assert len(limiti) == 1, f"{workflow}: timeout di gh_request non riconosciuto: {limiti}"
    # `\s*`: una chiamata scritta su piu' righe (`gh_request(\n    "GET", ...`)
    # e' una chiamata come le altre. La prima regex la saltava, e su tutti e
    # cinque i reviewer contava 6 chiamate invece di 7 — 30 s di caso peggiore
    # che il test non vedeva (Codex sulla #492, in `comment_exists`).
    chiamate = re.findall(r'gh_request\(\s*"(?:GET|POST|PATCH|PUT|DELETE)"', testo)
    assert chiamate, f"{workflow}: nessuna chiamata a gh_request riconosciuta"
    # Ogni `gh_request(` che non e' la definizione deve essere riconosciuto
    # sopra: una forma nuova (metodo da variabile, keyword) uscirebbe dal conto
    # in silenzio e il caso peggiore risulterebbe piu' corto del vero.
    tutte = re.findall(r"(?<!def )gh_request\(", testo)
    assert len(chiamate) == len(tutte), (
        f"{workflow}: {len(tutte)} chiamate a gh_request, ma solo {len(chiamate)} "
        f"riconosciute col metodo HTTP letterale: il caso peggiore le conterebbe in meno"
    )
    return len(chiamate) * int(limiti[0])


def _timeout_job(workflow: str) -> int:
    valori = re.findall(r"^\s*timeout-minutes:\s*(\d+)\s*$", _testo(workflow), re.M)
    assert len(valori) == 1, f"{workflow}: atteso un solo job con timeout-minutes, trovati {valori}"
    return int(valori[0]) * 60


@pytest.mark.parametrize("workflow", _reviewer())
def test_block_il_caso_peggiore_sta_dentro_il_timeout_del_job(workflow: str) -> None:
    tentativi, limite, backoff = _attesa_modello(workflow)
    # Tre, non "almeno uno": togliere i retry abbrevierebbe il caso peggiore e
    # questo test passerebbe piu' facilmente, proprio mentre il reviewer diventa
    # meno affidabile (Codex sulla #484).
    assert tentativi == TENTATIVI_ATTESI, f"{workflow}: {tentativi} tentativi, attesi {TENTATIVI_ATTESI}"
    github = _attesa_github(workflow)
    peggiore = tentativi * limite + backoff + github + AVVIO_RUNNER
    assert limite > 0 and peggiore <= _timeout_job(workflow), (
        f"{workflow}: {tentativi} x {limite} s + {backoff} s di backoff + {github} s di chiamate a "
        f"GitHub + {AVVIO_RUNNER} s di avvio = {peggiore} s, oltre il timeout del job "
        f"({_timeout_job(workflow)} s): GitHub ucciderebbe il job prima del commento d'errore"
    )


def test_block_grok_aspetta_abbastanza_da_ricevere_la_risposta() -> None:
    _, limite, _ = _attesa_modello(GROK)
    assert limite >= LIMITE_MINIMO_GROK, (
        f"Grok 4.7 ha {limite} s per richiesta: a reasoning high non basta (DECISIONE-426 P24, "
        f"#478 e #483), servono almeno {LIMITE_MINIMO_GROK} s"
    )


# Sopra `high` il ragionamento e' ancora piu' lungo: Sol a `max` (decisione
# dell'owner del 07-10-2026) con i suoi 100 s di prima perderebbe la risposta
# come la perdeva Grok a `high`. Sol a `max` usa 1800 s, e il job sale di conseguenza.
EFFORT_MASSIMI = {"xhigh", "max"}
LIMITE_MINIMO_EFFORT_MASSIMO = 1800


def _effort(workflow: str) -> str:
    m = re.search(r'^      REVIEW_EFFORT: +"?([a-z]+)"?\s*$', _testo(workflow), re.M)
    return m.group(1) if m else ""


@pytest.mark.parametrize("workflow", _reviewer())
def test_block_a_effort_massimo_si_aspetta_abbastanza(workflow: str) -> None:
    effort = _effort(workflow)
    if effort not in EFFORT_MASSIMI:
        pytest.skip(f"{workflow}: effort {effort or 'assente'!r}, non e' fra {sorted(EFFORT_MASSIMI)}")
    _, limite, _ = _attesa_modello(workflow)
    assert limite >= LIMITE_MINIMO_EFFORT_MASSIMO, (
        f"{workflow}: REVIEW_EFFORT={effort!r} con {limite} s per richiesta. La richiesta "
        f"non e' in streaming: a quel livello la risposta arriva a ragionamento finito, "
        f"servono almeno {LIMITE_MINIMO_EFFORT_MASSIMO} s"
    )


MERGE_READINESS = ".github/workflows/pr-merge-readiness.yml"
# Ritardo con cui un job di review parte rispetto al gate di merge readiness,
# che scatta sullo stesso push: misurato 48 s sulla #492 (Sol), con margine.
RITARDO_PARTENZA_REVIEWER = 120
# Checkout del merge-ref e verdetto finale, dentro il job del gate.
CONTORNO_GATE = 180


def _budget_merge_readiness() -> int:
    valori = re.findall(r"--wait-pending-seconds (\d+)", _testo(MERGE_READINESS))
    assert len(valori) == 1, f"{MERGE_READINESS}: atteso un solo --wait-pending-seconds, trovati {valori}"
    return int(valori[0])


@pytest.mark.parametrize("workflow", _reviewer())
def test_block_merge_readiness_aspetta_il_reviewer(workflow: str) -> None:
    """Il gate di merge aspetta i check dell'head, reviewer compresi.

    Se il suo budget e' piu' corto del job di review, una review valida ma
    lenta lascia il gate ROSSO, e il gate non si rivaluta da solo (serve un
    `workflow_dispatch` o un nuovo push). Con 900 s succedeva gia' con Grok
    (job da 20 minuti); con Sol a effort `max` (105 minuti) sarebbe diventato
    il caso ordinario dei push lenti. Rilievo di GPT-5.6 Sol sulla #492.
    """
    budget = _budget_merge_readiness()
    serve = _timeout_job(workflow) + RITARDO_PARTENZA_REVIEWER
    assert budget >= serve, (
        f"{MERGE_READINESS} aspetta {budget} s, ma il job di {workflow} puo' durare "
        f"{_timeout_job(workflow)} s e parte fino a {RITARDO_PARTENZA_REVIEWER} s dopo: "
        f"servono almeno {serve} s, altrimenti una review lenta ma valida lascia il gate rosso"
    )


def test_block_il_job_del_gate_contiene_il_suo_budget() -> None:
    """Un budget piu' lungo del job verrebbe troncato da GitHub senza verdetto."""
    budget = _budget_merge_readiness()
    assert _timeout_job(MERGE_READINESS) >= budget + CONTORNO_GATE, (
        f"{MERGE_READINESS}: timeout del job {_timeout_job(MERGE_READINESS)} s, budget "
        f"d'attesa {budget} s + {CONTORNO_GATE} s di contorno: GitHub ucciderebbe il gate "
        f"prima che scriva il verdetto"
    )


def test_block_i_percorsi_hanno_la_barra_anche_su_windows() -> None:
    """Su Windows `str()` di un percorso usa `\\`: `GROK` non si troverebbe e la
    raccolta dei test fallirebbe prima di eseguirli (GPT-6 Astra sulla #484)."""
    radice = PureWindowsPath("C:\\pickfair")
    assert _relativo(radice / ".github" / "workflows" / "pr-review-xai-grok46.yml", radice) == GROK

"""Un reviewer che non risponde entro il limite di attesa non revisiona.

La richiesta al modello non e' in streaming: la risposta arriva tutta alla
fine, dopo il ragionamento. Grok 4.7 a reasoning `high` ragiona a lungo, e con
100 s per richiesta il workflow chiudeva la connessione prima della risposta:
sulla #478 Grok non ha mai completato, sulla #483 ha completato 1 tentativo
su 8. La DECISIONE-426 P24 alza il suo limite a 240 s.

Il limite piu' alto va pagato col timeout del job. Tre tentativi e il backoff
devono stare dentro `timeout-minutes`, altrimenti GitHub uccide il job a meta'
e il reviewer tace senza nemmeno il commento d'errore. L'invariante vale per
tutti e cinque i reviewer.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import List, Tuple

import pytest

ROOT = Path(__file__).resolve().parents[2]
GROK = ".github/workflows/pr-review-xai-grok46.yml"
LIMITE_MINIMO_GROK = 240  # secondi per richiesta (DECISIONE-426 P24)
# Avvio del runner e chiamate a GitHub prima e dopo il modello (30 s di
# timeout ciascuna): quattro chiamate al tetto.
MARGINE_JOB = 120


def _reviewer() -> List[str]:
    trovati = sorted(str(p.relative_to(ROOT)) for p in (ROOT / ".github/workflows").glob("pr-review-*.yml"))
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
    rientro = len(testa) - len(testa.lstrip())
    corpo = []
    for riga in righe[inizio[0] + 1:]:
        if riga.strip() and len(riga) - len(riga.lstrip()) <= rientro:
            break
        corpo.append(riga)
    corpo_testo = "\n".join(corpo)
    tentativi = int(re.search(r"range\(1, (\d+)\)", testa).group(1)) - 1
    limiti = re.findall(r"urlopen\(req, timeout=(\d+)\)", corpo_testo)
    assert len(limiti) == 1, f"{workflow}: atteso un solo limite per richiesta nel ciclo, trovati {limiti}"
    attese = set(re.findall(r"time\.sleep\(min\((\d+), (\d+) \*\* attempt\)\)", corpo_testo))
    assert len(attese) == 1, f"{workflow}: backoff non riconosciuto nel ciclo: {attese}"
    tetto, base = map(int, attese.pop())
    # Il backoff scatta anche dopo l'ultimo tentativo, prima che il ciclo esca.
    return tentativi, int(limiti[0]), sum(min(tetto, base ** a) for a in range(1, tentativi + 1))


def _timeout_job(workflow: str) -> int:
    valori = re.findall(r"^\s*timeout-minutes:\s*(\d+)\s*$", _testo(workflow), re.M)
    assert len(valori) == 1, f"{workflow}: atteso un solo job con timeout-minutes, trovati {valori}"
    return int(valori[0]) * 60


@pytest.mark.parametrize("workflow", _reviewer())
def test_block_il_caso_peggiore_sta_dentro_il_timeout_del_job(workflow: str) -> None:
    tentativi, limite, backoff = _attesa_modello(workflow)
    peggiore = tentativi * limite + backoff + MARGINE_JOB
    assert tentativi >= 1 and limite > 0
    assert peggiore <= _timeout_job(workflow), (
        f"{workflow}: {tentativi} x {limite} s + {backoff} s di backoff + {MARGINE_JOB} s di margine "
        f"= {peggiore} s, oltre il timeout del job ({_timeout_job(workflow)} s): GitHub ucciderebbe "
        f"il job prima del commento d'errore"
    )


def test_block_grok_aspetta_abbastanza_da_ricevere_la_risposta() -> None:
    _, limite, _ = _attesa_modello(GROK)
    assert limite >= LIMITE_MINIMO_GROK, (
        f"Grok 4.7 ha {limite} s per richiesta: a reasoning high non basta (DECISIONE-426 P24, "
        f"#478 e #483), servono almeno {LIMITE_MINIMO_GROK} s"
    )

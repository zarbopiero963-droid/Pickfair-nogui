"""Cio' che i reviewer marcano come critico, devono anche redigerlo.

Rilievo di Fugu Ultra e Claude Fable 5.1 sulla full-range della #477, e la
prima risposta dell'agente ("`app_?key` non compare affatto, premessa
inventata") era SBAGLIATA: il grep cercava `app_?key` come espressione, non
come testo letterale, e non lo trovava. Il pattern c'e', in tutti e cinque i
workflow:

    CRITICAL_PATTERNS: ... |certlogin|app_?key|config|settings
    REDACTIONS       : ... (api[_-]?key|token|secret|password|...)

Le due liste dicono cose diverse sullo stesso segreto. La prima dichiara che
una `app_key` Betfair nel diff e' materia da controllo manuale; la seconda non
la redige, quindi quel valore arriva in chiaro al modello e dentro il commento
pubblicato. Una asimmetria fra "lo considero critico" e "lo nascondo" e'
proprio il caso in cui un segreto esce.

Il test non legge la lista: ESEGUE la regex estratta dal workflow vero e
verifica che il valore sparisca. Una lista che sembra giusta ma non matcha
sarebbe un verde falso.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Set

import pytest

ROOT = Path(__file__).resolve().parents[2]

# Segreti che i workflow dichiarano critici e che quindi devono sparire.
# `app_key` e' la Betfair application key: money-critical su questo repo.
SEGRETI_DA_REDIGERE = (
    "api_key=SEGRETISSIMO",
    "app_key=SEGRETISSIMO",
    "appKey: SEGRETISSIMO",
    "token=SEGRETISSIMO",
    "secret=SEGRETISSIMO",
    "password=SEGRETISSIMO",
    # Forma QUOTATA, ed e' quella che conta di piu': una app key Betfair vive
    # in `config.json`, non in una riga shell. Rilievo di Claude Fable 5.1
    # sulla #477 — la prima versione di questo test non aveva un campione JSON
    # e restava VERDE sul buco, perche' la `"` fra chiave e `:` rompeva il
    # pattern. Il fix copriva la forma meno probabile delle due.
    '"app_key": "SEGRETISSIMO"',
    "'app_key': 'SEGRETISSIMO'",
    '"appKey":"SEGRETISSIMO"',
    '"api_key": "SEGRETISSIMO"',
    '"password": "SEGRETISSIMO"',
    # Valore quotato CON SPAZI. Rilievo convergente di Claude Fable 5.1 e Fugu
    # Ultra sulla #477: la classe del valore era `[^"'\s,;]+`, quindi si
    # fermava al primo spazio e lasciava in chiaro il resto —
    # `"password": "SEGRETISSIMO con spazi"` diventava
    # `"password=[REDACTED] con spazi"`. Una fuga PARZIALE su un gate di
    # redazione e' peggio di nessuna redazione, perche' il commento sembra
    # ripulito. I campioni senza spazi passavano, quindi il test era verde.
    '"password": "prefisso SEGRETISSIMO"',
    '"app_key": "abc SEGRETISSIMO"',
    "'token': 'x SEGRETISSIMO'",
    # Delimitatore ESCAPATO dentro il valore, e valore NON CHIUSO. Rilievo
    # convergente di GPT-5.6 Sol e Fugu Ultra sulla #477. Il secondo e' il
    # peggiore dei due: con un apice non chiuso il ramo quotato falliva, il
    # fallback escludeva le virgolette, e la regola non redigeva NIENTE — non
    # una fuga parziale, una fuga intera su un gate di segreti.
    '"password": "abc\\" SEGRETISSIMO"',
    '"password": "abc SEGRETISSIMO',
    "'token': 'abc\\' SEGRETISSIMO'",
)


# Righe che devono sopravvivere intatte: una redazione che divora il resto del
# diff rende la review cieca, ed e' il modo opposto di rompere lo stesso gate.
NON_DA_DIVORARE = (
    ('"password": "x"\nquota = 3.5\n', "quota = 3.5"),
    ('"password": "aperto\nselezione = 2\n', "selezione = 2"),
    ('password=x  e poi altro_campo=visibile', "altro_campo=visibile"),
)
VALORE = "SEGRETISSIMO"


def _reviewer_su_disco() -> Set[str]:
    return {
        f".github/workflows/{p.name}"
        for p in (ROOT / ".github" / "workflows").glob("pr-review-*.yml")
    }


def _regex_generica(workflow: str) -> re.Pattern:
    """La regola `(chiave)\\s*[:=]\\s*valore` di REDACTIONS, compilata.

    Si cerca la riga che sostituisce con `\\1=[REDACTED]`: e' l'unica regola
    generica per coppie chiave/valore. Se non si trova, si FERMA invece di
    saltare il controllo: un workflow che perde la redazione generica senza
    farlo notare e' esattamente lo scenario da impedire.
    """
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    m = re.search(r're\.compile\(r"((?:[^"\\]|\\.)*)"\),\s*r"\\1=\[REDACTED\]"',
                  testo)
    assert m, (
        f"{workflow}: regola generica di REDACTIONS non trovata. Se la forma "
        f"e' cambiata, questo test va aggiornato nella stessa PR invece di "
        f"lasciare la redazione non verificata"
    )
    return re.compile(m.group(1))


@pytest.mark.parametrize("workflow", sorted(_reviewer_su_disco()))
@pytest.mark.parametrize("campione", SEGRETI_DA_REDIGERE)
def test_block_il_valore_del_segreto_non_sopravvive_alla_redazione(
    workflow: str, campione: str
) -> None:
    redatto = _regex_generica(workflow).sub(r"\1=[REDACTED]", campione)
    assert VALORE not in redatto, (
        f"{workflow}: {campione!r} resta in chiaro dopo la redazione "
        f"({redatto!r}). Il diff viene inviato al modello e ripubblicato nel "
        f"commento: un valore non redatto esce da entrambe le parti"
    )


def _chiavi_critiche(workflow: str) -> Set[str]:
    """Le chiavi-segreto nominate DENTRO `CRITICAL_PATTERNS`, non nel file.

    Rilievo di Claude Fable 5.1 sulla #477: cercare `[a-z]+key` in tutto il
    testo pescava qualunque parola che finisce per "key" — `hotkey`, `monkey`
    in un commento futuro — e avrebbe fatto diventare rosso un gate per
    rumore. Un gate che grida al lupo si smette di leggerlo, quindi il difetto
    era reale anche se il verso era l'opposto (falso allarme, non falsa calma).

    Qui si ritaglia il blocco `CRITICAL_PATTERNS = [ ... ]`, si buttano via le
    righe di solo commento e si prendono i membri di alternanza che finiscono
    per `key`.
    """
    testo = (ROOT / workflow).read_text(encoding="utf-8")
    m = re.search(r"CRITICAL_PATTERNS = \[(.*?)\n          \]", testo, re.S)
    assert m, (
        f"{workflow}: blocco CRITICAL_PATTERNS non trovato. Se la forma e' "
        f"cambiata, questo test va aggiornato nella stessa PR invece di "
        f"smettere di controllare in silenzio"
    )
    blocco = "\n".join(r for r in m.group(1).splitlines()
                       if not r.strip().startswith("#"))
    return set(re.findall(r"[a-z]+[_-]?\??key\b", blocco.lower()))


def test_block_ogni_chiave_critica_dichiarata_e_anche_redatta() -> None:
    """`CRITICAL_PATTERNS` e `REDACTIONS` non devono dire cose diverse.

    Se un workflow dichiara critica una chiave-segreto, deve anche redigerla:
    marcare `manual-review-required` e poi spedire il valore in chiaro e' la
    combinazione peggiore, perche' sembra che il caso sia gestito.
    """
    mancanti = []
    for workflow in sorted(_reviewer_su_disco()):
        critiche = _chiavi_critiche(workflow)
        generica = _regex_generica(workflow).pattern
        for chiave in sorted(critiche):
            # `app_?key` nel sorgente e' una regex: il caso concreto e' `app_key`
            concreta = chiave.replace("?", "")
            if not _regex_generica(workflow).search(f"{concreta}=X"):
                mancanti.append(f"  {workflow}: '{chiave}' critica ma non redatta "
                                f"(generica: {generica[:60]}...)")
    assert not mancanti, (
        "chiavi dichiarate critiche che la redazione non copre —\n"
        + "\n".join(mancanti)
    )


@pytest.mark.parametrize("workflow", sorted(_reviewer_su_disco()))
@pytest.mark.parametrize("campione,deve_restare", NON_DA_DIVORARE)
def test_block_la_redazione_non_divora_il_resto_del_diff(
    workflow: str, campione: str, deve_restare: str
) -> None:
    """Un ramo quotato troppo avido mangerebbe le righe successive.

    Il valore non chiuso e' il caso rischioso: per redigerlo bisogna
    consumare fino a fine riga, e se si dimentica di escludere il newline si
    divora il diff da li' in poi. Un reviewer che non vede il codice e' un
    check verde falso, cioe' lo stesso danno visto dall'altra parte.
    """
    redatto = _regex_generica(workflow).sub(r"\1=[REDACTED]", campione)
    assert deve_restare in redatto, (
        f"{workflow}: la redazione ha divorato {deve_restare!r} "
        f"({redatto!r}). Sopra-redigere acceca la review"
    )

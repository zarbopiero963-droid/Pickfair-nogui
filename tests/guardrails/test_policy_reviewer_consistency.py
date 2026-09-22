"""I documenti di policy devono nominare TUTTI i reviewer, non solo contarli.

Nato da un difetto reale, trovato da Fugu Ultra e Claude Fable 5.1 sulla #477.
Aggiungendo il terzo reviewer forte (GPT-6 Astra) erano stati aggiornati i
NUMERALI — «quattro reviewer» -> «cinque», «le due label» -> «le tre» — ma non
le ENUMERAZIONI che li seguono. Il risultato:

    "L'unico gate finale vincolante sono i tre reviewer forti a label
     (Fugu Ultra + Fable 5.1)"

cioe' un documento che dice «tre» e poi ne elenca due. Ventotto paragrafi erano
cosi'. Chi legge la lista invece del numero — ed e' la lista che dice *chi* —
conclude che il gate e' soddisfatto con due reviewer su tre: fail-open su un
gate di merge, prodotto da una modifica che sembrava completa.

Un grep sui numerali non lo trovava, ed e' proprio quel grep che aveva fatto
dichiarare «verificato, nessun residuo». Serviva un controllo che guardasse i
NOMI, e che li prendesse dai workflow che esistono davvero invece che da una
lista ricopiata a mano: aggiungere un quarto reviewer forte domani deve far
diventare rossi questi test, non passare in silenzio.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Set

import pytest

ROOT = Path(__file__).resolve().parents[2]

DOCUMENTI_DI_POLICY = ("CLAUDE.md", "AGENTS.md")

# Nome corto con cui ogni reviewer forte compare nella prosa. La chiave e' il
# workflow, cosi' il legame e' col file che gira e non con una convenzione.
NOME_NELLA_PROSA = {
    ".github/workflows/pr-review-openrouter-fugu-ultra.yml": "Fugu",
    ".github/workflows/pr-review-claude-fable5.yml": "Fable",
    ".github/workflows/pr-review-openrouter-gpt-astra.yml": "Astra",
}

# Passaggi che RACCONTANO un fatto accaduto, non la regola in vigore: quando li
# nomina, il documento sta citando chi disse cosa su una PR passata, e
# riscriverli falsificherebbe la storia. Ogni voce e' ancorata a una stringa
# distintiva e dice a quale PR si riferisce.
RACCONTI_STORICI = (
    # #469, i file-policy: li rilevarono Sol e Fable; Fugu i workflow-gate.
    "Rilevato da GPT-5.6 Sol e Claude Fable 5.1 sulla #469",
    "Flagged on #469 independently by GPT-5.6 Sol and Claude Fable 5.1",
    # #469, allowed_scope.json: lo chiesero Sol, Fugu e Fable — tre su quattro.
    "perché tre reviewer su quattro hanno chiesto di metterlo",
    "because three reviewers out of four asked for it to be",
    # #469, la prima stesura a sola prosa: bloccarono tutti e quattro i pagati.
    "prima versione di questa sezione lo scriveva come prosa",
    "first version of this section wrote it as prose",
)

# I reviewer che NON passano dall'API Anthropic: sono i soli con la manopola
# `REVIEW_EFFORT`. Derivato dal file, non dall'elenco: vedi il test in fondo.
CHIAVE_EFFORT = "REVIEW_EFFORT"


def _reviewer_su_disco() -> Set[str]:
    return {
        f".github/workflows/{p.name}"
        for p in (ROOT / ".github" / "workflows").glob("pr-review-*.yml")
    }


def _testo(percorso: str) -> str:
    f = ROOT / percorso
    assert f.is_file(), f"{percorso} non esiste"
    return f.read_text(encoding="utf-8")


def _forti_su_disco() -> Dict[str, str]:
    """I reviewer che hanno una label finale, letti dai workflow reali.

    Il criterio e' meccanico — la presenza di `github.event.label.name ==
    'final-...-review'` nella condizione del job — perche' «i forti» come
    convenzione e' esattamente cio' che si dimentica di aggiornare.
    """
    trovati: Dict[str, str] = {}
    for w in _reviewer_su_disco():
        m = re.search(r"github\.event\.label\.name == '(final-[a-z0-9-]+-review)'",
                      _testo(w))
        if m:
            trovati[w] = m.group(1)
    assert trovati, (
        "nessun workflow di review dichiara una label finale: o il repo ha "
        "perso il gate a label, o e' cambiata la forma della condizione e "
        "questo parser va aggiornato nella stessa PR"
    )
    return trovati


def _paragrafi(testo: str) -> List[str]:
    return testo.split("\n\n")


@pytest.mark.parametrize("documento", DOCUMENTI_DI_POLICY)
def test_block_ogni_label_finale_e_documentata(documento: str) -> None:
    """Una label che esiste nei workflow ma non nei doc e' un gate invisibile."""
    testo = _testo(documento)
    mancanti = sorted(lab for lab in _forti_su_disco().values() if lab not in testo)
    assert not mancanti, (
        f"{documento} non nomina {mancanti}: il gate esiste nei workflow ma "
        f"chi legge la policy non sa di doverlo far scattare"
    )


# Voce che sta nella lista di esclusione per definizione e non e' un reviewer:
# serve ad ANCORARE la lista, cosi' la si trova senza dipendere dal titolo della
# sezione, che e' prosa e cambia.
ANCORA_ESCLUSIONE = "scripts/guardrail_check.py"


def _voci_della_lista_di_esclusione(testo: str) -> Set[str]:
    """I path elencati nel blocco di bullet che contiene l'ancora.

    Rilievo di Claude Fable 5.1 sulla #477: cercare il path in TUTTO il
    documento e' piu' debole di quel che sembra — un path nominato in una nota
    qualsiasi soddisfarebbe il controllo senza stare nell'esclusione. Qui si
    ritaglia il blocco contiguo di bullet che contiene l'ancora e si guarda solo
    dentro quello.
    """
    righe = testo.splitlines()
    bullet = re.compile(r"^- `([^`]+)`\s*$")
    ancora = [i for i, r in enumerate(righe)
              if (m := bullet.match(r)) and m.group(1) == ANCORA_ESCLUSIONE]
    assert len(ancora) == 1, (
        f"la lista di esclusione deve essere UNA e individuabile senza "
        f"ambiguita': trovate {len(ancora)} righe `- \u0060{ANCORA_ESCLUSIONE}\u0060` "
        f"(righe {[i + 1 for i in ancora]}).\n"
        f"Zero: la forma della lista e' cambiata, e questo controllo va "
        f"aggiornato nella stessa PR invece di passare guardando un documento "
        f"che non capisce piu'.\n"
        f"Piu' di una: rilievo di GPT-5.6 Sol sulla #477 — unendo i blocchi di "
        f"piu' ancore, un secondo elenco potrebbe contenere i reviewer e far "
        f"passare il test mentre la lista di esclusione VERA li omette. "
        f"Il gate resterebbe fail-open guardando la lista sbagliata."
    )
    voci: Set[str] = set()
    i = ancora[0]
    for passo in (-1, 1):                           # risalgo e scendo dal bullet
        j = i + (passo if passo > 0 else 0)
        while 0 <= j < len(righe) and (m := bullet.match(righe[j])):
            voci.add(m.group(1))
            j += passo
    return voci


@pytest.mark.parametrize("documento", DOCUMENTI_DI_POLICY)
def test_block_ogni_reviewer_e_nella_lista_di_esclusione(documento: str) -> None:
    """Un reviewer fuori dall'esclusione sarebbe auto-mergiabile dall'agente.

    E' il buco che l'esclusione chiude: se l'agente potesse mergiare da solo una
    modifica a un reviewer, potrebbe indebolire il proprio gate.
    """
    elencati = _voci_della_lista_di_esclusione(_testo(documento))
    fuori = sorted(w for w in _reviewer_su_disco() if w not in elencati)
    assert not fuori, (
        f"{documento}: {fuori} non compare nella lista dei file a merge manuale "
        f"dell'owner. Un gate che l'agente puo' mergiare da solo non e' un gate"
    )


# Come ciascun reviewer forte puo' essere scritto nella prosa. Le varianti sono
# quelle che i documenti usano davvero; gli spazi sono `\\s+` perche' un nome
# puo' finire a cavallo di due righe ("GPT-6\\nAstra") e un controllo che si
# ferma all'a-capo direbbe il falso proprio sulle righe lunghe.
ALIAS_NELLA_PROSA = {
    ".github/workflows/pr-review-openrouter-fugu-ultra.yml": r"Fugu(?:\s+Ultra)?",
    ".github/workflows/pr-review-claude-fable5.yml": r"(?:Claude\s+)?Fable(?:\s+5\.1)?",
    ".github/workflows/pr-review-openrouter-gpt-astra.yml": r"(?:GPT-6\s+)?Astra",
}

# I segni con cui la prosa incolla due nomi in un ELENCO: `+`, virgola, slash,
# «e»/«and», «o»/«or». Tutto il resto (una preposizione, un punto) chiude
# l'elenco, ed e' quello che distingue «Fugu e Fable» da due frasi diverse che
# citano un reviewer ciascuna.
_SEPARATORE = r"(?:\s*(?:\+|,|/)\s*|\s+(?:e|ed|o|and|or)\s+)"

# Elenchi che nominano un SOTTOINSIEME apposta, non per dimenticanza. Ognuno e'
# ammesso solo se il testo che lo precede lo dichiara: qui l'unico caso e'
# «i reviewer non-Anthropic», dove Fable manca perche' l'API Anthropic non
# espone `REVIEW_EFFORT` — il sottoinsieme E' l'informazione della frase.
# Marcatore, non nome del reviewer: cosi' l'eccezione vale per la ragione
# dichiarata e non per chi capita di essere escluso.
MARCATORI_DI_SOTTOINSIEME = ("non-Anthropic",)
_FINESTRA_MARCATORE = 100


def _enumerazioni(testo: str, alias: dict) -> list:
    """Ogni catena `Nome <sep> Nome [...]` trovata, con i reviewer che nomina.

    Si lavora sull'ELENCO e non sul paragrafo perche' il difetto della #477
    sopravvive a un controllo per paragrafo: in

        "i 5 workflow API (Sol, Grok, Fugu Ultra, Fable 5.1) ... i tre
         reviewer forti a label (Fugu Ultra + Fable 5.1)"

    il nome mancante compare altrove nello stesso paragrafo, quindi un
    controllo a grana di paragrafo lo vede e passa. Verificato: con la grana
    sbagliata questo test restava VERDE sul difetto che doveva bloccare.
    """
    nome = "(?:" + "|".join(alias.values()) + ")"
    catena = re.compile(rf"{nome}(?:{_SEPARATORE}{nome})+")
    singoli = {w: re.compile(a) for w, a in alias.items()}
    trovate = []
    for m in catena.finditer(testo):
        prima = testo[max(0, m.start() - _FINESTRA_MARCATORE):m.start()]
        if any(mk in prima for mk in MARCATORI_DI_SOTTOINSIEME):
            continue
        testo_catena = m.group(0)
        presenti = {w for w, r in singoli.items() if r.search(testo_catena)}
        trovate.append((testo_catena, presenti))
    return trovate


@pytest.mark.parametrize("documento", DOCUMENTI_DI_POLICY)
def test_block_nessuna_enumerazione_lascia_fuori_un_forte(documento: str) -> None:
    """Il difetto della #477: «i tre reviewer forti (Fugu Ultra + Fable 5.1)».

    Regola: un elenco che nomina DUE reviewer forti li deve nominare TUTTI.
    Chi ne nomina uno solo sta parlando di quello; chi ne mette due in fila sta
    descrivendo l'insieme, e in un gate di merge un insieme incompleto e'
    fail-open — chi legge la lista invece del numerale conclude che con due su
    tre il gate e' soddisfatto.
    """
    forti = set(_forti_su_disco())
    senza_alias = sorted(forti - set(ALIAS_NELLA_PROSA))
    assert not senza_alias, (
        f"{senza_alias} non ha un alias in ALIAS_NELLA_PROSA: aggiungilo nella "
        f"stessa PR che introduce il reviewer, altrimenti questo controllo lo "
        f"ignora in silenzio"
    )
    alias = {w: a for w, a in ALIAS_NELLA_PROSA.items() if w in forti}

    incomplete = []
    for par in _paragrafi(_testo(documento)):
        if any(s in par for s in RACCONTI_STORICI):
            continue
        for catena, presenti in _enumerazioni(par, alias):
            if 2 <= len(presenti) < len(alias):
                mancanti = sorted(NOME_NELLA_PROSA[w] for w in set(alias) - presenti)
                incomplete.append(
                    f"  manca {mancanti} in: {' '.join(catena.split())!r}"
                )
    assert not incomplete, (
        f"{documento}: elenchi di reviewer forti che ne lasciano fuori "
        f"qualcuno —\n" + "\n".join(incomplete)
    )


def _effort_dei_workflow() -> Dict[str, str]:
    """`REVIEW_EFFORT` del job-env, per ogni reviewer che la manopola ce l'ha.

    Si legge la riga a sei spazi (`      REVIEW_EFFORT: "high"`), non le
    occorrenze nel corpo Python: quelle sono default di lettura, non il valore
    impostato.

    Rilievo di Claude Fable 5.1 sulla #477: un parser ancorato a sei spazi e'
    fragile a una riformattazione YAML. Misurato, e la prima stesura FALLIVA
    proprio qui — portando l'indentazione di un workflow da 6 a 8 spazi il test
    restava VERDE, perche' il confronto e' fra INSIEMI di valori e gli altri tre
    workflow bastavano a produrre `{"high"}`. Il workflow riformattato usciva
    dalla copertura in silenzio. Da qui il controllo di completezza qui sotto:
    chi ha un modello non-Anthropic DEVE dare un valore, altrimenti si ferma.
    """
    valori: Dict[str, str] = {}
    senza_valore = []
    for w in sorted(_reviewer_su_disco()):
        testo = _testo(w)
        # L'API Anthropic non espone la manopola: Fable non deve averla.
        ha_manopola = not re.search(r"^      ANTHROPIC_MODEL:", testo, re.MULTILINE)
        m = re.search(rf'^      {CHIAVE_EFFORT}: +(.*?)\s*$', testo, re.MULTILINE)
        if m:
            valori[w] = m.group(1).strip().strip('"').strip("'")
        elif ha_manopola:
            senza_valore.append(w)
    assert not senza_valore, (
        f"{CHIAVE_EFFORT} non letto da {senza_valore}: o la chiave e' sparita "
        f"dal job-env, o l'indentazione non e' piu' quella attesa e il parser "
        f"va aggiornato nella stessa PR. Un workflow che esce dalla copertura "
        f"in silenzio lascerebbe il test verde su una divergenza reale"
    )
    return valori


@pytest.mark.parametrize("documento", DOCUMENTI_DI_POLICY)
def test_block_leffort_scritto_nei_doc_e_quello_dei_workflow(documento: str) -> None:
    """Il doc diceva «Grok è impostato `low`» mentre il workflow stava a `high`.

    Era falso gia' su `main` e la #477 lo aveva RINFRESCATO rinominando 4.6 in
    4.7 dentro la frase sbagliata: una bugia con la data di oggi si legge come
    verificata. Qui il valore dichiarato nel documento viene confrontato con
    quello che i workflow impostano davvero.
    """
    dichiarati = set(re.findall(rf'`{CHIAVE_EFFORT}: ([a-z]+)`', _testo(documento)))
    assert dichiarati, (
        f"{documento} non dichiara piu' nessun `{CHIAVE_EFFORT}: <valore>`: se "
        f"la frase e' stata riscritta in altra forma, questo controllo non "
        f"lega piu' niente e va riscritto nella stessa PR"
    )
    reali = set(_effort_dei_workflow().values())
    assert dichiarati == reali, (
        f"{documento} dichiara {CHIAVE_EFFORT} {sorted(dichiarati)} ma i "
        f"workflow impostano {sorted(reali)}: "
        f"{ {k: v for k, v in _effort_dei_workflow().items()} }"
    )

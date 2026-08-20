"""Costo totale delle review AI di una PR, raggruppato per `Range:`.

Serve al punto 5 del GATE DI CONSEGNA (CLAUDE.md): il costo si consegna insieme
al verdetto. Ricopiarlo a mano dai commenti è il modo tipico in cui un passo di
processo smette di essere fatto, quindi qui è uno script.

Cosa rende visibile, che altrimenti non si vede: **il numero di push si paga**.
I quattro reviewer girano a ogni push, ognuno sul proprio `push range`; se poi
serve il giro a label sul range completo, quello rifà tutto da capo. Su #427:
$2.58 in 14 review su 4 range, dove il giro a label da solo è costato quanto
l'intero primo giro.

Lettura: ogni commento di review porta un'intestazione con `**Range:**` e, in
fondo, una riga `Costo stimato base: ~$…`. Si raggruppa per range, si somma.

Invarianti:

- **Provenienza verificata** (rilievo di GPT-5.6 Sol su #428). Il testo di un
  commento non è una prova di chi l'ha scritto: chiunque possa commentare
  potrebbe incollare un'intestazione di review e un costo inventato, e il totale
  mostrato all'owner cambierebbe. Si contano solo le review che portano il
  marker HTML emesso dal workflow (`<!-- <id>-pr-review:… -->`) oppure che
  arrivano da un autore fidato. Le altre finiscono fra le `non_verificate`, e
  vengono MOSTRATE: scartarle in silenzio sarebbe lo stesso difetto al contrario.
- **Niente cap silenziosi.** Un commento di review SENZA riga di costo (review
  fallita a metà, output troncato) viene contato a parte e SEGNALATO. Un totale
  che tace su ciò che non è riuscito a leggere è peggio di nessun totale: si
  legge come «ecco quanto è costato» mentre in realtà è «ecco quanto sono
  riuscito a vedere».
- **Aritmetica esatta** (rilievo di GPT-5.6 Sol su #428). Gli importi sono
  `Decimal` costruiti dalla stringa originale, non `float`: qui si sommano soldi,
  e l'arrotondamento binario piu' il round-half-even di `format` possono spostare
  il totale di mezzo centesimo proprio nei casi di parità. L'arrotondamento per
  la visualizzazione è dichiarato: `ROUND_HALF_UP`.
- **Le review fallite contano.** Un giro andato in errore (429, quota) riporta
  comunque un costo: è stato speso, quindi entra nel totale.
- **I doppioni contano.** Due review sullo stesso range non sono un errore di
  lettura: sono due chiamate pagate davvero (es. un `Re-run jobs`).
- **Modulo puro**: la logica non fa I/O di rete. `main` legge da un file JSON di
  commenti oppure da stdin, così è testabile headless e usabile in CI.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import OrderedDict, namedtuple
from decimal import ROUND_HALF_UP, Decimal

# Intestazione di una review: "# Claude Fable 5 PR Review". Il nome del modello
# cambia nel tempo, quindi si cattura quello che c'è fra '#' e 'PR Review'
# invece di elencare i reviewer: un reviewer nuovo entra nel conto da solo.
_REVIEWER_RE = re.compile(r"^#\s+(.+?)\s+PR Review\s*$", re.MULTILINE)
_RANGE_RE = re.compile(r"\*\*Range:\*\*\s*`([^`]+)`")
_COSTO_RE = re.compile(r"Costo stimato base:\s*`~\$([0-9]+(?:\.[0-9]+)?)`")

# Marker HTML che i workflow di review antepongono al commento, es.
# `<!-- gpt56sol-pr-review:pickfair-nogui:aaa...bbb -->`. È la prova di
# provenienza piu' semplice disponibile nel testo: lo scrive il workflow.
_MARKER_RE = re.compile(r"<!--\s*[A-Za-z0-9_-]+-pr-review:")

# Autori che possono emettere review. Usato solo quando il commento arriva come
# dict dell'API e porta quindi un autore; una stringa nuda non ne ha, e in quel
# caso decide il marker.
AUTORI_FIDATI = frozenset({"github-actions[bot]"})

SENZA_RANGE = "(range non dichiarato)"

CENTESIMO = Decimal("0.01")
QUATTRO_DECIMALI = Decimal("0.0001")

#: `review` = `(range, reviewer, costo)` conteggiate; `incomplete` = review vere
#: ma senza costo leggibile; `non_verificate` = testo che sembra una review ma
#: non prova di esserlo. Le ultime due NON entrano nel totale e vanno mostrate.
Risultato = namedtuple("Risultato", "review incomplete non_verificate")


def _corpo(commento) -> str:
    """Testo di un commento, accettando sia il dict dell'API sia una stringa."""
    if isinstance(commento, str):
        return commento
    if isinstance(commento, dict):
        return str(commento.get("body") or "")
    return ""


def _autore(commento) -> str:
    """Login dell'autore, se il commento lo porta; stringa vuota altrimenti."""
    if not isinstance(commento, dict):
        return ""
    utente = commento.get("user")
    if isinstance(utente, dict):
        return str(utente.get("login") or "")
    return str(commento.get("author") or utente or "")


def _e_autentica(commento) -> bool:
    """La review porta una prova di provenienza?

    Basta UNA delle due: il marker scritto dal workflow, oppure un autore
    fidato. Il marker da solo copre il caso in cui i commenti arrivano come
    testo nudo (nessun autore da controllare), l'autore copre il caso di un
    workflow futuro che smettesse di emettere il marker."""
    return bool(_MARKER_RE.search(_corpo(commento))) or _autore(commento) in AUTORI_FIDATI


def estrai_review(commenti) -> Risultato:
    """Divide i commenti in conteggiate / senza costo / non verificate.

    Un commento che non è affatto una review (il testo dell'autore, un bot di
    lint) non combacia con `_REVIEWER_RE` e viene ignorato senza rumore: non è
    sospetto, è semplicemente altro.
    """
    review: list = []
    incomplete: list = []
    non_verificate: list = []
    for commento in commenti or []:
        corpo = _corpo(commento)
        m_rev = _REVIEWER_RE.search(corpo)
        if not m_rev:
            continue
        reviewer = m_rev.group(1).strip()
        m_rng = _RANGE_RE.search(corpo)
        rng = m_rng.group(1).strip() if m_rng else SENZA_RANGE

        if not _e_autentica(commento):
            non_verificate.append((rng, reviewer))
            continue

        m_costo = _COSTO_RE.search(corpo)
        if m_costo is None:
            incomplete.append((rng, reviewer))
            continue
        review.append((rng, reviewer, Decimal(m_costo.group(1))))
    return Risultato(review, incomplete, non_verificate)


def raggruppa(review) -> "OrderedDict[str, list]":
    """Review per range, nell'ordine in cui i range compaiono.

    L'ordine dei commenti è quello cronologico dell'API, quindi i range escono
    nell'ordine dei push: è la lettura che serve, perché mostra quanto è costato
    ogni giro successivo al primo."""
    out: OrderedDict[str, list] = OrderedDict()
    for rng, reviewer, costo in review:
        out.setdefault(rng, []).append((reviewer, costo))
    return out


def totale(review) -> Decimal:
    """Somma esatta (`Decimal`), senza arrotondamenti intermedi."""
    return sum((costo for _, _, costo in review), Decimal("0"))


def _euro(valore: Decimal, quanto: Decimal = CENTESIMO) -> str:
    return f"${valore.quantize(quanto, rounding=ROUND_HALF_UP)}"


def formatta(risultato: Risultato) -> str:
    """Rendering testuale: un blocco per range, subtotale, totale finale."""
    gruppi = raggruppa(risultato.review)
    righe: list = []
    for rng, voci in gruppi.items():
        righe.append(rng)
        larghezza = max((len(r) for r, _ in voci), default=0)
        for reviewer, costo in voci:
            righe.append(f"    {reviewer:<{larghezza}}  {_euro(costo, QUATTRO_DECIMALI)}")
        subtotale = sum((c for _, c in voci), Decimal("0"))
        righe.append(f"    {'— subtotale':<{larghezza}}  {_euro(subtotale, QUATTRO_DECIMALI)}")
        righe.append("")

    for titolo, elenco in (("SENZA riga di costo (non conteggiate)", risultato.incomplete),
                           ("SENZA prova di provenienza (non conteggiate)",
                            risultato.non_verificate)):
        if elenco:
            righe.append(f"⚠️  review {titolo}:")
            righe.extend(f"    {reviewer} su {rng}" for rng, reviewer in elenco)
            righe.append("")

    righe.append("=" * 52)
    righe.append(f"TOTALE: {_euro(totale(risultato.review))}  "
                 f"({len(risultato.review)} review, {len(gruppi)} range)")
    scartate = len(risultato.incomplete) + len(risultato.non_verificate)
    if scartate:
        righe.append(f"NB: {scartate} review non conteggiate — "
                     f"il totale è una SOTTOSTIMA.")
    return "\n".join(righe)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python3 scripts/pr_costo_review.py",
        description="Costo totale delle review AI di una PR, per range "
                    "(punto 5 del GATE DI CONSEGNA).")
    p.add_argument("path", nargs="?", default="-",
                   help="File JSON con i commenti della PR (lista di oggetti con "
                        "`body` e `user.login`, o lista di stringhe). '-' = stdin. "
                        "Nota: contano solo le review con prova di provenienza — "
                        "marker del workflow oppure autore fidato. Del testo "
                        "incollato a mano, senza ne' l'uno ne' l'altro, viene "
                        "elencato come non verificato invece che sommato.")
    p.add_argument("--json", action="store_true", dest="as_json",
                   help="Output JSON invece che testuale.")
    return p


def _leggi(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        commenti = json.loads(_leggi(args.path))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"❌ impossibile leggere i commenti: {exc}", file=sys.stderr)
        return 1

    risultato = estrai_review(commenti)
    if args.as_json:
        # Gli importi restano STRINGHE: passare da float per serializzarli
        # rimetterebbe l'imprecisione che `Decimal` serve a togliere.
        print(json.dumps({
            "totale": str(totale(risultato.review).quantize(
                QUATTRO_DECIMALI, rounding=ROUND_HALF_UP)),
            "review": len(risultato.review),
            "per_range": {r: [{"reviewer": x, "costo": str(c)} for x, c in v]
                          for r, v in raggruppa(risultato.review).items()},
            "senza_costo": [{"range": r, "reviewer": x} for r, x in risultato.incomplete],
            "non_verificate": [{"range": r, "reviewer": x}
                               for r, x in risultato.non_verificate],
        }, ensure_ascii=False, indent=2))
    else:
        print(formatta(risultato))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

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

- **Niente cap silenziosi.** Un commento di review SENZA riga di costo (review
  fallita a metà, output troncato) viene contato a parte e SEGNALATO. Un totale
  che tace su ciò che non è riuscito a leggere è peggio di nessun totale: si
  legge come «ecco quanto è costato» mentre in realtà è «ecco quanto sono
  riuscito a vedere».
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
from collections import OrderedDict

# Intestazione di una review: "# Claude Fable 5 PR Review". Il nome del modello
# cambia nel tempo, quindi si cattura quello che c'è fra '#' e 'PR Review'
# invece di elencare i reviewer: un reviewer nuovo entra nel conto da solo.
_REVIEWER_RE = re.compile(r"^#\s+(.+?)\s+PR Review\s*$", re.MULTILINE)
_RANGE_RE = re.compile(r"\*\*Range:\*\*\s*`([^`]+)`")
_COSTO_RE = re.compile(r"Costo stimato base:\s*`~\$([0-9]+(?:\.[0-9]+)?)`")

SENZA_RANGE = "(range non dichiarato)"


def _testo(commento) -> str:
    """Corpo di un commento, accettando sia il dict dell'API sia una stringa."""
    if isinstance(commento, str):
        return commento
    if isinstance(commento, dict):
        return str(commento.get("body") or "")
    return ""


def estrai_review(commenti) -> tuple[list, list]:
    """`(review, incomplete)` dai commenti di una PR.

    `review` è una lista di `(range, reviewer, costo)`; `incomplete` elenca i
    `(range, reviewer)` delle review riconosciute ma **senza** riga di costo, che
    il chiamante deve mostrare invece di far finta che non esistano.

    Un commento che non è una review (il testo dell'autore, un bot di lint) non
    combacia con `_REVIEWER_RE` e viene ignorato senza rumore.
    """
    review: list = []
    incomplete: list = []
    for commento in commenti or []:
        corpo = _testo(commento)
        m_rev = _REVIEWER_RE.search(corpo)
        if not m_rev:
            continue
        reviewer = m_rev.group(1).strip()
        m_rng = _RANGE_RE.search(corpo)
        rng = m_rng.group(1).strip() if m_rng else SENZA_RANGE
        m_costo = _COSTO_RE.search(corpo)
        if m_costo is None:
            incomplete.append((rng, reviewer))
            continue
        review.append((rng, reviewer, float(m_costo.group(1))))
    return review, incomplete


def raggruppa(review) -> "OrderedDict[str, list]":
    """Review per range, nell'ordine in cui i range compaiono.

    L'ordine dei commenti è quello cronologico dell'API, quindi i range escono
    nell'ordine dei push: è la lettura che serve, perché mostra quanto è costato
    ogni giro successivo al primo."""
    out: OrderedDict[str, list] = OrderedDict()
    for rng, reviewer, costo in review:
        out.setdefault(rng, []).append((reviewer, costo))
    return out


def totale(review) -> float:
    return sum(costo for _, _, costo in review)


def formatta(review, incomplete=None) -> str:
    """Rendering testuale: un blocco per range, subtotale, totale finale."""
    gruppi = raggruppa(review)
    righe: list = []
    for rng, voci in gruppi.items():
        righe.append(rng)
        larghezza = max((len(r) for r, _ in voci), default=0)
        for reviewer, costo in voci:
            righe.append(f"    {reviewer:<{larghezza}}  ${costo:.4f}")
        righe.append(f"    {'— subtotale':<{larghezza}}  ${sum(c for _, c in voci):.4f}")
        righe.append("")

    if incomplete:
        righe.append("⚠️  review SENZA riga di costo (non conteggiate):")
        for rng, reviewer in incomplete:
            righe.append(f"    {reviewer} su {rng}")
        righe.append("")

    righe.append("=" * 52)
    righe.append(f"TOTALE: ${totale(review):.2f}  ({len(review)} review, "
                 f"{len(gruppi)} range)")
    if incomplete:
        righe.append(f"NB: {len(incomplete)} review senza costo leggibile — "
                     f"il totale è una SOTTOSTIMA.")
    return "\n".join(righe)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m scripts.pr_costo_review",
        description="Costo totale delle review AI di una PR, per range "
                    "(punto 5 del GATE DI CONSEGNA).")
    p.add_argument("path", nargs="?", default="-",
                   help="File JSON con i commenti della PR (lista di oggetti con "
                        "campo `body`, o lista di stringhe). '-' = stdin.")
    p.add_argument("--json", action="store_true", dest="as_json",
                   help="Output JSON invece che testuale.")
    return p


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        grezzo = sys.stdin.read() if args.path == "-" else open(
            args.path, encoding="utf-8").read()
        commenti = json.loads(grezzo)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"❌ impossibile leggere i commenti: {exc}", file=sys.stderr)
        return 1

    review, incomplete = estrai_review(commenti)
    if args.as_json:
        print(json.dumps({
            "totale": round(totale(review), 4),
            "review": len(review),
            "per_range": {r: [{"reviewer": x, "costo": c} for x, c in v]
                          for r, v in raggruppa(review).items()},
            "senza_costo": [{"range": r, "reviewer": x} for r, x in incomplete],
        }, ensure_ascii=False, indent=2))
    else:
        print(formatta(review, incomplete))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

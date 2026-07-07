"""Motore di estrazione del Parser Personalizzato (portato: CP-02/03/05).

Applica le regole di un ``CustomParserDef`` al testo di un messaggio Telegram e
produce i valori dei campi-target del segnale (``PARSER_TARGETS``).

- ``extract_value`` estrae il valore **grezzo** (nessuna traduzione);
- ``apply_parser`` applica poi, nell'ordine, la **trasformazione** e la **value-map**
  della regola, producendo il valore canonico; un valore non mappato/non
  trasformabile resta vuoto (→ "Non pronto" se obbligatorio);
- NON risolve ``MarketId``/``SelectionId`` (stadio catalogo successivo);
- NON tocca broker/bus/runtime/GUI.

Semantica di una regola (``FieldRule``):

- ``fixed_value``: se valorizzato, il campo vale esattamente quello e l'estrazione
  dal messaggio viene ignorata.
- ``start_after`` ("Inizia dopo"): l'estrazione parte subito DOPO la prima occorrenza
  di questo testo (match case-sensitive, utile per emoji/simboli); se il testo non è
  presente → valore vuoto; se vuoto (o solo spazi/tab) → si parte dall'inizio.
- ``end_before`` ("Finisce prima di"): l'estrazione termina PRIMA della prima
  occorrenza di questo testo trovata dopo il punto di inizio; se il delimitatore è
  configurato ma NON è presente → estrazione **fallita** (vuoto): un messaggio non
  conforme non deve passare il gate. Se vuoto → fino a fine RIGA (primo a-capo dopo
  l'inizio), per non "ingoiare" il resto.
- match dei delimitatori **tollerante agli spazi**: spazi/tab ai bordi del
  delimitatore ignorati e i run di spazi/tab interni flessibili (uno o più); parole,
  simboli ed emoji restano letterali. I newline NON sono toccati (un delimitatore
  ``"\\n"`` resta "fino a fine riga").
- una regola **senza estrazione configurata** (né ``fixed_value`` né delimitatori)
  restituisce vuoto.
- il valore estratto viene rifilato degli spazi ai bordi.

Modulo **puro**: non solleva su input malformato, mai un valore inventato.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from services.custom_parser import transforms, value_maps
from services.custom_parser.model import PARSER_TARGETS, CustomParserDef, FieldRule

# Match dei delimitatori tollerante agli spazi: spazi/tab ai bordi ignorati e ogni
# run di spazi/tab INTERNO diventa flessibile (uno o più). Parole, simboli ed emoji
# restano LETTERALI. Si toccano solo spazi e tab (NON i newline), così un delimitatore
# strutturale come "\n" (end_before = "fino a fine riga") resta letterale.
_EDGE_WS = " \t"
_INNER_WS = re.compile(r"[ \t]+")


def _delim_pattern(delim: str):
    """Compila il delimitatore in una regex tollerante agli spazi, o ritorna ``None``
    se, tolti spazi/tab ai bordi, è vuoto (nessun ancoraggio → delimitatore non
    configurato)."""
    trimmed = delim.strip(_EDGE_WS)
    if trimmed == "":
        return None
    parts = _INNER_WS.split(trimmed)
    return re.compile(r"[ \t]+".join(re.escape(p) for p in parts))


# Motivi dell'estrazione, per la diagnostica (stadio builder).
EXTRACT_FIXED = "FIXED"                        # valore da `fixed_value`
EXTRACT_OK = "OK"                              # estratto un valore (anche vuoto se la riga lo è)
EXTRACT_NO_RULE = "NO_EXTRACTION"              # né fixed né start/end → niente da estrarre
EXTRACT_START_NOT_FOUND = "START_NOT_FOUND"    # "Inizia dopo" non presente nel testo
EXTRACT_END_NOT_FOUND = "END_NOT_FOUND"        # "Finisce prima" non presente dopo l'inizio


def extract_value(text: str, rule: FieldRule) -> str:
    """Estrae il valore di UNA regola dal testo (semantica nel docstring del modulo).
    Non solleva: un delimitatore mancante → valore vuoto."""
    return extract_value_traced(text, rule)[0]


def extract_value_traced(text: str, rule: FieldRule):
    """Come ``extract_value`` ma ritorna ``(valore, motivo)`` con ``motivo`` ∈
    ``EXTRACT_*``. Fonte unica: ``extract_value`` delega qui."""
    # Strip: un fixed_value di soli spazi (" ") non è un valore.
    fixed = (rule.fixed_value or "").strip()
    if fixed != "":
        return fixed, EXTRACT_FIXED

    start_pat = _delim_pattern(rule.start_after or "")
    end_pat = _delim_pattern(rule.end_before or "")

    # Regola senza estrazione configurata (anche dopo aver tolto spazi/tab ai bordi):
    # nessun ancoraggio → vuoto (resta "mancante" se obbligatoria).
    if start_pat is None and end_pat is None:
        return "", EXTRACT_NO_RULE
    if not text:
        # Delimitatori configurati ma testo vuoto: l'ancoraggio non c'è.
        return "", (EXTRACT_START_NOT_FOUND if start_pat is not None
                    else EXTRACT_END_NOT_FOUND)

    start = 0
    if start_pat is not None:
        m = start_pat.search(text)
        if m is None:
            return "", EXTRACT_START_NOT_FOUND
        start = m.end()

    if end_pat is not None:
        # Delimitatore di fine configurato ma assente → messaggio non conforme:
        # estrazione fallita (vuoto), così un obbligatorio resta "Non pronto".
        m = end_pat.search(text, start)
        if m is None:
            return "", EXTRACT_END_NOT_FOUND
        end = m.start()
    else:
        # Nessun end_before: fino a fine riga (non "ingoia" il resto del messaggio).
        nl = text.find("\n", start)
        end = nl if nl != -1 else len(text)

    return text[start:end].strip(), EXTRACT_OK


def extract_between(text: str, start_after: str = "", end_before: str = "") -> str:
    """Estrae il testo tra i delimitatori riusando la STESSA logica delle regole
    (``extract_value``): match tollerante agli spazi, fino a fine riga se manca
    ``end_before``, stringa vuota se l'ancoraggio non si trova."""
    return extract_value(text, FieldRule(target="", start_after=start_after, end_before=end_before))


# Pattern di un risultato esatto «N - N» dentro la regione estratta.
# - separatore INTERNO **solo `-`** (trattino), tollerante agli spazi: NON si usa `:`
#   per evitare di agganciare ORARI/quote come «20:30» (rischio SelectionName fantasma);
# - **1–2 cifre per lato** con confini di cifra: NON aggancia numeri più lunghi.
# I risultati sono riconosciuti via `findall`, quindi il separatore FRA i risultati è
# irrilevante. Lo spazio attorno al «-» è SOLO orizzontale (non `\s*`): un punteggio sta
# su UNA riga, quindi cifre di righe adiacenti non si fondono. Confini anti-DECIMALE
# (`(?<![.,])` / `(?![.,]\d)`): un handicap/quota «0-0.5»/«0-0,5» non produce un
# punteggio spurio (selezioni Correct Score valide → riga piazzabile errata).
_SCORE_RE = re.compile(r"(?<!\d)(?<![.,])(\d{1,2})[^\S\r\n]*-[^\S\r\n]*(\d{1,2})(?!\d)(?![.,]\d)")

# Cap DIFENSIVO sul numero di risultati estratti da UN messaggio (input Telegram NON
# attendibile). Un elenco reale di Correct Score ha ~20 voci; oltre è anomalo/malevolo.
_MAX_SCORES = 50


def extract_scores(text: str, start_after: str = "", end_before: str = "") -> "list[str]":
    """Estrae la LISTA dei risultati esatti dalla regione fra ``start_after``/
    ``end_before`` (stessa semantica di ``extract_between``) e li **normalizza** al
    formato canonico «N - N» (spazi orizzontali attorno al trattino, zeri iniziali
    rimossi; SOLO il trattino, mai «:»). **Deduplica preservando l'ordine**. Regione
    vuota o nessun punteggio → lista vuota (fail-closed). Puro: non solleva."""
    if str(start_after or "").strip() or str(end_before or "").strip():
        region = extract_between(text, start_after, end_before)
    else:
        region = text or ""
    if not region:
        return []
    out: list = []
    seen: set = set()
    for home, away in _SCORE_RE.findall(region):
        score = f"{int(home)} - {int(away)}"     # normalizza spazi + rimuove zeri iniziali
        if score not in seen:
            seen.add(score)
            out.append(score)
            if len(out) >= _MAX_SCORES:
                break                            # cap difensivo (input non attendibile)
    return out


@dataclass
class ExtractionResult:
    """Esito dell'applicazione di un parser a un messaggio."""

    ready: bool                              # True se nessun obbligatorio è vuoto
    values: "dict[str, str]" = field(default_factory=dict)        # target → valore
    missing_required: "list[str]" = field(default_factory=list)   # obbligatori vuoti

    def as_values(self) -> "dict[str, str]":
        """Dizionario completo ``target → valore`` su tutti i ``PARSER_TARGETS``: i
        campi senza regola restano vuoti. I valori riflettono l'output di
        ``apply_parser`` (value-map già applicata). Usare solo a parser ``ready``."""
        row = {t: "" for t in PARSER_TARGETS}
        for target, value in self.values.items():
            if target in row:
                row[target] = value
        return row


def apply_parser(defn: CustomParserDef, text: str, value_maps_registry: dict = None) -> ExtractionResult:
    """Applica tutte le regole del parser al messaggio.

    Per ogni regola: estrae il valore grezzo e, nell'ordine, applica la trasformazione
    e la value-map (una value-map sconosciuta o un valore non mappato → vuoto, così non
    si produce mai un segnale con un valore tradotto a caso).

    ``value_maps_registry`` (nome → mappa) è opzionale: se ``None`` usa i soli built-in
    (es. ``bettype``), costruito una volta qui senza alcun I/O.

    Ritorna lo stato di "piazzabilità": ``ready=False`` con ``missing_required`` se un
    campo obbligatorio è vuoto (gate "Non pronto"). Per ogni target vince l'ultima
    regola; ``missing_required`` è calcolato sul valore FINALE (dedup)."""
    if value_maps_registry is None:
        value_maps_registry = value_maps.registry()  # built-in, costruito una volta
    values: dict = {}
    required_targets: list = []
    for rule in defn.rules:
        value = extract_value(text, rule)
        # Ordine: estrazione → trasformazione → value-map.
        if rule.transform:
            value = transforms.apply(value, rule.transform)
        if rule.value_map:
            value = value_maps.resolve(value, rule.value_map, value_maps_registry)
        values[rule.target] = value
        if rule.required and rule.target not in required_targets:
            required_targets.append(rule.target)
    # `.strip()`: un valore di soli spazi NON conta come presente.
    missing = [t for t in required_targets
               if not str(values.get(t, "") or "").strip()]
    return ExtractionResult(ready=not missing, values=values, missing_required=missing)

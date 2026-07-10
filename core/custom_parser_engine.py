"""CP-02: motore di estrazione del Parser Personalizzato.

Applica le regole di un `CustomParserDef` (CP-01) al testo di un messaggio
Telegram e produce i valori per le colonne del contratto CSV XTrader.

Scope (estrazione CP-02 + applicazione value-map CP-03):
- `extract_value` estrae il valore GREZZO (nessuna traduzione);
- `apply_parser` applica poi, nell'ordine, la trasformazione (CP-05) e la
  value-map (CP-03) della regola, producendo il valore XTrader; un valore non
  mappato/non trasformabile resta vuoto (→ "Non pronto");
- NON scrive il CSV (CP-04);
- NON tocca la GUI (CP-06).

Semantica di una regola (`FieldRule`):
- `fixed_value`: se valorizzato, la colonna vale esattamente quello e
  l'estrazione dal messaggio viene ignorata.
- `start_after` ("Inizia dopo"): se valorizzato, l'estrazione parte subito DOPO
  la prima occorrenza di questo testo (match case-sensitive, utile per
  emoji/simboli); se il testo non è presente nel messaggio → valore vuoto; se
  vuoto (o solo spazi/tab) → si parte dall'inizio del messaggio.
- `end_before` ("Finisce prima di"): l'estrazione termina PRIMA della prima
  occorrenza di questo testo trovata dopo il punto di inizio; se il delimitatore
  è configurato ma NON è presente → estrazione **fallita** (valore vuoto): un
  messaggio non conforme non deve passare il gate. Se vuoto (o solo spazi/tab) →
  fino a fine RIGA (primo a-capo dopo l'inizio), per non "ingoiare" il resto.
- match dei delimitatori **tollerante agli spazi**: spazi/tab ai bordi del
  delimitatore vengono ignorati e i run di spazi/tab interni sono flessibili
  (uno o più), così uno spazio digitato per errore non rompe l'estrazione;
  parole, simboli ed emoji restano invece uguali. I newline NON sono toccati,
  quindi un delimitatore "\n" resta letterale ("fino a fine riga").
- una regola **senza estrazione configurata** (né `fixed_value`, né `start_after`,
  né `end_before`) restituisce vuoto: non sappiamo dove prendere il valore, quindi
  resta "mancante" finché non viene configurata (es. le regole di `skeleton()`).
- il valore estratto viene rifilato degli spazi ai bordi.
- `required`: se il valore finale è vuoto il parser è "Non pronto" (nessuna riga
  CSV); se opzionale e vuoto → colonna vuota (non blocca).
"""

import re
from dataclasses import dataclass, field

from . import recognition, transforms, value_maps
from .csv_writer import CSV_HEADER
from .custom_parser import CustomParserDef, FieldRule
from .dizionario import normalize

# Match dei delimitatori tollerante agli spazi: spazi/tab ai bordi del
# delimitatore vengono ignorati e ogni run di spazi/tab INTERNO diventa flessibile
# (uno o più). Parole, simboli ed emoji restano LETTERALI. NB: si toccano solo
# spazi e tab (NON i newline), così un delimitatore strutturale come "\n"
# (usato per `end_before` = "fino a fine riga") resta letterale e invariato.
_EDGE_WS = " \t"
_INNER_WS = re.compile(r"[ \t]+")


def _delim_pattern(delim: str):
    """Compila il delimitatore in una regex tollerante agli spazi (vedi nota in
    testa al modulo) o ritorna ``None`` se, tolti spazi/tab ai bordi, è vuoto
    (nessun ancoraggio → si comporta come delimitatore non configurato)."""
    trimmed = delim.strip(_EDGE_WS)
    if trimmed == "":
        return None
    parts = _INNER_WS.split(trimmed)
    return re.compile(r"[ \t]+".join(re.escape(p) for p in parts))


def extract_value(text: str, rule: FieldRule) -> str:
    """Estrae il valore di UNA regola dal testo (vedi semantica nel docstring
    del modulo). Non solleva eccezioni: un delimitatore mancante → valore vuoto.

    Il match dei delimitatori è tollerante agli spazi (`_delim_pattern`): uno
    spazio in più/in meno ai bordi o tra le parole non rompe l'estrazione, mentre
    parole/simboli/emoji devono restare uguali. Il valore estratto è preso dal
    testo ORIGINALE (spazi/accenti interni preservati), poi `.strip()`ato.

    I campi della regola sono normalizzati con `or ""`: anche se costruita a mano
    con `None` (la persistenza JSON forza già `str`), il match non esplode."""
    return extract_value_traced(text, rule)[0]


# Motivi dell'estrazione, per la diagnostica del builder (`parser_diagnostics`).
EXTRACT_FIXED = "FIXED"                  # valore da `fixed_value`
EXTRACT_OK = "OK"                        # estratto un valore (anche vuoto se la riga lo è)
EXTRACT_NO_RULE = "NO_EXTRACTION"        # né fixed né start/end → niente da estrarre
EXTRACT_START_NOT_FOUND = "START_NOT_FOUND"   # "Inizia dopo" non presente nel testo
EXTRACT_END_NOT_FOUND = "END_NOT_FOUND"       # "Finisce prima" non presente dopo l'inizio


def extract_value_traced(text: str, rule: FieldRule):
    """Come `extract_value` ma ritorna `(valore, motivo)` dove `motivo` è uno dei
    codici `EXTRACT_*`. Serve alla diagnostica per distinguere "inizio non trovato"
    da "fine non trovata" (entrambi danno valore vuoto). FONTE UNICA: `extract_value`
    delega qui, così il comportamento del runtime resta identico."""
    # Strip: un fixed_value di soli spazi (" ") non è un valore — prima passava il
    # gate dei required come "presente" e arrivava VALID fino al CSV (audit #259 B5).
    fixed = (rule.fixed_value or "").strip()
    if fixed != "":
        return fixed, EXTRACT_FIXED

    start_pat = _delim_pattern(rule.start_after or "")
    end_pat = _delim_pattern(rule.end_before or "")

    # Regola senza estrazione configurata (anche dopo aver tolto spazi/tab ai
    # bordi): nessun ancoraggio → vuoto (resta "mancante" se obbligatoria, es. le
    # regole di skeleton()).
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
    """Estrae il testo tra i delimitatori riusando la STESSA logica delle regole del Parser
    (`extract_value`): match tollerante agli spazi, fino a fine riga se manca `end_before`,
    stringa vuota se l'ancoraggio non si trova. Usato dal **dizionario mercati** per leggere
    il mercato da una posizione precisa del messaggio (es. fra «Quota» e «Prematch») invece
    di cercarlo in tutto il testo — così un banner/menu nel messaggio non crea falsi match."""
    return extract_value(text, FieldRule(target="", start_after=start_after, end_before=end_before))


# #325: pattern di un risultato esatto «N - N» dentro la regione estratta.
# - separatore INTERNO **solo `-`** (trattino), tollerante agli spazi: NON si usa `:` per evitare
#   di agganciare ORARI/quote come «20:30» o «45:12» (rischio di SelectionName fantasma → riga
#   spuria piazzabile per nome in NAME_ONLY, potenziale scommessa errata — review #341);
# - **1–2 cifre per lato** con **confini di cifra** (`(?<!\d)`/`(?!\d)`): NON aggancia numeri più
#   lunghi (maglie/ID come «100-1», «007-3», «1-100») né loro pezzi.
# I risultati sono riconosciuti via `findall`, perciò il separatore FRA i risultati
# (virgola/spazio/newline/slash/…) è irrilevante. La difesa PRIMARIA restano i delimitatori «Inizia
# dopo/Finisce prima» che limitano la regione al solo elenco di risultati; questo pattern è un cap
# difensivo aggiuntivo. Un punteggio non nel dizionario resta comunque fail-closed a valle (ID_ONLY).
# NB: i punteggi col separatore «:» (es. «1:0») NON sono riconosciuti di proposito; se un canale li
# usa, va aggiunta un'euristica orario-vs-punteggio dedicata (follow-up), non un `:` indiscriminato.
# Lo spazio attorno al «-» è SOLO orizzontale (`[^\S\r\n]*`, non `\s*`): un punteggio sta sempre su
# UNA riga, quindi cifre di righe adiacenti («3\n- 0») NON devono fondersi in un punteggio spurio
# (→ riga CSV/scommessa Betfair errata) quando la regione delimitata è multi-riga (Fugu #341).
# Confini anti-DECIMALE oltre a quelli anti-cifra: un handicap/quota come «0-0.5», «1-0.25» o «0.5-1»
# — e la stessa notazione con la **virgola** italiana «0-0,5», «0,5-1» (prevalente nei canali IT) —
# NON deve produrre un punteggio spurio «0 - 0»/«1 - 0»/«5 - 1» (selezioni Correct Score VALIDE →
# riga piazzabile errata). `(?<![.,])` (sul PRIMO numero) esclude il caso in cui è la parte decimale
# di un valore, anche **senza cifra iniziale** («0,5» ma pure «,5»/«.5»); `(?![.,]\d)` esclude il caso
# in cui il SECONDO numero è seguito da «[.,]d» — ma NON da un semplice punto/virgola di frase
# («1-0.» resta «1 - 0») (Fable/Fugu/GPT #341). Disambiguazione lista-vs-decimale: sul primo numero il
# confine morde su `[.,]` immediatamente prima; il separatore di lista comma-SPAZIO «1-0, 2-1» resta
# valido (il numero successivo è preceduto da uno SPAZIO, non da «[.,]»). Una lista a virgola SENZA
# spazio «1-0,2-1» è ambigua con un decimale → fail-closed (non estratta): usare «, » o newline.
_SCORE_RE = re.compile(r"(?<!\d)(?<![.,])(\d{1,2})[^\S\r\n]*-[^\S\r\n]*(\d{1,2})(?!\d)(?![.,]\d)")

# #325: cap DIFENSIVO sul numero di risultati estratti da UN messaggio (input Telegram NON
# attendibile, review #341). Un elenco reale di risultati esatti (Correct Score FT/1º tempo) ha
# ~20 voci; oltre è un messaggio anomalo o malevolo che, senza limite, genererebbe righe CSV (e
# quindi scommesse) in quantità arbitraria. 50 è ampiamente sopra il caso reale e taglia gli abusi.
_MAX_SCORES = 50


def extract_scores(text: str, start_after: str = "", end_before: str = "") -> "list[str]":
    """#325: estrae la LISTA dei risultati esatti dalla regione fra `start_after`/`end_before`
    (stessa semantica di `extract_between`) e li **normalizza** al formato canonico del dizionario
    XTrader «N - N» (spazi ORIZZONTALI attorno al trattino, zeri iniziali rimossi, es. «1-0»/«01 - 0»
    → «1 - 0»; SOLO il trattino, mai «:» — vedi `_SCORE_RE`). I punteggi sono riconosciuti per forma
    su una singola riga, quindi il separatore FRA i risultati non conta (ma cifre di righe diverse non
    si fondono).
    **Deduplica preservando l'ordine** (nessuna riga CSV doppia — quindi nessuna doppia scommessa —
    per lo stesso punteggio). Regione vuota o nessun punteggio → lista vuota (fail-closed: la regola
    dinamica non genera righe). Puro: nessuna GUI, non solleva."""
    # Con almeno un delimitatore → si scandisce la sola regione fra start_after/end_before (come le
    # regole del Parser); senza delimitatori → si scandisce l'INTERO testo (nel runtime una regola
    # dinamica ha sempre un delimitatore — la detection lo richiede — ma l'helper resta usabile
    # standalone su tutto il messaggio).
    if str(start_after or "").strip() or str(end_before or "").strip():
        region = extract_between(text, start_after, end_before)
    else:
        region = text or ""
    if not region:
        return []
    out = []
    seen = set()
    for home, away in _SCORE_RE.findall(region):
        score = f"{int(home)} - {int(away)}"     # normalizza spazi + rimuove zeri iniziali
        if score not in seen:
            seen.add(score)
            out.append(score)
            if len(out) >= _MAX_SCORES:
                break                            # cap difensivo (input non attendibile) — vedi sotto
    return out


def conditions_pass(defn: CustomParserDef, text: str) -> bool:
    """Gate delle CONDIZIONI (PR-1): True se il messaggio soddisfa le condizioni del parser.

    - nessuna condizione attiva (`defn.active_conditions()` vuoto) → True (nessun gate
      aggiuntivo, retro-compatibile);
    - `conditions_mode == "all"` → TUTTE le condizioni soddisfatte (E); `"any"` → almeno UNA (O);
    - una condizione è soddisfatta se il suo testo è PRESENTE (contiene) oppure ASSENTE
      (`negate` = «NON contiene») nel messaggio.

    Match **case-insensitive e tollerante agli spazi**: `text` e `condition.text` sono
    normalizzati con `dizionario.normalize` (lowercase + collasso spazi interni) su ENTRAMBI
    i lati, poi si fa il confronto sottostringa. Le condizioni a testo vuoto sono già escluse
    da `active_conditions`, quindi il needle è sempre non vuoto (niente match-tutto)."""
    conds = defn.active_conditions()
    if not conds:
        return True
    hay = normalize(text)
    results = []
    for c in conds:
        present = normalize(c.text) in hay
        results.append((not present) if c.negate else present)
    # Modo già normalizzato da from_dict; ricontrollo difensivo (default costruttore/GUI).
    if defn.conditions_mode == "any":
        return any(results)
    return all(results)


def matches_message(defn: CustomParserDef, text: str, mode: str = None) -> bool:
    """True se il messaggio ha attivato un'estrazione che rappresenta **contenuto di
    segnale**: una regola con `start_after`/`end_before` (non `fixed_value`) che ha
    trovato un valore non vuoto **e** è o **obbligatoria** (`required`) o su un **campo
    di riconoscimento rilevante per la modalità** (`recognition.recognition_fields_for_mode`).

    Gate di "contenuto" per il live (CP-09): un parser i cui obbligatori sono
    tutti `fixed_value` produrrebbe una riga piazzabile per QUALSIASI messaggio. Siccome
    il live bypassa il prefiltro marker per i parser custom attivi, senza questo gate si
    scriverebbe lo stesso bet fisso su ogni messaggio (rischio doppia/spuria scommessa).

    Non basta UN'estrazione qualsiasi: un'estrazione **opzionale** su un campo NON di
    riconoscimento (es. una nota "larga") non deve far passare un messaggio non-segnale
    (A10). I campi di riconoscimento ESTRATTI contano anche se non `required`, ma SOLO se
    rilevanti per la modalità (`mode`): in `BOTH` la GUI lascia opzionali nome/ID (basta un
    set), quindi entrambi i set contano; ma in `NAME_ONLY` un'estrazione opzionale su un
    campo ID (non usato da quella modalità) NON deve far passare un non-segnale (Codex P2,
    A10). Se `mode` è `None` si usa `defn.mode`. NON tocca pipeline/validator.

    **#74 (set di riconoscimento già FISSO-completo).** Se i soli valori FISSI completano già
    un set di riconoscimento per la modalità, la riga è **piazzabile per QUALSIASI messaggio**
    (es. `MarketId`+`SelectionId` fissi in `BOTH`): allora un'estrazione **opzionale** — anche
    su un campo di riconoscimento — NON basta come contenuto, altrimenti una regola "larga"
    farebbe scrivere un bet spurio su un non-segnale. In quel caso serve un'estrazione
    **obbligatoria** (contenuto reale dichiarato dall'utente). Quando invece il riconoscimento
    NON è già completo coi soli fissi, l'estrazione SERVE a riconoscere e continua a contare
    (così i parser basati su mappatura mercati — che estraggono solo l'evento — restano validi).

    **Condizioni di gate (PR-1).** Se il parser ha condizioni attive (contiene/NON contiene) e il
    messaggio NON le soddisfa, il parser NON scatta: `return False` immediato, PRIMA della logica
    di riconoscimento. Così un parser configurato agisce solo sui messaggi pertinenti (filtro
    fail-closed), a prescindere da cosa riuscirebbe a estrarre."""
    if not conditions_pass(defn, text):
        return False
    mode = recognition.normalize_mode(defn.mode if mode is None else mode)
    relevant = recognition.recognition_fields_for_mode(mode)
    # I soli FISSI completano già un set di riconoscimento? → riga piazzabile per ogni messaggio.
    fixed_targets = {r.target for r in defn.rules if r.is_fixed() and str(r.fixed_value).strip()}
    # Una mappatura MERCATI selezionata può AZZERARE `MarketId`/`SelectionId` fissi (stale-ID,
    # #192) e validare la riga sui nomi mappati: in quel caso gli ID fissi NON rendono il
    # riconoscimento "completo a prescindere dal messaggio", quindi non vanno contati — altrimenti
    # si bloccherebbe il path supportato «ID fissi + mappatura mercati + EventName estratto»
    # restituendo NO_CONTENT_MATCH per una riga mappata valida (#74 review Codex).
    if defn.market_mapping_profiles:
        fixed_targets -= {"MarketId", "SelectionId"}
    fixed_complete = recognition.is_valid({t: "x" for t in fixed_targets}, mode)
    for rule in defn.rules:
        if not rule.has_extraction() or rule.is_fixed() or extract_value(text, rule) == "":
            continue
        if rule.required:
            return True                                  # estrazione obbligatoria = contenuto reale
        if rule.target in relevant and not fixed_complete:
            return True                                  # recognition estratto NECESSARIO al set
    return False


@dataclass
class ExtractionResult:
    """Esito dell'applicazione di un parser a un messaggio."""

    ready: bool                              # True se nessun obbligatorio è vuoto
    values: "dict[str, str]" = field(default_factory=dict)        # target → valore
    missing_required: "list[str]" = field(default_factory=list)   # obbligatori vuoti

    def as_csv_row(self) -> "dict[str, str]":
        """Riga completa a 14 colonne: le colonne senza regola restano vuote.

        Le colonne sono quelle del contratto (`csv_writer.CSV_HEADER`, fonte
        unica) per evitare drift. NB: i valori riflettono l'output di
        `apply_parser` (value-map CP-03 già applicata; trasformazioni CP-05 no).
        Usare solo a parser `ready`."""
        row = {col: "" for col in CSV_HEADER}
        for target, value in self.values.items():
            if target in row:
                row[target] = value
        return row


def apply_parser(defn: CustomParserDef, text: str, value_maps_registry: dict = None) -> ExtractionResult:
    """Applica tutte le regole del parser al messaggio.

    Per ogni regola: estrae il valore grezzo (CP-02) e, se la regola indica una
    `value_map`, lo traduce nel valore esatto XTrader (CP-03). Una value-map
    sconosciuta o un valore non mappato → vuoto (→ "Non pronto" se obbligatorio),
    così non si scrive mai una riga CSV con un valore tradotto a caso.

    `value_maps_registry` (nome → mappa) è opzionale: se `None` usa i soli
    built-in (es. `bettype`) — registro costruito UNA volta qui, senza alcuna
    lettura del dizionario (nessun I/O nascosto). Passa
    `value_maps.registry(include_dizionario=True)` per abilitare anche le mappe
    derivate dal dizionario.

    Ritorna lo stato di "piazzabilità": `ready=False` con `missing_required` se
    un campo obbligatorio è vuoto (gate "Non pronto").

    `validate_parser_def` (CP-01) vieta già i target duplicati; qui il motore è
    comunque robusto: per ogni target vince l'ultima regola e `missing_required`
    è calcolato sul valore FINALE (dedup, niente doppioni o falsi mancanti)."""
    if value_maps_registry is None:
        value_maps_registry = value_maps.registry()  # built-in, costruito una volta
    values = {}
    required_targets = []
    for rule in defn.rules:
        value = extract_value(text, rule)
        # Ordine: estrazione → trasformazione (CP-05) → value-map (CP-03).
        if rule.transform:
            value = transforms.apply(value, rule.transform)
        if rule.value_map:
            value = value_maps.resolve(value, rule.value_map, value_maps_registry)
        values[rule.target] = value
        if rule.required and rule.target not in required_targets:
            required_targets.append(rule.target)
    # `.strip()`: un valore di soli spazi NON conta come presente (audit #259 B5) —
    # per i campi fuori dal riconoscimento (es. MarketName) questo è l'unico gate.
    missing = [t for t in required_targets
               if not str(values.get(t, "") or "").strip()]
    return ExtractionResult(ready=not missing, values=values, missing_required=missing)

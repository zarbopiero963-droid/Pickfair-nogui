"""CP-08b: diagnostica del Parser Personalizzato per "Prova messaggio".

Spiega, **campo per campo**, perché un messaggio produce (o no) una riga
piazzabile per XTrader, seguendo la stessa catena del runtime:

    estrazione (CP-02) → trasformazione (CP-05) → value-map (CP-03) → validazione (PR-10)

Quando il builder dice solo "Non pronto" non si capisce QUALE campo ha fallito né
PERCHÉ; questo modulo produce un esito strutturato (codici errore per colonna) di
cui la GUI (`custom_parser_gui`) è solo una vista. Logica pura, testabile in CI:
nessun widget, nessun I/O nascosto oltre al registro value-map condiviso del pipeline.
"""

from dataclasses import dataclass, field

from . import custom_pipeline, recognition, transforms, validator, value_maps
from .custom_parser import CustomParserDef
from .custom_parser_engine import (
    EXTRACT_END_NOT_FOUND,
    EXTRACT_START_NOT_FOUND,
    extract_value_traced,
    matches_message,
)

# ── Codici di stato per campo ───────────────────────────────────────────────
OK = "OK"                                # valore finale presente e valido
EMPTY_OPTIONAL = "EMPTY_OPTIONAL"        # vuoto ma non obbligatorio → non blocca
START_NOT_FOUND = "START_NOT_FOUND"      # "Inizia dopo" non trovato nel messaggio
END_NOT_FOUND = "END_NOT_FOUND"          # "Finisce prima" non trovato dopo l'inizio
REQUIRED_EMPTY = "REQUIRED_EMPTY"        # obbligatorio ma vuoto (nessuna estrazione)
TRANSFORM_FAILED = "TRANSFORM_FAILED"    # la trasformazione ha svuotato il valore
VALUE_MAP_MISS = "VALUE_MAP_MISS"        # la value-map non ha trovato il valore
INVALID_PRICE = "INVALID_PRICE"          # Price non numerico o ≤ 1.0
INVALID_BETTYPE = "INVALID_BETTYPE"      # BetType non PUNTA/BANCA
INVALID_HANDICAP = "INVALID_HANDICAP"    # Handicap valorizzato ma non numerico
INVALID_POINTS = "INVALID_POINTS"        # Points valorizzato ma non un numero > 0
INVALID_PRICE_BOUNDS = "INVALID_PRICE_BOUNDS"  # Min/Max incoerenti (invertiti o escludono Price)
MISSING_PROVIDER = "MISSING_PROVIDER"    # Provider assente (contratto)
MODE_REQUIRED_MISSING = "MODE_REQUIRED_MISSING"  # campo richiesto dalla Modalità mancante
MAPPING_MISSING = "MAPPING_MISSING"      # EventName non traducibile coi profili di mappatura nomi
MARKET_MAPPING_MISSING = "MARKET_MAPPING_MISSING"  # mercato non risolvibile (ambiguo o nessun match)

# ── Codice a livello messaggio ──────────────────────────────────────────────
NO_CONTENT_MATCH = "NO_CONTENT_MATCH"    # niente estratto: solo valori fissi / nessun match

_OK_CODES = (OK, EMPTY_OPTIONAL)

# Spiegazioni leggibili dei codici (per il report).
_EXPLAIN = {
    OK: "",
    EMPTY_OPTIONAL: "vuoto ma facoltativo",
    START_NOT_FOUND: "delimitatore «Inizia dopo» non trovato nel messaggio",
    END_NOT_FOUND: "delimitatore «Finisce prima» non trovato dopo l'inizio",
    REQUIRED_EMPTY: "obbligatorio ma vuoto (nessuna estrazione/valore)",
    TRANSFORM_FAILED: "la trasformazione non ha prodotto un valore",
    VALUE_MAP_MISS: "value-map: valore non presente nel dizionario",
    INVALID_PRICE: "quota non numerica o ≤ 1.0",
    INVALID_BETTYPE: "BetType non è PUNTA/BANCA",
    INVALID_HANDICAP: "Handicap valorizzato ma non numerico",
    INVALID_POINTS: "Points valorizzato ma non un numero positivo",
    INVALID_PRICE_BOUNDS: "limiti di prezzo incoerenti (Min > Max o intervallo che esclude Price)",
    MISSING_PROVIDER: "Provider mancante (richiesto dal contratto)",
    MODE_REQUIRED_MISSING: "campo richiesto dalla Modalità di riconoscimento",
    MAPPING_MISSING: "EventName non traducibile: separatore non trovato o squadra non nei profili di mappatura nomi",
    MARKET_MAPPING_MISSING: "mercato non risolvibile: frasi ambigue, o nessuna frase combacia e nessun mercato dalle regole",
    NO_CONTENT_MATCH: "nessun contenuto estratto dal messaggio (solo valori fissi / nessun match)",
}


def explain(code: str) -> str:
    """Spiegazione leggibile di un codice di stato/errore (stringa vuota se OK)."""
    return _EXPLAIN.get(code, code)


@dataclass
class FieldDiagnostic:
    """Esito della catena per UNA colonna."""

    target: str
    raw: str = ""                 # valore grezzo estratto (CP-02)
    after_transform: str = ""     # dopo la trasformazione (CP-05)
    final: str = ""               # dopo la value-map (CP-03) — valore XTrader
    required: bool = False
    error: str = OK               # uno dei codici sopra

    @property
    def ok(self) -> bool:
        return self.error in _OK_CODES


@dataclass
class Diagnosis:
    """Esito complessivo della diagnostica."""

    placeable: bool
    status: str                                  # status del pipeline (VALID/INVALID_*/NOT_READY)
    fields: "list[FieldDiagnostic]" = field(default_factory=list)
    message_error: str = ""                      # NO_CONTENT_MATCH o ""


def _classify_extraction(rule, raw, reason, after, final) -> str:
    """Codice per UN campo guardando SOLO estrazione→transform→value-map.
    Gli errori del validator (prezzo/bettype/modalità) sono sovrapposti dopo."""
    if final != "":
        return OK
    # final vuoto → individua lo stadio che l'ha svuotato (dal più "a monte").
    if reason == EXTRACT_START_NOT_FOUND:
        return START_NOT_FOUND
    if reason == EXTRACT_END_NOT_FOUND:
        return END_NOT_FOUND
    if rule.transform and raw != "" and after == "":
        return TRANSFORM_FAILED
    if rule.value_map and after != "" and final == "":
        return VALUE_MAP_MISS
    return REQUIRED_EMPTY if rule.required else EMPTY_OPTIONAL


def _field_diag(rule, text, registry) -> FieldDiagnostic:
    raw, reason = extract_value_traced(text, rule)
    after = transforms.apply(raw, rule.transform) if rule.transform else raw
    final = value_maps.resolve(after, rule.value_map, registry) if rule.value_map else after
    return FieldDiagnostic(
        target=rule.target, raw=raw, after_transform=after, final=final,
        required=bool(rule.required),
        error=_classify_extraction(rule, raw, reason, after, final),
    )


def _mark(by_target, fields, target, error, *, required=False) -> None:
    """Imposta/sovrascrive il codice errore su una colonna (la crea se assente:
    es. un campo richiesto dalla Modalità per cui non esiste alcuna regola)."""
    fd = by_target.get(target)
    if fd is None:
        fd = FieldDiagnostic(target=target, required=required, error=error)
        fields.append(fd)
        by_target[target] = fd
    else:
        fd.error = error
        if required:
            fd.required = True


def _mark_mode_required(by_target, fields, col) -> None:
    """Marca `col` come richiesta dalla MODALITÀ ma mancante. Se la colonna ha GIÀ un errore
    di ESTRAZIONE azionabile (START_NOT_FOUND/END_NOT_FOUND/TRANSFORM_FAILED/VALUE_MAP_MISS/
    REQUIRED_EMPTY) lo PRESERVA (solo `required=True`): sostituirlo col generico
    MODE_REQUIRED_MISSING nasconderebbe il motivo utile — es. il delimitatore sbagliato
    (Codex #70). Se la colonna è OK/vuota-facoltativa (o assente) usa MODE_REQUIRED_MISSING."""
    fd = by_target.get(col)
    if fd is not None and fd.error not in _OK_CODES:
        fd.required = True
        return
    _mark(by_target, fields, col, MODE_REQUIRED_MISSING, required=True)


def _overlay_validator(result, by_target, fields) -> None:
    """Sovrappone gli errori del validator/pipeline ai campi. Due parti:

    (A) i gate STRUTTURALI (dallo `status` del pipeline: modalità/prezzo mancante/provider/
        handicap/mappature), e
    (B) gli errori di VALORE per-colonna (BetType/prezzi/Points/limiti), controllati
        INDIPENDENTEMENTE così un messaggio con PIÙ colonne invalide le mostra TUTTE —
        `validator.validate` si ferma al primo errore (short-circuit), la diagnostica no
        (Codex #70): es. `BetType=BACK` **e** `Price=abc` marca entrambe, non solo BetType.

    (B) non cambia il verdetto (`placeable`), solo l'attribuzione per-colonna."""
    status = result.status
    row = result.row

    # (A) Gate strutturali: la riga non è abbastanza completa/coerente per il contratto.
    if status == validator.INVALID_MISSING_FIELDS:
        for col in (result.detail or []):
            _mark_mode_required(by_target, fields, col)
    elif status == validator.INVALID_MISSING_PRICE:
        _mark(by_target, fields, "Price", REQUIRED_EMPTY, required=True)
    elif status == custom_pipeline.INVALID_MISSING_PROVIDER:
        _mark(by_target, fields, "Provider", MISSING_PROVIDER, required=True)
    elif status == custom_pipeline.INVALID_HANDICAP:
        _mark(by_target, fields, "Handicap", INVALID_HANDICAP)
    elif status == custom_pipeline.MAPPING_MISSING:
        _mark(by_target, fields, "EventName", MAPPING_MISSING, required=True)
    elif status == custom_pipeline.MARKET_MAPPING_MISSING:
        # Il mercato non è risolvibile: segnala su Mercato e Selezione (le due colonne
        # che la mappatura mercati avrebbe dovuto impostare).
        _mark(by_target, fields, "MarketName", MARKET_MAPPING_MISSING, required=True)
        _mark(by_target, fields, "SelectionName", MARKET_MAPPING_MISSING, required=True)

    # (B) Errori di VALORE per-colonna, indipendenti dallo short-circuit del validator.
    # Solo su valori NON vuoti (un campo vuoto è spiegato dall'estrazione, non "invalido").
    if (str(row.get("BetType", "")).strip()
            and validator.bettype_status(row.get("BetType", "")) != validator.VALID):
        _mark(by_target, fields, "BetType", INVALID_BETTYPE)
    for col in ("Price", "MinPrice", "MaxPrice"):
        v = str(row.get(col, "")).strip()
        if v and validator.price_status(v) != validator.VALID:
            _mark(by_target, fields, col, INVALID_PRICE)
    if (str(row.get("Points", "")).strip()
            and validator.points_status(row.get("Points", "")) != validator.VALID):
        _mark(by_target, fields, "Points", INVALID_POINTS)
    # Limiti incoerenti: solo se ogni prezzo/limite PRESENTE è già una quota valida (altrimenti
    # è già marcato INVALID_PRICE sopra, ed evita `float()` su valori malformati). Non sovrascrive
    # un errore più specifico già presente sulla colonna.
    if all(validator.price_status(str(row.get(c, "")).strip()) == validator.VALID
           for c in ("Price", "MinPrice", "MaxPrice") if str(row.get(c, "")).strip()):
        for col in validator.price_bounds_offenders(row):
            fd = by_target.get(col)
            if fd is None or fd.error in _OK_CODES:
                _mark(by_target, fields, col, INVALID_PRICE_BOUNDS)


def diagnose(defn: CustomParserDef, text: str, *, value_maps_registry: dict = None,
             provider: str = "", mode: str = recognition.DEFAULT_MODE,
             require_price: bool = True, name_mapping_profiles=None,
             market_mapping_profiles=None, id_resolver=None) -> Diagnosis:
    """Diagnostica completa di `text` col parser `defn`.

    Per ogni regola traccia grezzo→transform→value-map→finale e ne classifica
    l'esito; poi esegue la stessa pipeline del runtime (`build_validated_row`) e
    sovrappone gli errori del validator alle colonne giuste. Il verdetto
    (`placeable`) coincide con quello del runtime, così "Prova messaggio" non mente
    mai rispetto a ciò che il bridge scriverebbe."""
    registry = (value_maps_registry if value_maps_registry is not None
                else custom_pipeline._default_registry())
    fields = [_field_diag(rule, text, registry) for rule in defn.rules]
    # Ultima regola vince per target (come `apply_parser`): allinea l'overlay.
    by_target = {}
    for fd in fields:
        by_target[fd.target] = fd

    result = custom_pipeline.build_validated_row(
        defn, text, value_maps_registry=registry, provider=provider,
        mode=mode, require_price=require_price,
        name_mapping_profiles=name_mapping_profiles,
        market_mapping_profiles=market_mapping_profiles, id_resolver=id_resolver)
    _overlay_validator(result, by_target, fields)

    # Il runtime (`signal_router.resolve_row`) scrive SOLO se la riga è piazzabile
    # **e** qualcosa è stato estratto dal messaggio (gate di contenuto
    # `matches_message`, altrimenti `NO_CONTENT_MATCH`). Riflettiamolo nel verdetto,
    # così un parser a soli valori fissi che "validerebbe" non risulta PRONTO quando
    # il bridge in realtà lo scarterebbe (Codex).
    message_error = "" if matches_message(defn, text, mode) else NO_CONTENT_MATCH
    placeable = result.placeable and not message_error
    status = message_error if (message_error and result.placeable) else result.status
    return Diagnosis(placeable=placeable, status=status,
                     fields=fields, message_error=message_error)


@dataclass
class TableRow:
    """Una riga della tabella diagnostica (vista del builder, CP-08b).

    Pensata per essere disegnata 1:1 dalla GUI senza altra logica: la formattazione
    leggibile (delimitatori, valore estratto, motivo) è già risolta qui."""

    target: str
    status: str            # "✅ OK" / "⛔ ERR"
    reason: str            # spiegazione leggibile (vuota se OK)
    start_after: str       # "Inizia dopo" leggibile
    end_before: str        # "Finisce prima" leggibile
    extracted: str         # valore estratto ("grezzo" o "grezzo → finale" se mappato)
    ok: bool = True
    required: bool = True
    banner: bool = False    # True = riga d'avviso a livello messaggio (NO_CONTENT_MATCH)


def _fmt_delim(rule) -> "tuple[str, str]":
    """('Inizia dopo', 'Finisce prima') leggibili: un valore fisso non estrae →
    '(valore fisso)'; vuoto → semantica di default; i newline come «↵» (fine riga)."""
    if rule is not None and rule.is_fixed():
        return ("(valore fisso)", "(valore fisso)")

    def show(v, empty_label):
        v = (v or "").replace("\n", "↵")
        return v if v != "" else empty_label
    start = show(getattr(rule, "start_after", ""), "(dall'inizio)")
    end = show(getattr(rule, "end_before", ""), "(fine riga)")
    return (start, end)


def diagnostic_table(diag: Diagnosis, defn: CustomParserDef) -> "list[TableRow]":
    """Righe della tabella diagnostica per "Prova messaggio" (CP-08b).

    Una riga per colonna del parser, con stato/motivo/delimitatori/valore estratto
    già formattati. Se il gate di contenuto fallisce (`NO_CONTENT_MATCH`) la prima
    riga è un **banner** d'avviso. Logica pura: la GUI la disegna e basta."""
    rules_by_target = {r.target: r for r in defn.rules}
    rows: "list[TableRow]" = []
    if diag.message_error:
        rows.append(TableRow(
            target=diag.message_error, status="⛔ ERR", reason=explain(diag.message_error),
            start_after="", end_before="", extracted="", ok=False, banner=True))
    for fd in diag.fields:
        start, end = _fmt_delim(rules_by_target.get(fd.target))
        extracted = fd.raw if (fd.final == fd.raw or not fd.final) else f"{fd.raw} → {fd.final}"
        rows.append(TableRow(
            target=fd.target, status="✅ OK" if fd.ok else "⛔ ERR",
            reason="" if fd.ok else explain(fd.error),
            start_after=start, end_before=end, extracted=extracted,
            ok=fd.ok, required=fd.required))
    return rows


def format_report(diag: Diagnosis) -> str:
    """Report testuale leggibile della diagnostica (per la GUI / copia negli appunti)."""
    head = "PRONTO ✅" if diag.placeable else f"NON PRONTO ⛔  (status: {diag.status})"
    lines = [head]
    if diag.message_error:
        lines.append(f"• {diag.message_error} — {explain(diag.message_error)}")
    for fd in diag.fields:
        flag = "OK " if fd.ok else "ERR"
        kind = "obbl" if fd.required else "opz "
        chain = f"grezzo={fd.raw!r}"
        if fd.after_transform != fd.raw:
            chain += f" →tr={fd.after_transform!r}"
        if fd.final != fd.after_transform:
            chain += f" →map={fd.final!r}"
        why = explain(fd.error)
        reason = f" — {why}" if (why and not fd.ok) else ""
        lines.append(f"[{flag}] {fd.target} ({kind}): {fd.error}{reason}  |  {chain}")
    return "\n".join(lines)

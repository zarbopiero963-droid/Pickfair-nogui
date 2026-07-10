"""Modello dati del Parser Personalizzato (CP-01).

Il Parser Personalizzato permette all'utente di definire — da GUI, in un passo
successivo — *come* estrarre i campi del contratto CSV XTrader da un messaggio
Telegram, senza dipendere dal parser hardcoded (PR-09). Questo modulo contiene
**solo** il modello dati e la sua persistenza/validazione strutturale:

- `FieldRule`        — una regola per UNA colonna CSV (target).
- `CustomParserDef`  — un parser con nome + elenco di regole.
- (de)serializzazione JSON, validazione strutturale, skeleton di default,
  salvataggio/caricamento in `<cartella utente persistente>/parsers/<nome>.json`
  (riusa `config_store.config_dir()`, non la cartella temporanea dell'EXE).

NON è incluso (scope dei CP successivi):
- il motore di estrazione a runtime (applicare le regole a un messaggio);
- la risoluzione delle value-map / dizionario;
- le trasformazioni configurabili (es. somma-gol → Over (somma).5, CP-05);
- la GUI.

Semantica delle regole (interpretata dal motore runtime, NON qui):
- `start_after` / `end_before`: testo libero (anche emoji/simboli) che delimita
  il valore dentro il messaggio ("Inizia dopo" / "Finisce prima di").
- `fixed_value`: valore costante (es. `Provider=TG_CUSTOM`, `Handicap=0`); se
  presente, la colonna NON viene estratta dal messaggio.
- `value_map`: nome di una value-map (il dizionario diventa selezionabile) per
  tradurre il valore estratto nel valore esatto atteso da XTrader.
- `required`: se True e il valore risulta vuoto → parser "Non pronto" (blocca,
  nessuna riga CSV). Se False e vuoto → colonna CSV vuota (NON blocca).
"""

import dataclasses
import json
import os
from dataclasses import dataclass, field

from . import atomic_io, config_store, recognition, sports, transforms, validators
from .csv_writer import CSV_HEADER

# Versione dello schema del file parser: serve a gestire migrazioni future
# senza rompere i file salvati dagli utenti.
SCHEMA_VERSION = 1

# Le colonne ammesse come `target` di una regola sono esattamente quelle del
# contratto CSV XTrader (fonte unica: csv_writer.CSV_HEADER), così il modello
# non può andare in drift rispetto al contratto.
VALID_TARGETS = tuple(CSV_HEADER)


def _normalize_parser_mode(raw) -> str:
    """Normalizza il campo `mode` letto da JSON (vedi `from_dict`):

    - chiave assente / `null` → `""` = eredita il globale (file legacy pre-feature);
    - `""` esplicito → `""` (eredità scelta dalla GUI);
    - valore valido (`ID_ONLY`/`NAME_ONLY`/`BOTH`) → tenuto;
    - valore MALFORMATO (typo, corrotto) → `NAME_ONLY` (fail-safe: non eredita un
      globale potenzialmente sbagliato; non lascia passare un modo ignoto).
    """
    if raw is None:
        return ""
    s = str(raw).strip()
    if s == "" or s in recognition.VALID_MODES:
        return s
    return recognition.DEFAULT_MODE

# Token booleani riconosciuti nei file JSON scritti/modificati a mano.
_TRUE_TOKENS = {"true", "1", "yes", "si", "sì", "y", "on"}
_FALSE_TOKENS = {"false", "0", "no", "n", "off", ""}


def _as_bool(v) -> bool:
    """Normalizza un valore JSON in bool senza la trappola di `bool(v)` (che
    tratterebbe la stringa "false"/"0" come True). Accetta bool, numeri e le
    rappresentazioni testuali comuni; su un valore ambiguo solleva ValueError
    invece di indovinare (un parser è safety-critical)."""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in _TRUE_TOKENS:
            return True
        if s in _FALSE_TOKENS:
            return False
    raise ValueError(f"valore booleano non riconosciuto per 'required': {v!r}")


@dataclass
class FieldRule:
    """Regola di estrazione per UNA colonna del CSV XTrader."""

    target: str                 # colonna CSV di destinazione (∈ CSV_HEADER)
    start_after: str = ""       # "Inizia dopo": delimitatore sinistro (testo/emoji)
    end_before: str = ""        # "Finisce prima di": delimitatore destro (testo/emoji)
    fixed_value: str = ""       # valore costante (alternativo all'estrazione)
    transform: str = ""         # nome trasformazione (CP-05), applicata dopo l'estrazione
    value_map: str = ""         # nome value-map per tradurre il valore (opz.)
    required: bool = False      # obbligatorio: se vuoto → parser "Non pronto"

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "FieldRule":
        """Crea una regola da dict tollerando chiavi mancanti (default) ed extra
        (ignorate: forward-compatibilità con schema più recenti)."""
        if not isinstance(data, dict):
            raise ValueError(f"regola non è un oggetto JSON: {type(data).__name__}")
        known = {f.name for f in dataclasses.fields(cls)}
        kwargs = {k: data[k] for k in known if k in data}
        if "target" not in kwargs:
            raise ValueError("FieldRule senza 'target'")
        rule = cls(target=str(kwargs.pop("target")))
        for k, v in kwargs.items():
            if k == "required":
                setattr(rule, k, _as_bool(v))
            else:
                setattr(rule, k, "" if v is None else str(v))
        return rule

    def is_fixed(self) -> bool:
        return self.fixed_value != ""

    def has_extraction(self) -> bool:
        return self.start_after != "" or self.end_before != ""


@dataclass
class MultiRowRule:
    """Una riga MultiMarket/MultiSelection (#192): valori che SOVRASCRIVONO i campi
    mercato/selezione della riga base. Un campo vuoto EREDITA dalla riga base.

    Estrazione per-riga (#325): una regola **MultiSelection** con `selection_name` **vuoto** e
    `start_after`/`end_before` valorizzati è **dinamica** — dalla regione fra i delimitatori si
    estrae la LISTA dei risultati esatti («N - N», normalizzati) e si genera una riga per ciascuno
    (`custom_pipeline._selection_rows`/`custom_parser_engine.extract_scores`). Con `selection_name`
    fisso (o senza delimitatori) i valori restano **fissi** (override diretto, percorso #192).
    `enabled=False` esclude la riga dalla generazione (fail-closed sui valori malformati)."""

    start_after: str = ""
    end_before: str = ""
    market_type: str = ""
    market_name: str = ""
    selection_name: str = ""
    price: str = ""
    min_price: str = ""
    max_price: str = ""
    bet_type: str = ""
    points: str = ""
    handicap: str = ""
    enabled: bool = True

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "MultiRowRule":
        """Crea una riga multi da dict tollerando chiavi mancanti (default) ed extra
        (ignorate). `enabled` malformato → ``False`` (fail-closed: nessun bet ambiguo)."""
        if not isinstance(data, dict):
            raise ValueError(f"riga multi non è un oggetto JSON: {type(data).__name__}")
        known = {f.name for f in dataclasses.fields(cls)}
        rule = cls()
        for k, v in data.items():
            if k not in known:
                continue
            if k == "enabled":
                try:
                    rule.enabled = _as_bool(v)
                except ValueError:
                    rule.enabled = False
            else:
                setattr(rule, k, "" if v is None else str(v))
        return rule


# Modi di combinazione delle condizioni di gate (PR-1): "all" = TUTTE (E), "any" = una qualsiasi (O).
CONDITION_MODES = ("all", "any")


def _normalize_conditions_mode(raw) -> str:
    """Normalizza il modo delle condizioni letto da JSON: solo 'all'/'any'. Qualsiasi altro
    valore (assente, null, typo, corrotto) → 'all' — fail-closed sul gate PIÙ restrittivo (E),
    così un file manomesso non allarga per errore l'accettazione dei messaggi."""
    s = str(raw or "").strip().lower()
    return s if s in CONDITION_MODES else "all"


@dataclass
class Condition:
    """Condizione di gate (PR-1): il parser scatta SOLO se il messaggio la soddisfa.

    - `text`: sottostringa cercata nel messaggio (match case-insensitive e tollerante agli
      spazi via `dizionario.normalize`, applicato a ENTRAMBI i lati);
    - `negate`: `False` = «contiene» (soddisfatta se il testo è PRESENTE); `True` = «NON
      contiene» (soddisfatta se il testo è ASSENTE).

    Per il GATE basta la presenza del testo (sottostringa): niente delimitatori
    `start_after`/`end_before` (quelli ESTRAGGONO un valore; qui si VALIDA soltanto)."""

    text: str = ""
    negate: bool = False

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Condition":
        """Da dict, tollerante a chiavi mancanti/extra. `negate` malformato → `False`
        (fail-closed sul default «contiene»)."""
        if not isinstance(data, dict):
            raise ValueError(f"condizione non è un oggetto JSON: {type(data).__name__}")
        cond = cls()
        raw_text = data.get("text", "")
        cond.text = "" if raw_text is None else str(raw_text)
        if "negate" in data:
            try:
                cond.negate = _as_bool(data["negate"])
            except ValueError:
                cond.negate = False
        return cond


@dataclass
class CustomParserDef:
    """Definizione di un Parser Personalizzato: nome + elenco di regole."""

    name: str
    description: str = ""
    version: int = SCHEMA_VERSION
    # Modalità di riconoscimento del parser (per-parser): decide quali colonne servono
    # per riconoscere il segnale (ID vs Nomi vs Both) e guida l'auto-obbligatorietà nel
    # builder. Default `NAME_ONLY` per costruzione diretta/template (skeleton, example):
    # un parser nuovo porta una modalità esplicita. Il sentinella `""` (= eredita la
    # modalità globale `recognition_mode`) è prodotto SOLO da `from_dict` per i file
    # salvati PRIMA di questa feature (campo `mode` assente), per retro-compatibilità.
    mode: str = recognition.DEFAULT_MODE
    rules: "list[FieldRule]" = field(default_factory=list)
    # Mappatura nomi squadra (name_mapping_store): profili selezionati per tradurre
    # l'EventName provider → nome Betfair/XTrader. Vuoto = nessuna mappatura (EventName
    # invariato, retro-compatibile). `team_separator` è il separatore casa/trasferta
    # nei messaggi del canale (testo libero: "v"/"vs"/"-"/"/"); vuoto = default "v".
    name_mapping_profiles: "list[str]" = field(default_factory=list)
    team_separator: str = ""
    # Mappatura mercati a frase (market_mapping_store, FASE 2): profili selezionati per
    # tradurre una frase-mercato del provider ("goal prima di 70") nel Mercato/Selezione
    # XTrader canonici. Vuoto = nessuna mappatura mercati (colonne MarketName/SelectionName
    # restano quelle delle regole, retro-compatibile). Vedi docs/audit/mercati_mapping_design.md.
    market_mapping_profiles: "list[str]" = field(default_factory=list)
    # Sport del parser (PR-P9): uno fra `sports.SPORTS` (Calcio/Tennis/Basket/Rugby
    # Union) oppure `""` = non specificato (parser **agnostico**, retro-compatibile con
    # i file salvati prima di PR-P9). Lo sport non cambia le colonne CSV (sempre generiche)
    # ma — nelle PR successive — restringe la risoluzione degli ID Betfair all'event_type_id
    # corretto. Il parser per-profilo cambia con il profilo (active_parser nello snapshot).
    sport: str = ""
    # Output multi-riga (#192): un solo messaggio → più righe CSV. MultiMarket = più mercati
    # diversi della stessa partita; MultiSelection = più selezioni dello stesso mercato. Vuoti/
    # disattivati = comportamento single-row invariato (retro-compatibile con i file pre-#192).
    multi_market_enabled: bool = False
    multi_selection_enabled: bool = False
    multi_markets: "list[MultiRowRule]" = field(default_factory=list)
    multi_selections: "list[MultiRowRule]" = field(default_factory=list)
    # Condizioni di gate (PR-1): il parser scatta SOLO se il messaggio le soddisfa
    # (contiene / NON contiene ⟨testo⟩). `conditions_mode`: "all" = TUTTE (E), "any" = una
    # qualsiasi (O). Vuote = nessun gate aggiuntivo (retro-compatibile coi file pre-feature).
    # Il gate è valutato in `custom_parser_engine.matches_message` (case/space-insensitive).
    conditions: "list[Condition]" = field(default_factory=list)
    conditions_mode: str = "all"

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "mode": self.mode,
            "sport": self.sport,
            "name_mapping_profiles": list(self.name_mapping_profiles),
            "team_separator": self.team_separator,
            "market_mapping_profiles": list(self.market_mapping_profiles),
            "rules": [r.to_dict() for r in self.rules],
            "multi_market_enabled": bool(self.multi_market_enabled),
            "multi_selection_enabled": bool(self.multi_selection_enabled),
            "multi_markets": [r.to_dict() for r in self.multi_markets],
            "multi_selections": [r.to_dict() for r in self.multi_selections],
            "conditions": [c.to_dict() for c in self.conditions],
            "conditions_mode": self.conditions_mode,
        }

    def active_multi_markets(self) -> "list[MultiRowRule]":
        """Righe MultiMarket attive (#192): solo se la modalità è abilitata e la riga è enabled."""
        return [r for r in self.multi_markets if r.enabled] if self.multi_market_enabled else []

    def active_multi_selections(self) -> "list[MultiRowRule]":
        """Righe MultiSelection attive (#192): solo se la modalità è abilitata e la riga è enabled."""
        return [r for r in self.multi_selections if r.enabled] if self.multi_selection_enabled else []

    def active_conditions(self) -> "list[Condition]":
        """Condizioni di gate con testo NON vuoto (PR-1). Le condizioni a testo vuoto sono
        ignorate: una «contiene ''» matcherebbe qualsiasi messaggio (gate no-op pericoloso),
        quindi non deve gattare nulla. Usato da `custom_parser_engine.matches_message`."""
        return [c for c in self.conditions if str(c.text).strip() != ""]

    def is_multi_row(self) -> bool:
        """True se il parser produce **multi-riga** (#192): ha almeno una riga MultiMarket o
        MultiSelection **attiva** (modalità abilitata **e** riga `enabled`). Si basa sulle
        righe ATTIVE, non sul solo toggle: se una modalità è accesa ma senza righe attive,
        `build_validated_rows` ripiega sulla singola riga BASE → resta **single-row** con la
        dedup legacy a hash-messaggio (Codex #281). Quando invece esistono righe attive, il
        parser usa la **deduplica PER-RIGA** anche se ORA ne produce una sola piazzabile: così
        una successiva generazione multi dello stesso messaggio non riscrive la riga già
        scritta → niente **doppia scommessa** (Codex #239/#192)."""
        return bool(self.active_multi_markets() or self.active_multi_selections())

    def event_type_id(self):
        """`event_type_id` Betfair dello sport del parser, o ``None`` se lo sport non è
        specificato/supportato (i chiamati gestiscono ``None`` fail-closed)."""
        return sports.event_type_id_for_sport(self.sport)

    @classmethod
    def from_dict(cls, data: dict) -> "CustomParserDef":
        if not isinstance(data, dict):
            raise ValueError(f"parser JSON non è un oggetto: {type(data).__name__}")
        rules_data = data.get("rules", [])
        if rules_data is None:
            rules_data = []
        if not isinstance(rules_data, list):
            raise ValueError(f"'rules' non è una lista: {type(rules_data).__name__}")
        rules = [FieldRule.from_dict(r) for r in rules_data]
        version = data.get("version", SCHEMA_VERSION)
        try:
            version = int(version)
        except (TypeError, ValueError):
            version = SCHEMA_VERSION
        # Profili di mappatura nomi: lista di stringhe non vuote (chiave assente o
        # malformata → nessun profilo = nessuna mappatura, retro-compatibile).
        raw_profiles = data.get("name_mapping_profiles", [])
        if not isinstance(raw_profiles, list):
            raw_profiles = []
        profiles = [str(p).strip() for p in raw_profiles if str(p or "").strip()]
        # Profili mappatura mercati: stessa pulizia dei nomi (chiave assente/malformata →
        # nessun profilo = nessuna mappatura mercati, retro-compatibile con file pre-FASE 2).
        raw_market = data.get("market_mapping_profiles", [])
        if not isinstance(raw_market, list):
            raw_market = []
        market_profiles = [str(p).strip() for p in raw_market if str(p or "").strip()]
        # Sport (PR-P9): distingui «non specificato» da «valore malformato presente».
        # - chiave assente / null / stringa vuota o di soli spazi → "" (agnostico,
        #   retro-compatibile con i file pre-P9);
        # - stringa valorizzata → strippata e tenuta COM'È (un eventuale typo lo segnala
        #   la validazione, non lo si sceglie a caso);
        # - tipo NON stringa presente (false/0/[]/{}): NON è uno sport → preserva una
        #   rappresentazione NON vuota così `validate_parser_def` fa fail-closed
        #   ("Sport non valido") invece di convertirlo in silenzio in agnostico (Codex).
        raw_sport = data.get("sport", "")
        if raw_sport is None:
            sport = ""
        elif isinstance(raw_sport, str):
            sport = raw_sport.strip()
        else:
            sport = str(raw_sport)
        # Output multi-riga (#192). Migrazione/retro-compatibilità: chiave assente/malformata →
        # flag False e liste vuote (single-row come prima). I valori non-dict nelle liste sono
        # ignorati; `enabled` malformato per riga → fail-closed (riga disabilitata).
        def _flag(key):
            v = data.get(key, False)
            if isinstance(v, bool):
                return v
            try:
                return _as_bool(v)
            except (ValueError, TypeError):
                return False

        def _multi_list(key):
            raw = data.get(key, [])
            if not isinstance(raw, list):
                return []
            return [MultiRowRule.from_dict(r) for r in raw if isinstance(r, dict)]

        # Condizioni di gate (PR-1). Retro-compat: chiave assente/malformata → lista vuota
        # (nessun gate aggiuntivo, come i file pre-feature). Elementi non-dict ignorati.
        def _cond_list(key):
            raw = data.get(key, [])
            if not isinstance(raw, list):
                return []
            return [Condition.from_dict(r) for r in raw if isinstance(r, dict)]

        return cls(
            name=str(data.get("name", "")),
            description=str(data.get("description", "")),
            version=version,
            sport=sport,
            name_mapping_profiles=profiles,
            team_separator=str(data.get("team_separator", "") or ""),
            market_mapping_profiles=market_profiles,
            multi_market_enabled=_flag("multi_market_enabled"),
            multi_selection_enabled=_flag("multi_selection_enabled"),
            multi_markets=_multi_list("multi_markets"),
            multi_selections=_multi_list("multi_selections"),
            conditions=_cond_list("conditions"),
            conditions_mode=_normalize_conditions_mode(data.get("conditions_mode", "all")),
            # Modalità: SOLO la chiave assente/null (file legacy pre-feature) → "" =
            # eredita il globale. Un `mode` ESPLICITO valido è tenuto; `""` esplicito è
            # l'eredità scelta dalla GUI; un valore malformato (typo, file corrotto) →
            # NAME_ONLY (fail-safe, NON eredita un globale magari sbagliato: Codex).
            mode=_normalize_parser_mode(data.get("mode", None)),
            rules=rules,
        )

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)

    @classmethod
    def from_json(cls, text: str) -> "CustomParserDef":
        return cls.from_dict(json.loads(text))

    def required_targets(self) -> list:
        """Colonne marcate obbligatorie: se a runtime restano vuote il parser
        è "Non pronto" e non si scrive il CSV."""
        return [r.target for r in self.rules if r.required]

    def price_required(self) -> bool:
        """True se la colonna `Price` è marcata obbligatoria nel parser.

        È l'**unico comando della quota**: se True il segnale deve avere una
        quota valida (`>1.0`) — gate `require_price` del validator + "Non pronto"
        se `Price` resta vuoto; se False la quota è opzionale (CSV con `Price`
        vuoto ammesso, la quota la mette poi l'azione XTrader). Sostituisce il
        vecchio interruttore globale `require_price`."""
        return "Price" in self.required_targets()


def validate_parser_def(defn: CustomParserDef) -> list:
    """Validazione *strutturale* del modello. Ritorna la lista degli errori
    (vuota = valido). NON applica le regole a un messaggio (è il runtime, CP
    successivi)."""
    errors = []

    if not defn.name or not str(defn.name).strip():
        errors.append("Il parser deve avere un nome non vuoto.")
    elif str(defn.name) != str(defn.name).strip():
        # Spazi iniziali/finali rendono incoerenti filename (normalizzato) e
        # selezione (strippata): il parser non si ricaricherebbe (fallback muto).
        errors.append("Il nome non deve avere spazi iniziali o finali.")

    if not isinstance(defn.version, int) or defn.version < 1:
        errors.append(f"Versione schema non valida: {defn.version!r} (atteso intero >= 1).")

    if not defn.rules:
        errors.append("Il parser deve avere almeno una regola.")

    # Sport (PR-P9): "" = non specificato (agnostico, ammesso). Se valorizzato deve essere
    # uno sport supportato: un valore ignoto (typo, file manomesso) NON deve passare,
    # altrimenti la risoluzione ID Betfair userebbe l'event_type_id sbagliato (o nessuno).
    if defn.sport and not sports.is_supported_sport(defn.sport):
        errors.append(
            f"Sport non valido: {defn.sport!r}; ammessi {', '.join(sports.SPORTS)} "
            "(oppure vuoto = non specificato)."
        )

    seen_targets = set()
    for i, rule in enumerate(defn.rules):
        where = f"regola #{i + 1} (target={rule.target!r})"
        if rule.target not in VALID_TARGETS:
            errors.append(
                f"{where}: colonna non valida; ammesse solo {', '.join(VALID_TARGETS)}."
            )
        elif rule.target in seen_targets:
            # Due regole sulla stessa colonna sarebbero ambigue (quale vince?).
            errors.append(f"{where}: colonna duplicata, ogni colonna una sola regola.")
        else:
            seen_targets.add(rule.target)

        # Costante ed estrazione insieme sono contraddittorie.
        if rule.is_fixed() and rule.has_extraction():
            errors.append(
                f"{where}: ha sia 'fixed_value' sia 'start_after'/'end_before' "
                "(scegline uno: valore costante OPPURE estrazione)."
            )

        # Trasformazione (CP-05) deve essere una nota.
        if rule.transform and not transforms.has_transform(rule.transform):
            errors.append(
                f"{where}: trasformazione sconosciuta {rule.transform!r}; "
                f"ammesse: {', '.join(transforms.available_transforms())}."
            )

    # Condizioni di gate (PR-1): modo valido e nessuna condizione a testo vuoto (una
    # «contiene ''» sarebbe un gate no-op che matcha tutto — errore di configurazione).
    if defn.conditions_mode not in CONDITION_MODES:
        errors.append(
            f"Modo condizioni non valido: {defn.conditions_mode!r}; ammessi {', '.join(CONDITION_MODES)}."
        )
    for i, cond in enumerate(defn.conditions):
        if str(cond.text).strip() == "":
            errors.append(
                f"Condizione #{i + 1}: testo vuoto (ogni condizione deve avere un testo da cercare)."
            )

    return errors


def is_valid(defn: CustomParserDef) -> bool:
    return not validate_parser_def(defn)


def skeleton(name: str = "Nuovo parser") -> CustomParserDef:
    """Scheletro di partenza valido: Provider costante + le colonne-nome usate
    dal riconoscimento NAME_ONLY (EventName/MarketName/SelectionName/BetType) e
    il Price obbligatorio. L'utente poi imposta start_after/end_before/value_map.
    """
    return CustomParserDef(
        name=name,
        description="Scheletro di partenza: personalizza delimitatori e value-map.",
        version=SCHEMA_VERSION,
        rules=[
            FieldRule(target="Provider", fixed_value="TG_CUSTOM"),
            FieldRule(target="EventName", required=True),
            # NAME_ONLY (recognition) richiede MarketType: obbligatorio nello
            # skeleton così, una volta configurato, la riga è riconoscibile.
            FieldRule(target="MarketType", required=True, value_map="markettype"),
            FieldRule(target="MarketName", value_map="marketname"),  # etichetta (opz.)
            FieldRule(target="SelectionName", required=True, value_map="selectionname"),
            FieldRule(target="Price", required=True),
            FieldRule(target="BetType", required=True, value_map="bettype"),
            FieldRule(target="Handicap", fixed_value="0"),
        ],
    )


# ── Persistenza: <cartella utente persistente>/parsers/<nome>.json ─────────

def default_parsers_dir() -> str:
    """Cartella persistente dei parser utente: `<config_dir>/parsers/`.

    Riusa `config_store.config_dir()` (`%APPDATA%\\XTraderBridge` su Windows,
    `~/.config/XTraderBridge` altrove): è una posizione **scrivibile e
    persistente**, che sopravvive a riavvii/aggiornamenti dell'EXE. NON usiamo
    `sys._MEIPASS` (la cartella di estrazione PyInstaller è temporanea e di sola
    lettura): lì stanno solo i dati bundled read-only come il dizionario."""
    return os.path.join(config_store.config_dir(), "parsers")


def _safe_filename(name: str) -> str:
    """Nome file sicuro dal nome del parser (nucleo condiviso `validators`): solo
    alfanumerici, '-', '_' e spazi (poi spazi → '_'); path traversal e NOMI DEVICE
    RISERVATI Windows gestiti. Fallback su ``"parser"`` se il nome si pulisce a vuoto,
    così un parser senza nome valido ottiene comunque un file (diverso da
    `profile_store`, che invece RIFIUTA il nome vuoto)."""
    return validators.safe_filename_core(name) or "parser"


def parser_path(name: str, dir_path: str = None) -> str:
    base = dir_path if dir_path is not None else default_parsers_dir()
    return os.path.join(base, _safe_filename(name) + ".json")


def save_parser(defn: CustomParserDef, dir_path: str = None) -> str:
    """Salva il parser in `<dir>/<nome>.json`. Rifiuta i parser non validi per
    non persistere una definizione che bloccherebbe/ corromperebbe il CSV."""
    errors = validate_parser_def(defn)
    if errors:
        raise ValueError("Parser non valido, non salvato:\n- " + "\n- ".join(errors))
    base = dir_path if dir_path is not None else default_parsers_dir()
    os.makedirs(base, exist_ok=True)
    path = parser_path(defn.name, base)
    # Due nomi diversi che si sanitizzano allo stesso file (es. "A/B" e "AB")
    # NON devono sovrascriversi in silenzio: si perderebbero le regole del primo
    # parser. Sovrascrivere è consentito solo se è lo *stesso* parser (update).
    if os.path.exists(path):
        try:
            existing_name = load_parser(path).name
        except (OSError, ValueError, json.JSONDecodeError):
            existing_name = None
        if existing_name is not None and existing_name != defn.name:
            raise ValueError(
                f"Il nome {defn.name!r} collide con il parser {existing_name!r} "
                f"(stesso file {os.path.basename(path)}): scegli un nome diverso."
            )
    # Scrittura atomica: file temporaneo nella stessa cartella + fsync + rename,
    # così un crash a metà scrittura non lascia un JSON parziale/corrotto (il
    # file esistente resta intatto finché il rename non riesce).
    payload = defn.to_json()
    atomic_io.atomic_write_text(path, payload, prefix=".parser_", suffix=".json")
    return path


def load_parser(path: str) -> CustomParserDef:
    """Carica un parser da file JSON."""
    with open(path, encoding="utf-8") as f:
        return CustomParserDef.from_json(f.read())


def list_parser_files(dir_path: str = None) -> list:
    """Elenca i path dei file parser (`*.json`) presenti nella cartella.

    Esclude i file che iniziano con `.` (es. il temporaneo `.parser_*.json`
    della scrittura atomica, eventualmente rimasto dopo un crash prima di
    `os.replace`): non sono parser reali e non devono apparire come "fantasmi".
    `_safe_filename()` non produce mai nomi che iniziano con `.`."""
    base = dir_path if dir_path is not None else default_parsers_dir()
    if not os.path.isdir(base):
        return []
    return sorted(
        os.path.join(base, f)
        for f in os.listdir(base)
        if f.endswith(".json") and not f.startswith(".")
    )


def rename_mapping_profile_in_files(old: str, new: str, dir_path: str = None) -> tuple:
    """Aggiorna i riferimenti a un profilo di mappatura **rinominato** (``old`` → ``new``)
    in tutti i parser salvati: i parser che hanno ``old`` in ``name_mapping_profiles``
    vengono riscritti con ``new`` nella **stessa posizione** (l'ordine conta per la
    precedenza in `name_mapping_store.resolve_team`), senza duplicati.

    Ritorna la coppia ``(updated, failed)``: nomi dei parser aggiornati con successo e
    nomi di quelli che referenziavano ``old`` ma **non si sono potuti riscrivere**
    (cartella in sola lettura, collisione di nome file, I/O transitorio). I `failed` NON
    vengono nascosti: il chiamante deve segnalarli, perché restano col vecchio nome mentre
    la config ha già il nuovo → quei segnali andrebbero in ``MAPPING_MISSING`` (Codex).

    Serve perché il nome del profilo è memorizzato **per stringa** nel JSON del parser e
    risolto esatto dal `signal_router`. I file non caricabili/non validi vengono saltati
    (non referenziano in modo affidabile ``old``); i parser che non usano ``old`` non
    vengono toccati."""
    return _rename_profile_in_files("name_mapping_profiles", old, new, dir_path)


def rename_market_mapping_profile_in_files(old: str, new: str, dir_path: str = None) -> tuple:
    """Come :func:`rename_mapping_profile_in_files` ma per i profili **mercati**
    (``market_mapping_profiles``): rinominare un profilo mercati nel Dizionario deve
    aggiornare i parser che lo selezionano, altrimenti resterebbero a chiedere un profilo
    inesistente → ``MARKET_MAPPING_MISSING`` (segnali scartati). Stessa semantica di
    ritorno ``(updated, failed)``."""
    return _rename_profile_in_files("market_mapping_profiles", old, new, dir_path)


def _rename_profile_in_files(attr: str, old: str, new: str, dir_path: str = None) -> tuple:
    """Nucleo condiviso di rinomina di un profilo (``attr`` = ``name_mapping_profiles`` o
    ``market_mapping_profiles``) nei file dei parser. Vedi i wrapper pubblici per la
    semantica. Preserva ordine e unicità; ritorna ``(updated, failed)``."""
    o = str(old or "").strip()
    n = str(new or "").strip()
    if not o or not n or o == n:
        return [], []
    updated, failed = [], []
    for path in list_parser_files(dir_path):
        try:
            defn = load_parser(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        # default [] per robustezza sullo scan dell'intera cartella: un parser vecchio/
        # parziale privo dell'attributo non deve abortire la sincronizzazione (Sourcery).
        profiles = getattr(defn, attr, []) or []
        if o not in profiles:
            continue
        seen, newlist = set(), []
        for p in profiles:
            p2 = n if p == o else p
            if p2 not in seen:
                seen.add(p2)
                newlist.append(p2)
        setattr(defn, attr, newlist)
        try:
            save_parser(defn, dir_path)
            updated.append(defn.name)
        except (OSError, ValueError):
            failed.append(defn.name)
    return updated, failed


def parsers_using_mapping_profile(name: str, dir_path: str = None) -> list:
    """Nomi dei parser salvati che referenziano il profilo di mappatura ``name`` in
    ``name_mapping_profiles``. Serve ad **avvisare** prima di eliminare un profilo in
    uso: cancellarlo lascerebbe quei parser a chiedere un profilo inesistente → ogni
    segnale mappato diventa ``MAPPING_MISSING`` (scartato). Best-effort: i file non
    caricabili vengono saltati."""
    return _parsers_using_profile("name_mapping_profiles", name, dir_path)


def parsers_using_market_mapping_profile(name: str, dir_path: str = None) -> list:
    """Come :func:`parsers_using_mapping_profile` ma per i profili **mercati**
    (``market_mapping_profiles``): avvisa prima di eliminare un profilo mercati ancora
    selezionato in qualche parser (→ ``MARKET_MAPPING_MISSING``)."""
    return _parsers_using_profile("market_mapping_profiles", name, dir_path)


def _parsers_using_profile(attr: str, name: str, dir_path: str = None) -> list:
    """Nucleo condiviso: nomi dei parser salvati che referenziano ``name`` nell'attributo
    ``attr`` (``name_mapping_profiles`` o ``market_mapping_profiles``). Best-effort."""
    n = str(name or "").strip()
    if not n:
        return []
    out = []
    for path in list_parser_files(dir_path):
        try:
            defn = load_parser(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
        if n in (getattr(defn, attr, []) or []):    # default [] per robustezza (Sourcery)
            out.append(defn.name)
    return out


def delete_parser(name: str, dir_path: str = None) -> bool:
    """Elimina il file di un parser salvato, risolvendo il path **per nome** con
    `_safe_filename` (anti path-traversal: un `name` con `..`/separatori non può
    puntare fuori dalla cartella parser).

    Contratto: ritorna `True` se un file è stato rimosso, `False` se non esisteva
    (idempotente). La **non-esistenza** è il solo caso reso silenzioso; ogni altro
    `OSError` (permessi, filesystem in sola lettura, IO transitorio) **si propaga**
    al chiamante invece di essere nascosto come un finto "non trovato" — così un
    problema reale non passa inosservato. Il chiamante GUI lo gestisce e lo mostra
    (`custom_parser_gui._delete_selected`), come già fa per salva/carica."""
    path = parser_path(name, dir_path)
    try:
        os.remove(path)
        return True
    except FileNotFoundError:
        return False


"""Modello dati del Parser Personalizzato (portato: CP-01) — core P1.

Contiene **solo** il modello e la sua validazione strutturale:

- ``FieldRule``        — una regola per UN campo-target.
- ``CustomParserDef``  — un parser con nome + elenco di regole.
- (de)serializzazione dict/JSON tollerante, validazione strutturale, skeleton.

Adattamento per Pickfair rispetto al bridge:

- i **target** non sono più le 14 colonne del CSV XTrader ma il set essenziale del
  segnale pre-catalogo (``PARSER_TARGETS``): EventName, mercato/selezione a frase,
  quota, lato, handicap, provider. La risoluzione a ``MarketId``/``SelectionId`` è
  uno stadio successivo (catalogo Betfair), non qui.
- **non** inclusi in questo stadio (decomposti nelle PR successive): persistenza su
  file/SQLite, modalità di riconoscimento, output multi-riga, profili di mappatura
  nomi/mercati, sport.

Semantica delle regole (interpretata dal motore in :mod:`~services.custom_parser.engine`):

- ``start_after`` / ``end_before``: testo libero (anche emoji/simboli) che delimita
  il valore dentro il messaggio ("Inizia dopo" / "Finisce prima di").
- ``fixed_value``: valore costante (es. ``Provider=TG_CUSTOM``, ``Handicap=0``); se
  presente, il campo NON viene estratto dal messaggio.
- ``transform``: nome trasformazione applicata dopo l'estrazione (es. ``score_to_over``).
- ``value_map``: nome value-map per tradurre il valore estratto (es. ``bettype``).
- ``required``: se True e il valore risulta vuoto → parser "Non pronto" (blocca, nessun
  segnale). Se False e vuoto → campo vuoto (NON blocca).

Modulo **puro**: nessun I/O, nessuna dipendenza da broker/DB/runtime.
"""
from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field

from services.custom_parser import transforms

# Versione dello schema del parser: serve a gestire migrazioni future senza rompere
# le definizioni già salvate.
SCHEMA_VERSION = 1

# Campi-target ammessi come `target` di una regola: il set essenziale del segnale
# pre-catalogo di Pickfair. Fonte unica, così il modello non va in drift.
PARSER_TARGETS = (
    "Provider",
    "EventName",
    "MarketType",
    "MarketName",
    "SelectionName",
    "Price",
    "BetType",
    "Handicap",
)

# Token booleani riconosciuti nei dict/JSON scritti/modificati a mano.
_TRUE_TOKENS = {"true", "1", "yes", "si", "sì", "y", "on"}
_FALSE_TOKENS = {"false", "0", "no", "n", "off", ""}


def _as_bool(v) -> bool:
    """Normalizza un valore in bool senza la trappola di ``bool(v)`` (che tratterebbe
    la stringa ``"false"``/``"0"`` come True). Accetta bool, numeri e le
    rappresentazioni testuali comuni; su un valore ambiguo solleva ``ValueError``
    invece di indovinare (un parser è safety-critical).
    """
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
    """Regola di estrazione per UN campo-target del segnale."""

    target: str                 # campo di destinazione (∈ PARSER_TARGETS)
    start_after: str = ""       # "Inizia dopo": delimitatore sinistro (testo/emoji)
    end_before: str = ""        # "Finisce prima di": delimitatore destro (testo/emoji)
    fixed_value: str = ""       # valore costante (alternativo all'estrazione)
    transform: str = ""         # nome trasformazione, applicata dopo l'estrazione
    value_map: str = ""         # nome value-map per tradurre il valore (opz.)
    required: bool = False      # obbligatorio: se vuoto → parser "Non pronto"

    def to_dict(self) -> dict:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "FieldRule":
        """Crea una regola da dict tollerando chiavi mancanti (default) ed extra
        (ignorate: forward-compatibilità con schema più recenti)."""
        if not isinstance(data, dict):
            raise ValueError(f"regola non è un oggetto: {type(data).__name__}")
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
class CustomParserDef:
    """Definizione di un Parser Personalizzato: nome + elenco di regole."""

    name: str
    description: str = ""
    version: int = SCHEMA_VERSION
    rules: "list[FieldRule]" = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "rules": [r.to_dict() for r in self.rules],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CustomParserDef":
        if not isinstance(data, dict):
            raise ValueError(f"parser non è un oggetto: {type(data).__name__}")
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
        return cls(
            name=str(data.get("name", "")),
            description=str(data.get("description", "")),
            version=version,
            rules=rules,
        )

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=indent)

    @classmethod
    def from_json(cls, text: str) -> "CustomParserDef":
        return cls.from_dict(json.loads(text))

    def required_targets(self) -> list:
        """Campi marcati obbligatori: se a runtime restano vuoti il parser è "Non
        pronto" e non produce alcun segnale."""
        return [r.target for r in self.rules if r.required]

    def price_required(self) -> bool:
        """True se il campo ``Price`` è marcato obbligatorio nel parser.

        È l'unico comando della quota: se True il segnale deve avere una quota valida;
        se False la quota è opzionale.
        """
        return "Price" in self.required_targets()


def validate_parser_def(defn: CustomParserDef) -> list:
    """Validazione *strutturale* del modello. Ritorna la lista degli errori (vuota =
    valido). NON applica le regole a un messaggio (è il motore).
    """
    errors = []

    if not defn.name or not str(defn.name).strip():
        errors.append("Il parser deve avere un nome non vuoto.")
    elif str(defn.name) != str(defn.name).strip():
        # Spazi iniziali/finali rendono incoerenti nome salvato e selezione.
        errors.append("Il nome non deve avere spazi iniziali o finali.")

    if not isinstance(defn.version, int) or defn.version < 1:
        errors.append(f"Versione schema non valida: {defn.version!r} (atteso intero >= 1).")

    if not defn.rules:
        errors.append("Il parser deve avere almeno una regola.")

    seen_targets = set()
    for i, rule in enumerate(defn.rules):
        where = f"regola #{i + 1} (target={rule.target!r})"
        if rule.target not in PARSER_TARGETS:
            errors.append(
                f"{where}: campo non valido; ammessi solo {', '.join(PARSER_TARGETS)}."
            )
        elif rule.target in seen_targets:
            # Due regole sullo stesso campo sarebbero ambigue (quale vince?).
            errors.append(f"{where}: campo duplicato, ogni campo una sola regola.")
        else:
            seen_targets.add(rule.target)

        # Costante ed estrazione insieme sono contraddittorie.
        if rule.is_fixed() and rule.has_extraction():
            errors.append(
                f"{where}: ha sia 'fixed_value' sia 'start_after'/'end_before' "
                "(scegline uno: valore costante OPPURE estrazione)."
            )

        # Trasformazione deve essere nota.
        if rule.transform and not transforms.has_transform(rule.transform):
            errors.append(
                f"{where}: trasformazione sconosciuta {rule.transform!r}; "
                f"ammesse: {', '.join(transforms.available_transforms())}."
            )

    return errors


def is_valid(defn: CustomParserDef) -> bool:
    return not validate_parser_def(defn)


def skeleton(name: str = "Nuovo parser") -> CustomParserDef:
    """Scheletro di partenza valido: Provider costante + i campi essenziali del
    segnale. L'utente poi imposta ``start_after``/``end_before`` (e le value-map dei
    nomi mercato/selezione, che arriveranno con lo stadio catalogo/Provider).

    Usa solo la value-map ``bettype`` (esistente in questo stadio); mercato/selezione
    restano estrazione pura finché non c'è il catalogo.
    """
    return CustomParserDef(
        name=name,
        description="Scheletro di partenza: personalizza delimitatori e value-map.",
        version=SCHEMA_VERSION,
        rules=[
            FieldRule(target="Provider", fixed_value="TG_CUSTOM"),
            FieldRule(target="EventName", required=True),
            FieldRule(target="MarketType", required=True),
            FieldRule(target="MarketName"),          # etichetta (opz.)
            FieldRule(target="SelectionName", required=True),
            FieldRule(target="Price", required=True),
            FieldRule(target="BetType", required=True, value_map="bettype"),
            FieldRule(target="Handicap", fixed_value="0"),
        ],
    )

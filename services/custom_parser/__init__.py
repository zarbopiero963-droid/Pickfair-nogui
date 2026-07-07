"""Parser Personalizzato — core di estrazione PURO (portato da xtrader-bridge).

Pacchetto **puro** (zero broker/DB/runtime/GUI): definisce *come* estrarre i campi
di un segnale da un messaggio Telegram tramite regole a delimitatori configurabili,
senza dipendere da un parser hardcoded. È il primo stadio della catena di copy:

    messaggio Telegram
      → parser (questo pacchetto: estrae EventName, mercato/selezione, prezzo, lato)
      → [mappa alias→canonico del Provider]        (stadio successivo, non incluso)
      → [lookup catalogo Betfair → MarketId/SelectionId]   (stadio successivo)
      → SegnaleRisolto → (placement: lo fa già Pickfair)

Scope di questo pacchetto (P1, core estrazione):

- :mod:`~services.custom_parser.model`      — modello dati (``FieldRule`` / ``CustomParserDef``)
  + validazione strutturale, sul set di target di Pickfair (``PARSER_TARGETS``).
- :mod:`~services.custom_parser.engine`      — motore di estrazione runtime
  (``extract_value`` con delimitatori tolleranti agli spazi, ``apply_parser`` →
  ``ExtractionResult``).
- :mod:`~services.custom_parser.transforms`  — trasformazioni configurabili (``score_to_over``).
- :mod:`~services.custom_parser.value_maps`  — value-map alias→valore canonico
  (built-in ``bettype`` → BACK/LAY).

**Non** incluso (stadi successivi, decomposti): persistenza (JSON vs SQLite),
modalità di riconoscimento, output multi-riga, profili di mappatura nomi/mercati,
routing per-chat/Provider, sync catalogo Betfair, GUI.

Fail-closed ovunque: delimitatore mancante, value-map sconosciuta, trasformazione
ignota, valore malformato ⇒ campo vuoto ⇒ (se obbligatorio) parser "Non pronto",
mai un valore inventato sul percorso d'ordine.
"""
from services.custom_parser.engine import (
    ExtractionResult,
    apply_parser,
    extract_between,
    extract_scores,
    extract_value,
    extract_value_traced,
)
from services.custom_parser.model import (
    PARSER_TARGETS,
    CustomParserDef,
    FieldRule,
    is_valid,
    skeleton,
    validate_parser_def,
)

__all__ = [
    "PARSER_TARGETS",
    "CustomParserDef",
    "FieldRule",
    "is_valid",
    "skeleton",
    "validate_parser_def",
    "ExtractionResult",
    "apply_parser",
    "extract_between",
    "extract_scores",
    "extract_value",
    "extract_value_traced",
]

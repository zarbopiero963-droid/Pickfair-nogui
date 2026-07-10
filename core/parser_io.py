"""CP-08: import/export dei Parser Personalizzati + parser d'esempio.

- `export_parser(defn, dest)`: salva un parser (validato) in un percorso scelto
  dall'utente, per condividerlo.
- `import_parser(src, dir)`: legge un file parser da un percorso qualsiasi, lo
  valida e lo salva nella cartella dei parser (riusando `save_parser`, con
  controllo collisione e scrittura atomica di CP-01).
- `example_parser()`: un parser realistico e VALIDO, punto di partenza
  documentato; con `fixture_message()` dimostra l'intera catena
  estrazione→value-map→validazione su un messaggio tipico.

Import/export non scrivono il CSV e non toccano il runtime: solo file dei parser.
Un file corrotto o invalido → `ValueError` (niente salvataggi parziali).
"""

import os

from . import atomic_io, custom_parser
from .custom_parser import CustomParserDef, FieldRule


def export_parser(defn: CustomParserDef, dest_path: str) -> str:
    """Scrive il parser (validato) come JSON in `dest_path`. Solleva ValueError
    se la definizione non è valida (non si esporta un parser rotto).

    Scrittura atomica (tempfile + fsync + os.replace, come `save_parser`): un
    export interrotto non lascia un file parziale al posto di uno esistente."""
    errors = custom_parser.validate_parser_def(defn)
    if errors:
        raise ValueError("Parser non valido, non esportato:\n- " + "\n- ".join(errors))
    payload = defn.to_json()
    atomic_io.atomic_write_text(dest_path, payload, prefix=".export_", suffix=".json")
    return dest_path


def import_parser(src_path: str, dir_path: str = None, overwrite: bool = False) -> CustomParserDef:
    """Importa un parser da `src_path`: legge, valida e lo salva nella cartella
    dei parser (con collisione/atomicità gestite da `save_parser`). Ritorna la
    definizione importata. Solleva ValueError se il file è corrotto o invalido.

    Se esiste già un parser con lo stesso nome, NON lo sovrascrive a meno che
    `overwrite=True`: importare un parser condiviso chiamato come uno locale/attivo
    cambierebbe di nascosto le regole che il live caricherà."""
    defn = custom_parser.load_parser(src_path)   # OSError/ValueError su file rotto
    errors = custom_parser.validate_parser_def(defn)
    if errors:
        raise ValueError("Parser non valido, non importato:\n- " + "\n- ".join(errors))
    target = custom_parser.parser_path(defn.name, dir_path)
    if not overwrite and os.path.exists(target):
        raise ValueError(
            f"Esiste già un parser di nome {defn.name!r}: importazione annullata "
            "(usa overwrite=True per sostituirlo)."
        )
    custom_parser.save_parser(defn, dir_path)
    return defn


def example_parser() -> CustomParserDef:
    """Parser d'esempio realistico (valido) per un messaggio tipo:

        Match: Inter v Milan
        Esito: GG
        Quota: 1,85
        Lato: BACK

    Usa value-map `selectionname`/`bettype`; pronto da personalizzare."""
    return CustomParserDef(
        name="Esempio P.Bet.",
        description="Esempio: Match/Esito/Quota/Lato con value-map dizionario.",
        rules=[
            FieldRule(target="Provider", fixed_value="TG_CUSTOM"),
            FieldRule(target="EventName", start_after="Match:", end_before="\n", required=True),
            FieldRule(target="MarketType", fixed_value="BOTH_TEAMS_TO_SCORE", required=True),
            FieldRule(target="SelectionName", start_after="Esito:", end_before="\n",
                      value_map="selectionname", required=True),
            FieldRule(target="Price", start_after="Quota:", end_before="\n", required=True),
            FieldRule(target="BetType", start_after="Lato:", value_map="bettype", required=True),
        ],
    )


def fixture_message() -> str:
    """Messaggio d'esempio che `example_parser()` rende piazzabile."""
    return "Match: Inter v Milan\nEsito: GG\nQuota: 1,85\nLato: BACK"

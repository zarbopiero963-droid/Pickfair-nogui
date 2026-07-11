"""#343 slice «selettore lingua»: logica PURA della scelta lingua al primo avvio.

Il proprietario del bridge sceglie la lingua (IT/EN/ES) al PRIMO avvio: la scelta
viene persistita in `app_language` (lingua dell'applicazione, base della futura UI
localizzata #343) e allineata a `csv_language` (già esistente: governa il separatore
decimale del CSV, #342). Finché l'utente non sceglie, `app_language` resta vuota e
il selettore ricompare al prossimo avvio — nel frattempo il bridge resta nel
comportamento storico (IT), quindi chiudere il selettore senza scegliere è sicuro.

Fail-closed: un `app_language` malformato in config NON viene "corretto" a IT in
silenzio — torna vuoto e il selettore ricompare (l'utente decide, non il default).
La GUI (`app._open_language_selector`) è solo vista: qui vivono valori, etichette
verbatim e transizioni, tutto testabile headless.
"""

from . import csv_writer

# Lingue supportate: stessa fonte unica del CSV (#342).
SUPPORTED = csv_writer.CSV_LANGUAGES            # ("IT", "EN", "ES")

# Etichette verbatim dei bottoni del selettore (codice, label).
LANGUAGE_LABELS = (("IT", "🇮🇹 Italiano"),
                   ("EN", "🇬🇧 English"),
                   ("ES", "🇪🇸 Español"))

# Promemoria (supporto XTrader §5, issue #343): col riconoscimento a NOMI la lingua
# della fonte in XTrader deve combaciare con quella del bridge.
SOURCE_LANGUAGE_HINT = ("Ricorda: in XTrader/Betting Toolkit imposta la LINGUA "
                        "DELLA FONTE uguale a quella scelta qui — col riconoscimento "
                        "a nomi i nomi dipendono dalla lingua del palinsesto.")

TITLE = "🌐 Scegli la lingua del bridge"


def normalize_app_language(value) -> str:
    """Normalizza `app_language`: IT/EN/ES (case-insensitive, spazi tollerati) oppure
    STRINGA VUOTA (= non ancora scelta → il selettore ricompare). Diversamente da
    `csv_language` NON c'è fallback a IT: un valore sporco non deve zittire il
    selettore spacciandosi per una scelta mai fatta (fail-closed)."""
    if isinstance(value, str) and value.strip().upper() in SUPPORTED:
        return value.strip().upper()
    return ""


def needs_language_selection(cfg) -> bool:
    """True se la lingua non è mai stata scelta (primo avvio o valore malformato)."""
    if not isinstance(cfg, dict):
        return True
    return normalize_app_language(cfg.get("app_language")) == ""


def apply_language(cfg, lang):
    """Ritorna una COPIA di `cfg` con `app_language` = `lang` e `csv_language`
    ALLINEATA, o `None` se `lang` non è supportata (fail-closed: nessuna modifica).
    Non muta mai `cfg` (la persistenza è del chiamante, via `save_config`).

    Percorso UPGRADE (review Fable #356): una `csv_language` PERSONALIZZATA (diversa
    sia dal default IT sia dalla lingua scelta — es. `EN` impostata a mano in
    `config.json` come da README #342) viene PRESERVATA, non sovrascritta: su
    installazioni XTrader vecchie senza l'update «decimali intelligenti» cambiare
    separatore a sorpresa può far rifiutare il CSV. L'allineamento avviene solo dal
    default o verso la stessa lingua; chi vuole cambiare anche il CSV lo fa in
    `config.json` (documentato)."""
    code = normalize_app_language(lang)
    if not code:
        return None
    # Copia INTENZIONALMENTE shallow (CodeRabbit #356): qui si mutano solo chiavi
    # stringa top-level (app_language/csv_language); i valori annidati restano
    # condivisi by design — la persistenza (`save_config`) fa comunque deepcopy.
    new_cfg = dict(cfg) if isinstance(cfg, dict) else {}
    new_cfg["app_language"] = code
    current_csv = csv_writer.normalize_csv_language(new_cfg.get("csv_language"))
    if current_csv in (csv_writer.DEFAULT_CSV_LANGUAGE, code):
        new_cfg["csv_language"] = code
    return new_cfg


def csv_language_preserved(cfg_applied) -> str:
    """Se `apply_language` ha PRESERVATO una `csv_language` personalizzata diversa
    dalla lingua app scelta, ritorna quel codice (per il log/UX); altrimenti ""."""
    if not isinstance(cfg_applied, dict):
        return ""
    csv_lang = csv_writer.normalize_csv_language(cfg_applied.get("csv_language"))
    app_lang = normalize_app_language(cfg_applied.get("app_language"))
    return csv_lang if (app_lang and csv_lang != app_lang) else ""

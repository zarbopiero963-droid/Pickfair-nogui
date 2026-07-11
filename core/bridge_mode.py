"""#311 §3.1: «Modalità Collaudo» esplicita — tri-stato NOMINATO sopra `dry_run`.

Tre modalità visibili invece del solo flag `dry_run`:

- **SIMULAZIONE** (Simulazione Bridge): riconosce i segnali ma NON scrive il CSV
  operativo (l'attuale `dry_run=True`).
- **COLLAUDO** (Collaudo XTrader): scrive il CSV operativo, con banner permanente
  «XTrader deve essere in Modalità Simulazione» — è la modalità del collaudo end-to-end
  (#311 Fase 0) in cui il bridge lavora davvero ma XTrader non piazza nulla di reale.
- **REALE**: scrive il CSV per scommesse vere, con la conferma "frictionful" esistente
  (`real_mode`, frase digitata) e il banner rosso persistente.

Principio di sicurezza (non negoziabile): **`dry_run` resta l'UNICA fonte del percorso
di scrittura** (`safety_guard.is_dry_run` → `live_guard`/`write_path`, invariati). La
modalità è stato DERIVATO e coerente: `SIMULAZIONE ⇔ dry_run=True`; `COLLAUDO`/`REALE`
⇒ `dry_run=False`. Su config incoerente (es. `bridge_mode:"REALE"` con `dry_run:true`
editata a mano) **vince `dry_run`** → `SIMULAZIONE` (fail-closed: un'etichetta sporca
non può accendere la scrittura). Config legacy senza `bridge_mode` e `dry_run=false`
→ `REALE` (era il reale già confermato: nessun declassamento silenzioso a collaudo).

Gate mode-aware: `real_mode.requires_confirmation` guarda solo `dry_run` True→False,
quindi NON vedrebbe il passaggio COLLAUDO→REALE (entrambi `dry_run=False`) — che
attiverebbe scommesse vere senza conferma. `requires_real_confirmation` chiude il buco:
scatta ogni volta che la NUOVA config è REALE e la vecchia non lo era.
"""

from . import safety_guard

SIMULAZIONE = "SIMULAZIONE"
COLLAUDO = "COLLAUDO"
REALE = "REALE"
VALID_MODES = (SIMULAZIONE, COLLAUDO, REALE)

# Etichette per il selettore GUI (tab Sicurezza). L'ordine segue VALID_MODES.
LABELS = {
    SIMULAZIONE: "🧪 Simulazione Bridge — NON scrive il CSV operativo",
    COLLAUDO: "🔬 Collaudo XTrader — scrive il CSV (XTrader in simulazione)",
    REALE: "⚠️ Reale — scommesse vere (richiede conferma)",
}

# Banner permanente della modalità COLLAUDO (il banner ROSSO di `real_mode` resta
# quello della modalità REALE e ha PRIORITÀ quando entrambi sarebbero attivi).
COLLAUDO_BANNER_TEXT = ("🔬 MODALITÀ COLLAUDO XTRADER — il CSV operativo VIENE scritto: "
                        "XTrader deve essere in Modalità Simulazione "
                        "(nessuna scommessa reale).")

# Conferma leggera (sì/no) all'attivazione del COLLAUDO da simulazione: il CSV
# operativo inizia a essere scritto, quindi XTrader DEVE già essere in simulazione.
COLLAUDO_CONFIRM_TEXT = ("Stai attivando la MODALITÀ COLLAUDO XTRADER:\n"
                         "il CSV operativo verrà scritto e XTrader lo importerà.\n\n"
                         "XTrader è impostato in Modalità Simulazione?\n"
                         "(Se è in reale, le scommesse sarebbero VERE.)")


def normalize_mode(value) -> str:
    """Canonicalizza a un modo valido (case-insensitive, spazi ignorati); qualsiasi
    altro valore → ``""`` (sconosciuto: decide `mode_from_cfg` con `dry_run`)."""
    v = str(value or "").strip().upper() if isinstance(value, str) else ""
    return v if v in VALID_MODES else ""


def mode_from_cfg(cfg) -> str:
    """Modalità EFFETTIVA della config. `dry_run` è autoritativo (fail-closed):

    - `dry_run=True` (o assente/malformato) → SIMULAZIONE, qualunque cosa dica
      `bridge_mode` (un'etichetta incoerente non accende la scrittura);
    - `dry_run=False` → COLLAUDO solo se dichiarato esplicitamente; altrimenti REALE
      (config legacy pre-tristato o etichetta sconosciuta: era il reale confermato)."""
    if safety_guard.is_dry_run(cfg):
        return SIMULAZIONE
    raw = normalize_mode(cfg.get("bridge_mode")) if isinstance(cfg, dict) else ""
    return COLLAUDO if raw == COLLAUDO else REALE


def apply_mode(cfg: dict, mode: str) -> dict:
    """Imposta su `cfg` la coppia coerente `bridge_mode` + `dry_run` per `mode`
    (sconosciuto → SIMULAZIONE, fail-closed). Muta e ritorna `cfg`."""
    mode = normalize_mode(mode) or SIMULAZIONE
    cfg["bridge_mode"] = mode
    cfg["dry_run"] = (mode == SIMULAZIONE)
    return cfg


def requires_real_confirmation(old_cfg, new_cfg) -> bool:
    """`True` se `new_cfg` attiva il REALE mentre `old_cfg` non lo era — incluso il
    passaggio COLLAUDO→REALE che il check basato su `dry_run` non vede (entrambi
    `dry_run=False`). La conferma frase (`real_mode`) serve solo all'ATTIVAZIONE."""
    return mode_from_cfg(new_cfg) == REALE and mode_from_cfg(old_cfg) != REALE


def requires_collaudo_confirmation(old_cfg, new_cfg) -> bool:
    """`True` se `new_cfg` attiva il COLLAUDO partendo dalla SIMULAZIONE: il CSV
    operativo inizia a essere scritto → conferma leggera (sì/no). Da REALE→COLLAUDO
    nessuna conferma (il rischio non aumenta); stesso stato → nessuna conferma."""
    return (mode_from_cfg(new_cfg) == COLLAUDO
            and mode_from_cfg(old_cfg) == SIMULAZIONE)


def real_banner_active(live_cfg, *, session_active=False, session_mode="") -> bool:
    """Banner ROSSO «MODALITÀ REALE» in chiave MODE-AWARE (Fugu #349): il criterio
    storico (`real_mode.banner_active`, basato su `dry_run=False`) si accenderebbe
    anche in COLLAUDO — mostrando «REALE ATTIVA» durante il collaudo e sopprimendo
    l'avviso ambra «XTrader in simulazione» (stato di sicurezza fuorviante). Qui il
    rosso si accende SOLO per il REALE: config viva in REALE, oppure sessione in
    corso partita in REALE (sticky fino a STOP, come prima)."""
    return (mode_from_cfg(live_cfg) == REALE
            or (bool(session_active) and session_mode == REALE))


def collaudo_banner_active(live_cfg, *, session_active=False, session_mode="") -> bool:
    """`True` se il banner COLLAUDO va mostrato: config viva in COLLAUDO, oppure una
    sessione in corso è PARTITA in collaudo (l'esecuzione scrive il CSV finché non si
    fa STOP, anche se la config viva è tornata in simulazione — stessa logica sticky
    del banner REALE). Il chiamante dà priorità al banner ROSSO quando anche il reale
    è attivo (mai due banner insieme)."""
    return (mode_from_cfg(live_cfg) == COLLAUDO
            or (bool(session_active) and session_mode == COLLAUDO))


def banners_for(live_cfg, *, session_active=False, session_mode=""):
    """Decisione PURA dei banner: `(rosso, ambra)` — MAI entrambi `True` (il rosso ha
    priorità: rischio maggiore vince). Estratta così la vista (`_update_real_mode_banner`)
    resta sottile e la priorità è testabile con input concreti (CodeRabbit #349), non con
    pin sul sorgente. Caso limite coperto: config viva REALE + sessione partita in
    COLLAUDO → solo rosso (l'ambra sarebbe fuorviante sul rischio in corso)."""
    red = real_banner_active(live_cfg, session_active=session_active,
                             session_mode=session_mode)
    amber = (not red) and collaudo_banner_active(
        live_cfg, session_active=session_active, session_mode=session_mode)
    return red, amber


def mode_options() -> list:
    """Etichette del selettore GUI, nell'ordine di `VALID_MODES`."""
    return [LABELS[m] for m in VALID_MODES]


def label_for(mode) -> str:
    """Etichetta GUI del modo (sconosciuto → SIMULAZIONE, fail-closed)."""
    return LABELS.get(normalize_mode(mode) or SIMULAZIONE, LABELS[SIMULAZIONE])


def mode_for_form_value(value):
    """Modo canonico da un valore del form (etichetta della tendina O nome canonico,
    case-insensitive). Sconosciuto → ``None`` (il chiamante segnala errore e NON
    applica: mai indovinare una modalità)."""
    canon = normalize_mode(value)
    if canon:
        return canon
    for mode, label in LABELS.items():
        if str(value or "").strip() == label:
            return mode
    return None


def start_log_text(mode) -> str:
    """Riga di log a START per la modalità effettiva (tre stati nominati)."""
    mode = normalize_mode(mode) or SIMULAZIONE
    if mode == SIMULAZIONE:
        return "🧪 SIMULAZIONE BRIDGE attiva: il CSV operativo NON verrà scritto."
    if mode == COLLAUDO:
        return ("🔬 COLLAUDO XTRADER attivo: il CSV operativo VIENE scritto — "
                "assicurati che XTrader sia in Modalità Simulazione.")
    return "⚠️ Modalità REALE: i segnali validi verranno scritti nel CSV."

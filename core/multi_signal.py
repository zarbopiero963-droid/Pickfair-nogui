"""UX delle modalità coda MULTI-segnale (#136 punto 5) — logica pura, testabile.

Il bridge è sicuro di default con `queue_mode=OVERWRITE_LAST`: nel CSV c'è **una sola
riga attiva** alla volta → al massimo **una scommessa** per volta. Le modalità
`APPEND_ACTIVE` / `QUEUE_UNTIL_CONFIRMED` tengono **più righe attive** insieme → XTrader
può piazzare **più scommesse simultanee**. È una scelta legittima ma rischiosa, quindi va
resa esplicita e limitata.

Questo modulo concentra le decisioni/testi che la GUI usa, **senza** dipendere da
GUI/CSV/Telegram (il TETTO vero e proprio è applicato da `signal_queue.SignalQueue`,
parametro `max_active`):

- `is_multi_mode(mode)`: True se la modalità può tenere più righe attive insieme.
- `requires_warning(old_cfg, new_cfg)`: True quando si passa da una-riga a multi-riga →
  la GUI mostra un **warning modale** (più scommesse simultanee).
- `warning_text(max_active)`: testo del warning all'attivazione.
- `active_count_text(n, max_active)`: testo dell'indicatore "righe attive" (`N/M`).
- `blocked_message(max_active)`: messaggio quando un segnale è bloccato dal tetto.
"""

from . import signal_queue


def is_multi_mode(mode) -> bool:
    """``True`` se la modalità coda può tenere **più** righe attive insieme
    (`APPEND_ACTIVE` / `QUEUE_UNTIL_CONFIRMED`). `OVERWRITE_LAST` (default) tiene sempre una
    sola riga → ``False``. Valore ignoto/mancante → modalità di default → ``False``."""
    return signal_queue.normalize_mode(mode) != signal_queue.OVERWRITE_LAST


def requires_warning(old_cfg, new_cfg) -> bool:
    """``True`` quando si passa da una modalità a **una sola** riga attiva
    (`OVERWRITE_LAST`) a una modalità **multi-riga**: solo questa transizione introduce il
    rischio "più scommesse simultanee" → warning. multi→multi o →overwrite non avvisano."""
    old_multi = is_multi_mode((old_cfg or {}).get("queue_mode"))
    new_multi = is_multi_mode((new_cfg or {}).get("queue_mode"))
    return (not old_multi) and new_multi


def warning_text(max_active) -> str:
    """Testo del warning modale all'attivazione di una modalità multi-segnale."""
    return ("Stai attivando una modalità coda MULTI-segnale: nel CSV potranno esserci PIÙ "
            "righe attive contemporaneamente, quindi XTrader può piazzare PIÙ scommesse "
            f"simultanee (tetto attuale: {max_active} righe attive). Confermi?")


def active_count_text(n, max_active) -> str:
    """Testo dell'indicatore delle righe attive per la GUI: ``N`` oppure ``N/M`` se è
    impostato un tetto (`max_active` > 0)."""
    try:
        cap = int(max_active)
    except (TypeError, ValueError):
        cap = 0
    suffix = f"/{cap}" if cap > 0 else ""
    return f"Righe attive: {int(n)}{suffix}"


def blocked_message(max_active) -> str:
    """Messaggio per un segnale BLOCCATO dal tetto di righe attive (ritentabile)."""
    return (f"🚧 Segnale bloccato: raggiunto il tetto di {max_active} righe attive "
            "(max segnali attivi). Sarà ritentabile quando una riga scade o viene confermata.")

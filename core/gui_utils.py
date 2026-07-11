"""Helper GUI condivisi tra le finestre del bridge.

Un solo posto per la logica "apri la finestra in modo che entri nello schermo",
così tutte le finestre (principale + builder/dizionario/profili/provider/chat)
si comportano allo stesso modo su schermi piccoli.
"""


def clamp_to_screen(width, height, min_width, min_height, screen_w, screen_h,
                    margin=80):
    """PURA (#311 §3.5): riduce ``width × height`` all'area schermo disponibile
    (``screen − margin`` su entrambi gli assi), con PAVIMENTO ai minimi dichiarati:
    il clamp non apre mai una finestra più piccola del suo ``minsize``.

    Returns:
        Tupla ``(width, height)`` clampata.
    """
    avail_w = max(min_width, screen_w - margin)
    avail_h = max(min_height, screen_h - margin)
    return min(width, avail_w), min(height, avail_h)


def fit_to_screen(window, width, height, min_width, min_height, margin=80):
    """Apre ``window`` a ``width × height`` ma CLAMPA larghezza E altezza all'area
    schermo disponibile (``screen − margin``, vedi ``clamp_to_screen``).

    Su schermi piccoli (portatili) una finestra più alta dello schermo finirebbe
    sotto la taskbar lasciando la parte bassa irraggiungibile, e una più larga
    (Strumenti/dizionario: fino a 1140px) uscirebbe di lato (#311 §3.5): clampando
    entrambe le dimensioni la finestra resta interamente visibile. Imposta anche
    ``minsize`` perché resti usabile quando viene ridotta; i minimi fanno da
    pavimento al clamp (mai sotto il layout minimo dichiarato dal chiamante).

    Args:
        window: la finestra (``CTk``/``CTkToplevel``) su cui agire.
        width: larghezza desiderata in px (verrà ridotta se non entra).
        height: altezza desiderata in px (verrà ridotta se non entra).
        min_width: larghezza minima consentita (``minsize``); pavimento del clamp.
        min_height: altezza minima consentita (``minsize``); pavimento del clamp.
        margin: px lasciati liberi ai bordi (taskbar, barra del titolo). Default 80.
    """
    try:
        screen_w, screen_h = window.winfo_screenwidth(), window.winfo_screenheight()
    except Exception:
        # winfo_screen* può fallire se la finestra non è ancora mappata: in tal
        # caso si usano le dimensioni richieste così come sono (nessun clamp).
        w, h = width, height
    else:
        w, h = clamp_to_screen(width, height, min_width, min_height,
                               screen_w, screen_h, margin)
    window.geometry(f"{w}x{h}")
    window.minsize(min_width, min_height)

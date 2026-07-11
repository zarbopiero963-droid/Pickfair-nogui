"""PR-14b: filtro del log per livello per la GUI (logica pura, testabile in CI).

La GUI tiene in memoria le righe del log già formattate da `event_log.format_entry`
(`[HH:MM:SS] [LEVEL] messaggio`) e mostra solo quelle del livello selezionato in un
menu a tendina. Qui vive SOLO la logica di selezione: la voce "Tutti" (mostra tutto)
e i livelli noti, delegando il filtro per livello a `event_log.filter_by_level`
(che legge il campo header strutturale, non il testo). Nessun widget.
"""

from . import event_log

# Voce del menu "mostra tutto" (non è un livello di log).
ALL = "Tutti"
# Opzioni del menu a tendina, nell'ordine: Tutti + i livelli di event_log
# (fonte UNICA dei livelli → niente disallineamento con classify/format_entry).
OPTIONS = (ALL,) + event_log.LEVELS


def matches(line, selected) -> bool:
    """True se `line` (riga formattata) va mostrata col filtro `selected`.

    `ALL` (o un valore non riconosciuto) → sempre True; un livello noto → True solo
    se è il livello della riga, letto dal **campo header** (`event_log.entry_level`),
    non dal testo. È il predicato UNICO usato sia riga-per-riga sia da `filter_lines`
    (niente doppia logica, e niente allocazioni per il check incrementale)."""
    sel = str(selected or "").strip()
    if sel not in event_log.LEVELS:      # ALL o valore ignoto → nessun filtro
        return True
    return event_log.entry_level(line) == sel


def filter_lines(lines, selected) -> list:
    """Righe da mostrare dato il valore selezionato nel menu (vedi `matches`).
    Ritorna sempre una nuova lista (non muta l'input)."""
    return [line for line in lines if matches(line, selected)]

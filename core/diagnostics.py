"""PR-14c: report diagnostico testuale per il supporto (logica pura, testabile).

Costruisce il testo che il pulsante "Copia diagnostica" mette negli appunti: stato
del bridge, contatori, ultimi eventi e percorsi utili. È **puro** (nessun widget) e
applica sempre la redazione dei segreti (`event_log.redact_secrets`), così un token
incollato per sbaglio in un campo non finisce mai nel report condiviso col supporto.
"""

from . import __version__, event_log

_TITLE = "XTrader Signal Bridge — diagnostica"


def build_report(info) -> str:
    """Report multilinea da una sequenza ordinata di `(etichetta, valore)` (o un
    dict). Un valore vuoto/None è mostrato come ``—``. L'intero testo è passato per
    `redact_secrets` (difesa: mai un token in chiaro nel report)."""
    items = info.items() if isinstance(info, dict) else list(info or [])
    lines = [_TITLE, f"versione: {__version__}", "-" * len(_TITLE)]
    for label, value in items:
        # Normalizza PRIMA di decidere: un valore di soli spazi (es. "   ", "\t") va
        # mostrato come "—", non come stringa vuota. Strippare e poi fare il fallback
        # copre None/""/whitespace-only con un unico controllo (#184 LOW).
        text = str(value).strip() if value is not None else ""
        text = text or "—"
        lines.append(f"{label}: {text}")
    return event_log.redact_secrets("\n".join(lines))

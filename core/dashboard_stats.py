"""PR-14: contatori di stato del bridge (logica pura, testabile in CI).

Tiene i conteggi **di sessione** (dall'ultimo START) degli esiti del flusso live —
segnali ricevuti, scritti nel CSV, scartati, duplicati, soppressi dai limiti, in
simulazione (DRY_RUN), errori di scrittura — e li espone come righe etichettate
per la dashboard GUI. Nessun widget: la vista (`app`) crea una label per contatore
da `COUNTERS` e le aggiorna leggendo `as_dict()` (chiavi = nomi dei contatori);
`summary()` è una comodità di presentazione `(etichetta, valore)` per chi vuole
già l'ordine + le etichette. Sul modello del controller del Parser Personalizzato
(CP-06) e di `settings_controller` (PR-13): tutta la logica qui, testabile headless.
"""

# Nome interno → etichetta GUI, nell'ordine di visualizzazione (fonte UNICA: la
# dashboard e i test derivano da qui, così aggiungere un contatore non desincronizza).
COUNTERS = (
    ("received", "📥 Ricevuti"),
    ("written", "✅ Scritti"),
    ("discarded", "⚠️ Scartati"),
    ("duplicate", "♻️ Duplicati"),
    ("limited", "🚦 Limitati"),
    ("dry_run", "🧪 Simulati"),
    ("errors", "❌ Errori"),
)
COUNTER_NAMES = tuple(name for name, _ in COUNTERS)


class DashboardStats:
    """Conteggi di sessione degli esiti del flusso live. Non thread-safe per
    progetto: la GUI lo muta solo dal thread Tk (via `after`), come gli altri
    aggiornamenti dei widget."""

    def __init__(self):
        self.reset()

    def reset(self) -> None:
        """Azzera tutti i contatori (chiamato a ogni START = nuova sessione)."""
        self._counts = {name: 0 for name in COUNTER_NAMES}

    def bump(self, name: str, n: int = 1) -> int:
        """Incrementa un contatore noto di `n` (default 1) e ritorna il nuovo valore.
        Un nome sconosciuto è un errore di programmazione → `KeyError` esplicito."""
        if name not in self._counts:
            raise KeyError(f"contatore sconosciuto: {name!r}")
        self._counts[name] += n
        return self._counts[name]

    def get(self, name: str) -> int:
        """Valore corrente di un contatore noto."""
        if name not in self._counts:
            raise KeyError(f"contatore sconosciuto: {name!r}")
        return self._counts[name]

    def as_dict(self) -> dict:
        """Copia dei contatori correnti (mutarla non altera lo stato interno).
        È l'accessor principale: la GUI legge da qui per nome."""
        return dict(self._counts)

    def summary(self) -> list:
        """Comodità di presentazione: lista `(etichetta, valore)` nell'ordine di
        `COUNTERS`. Deriva da `as_dict()` (fonte unica), così resta allineata."""
        counts = self.as_dict()
        return [(label, counts[name]) for name, label in COUNTERS]

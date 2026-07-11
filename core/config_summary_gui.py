"""#293 (slice 3): pannello «📋 Riepilogo configurazione» (SOLA LETTURA).

Rende il riepilogo prodotto da `config_summary.summarize_config` (modulo puro): modalità
Simulazione/REALE, stato Betfair, e per ogni canale → parser → traduzioni → «Pronto?».
Non scrive né modifica nulla: legge la config viva e i conteggi Betfair passati dalla GUI
principale. Le decisioni di testo/colore sono in helper **puri** a livello di modulo
(testabili headless); la costruzione dei widget è verifica manuale su Windows (come gli
altri pannelli GUI, non testati in CI perché richiedono un display).
"""

import customtkinter as ctk

# Colori semantici theme-aware `(chiaro, scuro)`, coerenti con il resto della GUI
# (stessa palette di custom_parser_gui/app: verde OK, arancio avviso, rosso reale).
_COLOR_OK = ("#2e7d32", "#66bb6a")
_COLOR_WARN = ("#bf360a", "#ffa726")
_COLOR_REAL = ("#c62828", "#ef5350")
_COLOR_MUTED = "gray"


def mode_label(real_mode: bool) -> str:
    """Testo della riga modalità."""
    return "🔴 MODALITÀ REALE" if real_mode else "🧪 Simulazione (DRY_RUN)"


def mode_color(real_mode: bool):
    """Colore della riga modalità: rosso in reale, verde in simulazione (sicura)."""
    return _COLOR_REAL if real_mode else _COLOR_OK


def betfair_label(synced: bool, logged_in: bool) -> str:
    """Testo dello stato Betfair: dizionario sincronizzato sì/no + login attivo sì/no."""
    sync = "sincronizzato" if synced else "non sincronizzato"
    login = "login attivo" if logged_in else "login non attivo"
    return f"Dizionario Betfair: {sync} · {login}"


def _one_translation_label(prefix: str, ts) -> str:
    """`Nomi ✓2` se ci sono traduzioni attive, `Nomi —` se nessuna."""
    return f"{prefix} ✓{ts.count}" if ts.count else f"{prefix} —"


def translations_label(channel) -> str:
    """Traduzioni attive del canale in forma compatta, es. `Nomi ✓2 · Mercati —`."""
    return (_one_translation_label("Nomi", channel.names) + " · "
            + _one_translation_label("Mercati", channel.markets))


def readiness_label(channel) -> str:
    """`✅ Pronto` oppure `⚠ <motivo>`."""
    return "✅ Pronto" if channel.ready else f"⚠ {channel.reason}"


def readiness_color(channel):
    return _COLOR_OK if channel.ready else _COLOR_WARN


def channel_title(channel) -> str:
    """Intestazione leggibile del canale: nome se presente, altrimenti l'id; se manca
    anche l'id, un segnaposto esplicito."""
    if channel.name:
        return f"{channel.name} ({channel.chat_id})" if channel.chat_id else channel.name
    return channel.chat_id or "(canale senza chat_id)"


def parser_label(channel) -> str:
    """Riga parser del canale. Un parser risolto ma NON caricabile (file mancante/invalido,
    fail-closed) porta un `⚠` sulla riga stessa, così il guasto è visibile qui e non solo
    nella riga «Pronto?» sottostante (CodeRabbit #337)."""
    if not channel.parser_name:
        return "Parser: —"
    names = list(getattr(channel, "parser_names", ()) or ())
    unloaded = tuple(getattr(channel, "parser_names_unloaded", ()) or ())
    # ⚠ se il primario non carica OPPURE un qualsiasi parser configurato (anche secondario) non
    # carica: un secondario rotto perderebbe bet in silenzio (Fable #391) → deve essere visibile.
    warn = " ⚠" if (not channel.parser_loaded or unloaded) else ""
    if len(names) > 1:
        # PR-2 (router multi-parser): più parser sulla chat → lista in ordine di priorità. Il ⚠
        # segnala che almeno un parser della lista non è caricabile (dettaglio nella riga «Pronto?»).
        return f"Parser ({len(names)}): " + ", ".join(names) + warn
    return f"Parser: {channel.parser_name}{warn}"


def ready_count_label(summary) -> str:
    """Riga «Canali pronti: N/M»."""
    return f"Canali pronti: {summary.ready_channels}/{summary.total_channels}"


# Stato vuoto (nessuna sorgente/chat configurata): stringa in un helper puro, come le altre
# etichette, così è coperta dai test e non diverge dall'intento di design (CodeRabbit #337).
_NO_CHANNELS_LABEL = "Nessun canale configurato (nessuna sorgente / chat)."


def no_channels_label() -> str:
    return _NO_CHANNELS_LABEL


class ConfigSummaryPanel(ctk.CTkFrame):
    """Pannello sola-lettura del riepilogo configurazione.

    Args:
        master: contenitore padre (una scheda dell'hub Strumenti).
        summary_provider: callable che ritorna un `config_summary.ConfigSummary` fresco
            (la GUI principale lo cabla con la config viva + stato Betfair). Chiamato alla
            costruzione e ad ogni `refresh_options()` (cambio scheda), così il riepilogo
            resta aggiornato senza riaprire la finestra.
    """

    def __init__(self, master=None, summary_provider=None):
        super().__init__(master)
        self._summary_provider = summary_provider
        self._body = None
        self._render()

    def refresh_options(self) -> None:
        """Ricostruisce il riepilogo dalla sorgente fresca (hook chiamato dall'hub al
        cambio scheda). Best-effort: un provider che solleva mostra un avviso invece di
        rompere la finestra."""
        self._render()

    # ── rendering (verifica manuale; la logica testo/colore è negli helper puri) ──
    def _render(self) -> None:
        if self._body is not None:
            self._body.destroy()
        self._body = ctk.CTkScrollableFrame(self)
        self._body.pack(fill="both", expand=True, padx=8, pady=8)

        try:
            summary = self._summary_provider() if self._summary_provider else None
        except Exception as exc:            # noqa: BLE001 — provider best-effort
            ctk.CTkLabel(self._body, text=f"⚠️ Impossibile leggere la configurazione:\n{exc}",
                         text_color=_COLOR_WARN, justify="left", anchor="w").pack(
                             fill="x", padx=8, pady=8)
            return
        if summary is None:
            ctk.CTkLabel(self._body, text="Nessun dato di configurazione.",
                         text_color=_COLOR_MUTED, anchor="w").pack(fill="x", padx=8, pady=8)
            return

        ctk.CTkLabel(self._body, text="📋 Riepilogo configurazione",
                     font=ctk.CTkFont(size=15, weight="bold"), anchor="w").pack(
                         fill="x", padx=4, pady=(2, 8))

        # Stato globale: modalità + Betfair.
        ctk.CTkLabel(self._body, text=mode_label(summary.real_mode),
                     text_color=mode_color(summary.real_mode), anchor="w",
                     font=ctk.CTkFont(weight="bold")).pack(fill="x", padx=4, pady=1)
        ctk.CTkLabel(self._body, text=betfair_label(summary.betfair_synced,
                                                     summary.betfair_logged_in),
                     anchor="w").pack(fill="x", padx=4, pady=(1, 8))

        ctk.CTkLabel(
            self._body, text=ready_count_label(summary),
            anchor="w", font=ctk.CTkFont(weight="bold")).pack(fill="x", padx=4, pady=(0, 6))

        if not summary.channels:
            ctk.CTkLabel(self._body, text=no_channels_label(),
                         text_color=_COLOR_MUTED, anchor="w").pack(fill="x", padx=8, pady=4)
            return

        for ch in summary.channels:
            card = ctk.CTkFrame(self._body)
            card.pack(fill="x", padx=4, pady=3)
            ctk.CTkLabel(card, text=channel_title(ch), anchor="w",
                         font=ctk.CTkFont(weight="bold")).pack(fill="x", padx=8, pady=(6, 1))
            ctk.CTkLabel(card, text=parser_label(ch), anchor="w").pack(
                fill="x", padx=8, pady=1)
            ctk.CTkLabel(card, text=translations_label(ch), anchor="w").pack(
                fill="x", padx=8, pady=1)
            ctk.CTkLabel(card, text=readiness_label(ch), text_color=readiness_color(ch),
                         anchor="w").pack(fill="x", padx=8, pady=(1, 6))

"""#311 §3.3: Health check a SEMAFORI — logica pura, testabile headless.

Il pannello «🚦 Salute» mostra a colpo d'occhio lo stato operativo del bridge con i
sette semafori dell'issue: Telegram connesso · ultimo messaggio ricevuto · parser
attivo · ultimo segnale (valido/scartato col motivo) · CSV scrivibile · ultima
conferma XTrader · modalità corrente. La GUI è solo vista: TUTTE le decisioni
(stato/colore/dettaglio) vivono qui, alimentate dai dati già esistenti (stato
listener, campi «Ultimo …», config, `bridge_mode`).

Principi:
- **fail-safe onesto**: un dato assente non è mai «verde per default» — è giallo
  («non ancora osservato in questa sessione») o rosso se blocca l'operatività;
- **nessun side-effect**: la sonda CSV (`csv_writable`) NON apre il file in
  scrittura (niente lock/contese con XTrader): usa solo `os.access`/`os.path`;
- il semaforo «Modalità» usa la semantica di rischio dei banner (#311 §3.1):
  verde = Simulazione (sicura), giallo = Collaudo (scrive il CSV), rosso = Reale.
"""

import os
from dataclasses import dataclass

from . import bridge_mode

GREEN = "GREEN"
YELLOW = "YELLOW"
RED = "RED"

# Stati del listener come mostrati dall'header dell'app (fonte: `_status_lbl`).
LISTENER_ACTIVE = "ATTIVO"
LISTENER_RECONNECTING = "RICONNESSIONE"
LISTENER_OFFLINE = "OFFLINE"


@dataclass
class HealthItem:
    """Un semaforo del pannello: `state` ∈ {GREEN, YELLOW, RED}."""

    key: str
    label: str
    state: str
    detail: str = ""


def csv_writable(path, *, platform=None) -> "tuple[str, str]":
    """Sonda NON INVASIVA di scrivibilità del CSV: `(stato, motivo)` con stato ∈
    {GREEN, YELLOW, RED}. Non apre mai il file (nessun lock che disturbi XTrader):
    controlla esistenza/permessi con `os.access`. File assente → la CARTELLA deve
    esistere ed essere scrivibile (il bridge lo creerà).

    Onestà su **Windows** (Fable/Fugu #351/#352): su NTFS `os.access(W_OK)` ignora
    ACL e lock attivi (es. XTrader che tiene il file) → un verde sarebbe FALSO proprio
    nello scenario target. Su ENTRAMBI i rami — file esistente E file da creare — la
    sonda non-POSIX si ferma a GIALLO («probabilmente scrivibile»), mai verde non
    verificabile. FAIL-CLOSED sul `platform` (Fable #353): solo `platform == "posix"`
    guadagna il verde; valori sconosciuti/sporchi restano al giallo."""
    platform = os.name if platform is None else platform   # iniettabile nei test
    p = str(path or "").strip()
    if not p:
        return RED, "csv_path non configurato"
    if os.path.isdir(p):
        return RED, "il percorso è una cartella, non un file"
    if os.path.exists(p):
        if not os.access(p, os.W_OK):
            return RED, "file esistente ma NON scrivibile (permessi/lock)"
        if platform != "posix":
            # FAIL-CLOSED sul platform (Fable #353): solo POSIX guadagna il verde
            # verificabile; "nt", valori esotici o sporchi restano al giallo onesto.
            return YELLOW, ("file esistente, probabilmente scrivibile — su Windows "
                            "ACL/lock (es. XTrader) non sono rilevabili senza aprirlo")
        return GREEN, "file esistente e scrivibile"
    parent = os.path.dirname(p) or "."
    if not os.path.isdir(parent):
        return RED, f"cartella inesistente: {parent}"
    if os.access(parent, os.W_OK):
        if platform != "posix":
            # Coerenza (Fugu #351) + fail-closed sul platform (Fable #353): su NTFS
            # anche os.access sulla CARTELLA ignora le ACL, e un platform sconosciuto
            # non guadagna il verde → il ramo «file da creare» resta al giallo onesto.
            return YELLOW, ("il file verrà creato, cartella probabilmente scrivibile — "
                            "su Windows le ACL non sono rilevabili senza scrivere")
        return GREEN, "il file verrà creato (cartella scrivibile)"
    return RED, f"cartella NON scrivibile: {parent}"


def evaluate(*, listener_status=LISTENER_OFFLINE, last_message="", parser_active=False,
             last_signal="", last_error="", csv_state=RED, csv_detail="",
             confirmations_enabled=False, last_confirmation="", mode="") -> list:
    """I sette semafori (#311 §3.3) dagli input GIÀ disponibili nell'app. Puro."""
    items = []

    status = str(listener_status or "").upper()
    if LISTENER_ACTIVE in status:
        items.append(HealthItem("telegram", "Telegram", GREEN, "connesso, in ascolto"))
    elif LISTENER_RECONNECTING in status:
        items.append(HealthItem("telegram", "Telegram", YELLOW,
                                "riconnessione in corso (backoff)"))
    else:
        items.append(HealthItem("telegram", "Telegram", RED,
                                "OFFLINE — premi AVVIA per ascoltare"))

    msg = str(last_message or "").strip()
    items.append(HealthItem("message", "Ultimo messaggio", GREEN if msg else YELLOW,
                            msg or "nessun messaggio ricevuto in questa sessione"))

    items.append(HealthItem(
        "parser", "Parser Personalizzato", GREEN if parser_active else RED,
        "configurato e attivo" if parser_active else
        "NESSUN parser attivo: lo START è bloccato (scheda 🧩 Parser)"))

    sig = str(last_signal or "").strip()
    err = str(last_error or "").strip()
    if sig:
        items.append(HealthItem("signal", "Ultimo segnale", GREEN, sig))
    elif err:
        # Nessun segnale ma un errore recente: il motivo va MOSTRATO (mai nascosto).
        items.append(HealthItem("signal", "Ultimo segnale", YELLOW,
                                f"nessun segnale; ultimo errore: {err}"))
    else:
        items.append(HealthItem("signal", "Ultimo segnale", YELLOW,
                                "nessun segnale in questa sessione"))

    csv_state = csv_state if csv_state in (GREEN, YELLOW, RED) else RED  # fail-closed
    items.append(HealthItem("csv", "CSV scrivibile", csv_state, str(csv_detail or "")))

    if not confirmations_enabled:
        items.append(HealthItem("confirmation", "Conferme XTrader", YELLOW,
                                "non attive (chat notifiche non configurata)"))
    else:
        conf = str(last_confirmation or "").strip()
        items.append(HealthItem("confirmation", "Conferme XTrader",
                                GREEN if conf else YELLOW,
                                conf or "attive, nessuna conferma ricevuta finora"))

    eff = bridge_mode.normalize_mode(mode) or bridge_mode.SIMULAZIONE
    mode_state = {bridge_mode.SIMULAZIONE: GREEN, bridge_mode.COLLAUDO: YELLOW,
                  bridge_mode.REALE: RED}[eff]
    items.append(HealthItem("mode", "Modalità", mode_state, bridge_mode.label_for(eff)))
    return items

"""L'interfaccia non deve dichiarare inattivo un cablaggio che esiste.

Fino al 2026-08-20 la tab Telegram diceva, sotto la sezione «Bot (Bot API)»:

    Nota: configurazione persistita, non ancora attiva a runtime.

Era vero quando la sezione fu scritta (epica #374 PR-4, sola persistenza), ed e'
rimasto li' dopo PR-5a/5b, che il wiring l'hanno portato:

    services/telegram_service.py, start()
        if not cfg.api_id or not cfg.api_hash:      -> path Bot API
        ...
        len(usable) == 1  -> runtime singolo
        len(usable)  > 1  -> _build_multibot_runtime(...)   (PR-5b)

Una nota cosi' non e' un dettaglio estetico: dice all'utente che i bot che sta
configurando NON verranno ascoltati, quindi lo porta a non usare una funzione
che funziona — o a cercare altrove un problema che non esiste.

Questo test tiene insieme le due cose: la frase non deve tornare, E il cablaggio
che la rende falsa deve restare. Se un domani il wiring venisse rimosso, il test
diventa rosso e ricorda che va corretto anche il testo mostrato all'utente,
invece di lasciare l'interfaccia a mentire nella direzione opposta.
"""
from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

# Le tre superfici che dichiaravano il cablaggio assente.
FILE_CON_LA_NOTA = [
    "telegram_tab_ui.py",
    "telegram_module.py",
    "services/telegram_service.py",
]

# Affermazioni sullo STATO ATTUALE: non devono comparire da nessuna parte,
# ne' in un commento ne' in una stringa mostrata all'utente.
FRASI_SUPERATE = [
    "non ancora attiva a runtime",
    "non ancora ascoltati",
    "Nessun wiring runtime",
]

# Il motivo di fail-closed di PR-5a. Nominarlo in un commento per dire "e' stato
# rimpiazzato" e' storia utile e va lasciata stare; quello che non deve tornare
# e' il suo USO come motivo restituito al chiamante.
MOTIVO_RIMOSSO = "multi_bot_runtime_not_yet_supported"


@pytest.mark.parametrize("nome", FILE_CON_LA_NOTA)
@pytest.mark.parametrize("frase", FRASI_SUPERATE)
def test_block_nessuna_dichiarazione_superata_di_cablaggio_assente(
        nome: str, frase: str) -> None:
    percorso = ROOT / nome
    assert percorso.is_file(), f"{nome} non esiste: il test va aggiornato"
    testo = percorso.read_text(encoding="utf-8")

    assert frase not in testo, (
        f"{nome} contiene ancora «{frase}». Il path Bot API viene selezionato e "
        f"avviato da TelegramService.start(): dichiararlo inattivo dice all'utente "
        f"che i bot configurati non vengono ascoltati, quando invece lo sono."
    )


def test_block_il_cablaggio_che_rende_vera_la_nota_deve_esistere() -> None:
    """L'altra meta': la nota nuova e' vera solo finche' il wiring c'e'.

    Non duplica i test di comportamento in tests/services/test_telegram_service_botapi.py:
    qui si afferma soltanto che i punti di aggancio citati dal testo mostrato
    all'utente non spariscano senza che nessuno se ne accorga.
    """
    servizio = (ROOT / "services" / "telegram_service.py").read_text(encoding="utf-8")

    mancanti = [
        aggancio for aggancio in (
            "def _select_bot_api_source",     # legge i bot attivi dal DB
            "_build_multibot_runtime",        # orchestratore N-bot (PR-5b)
            "if not cfg.api_id or not cfg.api_hash",   # il gate che sceglie il path
        )
        if aggancio not in servizio
    ]

    assert not mancanti, (
        f"Spariti da telegram_service.py: {', '.join(mancanti)}. Se il cablaggio "
        f"Bot API viene rimosso o rinominato, va corretto anche il testo della tab "
        f"Telegram, che oggi dice all'utente che i suoi bot vengono ascoltati."
    )


def test_block_il_fail_closed_su_piu_bot_non_deve_tornare() -> None:
    """PR-5b ha sostituito il rifiuto su >1 bot con l'orchestratore multi-bot.

    Il nome puo' comparire in un commento storico — anzi, e' li' che spiega cosa
    e' cambiato. Quello che non deve tornare e' il suo uso come valore: se
    riapparisse assegnato o restituito, la tab tornerebbe a promettere un
    multi-bot che il runtime rifiuta.
    """
    righe = (ROOT / "services" / "telegram_service.py").read_text(
        encoding="utf-8").splitlines()

    usi = [
        f"riga {i + 1}: {r.strip()}"
        for i, r in enumerate(righe)
        if MOTIVO_RIMOSSO in r and not r.lstrip().startswith("#")
    ]

    assert not usi, (
        f"`{MOTIVO_RIMOSSO}` e' tornato come valore, non come nota storica:\n  "
        + "\n  ".join(usi)
        + "\nLa tab Telegram promette che piu' bot attivi avviano l'orchestratore: "
          "se il runtime torna a rifiutarli, va corretto anche quel testo."
    )

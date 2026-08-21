"""Contratto dei campi di un segnale, e dove vivono i parser.

## Perche' questo modulo esiste

I Parser Personalizzati estraggono valori da un messaggio Telegram e li
depositano in campi con un nome. Quell'elenco di nomi era `CSV_HEADER`, e
viveva in `core/csv_writer.py` — cioe' nel modulo che scrive il CSV per
XTrader.

**Non era una scelta: era un accidente.** I quattordici nomi non descrivono
un CSV, descrivono una scommessa: quale evento, quale mercato, quale
selezione, a che quota, punta o banca. Sono gli stessi dati che servono a
Pickfair per piazzare su Betfair.

Tenendoli in `csv_writer` bastava importarne l'elenco per trascinarsi dietro
il CSV, la configurazione del Bridge (`%APPDATA%/XTraderBridge`) e la sua
catena di conferme: importare il motore dei parser costava 7.407 righe di un
altro prodotto.

Qui sono definiti una volta sola, senza dipendenze. Il Bridge li reimporta
finche' esiste; Pickfair li usa senza sapere che il Bridge esista.

## Perche' i nomi restano quelli

Sono in CamelCase inglese (`MarketId`, non `market_id`) perche' **sono dati
dell'owner, non codice**: ogni parser gia' scritto o che verra' scritto
nomina il campo di destinazione con questa stringa esatta. Rinominarli qui
significherebbe invalidare i parser dell'utente a ogni refactor. La
traduzione verso i nomi interni di Pickfair sta in `VERSO_SEGNALE`, che e'
il posto giusto: un solo punto, esplicito, modificabile senza toccare i dati.
"""

from __future__ import annotations

import os
from typing import Dict, Tuple

# Nome della cartella dati di Pickfair. Coerente con `~/.pickfair/db.key`
# gia' usato da `config_registry`.
NOME_CARTELLA_DATI = ".pickfair"

# Variabile d'ambiente per spostare la cartella dei parser (test, piu'
# installazioni sulla stessa macchina, percorsi non standard).
ENV_CARTELLA_PARSER = "PICKFAIR_PARSERS_DIR"

# ---------------------------------------------------------------------------
# Il contratto
# ---------------------------------------------------------------------------

# I campi che un parser puo' valorizzare. L'ordine e' significativo: la GUI e
# le diagnostiche lo usano per presentarli, e i test lo fissano.
CAMPI_SEGNALE: Tuple[str, ...] = (
    "Provider",
    "EventId",
    "EventName",
    "MarketId",
    "MarketName",
    "MarketType",
    "SelectionId",
    "SelectionName",
    "Handicap",
    "Price",
    "MinPrice",
    "MaxPrice",
    "BetType",
    "Points",
)

# Traduzione verso i nomi che usa il percorso di esecuzione di Pickfair
# (`TelegramSignalProcessor` → `TradingEngine`).
#
# Un campo ASSENTE da questa mappa e' deliberato, non dimenticato:
#
#   - `MinPrice` / `MaxPrice` sono una forbice di accettazione del prezzo, non
#     un prezzo da inviare;
#   - `Points` nel CSV del Bridge trasportava lo stake, ma in Pickfair lo
#     stake NON arriva dal messaggio: lo decide il money management sui
#     tavoli. Mapparlo qui vorrebbe dire lasciare che un messaggio Telegram
#     detti quanto si scommette, ed e' esattamente cio' che il risk gate
#     esiste per impedire.
#
# Chi aggiunge una riga qui sta ampliando cio' che un messaggio esterno puo'
# decidere: e' una modifica al percorso dei soldi, non una comodita'.
VERSO_SEGNALE: Dict[str, str] = {
    "Provider": "provider",
    "EventId": "event_id",
    "EventName": "event_name",
    "MarketId": "market_id",
    "MarketName": "market_name",
    "MarketType": "market_type",
    "SelectionId": "selection_id",
    "SelectionName": "selection_name",
    "Handicap": "handicap",
    "Price": "price",
    "BetType": "action",
}

# Campi presenti nel contratto ma deliberatamente NON inoltrati (vedi sopra).
CAMPI_NON_INOLTRATI: Tuple[str, ...] = ("MinPrice", "MaxPrice", "Points")

# `BetType` arriva dai parser nella forma italiana del Bridge. Pickfair usa
# BACK/LAY. La mappa e' tollerante sulle due lingue e sul maiuscolo/minuscolo.
AZIONI: Dict[str, str] = {
    "PUNTA": "BACK",
    "BANCA": "LAY",
    "BACK": "BACK",
    "LAY": "LAY",
}


def riga_vuota() -> Dict[str, str]:
    """Una riga con tutti i campi del contratto a stringa vuota."""
    return {campo: "" for campo in CAMPI_SEGNALE}


def normalizza_azione(valore: object) -> str:
    """`BetType` → `BACK`/`LAY`, oppure `""` se non riconosciuto.

    Fail-closed di proposito: un `BetType` che non si riconosce diventa
    stringa vuota e non un default. Con un default, un parser che sbaglia a
    scrivere il campo produrrebbe silenziosamente una PUNTA — e la direzione
    di una scommessa non e' un dettaglio su cui indovinare.
    """
    return AZIONI.get(str(valore or "").strip().upper(), "")


# Dove XTrader Signal Bridge teneva i suoi parser. Serve SOLO a diagnosticare:
# se qualcuno arriva dal Bridge e non trova piu' i suoi file, deve leggere un
# messaggio che dice dove sono, non un silenzioso "nessun parser".
#
# E' una stringa calcolata qui, NON un import di `core.config_store`: leggere
# da quella cartella come fonte riattaccherebbe Pickfair al Bridge, che e'
# esattamente cio' che questo modulo esiste per impedire. Non si carica mai
# nulla da li'.
NOME_CARTELLA_BRIDGE = "XTraderBridge"


def cartella_parser_del_bridge() -> str:
    """Percorso storico dei parser del Bridge. Solo per diagnostica."""
    base = (os.environ.get("APPDATA")
            or os.environ.get("XDG_CONFIG_HOME")
            or os.path.join(os.path.expanduser("~"), ".config"))
    return os.path.join(base, NOME_CARTELLA_BRIDGE, "parsers")


def cartella_parser() -> str:
    """Dove Pickfair tiene i Parser Personalizzati.

    Precedenza: `PICKFAIR_PARSERS_DIR` se valorizzata, altrimenti
    `~/.pickfair/parsers`.

    NON e' `%APPDATA%/XTraderBridge/parsers`: quella e' la cartella di un
    altro prodotto, e usarla legherebbe i dati di Pickfair alla presenza del
    Bridge.
    """
    esplicito = (os.environ.get(ENV_CARTELLA_PARSER) or "").strip()
    if esplicito:
        return esplicito
    return os.path.join(os.path.expanduser("~"), NOME_CARTELLA_DATI, "parsers")

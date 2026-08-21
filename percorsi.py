"""Dove Pickfair tiene le sue cose. Unica fonte.

## Perche' questo modulo esiste

`betfair_client.py` — il client Betfair, cioe' il percorso dei soldi —
cercava la configurazione del proxy chiamando `core.config_store.config_path()`,
che restituisce `%APPDATA%/XTraderBridge/config.json`.

Due problemi in una riga sola:

1. **Semantico.** Pickfair andava a cercare la propria configurazione dentro
   la cartella dati di **un altro prodotto**. Se qualcuno disinstallasse
   XTrader Signal Bridge, o non l'avesse mai avuto, quel percorso non
   direbbe niente — e il proxy tornerebbe a non essere configurato, che e'
   esattamente il difetto che #430 doveva chiudere.

2. **Strutturale.** Importare `core.config_store` trascina nel grafo vivo di
   Pickfair sei moduli del Bridge: `csv_writer`, `bridge_mode`,
   `confirmation_reader`, `token_store`, `language_select` e lo stesso
   `config_store`. Il commento che accompagnava quell'import diceva
   *«`core.config_store` importa a sua volta parti del progetto»*: il costo
   era stato notato e pagato lo stesso.

La convenzione di Pickfair esisteva gia' — `~/.pickfair/db.key` in
`config_registry` e `headless_main`, `~/.pickfair/parsers` dopo X1 — ma non
era scritta in nessun posto solo. Adesso lo e'.

## Regola

Nessun modulo di Pickfair chiede a `core.*` dove stanno i propri dati.
Chi ha bisogno di un percorso lo chiede qui.
"""

from __future__ import annotations

import os

# Cartella dati dell'utente. Il punto davanti la nasconde su Unix ed e'
# coerente con `~/.pickfair/db.key`, che il progetto usa gia'.
NOME_CARTELLA_DATI = ".pickfair"

# Sposta l'intera cartella dati: utile per i test, per piu' installazioni
# sulla stessa macchina, e per chi tiene i dati su un disco diverso.
ENV_CARTELLA_DATI = "PICKFAIR_DATA_DIR"

# Nome del file di configurazione.
NOME_CONFIG = "config.json"

# Cartella dati di XTrader Signal Bridge. Sta qui **solo per diagnostica**:
# serve a dire all'utente dove sono finiti i suoi file, mai a leggerli.
# E' una stringa calcolata, NON un import di `core.config_store`: importarlo
# ricreerebbe la dipendenza che questo modulo esiste per togliere.
NOME_CARTELLA_BRIDGE = "XTraderBridge"


def _base_utente() -> str:
    """La cartella di configurazione dell'utente, per piattaforma."""
    return (
        os.environ.get("APPDATA")
        or os.environ.get("XDG_CONFIG_HOME")
        or os.path.join(os.path.expanduser("~"), ".config")
    )


def cartella_dati() -> str:
    """La cartella dati di Pickfair.

    Precedenza: `PICKFAIR_DATA_DIR` se valorizzata, altrimenti
    `~/.pickfair`.
    """
    esplicito = (os.environ.get(ENV_CARTELLA_DATI) or "").strip()
    if esplicito:
        return esplicito
    return os.path.join(os.path.expanduser("~"), NOME_CARTELLA_DATI)


def dentro(*parti: str) -> str:
    """Un percorso dentro la cartella dati: `dentro("parsers")`."""
    return os.path.join(cartella_dati(), *parti)


def percorso_config() -> str:
    """Il `config.json` di Pickfair nella sua cartella dati."""
    return dentro(NOME_CONFIG)


def cartella_dati_del_bridge() -> str:
    """Dove XTrader Signal Bridge teneva i suoi dati. **Solo diagnostica.**

    Nessuna funzione di Pickfair deve leggere da qui. Serve a produrre un
    messaggio comprensibile — *«i tuoi file sono in questa cartella»* —
    invece di un silenzio, per chi arriva dal Bridge.
    """
    return os.path.join(_base_utente(), NOME_CARTELLA_BRIDGE)

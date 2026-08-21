"""Parser personalizzati di Pickfair.

Questo package esiste per una ragione precisa, scritta qui perche' non si
perda: nel repository convivono DUE prodotti. `core/` contiene sia moduli di
Pickfair (`core.trading_engine`, `core.risk_gate`, `core.session_recorder`)
sia moduli di **XTrader Signal Bridge**, un'applicazione diversa che legge
Telegram e scrive righe in un CSV che un programma terzo esegue.

La macchina dei Parser Personalizzati e' nata nel Bridge. Pickfair ne ha
bisogno — l'owner costruisce i parser a mano e non vuole segnali strutturati
dentro al codice — ma NON ha bisogno del CSV ne' della configurazione del
Bridge.

`parsers.campi` e' il punto in cui quella dipendenza viene tagliata: definisce
il contratto dei campi e la cartella dei parser **in termini di Pickfair**,
cosi' che il motore di estrazione non debba piu' importare `core.csv_writer`
ne' `core.config_store`.
"""

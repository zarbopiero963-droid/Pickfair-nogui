# Età del book per mercato (MarketTracker)

`core.market_tracker.MarketTracker` registra, per ogni mercato, l'istante **monotono** (`time.monotonic`, mai
l'orologio di sistema) in cui ha ricevuto l'ultimo book valido, da stream o da polling.

- `received_at(market_id)`: istante monotono dell'ultimo book; `None` se il mercato non è mai stato ricevuto.
- `book_age_s(market_id)`: età in secondi dell'ultimo book; `None` se mai ricevuto o se l'orologio non è coerente
  (età negativa). Un'età non dimostrabile non vale mai "fresco".
- `snapshot()["received_at"]`: mappa mercato → istante, per la diagnostica.

Un book non valido (non dict o senza `marketId`) non aggiorna nulla. Serve alla Control API (Claude C) per dichiarare
la freschezza del book senza dipendere da `lastMatchTime` dell'exchange; nessuna soglia d'età è introdotta qui e
nessuna decisione di trading cambia. Test: `tests/acceptance/test_market_book_age.py`.

# CatalogSync — sincronizzazione catalogo Betfair (#367)

`CatalogSyncService` (`services/catalog_sync_service.py`) popola il catalogo
locale (`bf_events`, `bf_markets`, `bf_runners`) dagli eventi/mercati Betfair,
prerequisito per identificare i mercati su cui operare.

## Come funziona

Riceve il client via `betfair_service.get_client()` (in LIVE `BetfairClient`, in
SIM `SimulationBroker`) e chiama i metodi di discovery **reali** dell'API Betfair
SportsAPING:

- `client.list_events([event_type_id], in_play_only=False)` → eventi (formato
  nativo Betfair: `{"event": {"id","name","openDate"}, "marketCount"}`).
- `client.list_market_catalogue([event_type_id], event_ids=[...], market_type_codes=[...])`
  → mercati con `runners`, `competition`, `marketStartTime`, `description.marketType`.

Il servizio mappa il formato nativo Betfair nelle colonne del DB
(`event.id`→`event_id`, `marketId`→`market_id`, `runners[].selectionId`, ecc.).
La `competition` non è in `listEvents`: viene presa dal catalogo mercati
(marketProjection `COMPETITION`).

### Batch, limite di peso e guard anti-troncamento

I mercati sono recuperati **in batch** (chunk di 20 eventi per chiamata,
`eventIds` multipli) per evitare il pattern N+1 e il rate-limit 429 in LIVE. Il
raggruppamento per evento usa `mk['event']['id']`, quindi il client **deve**
richiedere `EVENT` (e `COMPETITION`) nella `marketProjection` — garantito da
`BetfairClient.list_market_catalogue` e coperto da test di contratto.

`maxResults` è fissato a **200**, non 1000: con la projection `MARKET_DESCRIPTION`
Betfair applica un limite di peso dati e un valore troppo alto genera
`APINGException TOO_MUCH_DATA` in LIVE (stessa classe d'errore già rimossa da
`listEvents`, che non accetta `maxResults`). Un chunk da 20 eventi × ~5 tipi
mercato ≈ 100 mercati, ben sotto 200.

**Guard anti-troncamento (fail-safe money-path)**: se un batch ritorna un numero
di mercati ≥ `maxResults`, la risposta potrebbe essere troncata → sync
**incompleto**. In quel caso il servizio **salta `cleanup_stale_bf_data` e
`update_sync_meta`** e preserva il catalogo esistente, per non cancellare
mercati/runner ancora validi (che bloccherebbero/altererebbero il trading LIVE).
Idem su sync a 0 eventi (SIM o risposta vuota/anomala in LIVE).

## Storia (#367)

Prima del fix il servizio chiamava `list_soccer_events(live_only=...)` e
`list_market_catalogue(market_types=...)`, metodi **inesistenti** su
`BetfairClient`/`SimulationBroker` → `AttributeError`, 0 eventi sincronizzati
(bloccava il trading, sia in LIVE sia in SIM). Il fix espone `list_events` e
`list_market_catalogue` (con filtro `marketTypeCodes`) sul `BetfairClient`
(riusando `_post_jsonrpc`) e sul `SimulationBroker` (no-op fail-safe: in SIM il
catalogo arriva dai feed streaming, non via API), e riscrive il servizio sulle
firme/formati reali.

> **Verifica**: i test coprono mapping e firme con client mockato. La
> validazione end-to-end con dati reali dell'API Betfair va fatta **sul VPS**
> (numero eventi/mercati/runner > 0 in `bf_events`/`bf_markets`/`bf_runners`).

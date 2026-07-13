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

`maxResults` è fissato a **200**. Betfair accetta `maxResults` tra **1 e 1000**
(200 **non** è un default imposto dall'API: è il cap **adottato da questa
integrazione**). Il vincolo reale non è il conteggio ma il **limite di dati
pesati**: `somma(peso projection) × numero mercati ≤ 200 punti`, e **solo**
`MARKET_DESCRIPTION`/`RUNNER_METADATA` pesano. La projection qui usata include
`MARKET_DESCRIPTION` (peso 1) ma **non** `RUNNER_METADATA`; `RUNNER_DESCRIPTION`
non pesa → peso totale 1. `TOO_MUCH_DATA` (`APINGException`) scatta quando quel
prodotto pesato supera 200 punti: con peso 1, un `maxResults` alto (es. 1000)
può superarlo in LIVE. 200 tiene il prodotto entro il limite.

Il chunk da 20 eventi è una **euristica** per tenere basso il volume per
richiesta (20 eventi × ~5 tipi mercato ≈ 100 mercati, ben sotto 200): NON è una
garanzia formale, perché il numero reale di mercati dipende dal catalogo. Le
**guard** sotto restano l'autorità sul completamento del sync.

**Guard anti-troncamento e anti-fetch-parziale (fail-safe money-path)**: il
servizio **salta `cleanup_stale_bf_data` e `update_sync_meta`** — preservando il
catalogo esistente — in tutti i casi di sync non completo:
- **0 eventi** (SIM, o risposta vuota/anomala in LIVE);
- **batch troncato**: un batch ritorna un numero di mercati ≥ `maxResults`
  (risposta potenzialmente troncata → mercati mancanti);
- **eventi presenti ma 0 mercati** su tutti: guard aggregato per market-fetch
  interamente vuoto/anomalo, non un catalogo realmente vuoto;
- **chunk market-catalogue malformato** (risposta non-lista da errore upstream):
  `BetfairClient.list_market_catalogue` **solleva** invece di degradare a `[]`,
  quindi anche un fallimento su **un solo chunk** di un batch misto interrompe il
  sync prima del cleanup. Una lista vuota **genuina** (eventi senza mercati) è
  invece lecita e non blocca il cleanup.
Saltare il cleanup evita di cancellare mercati/runner ancora validi (che
bloccherebbero/altererebbero il trading LIVE).

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
> validazione end-to-end con dati reali dell'API Betfair va fatta **sul VPS**.
> Criteri di accettazione VPS:
> - numero eventi/mercati/runner > 0 in `bf_events`/`bf_markets`/`bf_runners`;
> - nessun `AttributeError` su `list_soccer_events` nei log del sync;
> - nessun `TOO_MUCH_DATA` / HTTP 429 / errore di rate-limit da `listEvents`
>   o `listMarketCatalogue`;
> - nessun warning "market-fetch sospetto incompleto" / "batch troncato"
>   ricorrente (indicherebbe fetch parziale cronico).

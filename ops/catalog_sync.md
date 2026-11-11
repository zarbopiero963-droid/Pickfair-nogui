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

## Esito del sync, concorrenza e pulizia (PR-20)

`run_sync()` restituisce sempre un esito `{"status", "reason", "events", "markets", "runners"}`:

| `status` | Significato | Catalogo e `sync_meta` |
| --- | --- | --- |
| `COMPLETED` | sync completo | aggiornati e ripuliti |
| `SKIPPED_FRESH` | ultimo sync leggibile e più recente di 12 ore (solo senza `force`) | invariati |
| `SKIPPED_BUSY` | un altro sync è in corso nello stesso processo | invariati |
| `INCOMPLETE` | `nessun_evento`, `nessun_evento_valido`, `zero_mercati`, `batch_troncato` | invariati |
| `FAILED` | eccezione (il `reason` è il tipo dell'errore) | invariati |

Solo `COMPLETED` significa catalogo aggiornato. I chiamanti attuali (avvio del runtime e comando della
mini GUI) non leggono ancora l'esito: la mini GUI scrive «Sincronizzazione catalogo completata» in ogni
caso — limite noto, fuori dallo scope di PR-20.

- **Un sync alla volta.** Il lock è di classe: l'avvio e il comando manuale creano istanze diverse sullo
  stesso DB, e due sync intrecciati si cancellerebbero le righe a vicenda. Il secondo riceve `SKIPPED_BUSY`
  senza chiamare Betfair. Il lock vale dentro un processo; fra processi vale l'autorità di runtime (#536).
- **Metadati illeggibili.** `last_sync_at` vuoto, non ISO o nel futuro non vale come «recente»: il sync
  parte. Prima l'eccezione di parsing usciva da `run_sync` e fermava il thread di avvio.
- **Bisezione.** Un gruppo di eventi che raggiunge `maxResults` viene diviso a metà e richiesto di nuovo
  finché ogni risposta sta sotto il limite. Resta `batch_troncato` solo un singolo evento che da solo
  raggiunge il limite.
- **`sync_id`** è un timestamp UTC ISO con i microsecondi sempre presenti.
- **Upsert.** Aggiornano tutti i campi mutabili: `open_date` ed `event_type_id` dell'evento; `event_id`,
  `market_name`, `market_type`, `open_date` del mercato; `handicap` e `sort_priority` del runner.
- **Pulizia.** `cleanup_stale_bf_data(sync_id)` rimuove in **una transazione** runner, mercati ed eventi
  non rivisti dal sync: un mercato chiuso o un runner ritirato di un evento ancora presente non resta
  risolvibile. `sync_id` vuoto o non stringa = `ValueError`, nessuna cancellazione.

## Resolver deterministico (PR-20)

`DeterministicResolver(db, max_catalog_age_sec=...)` (`services/deterministic_resolver.py`) traduce il testo
del provider in `event_id`, mercato e `selection_id`. `max_catalog_age_sec` è **obbligatorio**: non esiste
un'età massima implicita, la sceglie chi costruisce il resolver. Oggi nessun percorso di produzione lo
costruisce (lo usano i test del parser a delimitatori): il valore operativo va deciso quando verrà cablato.

I metodi `resolve_event_strict`, `resolve_market_strict`, `resolve_runner_strict` restituiscono una
`Resolution(ok, reason, value, candidates)`; i metodi storici `resolve_event`/`resolve_market`/
`resolve_runner` restituiscono il valore oppure `None` su qualunque rifiuto.

| `reason` | Quando |
| --- | --- |
| `OK` | un solo candidato |
| `INVALID_INPUT` | testo assente, vuoto o non stringa; `provider_id` non intero; id evento/mercato vuoto |
| `CATALOG_STALE` | `sync_meta` assente o illeggibile, sync più vecchio di `max_catalog_age_sec`, o nel futuro di oltre 60 s |
| `NOT_FOUND` | nessun candidato |
| `AMBIGUOUS` | più candidati diversi (`candidates` li elenca): mai «il primo che capita» |

Il confronto avviene su nomi normalizzati (senza accenti, minuscolo, spazi singoli) e **non** con `LIKE`:
`%` e `_` nel testo del provider sono caratteri qualsiasi. Un evento corrisponde se il nome coincide con
l'intero evento o con una delle due squadre di «Home v Away»; nessuna corrispondenza per sottostringa.
Un mercato si cerca per `market_type` dell'alias; con più mercati dello stesso tipo decide il
`market_name` dell'alias, altrimenti `AMBIGUOUS`.

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

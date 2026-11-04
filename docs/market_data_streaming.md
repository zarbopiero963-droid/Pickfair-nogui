# Streaming dei mercati Betfair (ESA)

Catena: `RuntimeController._start_market_data_feed` (solo LIVE, `market_data_mode` = `stream`/`hybrid` ed
`enabled` = True; default `poll`, spento) → `services.streaming_feed.StreamingFeed` → `BetfairClient.streaming`
(`core/betfair_streaming.py`) → `BetfairStream`/`StreamListener` di betfairlightweight → book di mercato →
`market_tracker`.

## Cosa è stato corretto (PF-BETFAIR-E2E-01)

1. **Interfaccia mancante.** Il `BetfairClient` non esponeva `streaming`: in modalità stream il feed falliva sempre
   (`STREAMING_INTERFACE_UNAVAILABLE`) e ricadeva sul REST. Ora `BetfairClient.streaming` restituisce lo `Streaming`
   di betfairlightweight legato ad `app_key` e `session_token` del client; senza sessione nessuna autenticazione
   (`STREAM_SESSION_MISSING`).
2. **TLS verificato.** betfairlightweight apre il socket con `ssl.CERT_NONE`; lo stream di Pickfair usa
   `ssl.create_default_context()` (certificato e nome host verificati).
3. **Sottoscrizione nel formato ESA.** I filtri viaggiavano con chiavi snake_case (`market_ids`, `ladder_levels`,
   campi estranei nel `marketDataFilter`) che Betfair non riconosce. Ora `streaming_market_filter` /
   `streaming_market_data_filter` di betfairlightweight (`marketIds`, `eventTypeIds`, `countryCodes`, `marketTypes`,
   `fields`, `ladderLevels`).
4. **Chiusura del trasporto rilevata subito.** La morte del thread lettore (chiusura dal server, errore di socket)
   non arrivava al ciclo del feed, che restava «connesso» fino al timeout dell'heartbeat. Ora è una disconnessione
   (`STREAM_READER_FAILED:...`) entro un secondo, con riconnessione e backoff esistenti.

## Uso dei dati e fail-closed

- Lo streaming fornisce solo dati di mercato: nessun ordine passa di qui.
- I **nuovi ingressi non si basano sulla cache dello stream**: l'ammissione (F-23) legge un book REST fresco. Stream
  assente, heartbeat mancante, riconnessione incompleta o divergenza REST/stream non possono quindi autorizzare un
  ingresso.
- Le uscite protettive (hedge del cashout) non dipendono dallo stream.

## Prove

`tests/acceptance/test_streaming_betfair_esa.py`: pila reale (`BetfairClient` → betfairlightweight → `StreamingFeed`)
con un socket finto che riproduce messaggi ESA **sintetici** (MOCK, non registrati): autenticazione e sottoscrizione
nel formato ESA, immagine iniziale, delta incrementale, OPEN → SUSPENDED → OPEN → CLOSED, selezione ritirata,
heartbeat, due mercati, chiusura dal server, TLS verificato.

## Da verificare al livello B (read-only reale, DEFERRED_ACCESS)

- Host dello stream per l'Exchange italiano: si usa quello della libreria (`stream-api.betfair.com`), UNVERIFIED.
- Forma reale dei messaggi `mcm` del conto (campi del `marketDefinition` italiano).
- Ripresa con `initialClk`/`clk`: con i resource di betfairlightweight il feed non conserva i `clk` e alla
  riconnessione ri-sottoscrive da un'immagine completa (sicuro, più traffico).

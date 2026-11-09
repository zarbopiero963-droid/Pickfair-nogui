# Scatola nera (PF-BLACKBOX-01)

La scatola nera è il registro cronologico permanente di Pickfair: un solo file, `blackbox.sqlite`, che il programma
scrive dal primo istante di avvio fino allo spegnimento.

Architettura e piano completo: `docs/roadmap/pf_blackbox_01.md`, sul branch del registro. Questo documento descrive
ciò che è **già nel codice**.

## Stato: BB-1 (nucleo e ciclo di vita)

| Parte | Stato |
|---|---|
| Registro unico, append-only, catena di hash, verifica | **presente** (`core/blackbox.py`) |
| Avvio prima dei componenti, arresto, crash, riavvio non pulito | **presente** (`core/blackbox_runtime.py`, `headless_main.main`) |
| Lettura per GUI, CLI, Control API e MCP | **presente** (`core/blackbox_query.py`) |
| Impostazioni (vecchio/nuovo valore) e comandi | BB-2, non ancora |
| Ordini, decisioni, fondi, P&L, gate fail-closed prima dell'invio | BB-3, non ancora |
| SL/TP/trailing, mercati sospesi | BB-4, non ancora |
| Visualizzatore nell'app (Windows/Linux) | BB-5, non ancora |
| Strumenti MCP read-only | BB-6 (Claude C), non ancora |

Finché BB-3 non è integrato, le operazioni finanziarie **non** sono ancora registrate qui: il registro copre il ciclo
di vita del processo.

## Dove si trova

- **Percorso:** `<cartella dati>/blackbox/blackbox.sqlite`, dove la cartella dati è `~/.pickfair` oppure
  `PICKFAIR_DATA_DIR`. Si può spostare con `PICKFAIR_BLACKBOX_PATH`.
- **Dump nativi:** nella stessa cartella c'è `fault.log`, con i dump dei crash nativi (`faulthandler`). Al riavvio
  l'ultima parte entra nel registro come `CRASH_RECOVERY` e il file viene ruotato in `fault.log.1`.

## Garanzie

- **Append-only.** Un trigger SQLite rifiuta UPDATE e DELETE sulla tabella `events`. Retention e rotazione
  arriveranno con una regola che non cancella mai eventi di ordini non riconciliati; oggi nulla viene cancellato.
- **Sequenza e integrità.**
  - `seq` è globale e persistente. È assegnato dentro `BEGIN IMMEDIATE`, quindi resta corretto con più thread e più
    processi.
  - `session_seq` numera gli eventi della singola sessione.
  - Ogni record porta `hash = sha256(prev_hash + JSON canonico)`.
  - `BlackBox.verify()` rileva record mancanti (`SEQ_GAP`), alterati (`HASH_MISMATCH`) o riordinati (`CHAIN_BROKEN`).
- **Durabilità.** WAL, `synchronous=FULL`, una transazione per evento.
- **Nessun segreto.**
  - I valori delle chiavi sensibili (password, token, chiavi) diventano `[REDACTED]`.
  - Token e segreti registrati vengono oscurati anche dentro i testi.
  - Gli argomenti della riga di comando come `--password` sono oscurati anche quando il valore è l'argomento
    successivo.
- **Richiesta ≠ conferma.**
  - `phase` vale ATTEMPT, RESULT oppure OBSERVATION.
  - `outcome` vale VERIFIED, PENDING, UNKNOWN, REJECTED, FAILED oppure NOT_APPLICABLE.
  - Nessuno dei due viene mai dedotto.

## Tempo (decisione owner D2)

- Ogni evento ha `ts_utc` in RFC 3339 UTC canonico `YYYY-MM-DDTHH:MM:SS.ffffffZ`; un orario fornito dal chiamante in
  altra forma (secondi senza frazione, millisecondi, fuso diverso da `Z`, data impossibile) è rifiutato.
- `exchange_ts` è l'ora dichiarata dall'exchange, distinta dall'ora osservata da Pickfair; se manca resta `null` (mai
  inventata).
- L'ordine degli eventi fa fede su `seq`, mai sull'orario; la GUI converte in ora locale solo per la visualizzazione.

## Un solo runtime per registro (decisione owner D3)

- `start_session` prende un lock esclusivo su `<registro>.runtime.lock` (fcntl su Linux, msvcrt su Windows), tenuto
  fino alla chiusura. Un secondo runtime riceve `RUNTIME_ALREADY_ACTIVE` e `headless_main` non parte (exit 1).
- I lettori (Control API, MCP, CLI) non prendono il lock e non aprono sessioni.
- Non certifica il fencing finanziario del motore (#536, separato).

## Avvio, arresto e crash

1. `headless_main.main()` installa la scatola nera **prima** di costruire qualunque componente. Il primo evento è
   `PROCESS_START`. Se il registro non si apre, il processo **non parte** (exit 1): senza cronologia non c'è trading.
2. Alla fine della costruzione viene registrato `COMPONENT_INIT`.
3. `--emergency-stop` registra la richiesta (ATTEMPT) e l'esito (RESULT VERIFIED o FAILED).
4. Le eccezioni non gestite vengono registrate con tipo, messaggio e traceback redatti:
   - nel processo: `UNCAUGHT_EXCEPTION`;
   - nei thread: `TASK_EXCEPTION`.
5. All'uscita si registra `SHUTDOWN`, con `reason_code` uguale a `STOP` oppure `EXIT_<codice>`.
6. **Riavvio dopo un crash** (nessun `SHUTDOWN`):
   - `UNCLEAN_TERMINATION` con la sessione precedente;
   - una `BLACKBOX_GAP` con `outcome UNKNOWN` sull'intervallo `[ultimo evento, riavvio]`.

   Ciò che è successo in quell'intervallo non viene mai inventato.

## Guasti di scrittura (disco pieno, I/O)

`BlackBox.record` ha due modalità.

**Evento non critico (`critical=False`).** Non solleva mai: l'evento perso viene contato. Al ripristino si
registrano, nell'ordine:
- una `BLACKBOX_GAP` con il conteggio;
- `BLACKBOX_RECOVERED`.

**Evento critico (`critical=True`).** O viene scritto in modo durevole, o solleva `BlackBoxUnavailable`, e il
chiamante non esegue l'azione. Da BB-3 vale prima di ogni ordine, cancel, hedge o cashout.

**Uscita protettiva già decisa (`critical=True, buffer_on_failure=True`).** L'evento va nel buffer critico in memoria.
Al ripristino si scrivono, nell'ordine:
1. l'evento originale, con `data.buffered=true`;
2. una `BLACKBOX_GAP` con `from_ts`, `to_ts` e i `refs` degli ordini;
3. `BLACKBOX_RECOVERED`.

Un nuovo ingresso non si bufferizza mai.

## Lettura (`core/blackbox_query.BlackBoxReader`)

Contratto con il lato Control API/MCP di Claude C (`BLACKBOX_READ_CONTRACT.md` 1.13.0):

| Funzione | Cosa restituisce |
|---|---|
| `status()` | salute del registro, `as_of_seq`, eventi persi o in buffer, `not_reported` |
| `sessions()` | le sessioni, con l'indicazione di chiusura pulita |
| `query(filters, after_seq, limit)` | `records`, `next_after_seq` (0 se non ci sono altre pagine), `gaps_in_range`, `as_of_seq` |
| `get(event_id, chain_depth)` | l'evento con la catena di `causation_id` |
| `request_hook(record)` | registra una richiesta HTTP della Control API |

Dettagli:
- **Filtri ammessi:** `from_ts`, `to_ts` (ISO UTC), `session_id`, `table_id`, `market_id`, `selection_id`, `order_id`, `bet_id`,
  `strategy`, `type`, `category`, `source`, `severity`, `phase`, `outcome`, `correlation_id`, `causation_id`. Un
  filtro sconosciuto è un errore.
- **`request_hook(record)`:**
  - una richiesta servita produce ATTEMPT e RESULT; una richiesta rifiutata solo RESULT;
  - oltre 20 record/s (raffica 40) le richieste vengono aggregate in `coalesced_before`;
  - se restituisce False, la Control API risponde 503.
- **Identificativi dell'ordine** (decisione owner D1):
  - `order_id` interno: `orders.id` del ledger, un intero progressivo (uuid solo con DB degradato);
  - `customer_ref` (customerOrderRef inviato all'exchange) e `correlation_id`: identificativi applicativi;
  - `bet_id`: riferimento dell'exchange;
  - **verso Control API e MCP** esce solo `order_id = ord_<32 hex>`: HMAC-SHA256 dell'id interno con una chiave casuale
    del registro (`meta.order_alias_key`), stabile ai riavvii e non enumerabile. Dall'esterno si filtra per alias;
    l'id interno non è un filtro esterno. `order_ref`/`customer_ref` non escono mai, a nessuna profondità.
- **Identificativi:** `event_id`, `correlation_id` e `causation_id` sono uuid registrati in forma unica, 32 hex
  minuscoli: la forma con trattini (contratto MCP 1.14.0) viene normalizzata sia in scrittura sia in lettura, quindi le
  due forme sono lo stesso id (nessun duplicato, stessi risultati).
- **Vista esterna** (Control API/MCP): `order_ref`/`customer_ref` non vengono esposti. I campi non registrati sono
  elencati in `not_recorded`.
- **Lacune:** le lacune dichiarate tornano come tali in `gaps_in_range`, mai ricostruite.

## Vocabolario

È lo stesso di `core.event_journal.EVENT_CATEGORIES`, esteso con:
- le categorie `BLACKBOX` e `CONTROL`;
- i tipi del ciclo di vita, della catena di protezione SL/TP/trailing, delle riserve di fondi e della riconciliazione
  per ordine.

Un tipo sconosciuto viene rifiutato.

## Test

- `tests/blackbox/test_blackbox_core.py` e `tests/blackbox/test_blackbox_runtime.py`, con SQLite reale e offline:
  - integrità e manomissione;
  - redazione;
  - guasti di scrittura e buffer critico;
  - sessioni non chiuse;
  - concorrenza su 8 thread e su 4 processi;
  - contratto di lettura;
  - cablaggio dell'entrypoint;
  - hook dei crash;
  - dump nativi.
- Mutanti `bb_*` in `guardrails/mutation_catalog.json`, area `blackbox`.
- Suite `blackbox` nel catalogo dei test (`tests/catalog/pf-test-catalog.json`, OFFLINE, campagna `all`).

**Limiti onesti:**
- l'avvio reale su Windows e il crash nativo vero (segfault) restano MANUAL_ONLY;
- i test simulano il dump e il disco pieno.

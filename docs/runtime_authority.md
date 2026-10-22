# Autorità esclusiva del runtime — lock OS, identità del lock, lease con fencing (#536)

Fonte: issue **#536 PF-RUNTIME-LOCK-01** (mandato owner PF-536-ISOLATED-IMPLEMENTATION-01, 22/10/2026),
decisione owner ratificata «un solo runtime attivo per database». Codice: `core/runtime_authority.py`
(`RuntimeAuthority`, `RuntimeAuthorityLost`), `database.py` (tabella `runtime_lease`, `bind_runtime_fence`,
`RuntimeFenceViolation`), `core/runtime_controller.py` (acquisizione allo START, guard sui percorsi finanziari,
LOCKDOWN alla perdita), `core/trading_engine.py` (choke point prima dell'invio e ricontrollo dopo).
Test: `tests/acceptance/test_issue536_runtime_authority.py`.

## Problema

Il lock del sistema operativo su `<db>.runtime.lock` (PR29-c) impedisce un secondo `start()` sullo stesso
database, ma un `flock` protegge l'inode, non il percorso: se il file viene cancellato o sostituito mentre il
runtime è attivo, un secondo runtime prende un lock su un nuovo file e i due operano sullo stesso DB credendosi
entrambi proprietari. I tavoli Roserpina vivono in memoria per processo: due copie attive divergono e possono
piazzare ordini o azzerare cicli in modo non coordinato.

## Architettura: tre strati, tutti obbligatori

Nessuno dei tre strati è disattivabile da configurazione, né in SIM né in LIVE.

1. **Lock OS** (`RuntimeAuthority.acquire_os_lock`): `fcntl.flock` esclusivo non bloccante su POSIX,
   `msvcrt.locking` del byte 0 su Windows, sul percorso canonico `realpath(db) + ".runtime.lock"`. Fallito ⇒
   `RUNTIME_INSTANCE_ACTIVE`, come prima.
2. **Identità del file di lock**: all'acquisizione si memorizzano `(st_dev, st_ino)` del descrittore e nel file
   si scrive il record JSON del proprietario (`owner_id`, `pid`, `host`, `acquired_at`). Ogni verifica confronta
   il descrittore con `os.stat(percorso)`: percorso assente o `nlink == 0` ⇒ `lock_file_deleted`; inode diverso
   ⇒ `lock_file_replaced`. In entrambi i casi l'autorità è persa in modo definitivo.
3. **Lease persistente con fencing token** (tabella `runtime_lease`, riga singola `id = 1`, additiva —
   `CREATE TABLE IF NOT EXISTS`, nessuna colonna esistente toccata):
   - `acquire_runtime_lease` in `BEGIN IMMEDIATE`: riesce solo se la lease manca, è scaduta, è già di questo
     `owner_id`, o il detentore è **dimostrabilmente morto sullo stesso host** (`_holder_alive` restituisce
     False: pid inesistente). Non dimostrabile (altro host, pid non valido, errore) ⇒ non si scavalca. Il
     `fencing_token` è monotono (`+1` a ogni acquisizione) e l'UPDATE è condizionato al token letto
     (`concurrent_update` ⇒ rifiuto);
   - `renew_runtime_lease` (heartbeat ogni 10 s, TTL 30 s — costanti `RuntimeController.RUNTIME_LEASE_TTL_SEC`
     e `RUNTIME_LEASE_HEARTBEAT_SEC`, non chiavi di config) rinnova solo se owner e token coincidono **e la lease
     non è scaduta**; altrimenti `None` ⇒ `lease_superseded`;
   - `release_runtime_lease` scade subito la lease, solo se owner e token sono i propri;
   - `bind_runtime_fence(owner_id, token)`: da quel momento ogni scrittura del `Database` — `transaction()`
     (dopo `BEGIN`), `_settlement_transaction` (dopo `BEGIN IMMEDIATE`) e le scritture autocommit
     `INSERT/UPDATE/DELETE/REPLACE` in `_execute` — verifica **dentro la stessa transazione** che la riga
     `runtime_lease` sia ancora di questo owner con questo token e non scaduta; altrimenti
     `RuntimeFenceViolation(lease_missing | fencing_token_superseded | lease_expired)`, rollback, e callback
     `on_violation` che marca l'autorità persa. Un runtime superato non può scrivere stato finanziario, qualunque
     cosa creda di essere.

## Sequenza allo START e allo STOP

`start()` (serializzato dal lock di ciclo di vita): lock OS → lease (`RUNTIME_LEASE_HELD`, evento
`RUNTIME_START_REFUSED`, lock rilasciato se la lease è di un altro runtime vivo) → heartbeat → servizi. Un
database su file arma sempre l'autorità; con DB `:memory:` (solo test single-process) non c'è lock né lease.
`stop()` riuscito: heartbeat fermo, lease scaduta (solo se ancora nostra), fence sciolto, descrittore chiuso.
Uno STOP fallito trattiene lock e lease come in PR29-c; uno STOP ripetuto o concorrente non rilascia due volte.

## Percorsi finanziari protetti (`_runtime_authority_block(context)` ⇒ `verify`)

| Percorso | Punto di verifica | Esito al rifiuto |
|---|---|---|
| Segnale Telegram / Quick Bet | `_route_signal…` subito prima di `CMD_QUICK_BET` | riserva rilasciata, segnale rifiutato con `runtime_authority_lost:<motivo>` |
| Auto-next Roserpina | prima dell'invio dell'ordine successivo | tavolo sbloccato, nessun ordine |
| Cashout (routing) | `_route_cashout_signal_under_barrier` | segnale rifiutato |
| **Cashout (piazzamento dell'hedge)** | `CashoutExecutor` subito prima di `OrderRouter.place`, con `authority_check` = `RuntimeController.cashout_authority_check` cablato da `headless_main` e `mini_gui` tramite `cashout_wiring.cabla_catena_cashout(runtime_authority_check=...)` | `CASHOUT_FAILED` `REJECTED`, nessun hedge; errore della verifica ⇒ `runtime_authority_check_failed` |
| Reset di ciclo | `reset_cycle` | `{"reset": False, "ok": False, "reason": …}` |
| Sweep F028 (ordini mai abbinati) | `_release_final_unmatched_orders` | nessun rilascio |
| **Ogni invio** (SIM e LIVE, manuale, dutching, copy) | `TradingEngine._submit_to_order_path` → `RuntimeController.assert_runtime_authority("order_submit")` | `RuntimeAuthorityLost` prima della chiamata all'exchange |
| Dopo l'invio | `after_order_send_authority_check(result)` | evento `RUNTIME_AUTHORITY_LOST_AFTER_SEND` con `correlation_id`/`customer_ref` |
| LIVE | `is_live_allowed()` | False se lock o lease non sono dimostrati adesso (verifica completa, non solo lo stato in memoria) |

`verify` è fail-closed: lock non tenuto, file cancellato/sostituito, lease non acquisita, lease di un altro owner
o con token diverso, lease scaduta ⇒ `RuntimeAuthorityLost` **permanente** (`mark_lost`). DB non leggibile sul
momento ⇒ `RuntimeAuthorityLost("lease_unreadable", permanent=False)`: l'operazione è rifiutata ma il runtime
non è ancora dichiarato superato (l'heartbeat decide: se non si riesce più a rinnovare prima della scadenza ⇒
`lease_unrenewable`, perdita definitiva).

## Regole aggiuntive dopo la review di `fc8f136`

- **Scritture in autocommit**: con il fence vincolato, `_execute` apre `BEGIN IMMEDIATE`, verifica il fence, scrive
  e fa commit nella stessa transazione. Un subentro fra controllo e scrittura attende il commit.
- **Dopo lo STOP**: un'autorità rilasciata blocca (`runtime_authority_released`). Il bus resta vivo e un comando in
  coda non passa come se non ci fosse autorità da verificare. Senza autorità mai armata (DB in memoria, harness)
  non c'è nulla da verificare.
- **Autorità persa ⇒ STOP obbligatorio**: `acquire_lease` rifiuta (`runtime_authority_lost:<motivo>:stop_required`).
  Una nuova epoca parte solo dopo lo STOP che smonta servizi e autorità.
- **TTL degli unmatched**: `_poll_direct_unmatched_ttl` non cancella nulla senza autorità (il feed può restare vivo).
- **Istante del rinnovo**: `renew_runtime_lease` e `acquire_runtime_lease` leggono l'orologio dopo `BEGIN IMMEDIATE`.
  Un'attesa del lock del DB oltre la scadenza non resuscita una lease scaduta.
- **Ordine dei lock**: `RuntimeAuthority` non fa I/O sul database mentre tiene il proprio lock, e le violazioni del
  fence in autocommit si notificano fuori dal write lock: nessun deadlock fra heartbeat e segnalazione della perdita.
- `verify` su un'autorità mai armata rifiuta senza marcarla persa.

## Regole aggiuntive dopo la review di `125aa05`

- **Cancel del cashout e del TTL**: `CashoutCancelAdapter(authority_check=...)` verifica l'autorità al momento
  della chiamata al broker. Vale per il router del cashout, per il residuo (`cashout_wiring`) e per il TTL degli
  unmatched. Persa o in errore ⇒ cancel non confermato, nessuna chiamata.
- **Ammissione**: `TradingEngine` verifica l'autorità prima di persistere l'intento (nessuna riga `INFLIGHT` da un
  runtime superato) e di nuovo subito prima del trasporto.
- **STOP e operazioni in volo**: ogni invio gira dentro `RuntimeController.authority_operation()`. Lo STOP segna
  `runtime_authority_draining` (nessuna nuova operazione), attende fino a `RUNTIME_AUTHORITY_DRAIN_SEC` (15 s) che
  le operazioni in volo finiscano, poi scioglie lease e fence. Non svuotate ⇒ autorità, lock, lease e fence
  restano, evento `RUNTIME_AUTHORITY_RELEASE_PENDING`, e il prossimo STOP ritenta. Nessuna nuova epoca parte con
  operazioni della precedente in volo.
- **Rilascio fallito**: se la lease non si può scadere, `release()` restituisce False, l'heartbeat riprende e
  autorità, lock e fence restano: lo STOP non si dichiara pulito con una lease viva.
- **Rinnovo concorrente alla perdita**: se la perdita o lo STOP arrivano mentre il rinnovo è in corso, la lease
  rinnovata torna subito scaduta. `acquire_lease` non fa I/O sul database sotto il lock dell'autorità.
- **Hedge del cashout in volo** (Grok su `1542b5d`): `CashoutExecutor(authority_operation=...)` piazza l'hedge
  dentro `authority_operation()`, cablato da `cashout_wiring(runtime_authority_operation=...)` in headless e GUI;
  lo STOP lo attende come gli invii dell'engine; se l'operazione non si registra il cashout è `REJECTED`.
- **Transazioni `transaction()` con fence**: aprono `BEGIN IMMEDIATE`. La prima istruzione è la lettura della lease:
  in una `BEGIN` differita l'upgrade a scrittura fallirebbe subito (`database is locked`, senza busy timeout) se
  l'heartbeat rinnova nel mezzo, e la scrittura — per esempio la baseline F032 — andrebbe persa (CI, `F032_01`).
- **Transazioni esplicite** (`begin_transaction`/`commit_transaction`, usate dalla riconciliazione): con il fence
  vincolato aprono `BEGIN IMMEDIATE` e verificano la lease; superata ⇒ `RuntimeFenceViolation`, nessuna transazione;
  qualunque altro errore della verifica fa rollback (il lock di scrittura non resta preso).
- **Emergency stop**: il LOCKDOWN locale resta sempre; il cancel-all di mercato si interrompe, riverificando prima
  di ogni mercato, se l'autorità è persa in modo definitivo (lock cancellato/sostituito, lease superata o scaduta, autorità rilasciata), perché gli unmatched
  possono essere del nuovo proprietario. Autorità solo illeggibile sul momento (DB) ⇒ cancel ammesso.
- **TTL degli unmatched**: un cancel non tentato perché l'autorità non è dimostrata conserva il candidato
  (`DIRECT_UNMATCHED_CANCEL_DEFERRED`); solo un tentativo reale lo consuma. Dopo la review Grok di `589cdb5`: il
  motivo del rifiuto arriva da `CashoutCancelAdapter.cancel_with_outcome`, locale alla chiamata (non dallo stato
  d'istanza `last_block`, che un cancel concorrente sullo stesso adapter azzererebbe); il `bet_id` esce dal
  registry **prima** della chiamata al broker, come su main, e la presa in carico (`pop` per chiave) fa da confine
  fra giri TTL sovrapposti (stream e REST fallback): chi non la ottiene non invia un secondo cancel; il candidato
  torna nel registry solo se il cancel non è stato tentato. Dopo la review Sol di `2a5a6c2`: scansione e rimozione
  avvengono sotto `_DIRECT_TTL_CLAIM_LOCK`, così anche con più riferimenti allo stesso ordine tutte le voci vanno a
  un solo giro.
- **START**: l'autorità si riverifica subito prima dell'attivazione; persa durante connessione o recovery ⇒
  `RUNTIME_AUTHORITY_LOST_DURING_START`, servizi smontati, lock e lease rilasciati, nessun `RUNTIME_STARTED`.
- **STOP dopo una perdita**: la scadenza della lease si ritenta anche se `mark_lost` non era riuscito (scoped su
  owner e token).
- Limite residuo: un task dell'executor ancora in corso dopo un esito AMBIGUOUS (attesa scaduta) non è contato fra
  le operazioni in volo; il suo ordine resta alla riconciliazione.

## Perdita dell'autorità

`mark_lost(motivo)` (idempotente): lease scaduta subito se ancora nostra (così il proprietario legittimo può
subentrare senza aspettare il TTL), heartbeat fermato, fence lasciato vincolato (ogni scrittura successiva è
rifiutata), callback al controller. `RuntimeController._on_runtime_authority_lost`: `live_enabled = False`,
`mode = LOCKDOWN`, `last_error = RUNTIME_AUTHORITY_LOST:<motivo>`, poller dei settlement fermato, evento
`RUNTIME_AUTHORITY_LOST`. **Nessun cancel-all**: gli ordini aperti sull'exchange possono essere del nuovo
proprietario. Nessuna scrittura di LOCKDOWN nel DB: il fence la rifiuterebbe comunque.

## Recupero dopo crash, riavvio e avvii concorrenti

- Crash del processo: il SO rilascia il flock; la lease resta nel DB fino alla scadenza (≤ 30 s) **oppure**
  viene scavalcata subito se il pid del detentore non esiste più sullo stesso host. Il nuovo runtime ottiene
  `fencing_token + 1`, ricostruisce i tavoli (F018) e riconcilia; un eventuale processo fantasma con il token
  precedente è rifiutato dal fence alla prima scrittura.
- Riavvio ordinato: STOP scade la lease; il nuovo START la riacquisisce con token maggiore (monotono anche
  attraverso i riavvii: la riga non viene mai cancellata).
- Avvii simultanei: il flock serializza sullo stesso file; con file cancellato/sostituito la lease in
  `BEGIN IMMEDIATE` fa da secondo gate: uno solo acquisisce, gli altri ricevono `RUNTIME_LEASE_HELD`.
- Lease scaduta con il vecchio processo ancora vivo (DB lento, processo sospeso): il vecchio runtime perde
  l'autorità al primo `verify`/heartbeat (`lease_expired` o `lease_superseded`) e va in LOCKDOWN; il nuovo
  proprietario ha un token maggiore e il fence rifiuta le scritture del vecchio.

## Limiti dichiarati (nessun falso senso di sicurezza)

- **Finestra TOCTOU fra `verify` e la chiamata all'exchange.** Non può essere fenced: l'exchange non conosce il
  token. Un ordine può partire con autorità appena persa; il ricontrollo dopo l'invio lo segnala
  (`RUNTIME_AUTHORITY_LOST_AFTER_SEND`), la riga `orders` del runtime superato è rifiutata dal fence e l'ordine
  (con il `customer_ref` di quel runtime) resta sull'exchange per la riconciliazione del nuovo proprietario.
  Non si ritenta e non si cancella alla cieca.
- **Stesso host, orologio condiviso.** Il confronto dei pid e le scadenze (`time.time`) lo assumono. Un database
  su filesystem di rete (NFS/SMB) non è supportato: né il flock né `st_ino` sono affidabili.
- **Windows**: `st_ino` è l'indice del file NTFS; la sostituzione è riconosciuta se l'indice cambia. Non eseguito
  in CI (MANUAL_ONLY).
- **Processo sospeso oltre il TTL** (SIGSTOP, pausa del debugger): tra la ripresa e il primo `verify` nessun
  ordine parte, perché ogni invio passa da `verify`; ma una chiamata all'exchange già in volo al momento della
  sospensione rientra nella finestra TOCTOU sopra.
- **Crash fra la presa in carico del TTL e l'esito del cancel.** Il registry degli ordini DIRECT del TTL è in
  memoria per scelta di main (B6.3.2a: vuoto al riavvio, così un `bet_id` senza identità certa non è mai
  candidato). Dopo un crash l'ordine resta nel ledger persistente del bot (`get_bot_active_orders`), lo stato
  vero è quello dell'exchange (`list_current_orders`) e nessun cancel parte alla cieca; il cancel automatico per
  TTL di quell'ordine però **non si ripete**: resta all'intervento manuale o agli altri percorsi (cashout,
  settlement). Comportamento identico a main, provato da T57 (crash prima e dopo la chiamata al broker).
- **In LOCKDOWN** il consumer dei settlement può tentare scritture che il fence rifiuta: i settlement restano
  trattenuti e vengono riconciliati dal runtime che detiene l'autorità.
- Il bus degli eventi è **asincrono**: una verifica al routing non copre il momento in cui un consumer piazza
  l'ordine. Per questo la verifica sta anche nei punti di piazzamento (engine e `CashoutExecutor`). Il monitor
  SL/TP (`_trigger_auto_close_table`) pubblica `REQ_EXECUTE_CASHOUT` senza verifica propria: l'hedge è fermato
  dall'esecutore.
- Nessun cap monetario, nessuna nuova modalità, nessun flag: lo stesso motore SIM/LIVE.

## Prove (test HARD REAL, `tests/acceptance/test_issue536_runtime_authority.py`)

Lock cancellato e sostituito durante l'attività (T01/T02); tre avvii simultanei (T03); due processi OS reali con
lock cancellato e kill del detentore (T04); crash multi-tavolo con `os._exit` e ricostruzione con token
maggiore e fantasma rifiutato (T05); riavvio con token monotono (T06); STOP fallito, ripetuto e concorrente
(T07); lease scaduta con vecchio processo vivo (T08/T08b); DB temporaneamente indisponibile (T09); perdita
durante l'ammissione (T10) e fra verifica e invio (T11); fence nelle transazioni e in autocommit (T12/T12b);
nessun rilascio né scrittura da un runtime superato (T13); START arma sempre l'autorità (T15); heartbeat (T16);
`_holder_alive` (T17); `verify` fail-closed (T18); auto-next, engine e cashout rifiutati senza autorità
(T19–T21); sweep F028, perdita senza oggetto autorità, `is_live_allowed`, token superato dello stesso owner,
lease scaduta (T22–T26); hedge del cashout rifiutato al piazzamento, verifica in errore fail-closed, cablaggio
reale in `HeadlessApp` e `MiniPickfairGUI` (T27–T30); rilievi Codex su `fc8f136`: scrittura autocommit
serializzata col subentro, nessun invio dopo lo STOP, TTL unmatched senza autorità, START rifiutato dopo la
perdita senza STOP, rinnovo misurato dopo l'attesa del lock, nessun deadlock heartbeat/fence (T31–T36);
rilievi su `125aa05`: cancel del cashout/TTL/residuo rifiutati, nessuna riga INFLIGHT senza autorità, verifica
prima del trasporto, STOP che attende le operazioni in volo, rilascio fallito, rinnovo annullato (T37–T42, T20b);
STOP che attende l'hedge del cashout in volo (T43); rilievi Codex su `1542b5d`: transazione esplicita fenced,
scadenza ritentata allo STOP, emergency stop, candidato TTL conservato, START non attivo dopo la perdita (T44–T48);
rilievi Sol su `9a80162`: emergency stop riverificato per mercato, NO RETRY del TTL solo dopo un tentativo (T49–T50);
rilievi Grok su `9a80162`: rollback su ogni errore in `begin_transaction`, cashout REJECTED se l'operazione in volo
non si registra (T51–T52); transazione fenced che non fallisce se l'heartbeat rinnova nel mezzo (T53); rilievi Grok su `589cdb5`: esito del
cancel TTL locale alla chiamata, `bet_id` fuori dal registry durante il broker, nessun secondo cancel da un giro
sovrapposto (T54–T56); crash fra presa in carico ed esito del cancel TTL, prima e dopo il broker: ordine nel
ledger, stato dell'exchange riconciliabile, nessun cancel alla cieca al riavvio (T57);
rilievo Sol su `2a5a6c2`: presa in carico atomica per `bet_id` con più riferimenti e due giri concorrenti (T58). Mutazioni `i536_*` in `guardrails/mutation_catalog.json`.

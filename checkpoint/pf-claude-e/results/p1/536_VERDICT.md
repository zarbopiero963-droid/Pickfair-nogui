# Verifica indipendente #536 PF-RUNTIME-LOCK-01 — verdetto finale

Data: 2026-11-08. Solo lettura: nessuna modifica a issue, PR o repository. Ambiente: Linux, Python 3.13, venv
locale (`p1_536/venv`, requirements-test.txt). Alberi `git worktree` dal clone completo `scratchpad/fresh`:
- `p1_536/main` = main `a09d8d61`
- `p1_536/pre`  = `152613b3` (padre dello squash di #537 `3bb4477d`)
- `p1_536/mut`  = main `a09d8d61` con catalogo mutazioni filtrato ai soli `i536_*`

## VERDETTO: **CLOSE_WITH_TRANSFER**

Il difetto della issue (cancellazione/sostituzione di `.runtime.lock` ⇒ due runtime attivi) è **riprodotto prima del
fix e assente su main**, con due processi OS reali. Il meccanismo a tre strati è presente, cablato e coperto da test
e mutazioni che passano. Restano però criteri **non chiusi dentro #536** (checklist Windows, verifica multi-processo
della sostituzione nella suite, review indipendente sul diff assemblato) e limiti dichiarati che oggi vivono **solo**
in `docs/runtime_authority.md`, senza un tracker. Chiudere senza trasferirli li perderebbe. Testi da trasferire in fondo.

## 1. Implementazione su main `a09d8d61` (static audit)

| Strato | Prova (file:riga) |
|---|---|
| Lock OS | `core/runtime_authority.py:112-144` `acquire_os_lock`: `msvcrt.locking(...LK_NBLCK,1)` :122-124, `fcntl.flock(LOCK_EX\|LOCK_NB)` :127; percorso canonico `realpath(db)+".runtime.lock"` :91 |
| Identità del lock | `core/runtime_authority.py:133-134` memorizza `(st_dev, st_ino)`; `:146-165` `lock_identity_problem`: `fstat(handle)` vs `stat(path)` ⇒ `lock_file_deleted` (assente o `st_nlink==0`) / `lock_file_replaced` (inode diverso) |
| Verifica periodica | heartbeat `:238-290` (thread, `RUNTIME_LEASE_HEARTBEAT_SEC=10`, `core/runtime_controller.py:2791-2792`) e `verify` `:212-236` ad ogni operazione finanziaria ⇒ `mark_lost` |
| Lease + fencing | `database.py:440-` tabella `runtime_lease` (additiva); `acquire_runtime_lease` `:2028-2073` (`BEGIN IMMEDIATE`, token `+1`, UPDATE condizionato al token); `renew` `:2075-2090` (rifiuta se scaduta); `release` `:2092-2100`; `bind_runtime_fence` `:2102`; `_assert_runtime_fence` `:2114-2124` |
| Fence nelle scritture | `transaction()` `database.py:242-247`, `begin_transaction` `:168-173`, autocommit `_execute` `:282-287` → `_execute_fenced_write` `:314`, `_settlement_transaction` `:1966` (settlement_effects, save_settings) |
| START | `core/runtime_controller.py:2455-2469` lock OS (`RUNTIME_INSTANCE_ACTIVE`) poi lease (`RUNTIME_LEASE_HELD`, lock rilasciato); `_acquire_runtime_lease` :2821-2836 (fence+heartbeat); riverifica prima dell'attivazione :2718 |
| STOP | `stop()` :2987 serializzato; rilascio solo da fermo e dopo teardown (`_release_runtime_instance_lock_if_stopped` :2950, :2966 `authority.release()`) |
| Perdita ⇒ LOCKDOWN | `_on_runtime_authority_lost` :2905-2926 (`mode=LOCKDOWN`, `live_enabled=False`, evento `RUNTIME_AUTHORITY_LOST`, nessun cancel-all) |
| Percorsi finanziari | segnale :4856, auto-next :8200, cashout :3322, cashout cancel :3656, reset_cycle :3084, F028 :5227, TTL unmatched :7011/:7074, `is_live_allowed` :2003; engine `core/trading_engine.py:858,880,893,2323,2801`; hedge `cashout_executor.py` (authority_check/operation) cablato da `headless_main.py`/`mini_gui.py:487-494` |

Non è codice morto: lo dimostrano la riproduzione (§2, il secondo START è rifiutato da `RUNTIME_LEASE_HELD` e A va in
LOCKDOWN dall'heartbeat reale) e i test T29/T30 sul cablaggio in `HeadlessApp`/`MiniPickfairGUI`.
I commit successivi a #537 (`3bb4477d..a09d8d61`) non rimuovono nessuna chiamata di autorità/fence (diff controllato:
solo aggiunte, es. ledger F-05 nel `CashoutExecutor` con `authority_operation` preservato).

## 2. Riproduzione del difetto — due processi OS reali, stesso SQLite

Script: `p1_536/repro_536.py` (fuori dal repo). A avvia il runtime SIM reale (`HeadlessApp`), il driver cancella o
sostituisce il lock, B (altro processo) tenta `start()`, A dorme 13 s (> heartbeat) e poi tenta una scrittura nel DB.
Output integrale: `p1_536/repro_output.txt`.

| Commit | Variante | B | A dopo 13 s | Scrittura di A | Due runtime attivi |
|---|---|---|---|---|---|
| `152613b` (pre) | delete | `started=True mode=ACTIVE` | `mode=ACTIVE lost=None` | accepted | **True** (difetto riprodotto) |
| `152613b` (pre) | replace | `started=True mode=ACTIVE` | `mode=ACTIVE lost=None` | accepted | **True** |
| `a09d8d6` (main) | delete | `started=False RUNTIME_LEASE_HELD` (`runtime_lease_held_by:<owner A>:pid=…`) | `mode=LOCKDOWN lost=lock_file_replaced live_allowed=False` | `rejected: RuntimeFenceViolation: runtime_authority_lost:lease_expired` | **False** |
| `a09d8d6` (main) | replace | `started=False RUNTIME_LEASE_HELD` | `mode=LOCKDOWN lost=lock_file_replaced` | rejected (fence) | **False** |

Nota: nella variante delete, B ricrea il file allo stesso percorso, quindi A vede `lock_file_replaced` (atteso, come in T01).

## 3. Test dedicati su main `a09d8d6`

- `tests/acceptance/test_issue536_runtime_authority.py`: **61 passed, 0 failed** (9.45 s).
- Regressioni vicine (`test_issue461_pr29c_settlement_processes_and_schema.py`,
  `test_issue461_pr29c_settlement_identity_idempotency.py`, `tests/core/test_microstake_live_adapter.py`):
  **227 passed, 0 failed**.
- Mutation ufficiale (`scripts/run_mutation_guardrails.py`, solo le 47 voci `i536_*` del catalogo di main):
  **47/47 KILLED, 0 survived, score 100%** (report `p1_536/mutation_i536_report.json`). Il "GATE: FAIL" stampato è
  un artefatto del catalogo filtrato (aree richieste senza mutazioni nel sottoinsieme), non un esito su #536.
- Suite completa del repository: **non eseguita** qui (dichiarata 8893/0 sul head `ae10708` nel PR e CI post-merge
  14/14 nel commento owner; non riverificate da questo audit).

## 4. Criteri di accettazione della issue (corpo + autorizzazione owner c.6336248828)

| # | Criterio | Esito | Prova |
|---|---|---|---|
| 1 | Proprietà del lock: lock su file diverso non vale; verifica periodica fstat vs stat; fail-closed PAUSED/LOCKDOWN e segnalazione | **PASS** | §1 righe identità/heartbeat/LOCKDOWN; §2 main; T01/T02 |
| 2 | Fencing: token durevole nel DB con heartbeat, verificato nelle scritture di denaro/recovery; token superato rifiutato | **PASS** | `runtime_lease` + `_assert_runtime_fence` in tutte le vie di scrittura del `Database`; T05 (fantasma), T12/T12b/T25/T31/T44/T53; §2 scrittura rifiutata |
| 2b | "Coerente con `instance_id` di `settlement_effects`" | **PASS con nota** | Nessun conflitto: `_settlement_transaction` è fenced (`database.py:1966`). Però `instance_id` resta un uuid separato (`runtime_controller.py:5599`), non legato a `owner_id`/token: coerenza, non integrazione |
| 3 | Crash/restart: lease scaduta/lock liberato ⇒ nuovo avvio dopo riconciliazione; stesso processo e altro processo; STOP fallito non rilascia | **PASS** | T04 (kill del figlio ⇒ subentro, token +1), T05 (`os._exit` multi-tavolo), T06 (token monotono), T07/T41 (STOP fallito/ripetuto/concorrente) |
| 4 | Test multiprocesso reali: due processi OS, cancellazione **e sostituzione** durante l'esecuzione, kill -9, al più un runtime opera e l'altro fermo con motivo | **PARTIAL** | Nella suite: due processi solo per cancellazione + kill (T04, T05); la sostituzione è testata in-process (T02); T04 non verifica che il figlio vada in LOCKDOWN. Coperto da questo audit (§2, due processi, delete e replace, A LOCKDOWN + fence) ma **non** presidiato in CI |
| 5 | Windows verificato o MANUAL_ONLY **con checklist precisa** | **MISSING (checklist) / NOT VERIFIED** | `docs/runtime_authority.md` dice solo "Non eseguito in CI (MANUAL_ONLY)"; nessuna checklist in repo né in #453/#506/#351/#461. `_holder_alive` restituisce None su `nt` (subentro solo a TTL). Comportamento della cancellazione con handle aperto su Windows non verificato. Linux-only qui |
| 6 | Mutation nel catalogo ufficiale; docs `roserpina_bankroll_equity.md` + handoff nello stesso PR | **PASS** | 47 `i536_*` KILLED; `docs/roserpina_bankroll_equity.md` aggiornato in `3bb4477d`; `docs/runtime_authority.md` nuovo. Design handoff: nessuna superficie visibile cambiata (mini_gui solo cablaggio) ⇒ N/A ragionevole |
| 7 | Review indipendente Sol/Grok senza bloccanti + hard verify | **PARTIAL** | Ultimo range `2a5a6c2...ae10708`: Sol e Grok "Nessun bloccante evidente", ma (a) è un push-range di 1 commit; (b) il verdetto Grok termina con "Merge non approvato" pur senza bloccanti; (c) i range precedenti dichiarano `database.py`/test **NON VERIFICABILI** per troncamento. Nessuna review del diff assemblato. Questo audit è la verifica indipendente complementare |
| 8 | Fail-closed se lease invalida o DB non disponibile (owner c.6336248828) | **PASS** | `verify` ⇒ `lease_unreadable` (non permanente) / `lease_unrenewable` all'heartbeat; T09 |
| 9 | Multi-tavolo preservato dentro un runtime; nessun cap | **PASS** | T05 attiva due tavoli dopo il subentro; nessun cap introdotto |
| 10 | Nessuna dichiarazione di certificazione LIVE finché non verificata | **PASS** | Commenti owner: LIVE NO-GO, Gate A NON PASS |

## 5. Limiti dichiarati: dove sono tracciati e se il trasferimento è reale

| Limite (da `docs/runtime_authority.md`, commento #536 c.6347834878) | Tracciato altrove? |
|---|---|
| TTL registry in memoria (TTL-PERSIST P2) | **Sì, reale**: testo presente in #453 c.6345531244 e #506 c.6345533873 |
| Task dell'executor vivo dopo AMBIGUOUS non drenato dallo STOP | **Sì, reale**: #461 c.6350848120, PFC-033 / `M154` «STOP drena anche il task dell'executor ancora vivo dopo AMBIGUOUS (limite dichiarato #537)» |
| Windows MANUAL_ONLY | **No**. Solo in `docs/runtime_authority.md`. #351 T21 (c.6336016605 + tabella T21: "due istanze concorrenti sul DB test… Fencing/lock efficaci", ripetuto per OS) lo copre solo implicitamente, senza checklist #536 |
| Filesystem di rete non supportato | **No**. Solo doc; nessun rilevamento/rifiuto a runtime |
| Stesso host / orologio condiviso (pid, `time.time`) | **No**. Solo doc |
| Finestra TOCTOU verify → exchange | **No** come voce dedicata; solo doc (+ implicitamente riconciliazione #351 T21) |
| G7, RST-EXEC, OP-VIS | Tracciati (#453/#506) ma **non sono limiti di #536**: preesistenti, emersi durante #537 |

Le note "c.6345531244 / c.6345533873" citate come trasferimento coprono **solo** G7/RST-EXEC/OP-VIS/TTL-PERSIST.
Registro: #453 c.6340127508 diceva "L08-F1 non diventa FIXED finché #536 non è su main"; i commenti post-merge
(#453 c.6347836040, #506 c.6347837024) **non aggiornano lo stato di L08-F1**.

## 6. Rischi residui non trasferiti

1. **Windows**: semantica di `msvcrt.locking` + cancellazione/sostituzione con handle aperto, `st_ino` come indice NTFS,
   nessuno scavalcamento per pid (solo TTL 30 s): mai eseguito. Il prodotto ha un target Windows (PR53/PR54).
2. **Filesystem di rete / host diversi**: né flock né `st_ino` affidabili; la lease su SQLite in rete non è protetta
   (e SQLite su NFS è di per sé inaffidabile). Nessun rifiuto all'avvio: è solo un'avvertenza in docs.
3. **Namespace pid / hostname condiviso** (container con stesso hostname e volume condiviso): `_holder_alive` può
   dichiarare "morto" un detentore vivo e permettere il subentro immediato; il vecchio runtime perde l'autorità al
   primo `verify`/heartbeat e il fence rifiuta le sue scritture, ma resta la finestra TOCTOU verso l'exchange.
   Non documentato.
4. **TOCTOU verso l'exchange** (dichiarato): un ordine può partire con autorità appena persa; mitigato da
   `RUNTIME_AUTHORITY_LOST_AFTER_SEND` e riconciliazione del nuovo proprietario.
5. **Guardrail mancante**: nessun test di CI con due processi OS e **sostituzione** del lock, né che verifichi il
   LOCKDOWN del processo superato (lo prova solo questo audit).
6. **Copertura review**: nessuna review del diff assemblato di #537 (15 file, +3243); le review per range hanno
   dichiarato parti non verificabili.

## 7. Requisiti da trasferire (testo proposto)

**A → #351 (certificazione LIVE, T21) — e/o #461 accanto a PR53/PR54 (Windows):**
> PF-RUNTIME-LOCK-01 / #536 — collaudo Windows MANUAL_ONLY (implementazione su main `3bb4477d`). Checklist: (1) avvio
> runtime A su DB locale NTFS; secondo START sullo stesso DB ⇒ `RUNTIME_INSTANCE_ACTIVE`; (2) tentativo di cancellare
> `<db>.runtime.lock` con A attivo: registrare se Windows lo nega; se riesce, START di B ⇒ deve essere
> `RUNTIME_LEASE_HELD` e A entro 10 s in LOCKDOWN con `RUNTIME_AUTHORITY_LOST`; (3) sostituzione del file (rename +
> nuovo file) ⇒ stesso esito; (4) kill del processo A (Task Manager) ⇒ B parte solo dopo la scadenza della lease
> (≤ 30 s, nessuno scavalcamento per pid su Windows) con `fencing_token` maggiore; (5) scrittura da un processo con
> token superato ⇒ `RuntimeFenceViolation`. Verbale con versioni di Windows/Python e log. Fino ad allora Windows =
> NOT VERIFIED per #536.

**B → #453 (finding) e #506 (Gate A, collegato a L08-F1 / OWNER-MULTIPROCESS-03):**
> Limiti residui di #536 non coperti dal codice: (i) DB su filesystem di rete o condiviso fra host non supportato e non
> rifiutato a runtime; (ii) assunzione stesso host/orologio e pid univoci (container con hostname condiviso possono
> dichiarare morto un detentore vivo); (iii) finestra TOCTOU fra `verify` e chiamata all'exchange. Decidere: rifiuto
> esplicito all'avvio per FS di rete / owner_id per-namespace, oppure accettazione owner tracciata. Aggiornare anche lo
> stato di L08-F1 (lock fra processi) dopo il merge di #537.

**C → follow-up test (P2, GUARDRAIL_GAP):**
> Aggiungere un test a due processi OS con **sostituzione** del `.runtime.lock` durante l'esecuzione che verifichi
> sia il rifiuto di B (`RUNTIME_LEASE_HELD`) sia il LOCKDOWN e la scrittura rifiutata del processo A (oggi provato solo
> in-process, T02, e dall'audit indipendente `repro_536.py`).

Già trasferiti in modo reale (nessuna azione): TTL-PERSIST (#453/#506), drain dell'executor AMBIGUOUS (#461 PFC-033/M154).

## Etichetta hard verify

**IMPLEMENTED_WITH_NOTE** per Linux (difetto chiuso e provato); Windows NOT VERIFIED / MANUAL_ONLY senza checklist.

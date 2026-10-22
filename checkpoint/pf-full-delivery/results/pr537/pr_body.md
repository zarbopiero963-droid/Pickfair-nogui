**Mandato owner PF-536-ISOLATED-IMPLEMENTATION-01 (22/10/2026)** — issue #536 PF-RUNTIME-LOCK-01. Baseline `152613b` (main dopo #535). **Nessun merge automatico: anche con READY il merge richiede nuova autorizzazione owner. Nessun LIVE. Nessuna Factory.**

**PR safety-critical: sì** — `core/runtime_controller.py`, `core/trading_engine.py`, `database.py`, nuovo `core/runtime_authority.py`.

## Problema (Phase 0 su `152613b`, sonde P1–P3 RED)

Il lock `<db>.runtime.lock` di PR29-c protegge l'inode, non il percorso: cancellato o sostituito il file a runtime attivo, un secondo `start()` sullo stesso database riusciva (P1/P2) e il primo restava ACTIVE e continuava a operare (P3). Due runtime sullo stesso DB con tavoli Roserpina in memoria divergenti possono piazzare ordini, fare auto-next e azzerare cicli senza coordinamento.

## Architettura (dettaglio in `docs/runtime_authority.md`)

Tre strati obbligatori, nessuno disattivabile da configurazione, stesso motore SIM/LIVE:

1. **Lock OS** (flock / msvcrt byte 0) sul percorso canonico — come prima.
2. **Identità del file di lock**: `(st_dev, st_ino)` del descrittore confrontati con `os.stat(percorso)` a ogni verifica; record JSON del proprietario (owner_id, pid, host) nel file. Cancellato (`nlink == 0` o percorso assente) o sostituito (inode diverso) ⇒ autorità persa.
3. **Lease persistente** nella tabella **additiva** `runtime_lease` (riga singola, `CREATE TABLE IF NOT EXISTS`, nessuna colonna esistente toccata) con **fencing token monotono**: acquisita in `BEGIN IMMEDIATE`, scavalcabile solo se scaduta, dello stesso owner o se il detentore è dimostrabilmente morto sullo stesso host; rinnovata dall'heartbeat (TTL 30 s / periodo 10 s, costanti di classe, non chiavi di config) e **rifiutata se scaduta**; **fence vincolato al `Database`**: ogni scrittura (`transaction()`, `_settlement_transaction`, autocommit `INSERT/UPDATE/DELETE/REPLACE`) verifica owner e token **dentro la stessa transazione**, altrimenti `RuntimeFenceViolation` + rollback + perdita dell'autorità.

## Percorsi finanziari protetti

| Percorso | Verifica | Rifiuto |
|---|---|---|
| segnale → Quick Bet | subito prima di `CMD_QUICK_BET` | riserva rilasciata, `runtime_authority_lost:<motivo>` |
| auto-next Roserpina | prima dell'invio | tavolo sbloccato |
| cashout | `_route_cashout_signal_under_barrier` | `SIGNAL_REJECTED` |
| `reset_cycle` | all'ingresso | `{"reset": False, "ok": False}` |
| sweep F028 | all'ingresso | nessun rilascio |
| **ogni invio** (manuale, dutching, copy; SIM e LIVE) | `TradingEngine._submit_to_order_path` → `assert_runtime_authority` | `RuntimeAuthorityLost` prima della chiamata all'exchange |
| dopo l'invio | `after_order_send_authority_check` | evento `RUNTIME_AUTHORITY_LOST_AFTER_SEND` |
| LIVE | `is_live_allowed()` | False (anche se `live_enabled` viene riabilitato) |

`start()`: lock OS → lease (`RUNTIME_LEASE_HELD`, evento `RUNTIME_START_REFUSED`, lock rilasciato) → heartbeat → servizi. Perdita ⇒ `LOCKDOWN` in memoria **senza cancel-all** (gli ordini aperti possono essere del nuovo proprietario), `live_enabled=False`, poller fermato, lease scaduta subito così il proprietario legittimo subentra, evento `RUNTIME_AUTHORITY_LOST`.

## Semantica cambiata per chi usa il programma

- Un secondo avvio sullo stesso DB è rifiutato anche se il file di lock è stato cancellato o sostituito (`RUNTIME_LEASE_HELD`), finché il primo runtime è vivo (lease rinnovata).
- Il runtime che perde l'autorità (lock manomesso, lease superata/scaduta, DB non rinnovabile) va in LOCKDOWN e **non piazza più nulla**; va fermato e riavviato dopo riconciliazione.
- Dopo un crash il nuovo avvio parte subito se il pid del vecchio processo non esiste più (stesso host), altrimenti entro il TTL (30 s).
- Nuova tabella `runtime_lease` nel DB (additiva). Nessun cap monetario, nessuna nuova modalità, nessun flag.

## Limiti dichiarati (senza falso senso di sicurezza)

- **Finestra TOCTOU** fra `verify` e la chiamata all'exchange: non fenceable. L'ordine può partire con autorità appena persa; è segnalato dopo l'invio, la riga `orders` del runtime superato è rifiutata dal fence e l'ordine (con il suo `customer_ref`) resta sull'exchange per la riconciliazione del nuovo proprietario. Niente retry, niente cancel cieco.
- Stesso host e orologio condiviso; filesystem di rete non supportato; Windows (`st_ino` NTFS) MANUAL_ONLY.
- Processo sospeso oltre il TTL: nessun nuovo invio dopo la ripresa, ma una chiamata già in volo rientra nella finestra TOCTOU.
- `enable_fencing_token` del `ReconciliationEngine` (L08-F3, PR32) **non è toccato** da questa PR: resta OPEN.
- In LOCKDOWN i settlement restano trattenuti (scritture rifiutate dal fence) e li riconcilia il runtime con autorità.

## Validazione (tutto eseguito, esiti osservati)

- `tests/acceptance/test_issue536_runtime_authority.py`: **27 test HARD REAL** (processi OS, SQLite su file, catena runtime → engine → broker SIM): lock cancellato/sostituito a runtime attivo; tre avvii simultanei; due processi OS con lock cancellato e kill; crash multi-tavolo con `os._exit` e ricostruzione con token maggiore + fantasma rifiutato; riavvio con token monotono; STOP fallito/ripetuto/concorrente; lease scaduta con vecchio processo vivo; DB temporaneamente indisponibile; perdita in ammissione e fra verifica e invio; fence in transazione/settlement/autocommit anche con stesso owner e token superato; nessun rilascio da runtime superato; auto-next, engine e cashout rifiutati. Sonde Phase 0 P1–P3 RED su `152613b`, GREEN sul head.
- **Mutation**: 21 mutazioni `i536_*` nel catalogo, tutte KILLED; tre mutazioni PR29-c del lock (`second_runtime_instance_allowed`, `instance_lock_path_not_canonical`, `windows_lock_follows_append_offset`) riallineate a `core/runtime_authority.py` dove il codice si e' spostato, KILLED. Run ufficiale `scripts/run_mutation_guardrails.py` sul worktree del head `fc8f136`: **169 totali, 168 valide, 168 KILLED, 0 sopravvissute, 0 non applicabili, 1 equivalente preesistente — score 100%, GATE PASS**.
- **Regressione completa** `tests/` in parallelo: 8858 passed, 1 failed (il test del catalogo, causa le tre mutazioni spostate, poi corretto: 24/24). Regressione seriale su acceptance/scripts/guardrails/integration sul head: 4533 passed; l'unico fallimento (test GUI PR04-quater) era Xvfb caduto, rieseguito con display 18/18 sia sul head sia su main `152613b`. `py_compile` dei file toccati: OK.
- Guard simulato (`scripts/guardrail_check.py`): `files` == diff (10 file), solo la entry del task corrente. Full-diff (`pr_autonomy_policy.py full-diff`): PASS, nessun file a merge owner, nessuna nuova dipendenza.
- Docs nello stesso PR: `docs/runtime_authority.md` (nuovo), `docs/roserpina_bankroll_equity.md`, CHANGELOG. Design handoff: N/A (nessuna superficie GUI toccata). Build: not run in this environment.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01Wrefu68dTUWD1PXe8PTZjb

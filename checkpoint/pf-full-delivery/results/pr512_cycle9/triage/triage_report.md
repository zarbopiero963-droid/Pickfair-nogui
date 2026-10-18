## Triage ciclo 9 — 4 rilievi Codex su 2c6aa09 (verificati su HEAD 916fda4)

Test throwaway (non nel repo): `/tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/triage_c9/test_triage_c9.py` e `.../triage_c9/test_f1b.py`. Nessun file del repo modificato.

---

### 1. [P1] dedupe key trattenuta scade per TTL → secondo ordine sullo stesso evento

**Classificazione: CURRENT_DEFECT — PRE-EXISTING su main 028cee9 (la PR lo RESTRINGE, non lo introduce). CONFIRMED.**

Evidenza (HEAD):
- `core/duplication_guard.py:160-171` `_cleanup_locked` espelle ogni chiave con `now - ts > ttl` (120 s). `clear(keep_keys)` (:198-212, nuovo in PR) conserva la chiave **con il timestamp originale**: nessun refresh, nessun pin.
- `core/runtime_controller.py:3185` `acquire(event_key)` → `:3202` `table_manager.allocate(event_key=...)`. `allocate` (`core/table_manager.py:100-123`) **non guarda** se un altro tavolo porta già quell'`event_key`: prende il primo FREE. L'unico uso di `find_by_event_key` nel flusso è `_event_current_exposure` (:3213) → cap `max_event_exposure_pct` (18 % del bankroll, MM :494), che è un tetto, non un blocco di occupazione.
- Il dedupe engine (`trading_engine.py:959 _dedup_allow` → `db.find_duplicate_order` per `customer_ref`) blocca solo lo **stesso messaggio**: il ref `sig-…` è `sha256(signal_identity_material)` (`order_identity.py:72-89, 163-180`), cioè il contenuto del messaggio. Un secondo tip sullo stesso mercato/selezione/lato con quota diversa (2.1 vs 2.0) ha ref diverso e stesso `event_key`.

Test eseguito (`test_f1b.py`, catena SIM reale `_catena` → `_on_signal_received` → engine → `SimulationBroker`): primo segnale → 1 ordine su tavolo 1; `time.monotonic` +121 s; secondo segnale price=2.1 → `QUICK_BET_ROUTED`, **2 ordini** (`sig-1463…`, `sig-39c5…`), tavoli occupati `[(1,'1.234:11:BACK:default',30.0),(2,'1.234:11:BACK:default',27.27)]`, nessun `SIGNAL_REJECTED`. Identico esito **senza reset** e **dopo `reset_cycle` con la chiave marcata AMBIGUOUS e trattenuta**. (Con lo stesso messaggio identico il secondo è bloccato dall'engine `QUICK_BET_DUPLICATE`: falso negativo iniziale, non difesa.)

Su main: stesso TTL, stessa `allocate` senza controllo di `event_key`, stesso `_on_signal_received` (`git show 028cee9`: solo `acquire` + `allocate`); e `reset_cycle` faceva `duplication_guard.clear()` **immediato** → finestra 0 s dopo reset. La PR porta la finestra a 120 s: miglioramento parziale, non chiusura. Il rilievo è corretto nel merito ma attribuisce alla PR un buco di main.

Fix minimo fail-closed (due strati, il primo è quello che conta):
1. `_on_signal_received`, subito dopo l'`acquire` riuscito: `if self.table_manager.find_by_event_key(event_key): _reject_signal(signal, "evento_gia_in_posizione")` e `return` **senza** `release(event_key)` né `force_unlock` (la chiave appena ri-acquisita resta a protezione del tavolo vivo). Mirror dell'invariante già usato al riavvio in `_rebuild_open_positions` (:3749-3756, `held`).
2. Opzionale, nel guard: chiavi trattenute/seedate (`clear(keep_keys)`, `register_startup_order`, `acquire` al rebuild :3756/:3794) esenti dal TTL (`_pinned` set saltato da `_cleanup_locked`, tolto da `release`).

Rischio adiacente da evitare: non usare `_release_acquired_and_reject` (:3405-3416 fa `force_unlock(table_id)`: libererebbe il tavolo occupato); il check va fatto PRIMA di `allocate` (nessun tavolo da sbloccare). Non toccare l'event-exposure cap (resta un cap).

---

### 2. [P1] baseline committata, stato recovery + marker non scritti → ciclo incoerente dopo crash

**Classificazione: CURRENT_DEFECT — INTRODUCED dalla PR (F032 `_start_expansion_cycle` e RSR-12 `_persist_recovery_tables` non esistono su main). CONFIRMED.**

Evidenza: `reset_cycle` :2671-2675 e `_maybe_auto_reset` :4075-4080 fanno due `save_settings` separate: prima la baseline (`_start_expansion_cycle` :3920, chiave `roserpina.expansion_baseline_state` con `set_at_ts=reset_ts`), poi stato tavoli + marker (`_persist_recovery_tables` :4030-4043). Se la seconda fallisce: `_recovery_persist_failed=True` è **solo in memoria**.

Test eseguito (`test_f2`, `_rc` reale + `Database` sqlite): perdita 3.0 persistita su tavolo 1; `save_settings` guasta solo per `_RECOVERY_KEY`; `reset_cycle()` → `persisted: False`; in memoria baseline 100.0, recovery `{}`, flag True. DB: `expansion_baseline_state={"baseline":100.0,"reason":"reset_cycle",set_at_ts=T}`, `recovery_tables_state={"1":3.0}`, `cycle_reset_state=None`. Nuovo controller: `_load_expansion_baseline()` → **100.0 accettata** (marker assente → nessun confronto), `_restore_recovery_tables()` → **{1: 3.0} ripristinato**, `_recovery_persist_failed` assente. Risultato: baseline del nuovo ciclo + perdite del ciclo vecchio ri-inseguite dalla recovery, e il blocco di capacità pre-crash perso (fail-open rispetto allo stato pre-crash).

Fix minimo: **una sola transazione** — `Database.save_settings` è già `with self.transaction()` (BEGIN/COMMIT, `database.py:180-198, 416-419`), quindi basta far scrivere a `_persist_recovery_tables(cycle_reset=True, reset_ts=…)` anche la chiave baseline nello stesso payload: `_start_expansion_cycle(..., persist=False)` restituisce lo stato (o si aggiunge un parametro `expansion_state=`), e in caso di fallimento della transazione si pone `self._expansion_baseline = None` oltre a `_declare_recovery_persist_failure`. Dopo un crash il DB è o tutto "prima del reset" o tutto "dopo": coerente in entrambi i casi. Alternativa accettabile se non si vuole cambiare la firma: ordine marker+recovery PRIMA, baseline DOPO, e salto della baseline se la prima scrittura fallisce (baseline vecchia con `set_at_ts < marker` → stantia → nessuna espansione).

Rischio adiacente: vale per entrambi i chiamanti (`reset_cycle` e `_maybe_auto_reset`); `first_start` (:2488) resta scrittura singola senza marker (corretto, non è un reset). Test esistenti da tenere verdi: `tests/test_pr512_codex_f62f0e3.py::test_stale_expansion_baseline_is_rejected_after_a_durable_cycle_reset` (chiama le due funzioni separatamente: mantenere la firma retro-compatibile) e `test_baseline_fixed_by_a_real_reset_cycle_survives_restart`; `tests/test_pr512_codex_034af59.py` f5.

---

### 3. [P2] `recover_native_microstake_sagas` conta ROLLED_BACK come pendente

**Classificazione: CURRENT_DEFECT (direzione fail-closed, operativo, non real-money) — INTRODUCED dalla PR (funzione e `microstake_sim_port.py` assenti su main). CONFIRMED.**

Evidenza: `core/trading_engine.py:2314` `still = [ref for ref, phase in results.items() if phase != "ACTIVE"]`; `DbSagaStore.pending_refs` (`core/microstake_sim_port.py:125-129`) esclude `{ACTIVE, FAILED, ROLLED_BACK, BLOCKED}`; `Phase` (`core/microstake_safe.py:36-45`) ha ROLLED_BACK terminale e `MicrostakeCoordinator.recover` lo produce davvero da ROLLBACK_PENDING (`tests/test_pr512_codex_034af59.py::test_f4_rollback_pending_is_cancelled_at_restart_never_replaced`).

Test eseguito (`test_f3`, engine reale + `DbSagaStore` su DB): saga `rb` in ROLLBACK_PENDING, executor che la porta a ROLLED_BACK → `{'ok': False, 'status': 'PENDING', 'pending': ['rb'], 'results': {'rb': 'ROLLED_BACK'}}` con `store.pending_refs() == []`. Conseguenza (`runtime_controller.py:2527-2538`): avvio in PAUSED `RECONCILIATION_REQUIRED:MICROSTAKE_SAGA_PENDING:PENDING:rb`; `resume()` (:2629) rilancia, trova NOTHING_PENDING e passa — serve un resume manuale spurio.

Fix minimo: dopo `results = recover()` rileggere lo store ed esigere entrambe le condizioni:
```python
after = set(store.pending_refs())            # SagaStoreError => ok False, STORE_UNREADABLE
still = sorted(ref for ref, ph in results.items()
               if ph not in {"ACTIVE", "ROLLED_BACK"} or ref in after)
```
Rischio adiacente: NON usare `pending_refs()` da solo (un `ERROR:*` o un AMBIGUOUS devono restare pendenti anche se un bug di persistenza li avesse marcati terminali); FAILED/BLOCKED non vanno promossi a "riconciliato" senza una decisione sulla riserva (`reservation_held`) — lasciarli pendenti è la scelta severa.

---

### 4. [P2] `pf_trace`: evidenza senza `tests` associata a ogni test

**Classificazione: GUARDRAIL_GAP (validatore, non runtime) — INTRODUCED dalla PR (file nuovo). CONFIRMED per ispezione + conteggio sul registro.**

Evidenza: `tools/traceability/pf_trace.py:156` `any(t in (e.get("tests") or [t]) ...)` → fallback `[t]` rende vero il test per qualunque evidenza priva di `tests`. Registro `docs/traceability/requirements.json`: **24/24 evidenze senza campo `tests`**; `validate --strict` oggi: `PASS, errors [], warnings []`. Quindi un PASS di un comando qualsiasi "copre" tutti i test dichiarati.

Conteggio dell'impatto (script sul registro, 10 requisiti TESTED/VERIFIED/CERTIFIED):
- `tests` esplicito obbligatorio, assente = nessuna associazione: **60 warning** (tutti i test dichiarati) → `--strict` FAIL su tutto il registro.
- Associazione derivata dal `command` dell'evidenza (file o node-id contenuto nel comando): **11 warning** — PF-MS-01 (1), PF-MS-02 (1), PF-MS-05 (1), PF-CONT-01 (7: il comando è `pytest tests/agent_checkpoint`, directory), PF-TRACE-01 (1). Con match anche per prefisso di directory scendono ulteriormente.

Fix proposto: associazione = `e["tests"]` esplicito OPPURE derivazione dal `command` (file/node-id/directory-prefix), mai `[t]`; evidenza senza nessuna delle due → warning `evidenza senza tests`, e sotto `--strict` errore.

**Rischio adiacente decisivo:** le evidenze sono append-only (`:151-154`, confronto di prefisso con `HEAD:` del registro): **aggiungere `tests` alle 24 evidenze esistenti verrebbe rifiutato dal validatore stesso** (`evidenze precedenti rimosse o riscritte`). Quindi o (a) si deriva dal comando per le evidenze legacy e si richiede `tests` solo alle nuove, oppure (b) si appendono nuove evidenze con `tests` esplicito, oppure (c) si ammorbidisce l'append-only per la sola aggiunta della chiave `tests` (confronto su `{sha, command, timestamp, environment, result}`) — scelta da dichiarare, perché tocca l'invariante di integrità del registro. Eseguire `pf_trace validate --strict` prima e dopo nella PR.
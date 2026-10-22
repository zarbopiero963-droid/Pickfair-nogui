# PF-POST537-AUDIT-RECONCILIATION-01 — Piano operativo unico dopo il merge di #537

Mandato owner del 22/10/2026. Lavoro in sola lettura: nessun commit, push, merge, PR o modifica di issue o workflow. I benchmark sono girati in un worktree isolato nello scratchpad.

**Etichette:**
- **VERIFIED**: controllato direttamente su GitHub o sul codice;
- **MEASURED**: eseguito e misurato in questa sessione;
- **ESTIMATED**: stima;
- **NOT VERIFIED**: non provato.

## A. Stato GitHub verificato (22/10/2026, 19:23Z)

| Voce | Valore | Etichetta |
|---|---|---|
| PR #537 | **MERGED** il 22/10 alle 17:51:17Z, `merged_by` owner, head approvato `ae10708` | VERIFIED |
| main | `3bb4477d39fcfdff44abb372eb35061d4a9bde42`: un solo commit dopo la baseline degli audit (`152613b`) | VERIFIED |
| Albero di main | `5c976b5…` = albero del head approvato `ae10708` | VERIFIED |
| CI sul head `ae10708` | **40/40 run success** | VERIFIED |
| CI post-merge su main | **14/14 success** (soli workflow `push` veloci, ciascuno ≤1 minuto) | VERIFIED |
| PR aperte | **0** | VERIFIED |
| Differenza da `152613b` | 15 file, +3 243/−66; **nessuna modifica a `.github/`, `scripts/`, `pytest.ini`, `conftest.py` e ai manifest delle dipendenze** | VERIFIED |
| Registri dopo il merge | #453 c.6347836040, #506 c.6347837024, #536 c.6347834878 | VERIFIED |
| Follow-up ancora aperti | G7 P0, RST-EXEC P1, OP-VIS P1, TTL-PERSIST P2; L08-F3 (`enable_fencing_token`) non toccato | VERIFIED |
| Issue #536 | ancora **open** (chiusura documentale non eseguita) | VERIFIED |

**Nuovo limite dichiarato da #537:** un task dell'executor ancora vivo dopo un esito AMBIGUOUS non viene drenato dallo STOP. È registrato come nuovo requisito atomico **M154** dentro PFC-033, senza aggiungere una PR.

**Validità delle misure degli audit dopo il merge:**
- **Workflow:** invariati, quindi le misure CI di #533–#537 restano valide (VERIFIED).
- **Suite:** 8 851 → 8 912 test raccolti; worksteal con 8 worker su `3bb4477`: 254,8 s e 231,6 s, contro 242–245 s sulla baseline (MEASURED).
- **Suite seriale su `3bb4477`:** **1 170,5 s**, 8 892 passati; unico fallimento lo stesso test GUI, per mancanza di tkinter (MEASURED). Rispetto al seriale il worksteal a 8 worker è ×4,6–5,1.
- **Mutation su `3bb4477`:** catalogo 148 → **195** mutazioni.
  - seriale **788,4 s** (194 KILLED, 1 EQUIVALENT, gate PASS);
  - 4 shard per modulo **418,9 s**;
  - **esito identico mutazione per mutazione** (MEASURED).
  - In CI gira 3 volte per push: circa 22–40 minuti-job.

## B. Matrice di riconciliazione dei due audit

| # | Audit 1 (CI) | Audit 2 (micro-PR) | Esito | Decisione tecnica proposta | Prova |
|---|---|---|---|---|---|
| 1 | Suite completa 17× per push, mutation 3× | — | compatibili | deduplica in **PFC-002** | run di #537 e #535 (VERIFIED) |
| 2 | xdist worksteal ×4,9 | — | compatibili | in **PFC-003**, con step seriale per i test marcati `serial` | MEASURED su `152613b` e su `3bb4477` |
| 3 | «xdist_group per GUI/lock» | — | **correzione** | `xdist_group` vale solo con `--dist loadgroup` e viene **ignorato da worksteal**. Si usa invece un marker `serial` eseguito in uno step seriale dello stesso job, con un controllo che l'unione dei nodeid eseguiti sia uguale alla raccolta completa | documentazione di xdist; nessun marker presente oggi (VERIFIED) |
| 4 | PF-022 esteso | PF-022 = M025 (#528) | compatibili | **PFC-001** = M025 + M158 (solo test) | mappatura |
| 5 | PF-074 → 074a/074b, +1 PR | PF-074 = M130 (PR50); dipendenza da PR49 | **sovrapposizione risolta** | M130 (collection, workflow_run, policy reviewer) entra in **PFC-002** insieme alla deduplica (stesso dominio, gli orchestratori di P16 erano già di PR50). La dipendenza PR49 → PR50 era solo amministrativa e viene rimossa | lettura di #461 |
| 6 | Anticipare le PR CI | Sequenza: P0 finanziari in testa | **conflitto di priorità** | CI in testa con una **condizione di salvaguardia** (vedi E): se il merge owner delle PR CI non arriva entro 24 h, si passa al P0 G7 e le PR CI scalano dopo | stima in §E |
| 7 | 107 + 1 = 108 future, 101 eseguibili, 7 sospese | 107 future | **confermato** | 108 future: 101 eseguibili + 7 sospese; 164 requisiti atomici (157 attivi + 7 sospesi) | `gen2.py`: asserzione unicità, ordinamento topologico senza cicli |
| 8 | FULL CERTIFICATION su main dopo il merge | — | **lacuna reale** | oggi su main gira **nessuna** suite completa, solo i 14 workflow veloci. La prova post-merge è l'uguaglianza dell'albero con il head verificato. Il job FULL su main entra in **PFC-083** (PF-082, tooling di verifica) | run di `3bb4477` (VERIFIED) |
| 9 | «Mutation 1× per push con catalogo completo» | — | compatibili | il gate ufficiale mantiene **tutto** il catalogo (195), soglia 80% e aree obbligatorie; la selezione per hunk resta solo in FAST | MEASURED: parallelo identico al seriale |
| 10 | Cache pip | — | scartata | l'installazione costa 0,05–0,30 minuti per job: nessun beneficio | MEASURED |

## C. Roadmap canonica unica

**Numero ufficiale: 108 PR future = 101 eseguibili + 7 sospese.**

- **164 requisiti atomici**, ognuno in una sola PR:
  - i 160 dell'audit 2;
  - M154 (limite di #537);
  - M156, M157, M158 (CI).
- **Divisioni rispetto alle 85 PR originali: 20**, cioè le 19 dell'audit 2 più PR50. Le PR lasciate invariate sono 65.
- **Merge riservato all'owner: 11 PR**, quelle che toccano workflow, policy o manifest.
- **Eseguibili subito: 28**, senza dipendenze aperte. La serialità resta comunque di una PR alla volta.

### Verifica delle 20 divisioni
Ognuna separa obiettivi con test e stati finali indipendenti. Nessuna separa:
- riserva e rilascio;
- lease e fence;
- una transizione e il suo test integrato;
- un'attivazione e i suoi gate.

| Origine | PR risultanti | Motivo tecnico | Stato intermedio sicuro perché |
|---|---|---|---|
| G7 | 3 | place/cancel/reject → transizioni miste → riavvio | ogni passo aggiunge riserve senza mai rilasciarne di non provate; più conservativo del main |
| PR29-f | 2 | TTL 30 s, indipendente dal resto → doppio conteggio con FIN-02 | il solo TTL restringe gli ingressi |
| PR31-a | 2 | avvio fail-closed → decision log e ghost nei cap | il primo passo blocca ACTIVE |
| PR28-f | 2 | default t80u e cap fissi → drawdown e input non finiti | ogni passo aggiunge un blocco |
| PR34 | 2 | harness di test → crash drill con STOP persistente | il primo passo tocca solo test |
| PR35 | 2 | dipendenza in una PR minima a merge owner → migrazione | la primitiva non usata non cambia il runtime |
| PR19 | 2 | tasso commissione verificato → policy, dedupe e VOIDED | nessun importo cambia senza la propria prova |
| PR47 | 2 | OP-VIS (P1) → watchdog e storico | solo osservabilità |
| PR11-b | 2 | dati stantii ⇒ nessun ingresso (P0) → reconnect e endpoint Italia | il primo passo è fail-closed |
| Order Stream | 2 | trasporto → cache e cablaggio in sola lettura | nessuna autorità sullo stream |
| PR30-a | 3 | OrderManager e eventi → FSM AMBIGUOUS + M154 → parsing | ogni passo riduce un falso esito |
| PR30-b | 3 | preflight lotto → cablaggio → parziali e residuo | corsia Dutching **dormiente** fino a PFC-054 |
| PR09 | 2 | implementazione e 72 casi E2E → attivazione | l'attivazione è l'ultima PR, dietro gate PASS |
| PR25 | 2 | intento P35 durevole → trailing e auto-cashout | auto-cashout solo dopo l'intento durevole |
| PR43-b | 2 | target e netting → Void = hedge | Void dopo il netting provato |
| PR45-b | 2 | esito incerto dell'hedge = AMBIGUOUS → un solo cashout, F06, parità | il primo passo è fail-closed |
| cmd-b | 2 | preview e STOP → reconcile e cashout via API | la API resta senza mutazioni finché i gate non esistono |
| PR05 | 2 | framework scroll/DPI → controlli per tab | solo GUI |
| PR53 | 2 | Linux → Windows | piattaforme indipendenti |
| PR50 | 2 | deduplica e topologia → parallelismo | rollback per singolo commit |

Tabella completa con ID, titolo, requisiti, priorità, dipendenze, gate, autorità di merge e stato: §C-bis in fondo.

## D. Piano CI

| Modifica | PR | Benchmark disponibile | Equivalenza ancora da provare | Rollback | Risparmio atteso |
|---|---|---|---|---|---|
| Test timing: backoff a 0 nel test da 181 s, attese a condizione al posto di `Event.wait(2)`, harness F026 senza sleep fissi (#528), marker `serial` | **PFC-001** (solo test, merge agente) | 181 s su ogni suite seriale; suite worksteal 242 → 183 s senza quel test (MEASURED) | RED→GREEN; 3 esecuzioni seriali e parallele con insieme di nodeid ed esiti identico | revert | −1 minuto per suite in parallelo; meno falsi rossi (3 asserzioni instabili osservate) |
| Deduplica: togliere `dynamic` da `ci-master-gate` (oppure il trigger autonomo di dynamic); **un solo** backstop completo, senza `if` né filtri sui percorsi; `push` solo su `main` per i 12 workflow doppi; `concurrency` con `cancel-in-progress` solo per `pull_request` della stessa PR; aggiornare `test_false_green_semantics.py:471` e `test_orchestratori_ci_525.py` | **PFC-002** (merge OWNER) | 300 dei 342–391 minuti-job per push sono suite duplicate; 162 minuti-job sprecati su #535 (MEASURED) | push di prova: insieme dei check obbligatori uguale a oggi, meno i duplicati; backstop non saltabile | revert del commit sui workflow | circa −80% dei minuti-job (ESTIMATED); falsi rossi da instabilità 17× meno probabili |
| Backstop con `pytest -n auto --dist worksteal -m "not serial"` seguito da `pytest -m serial` nello stesso job; mutation **1×** per push con `--jobs` (sharding per mutazione), catalogo completo; FAST incrementale solo in locale | **PFC-003** (merge OWNER) | suite 1 183 → 242 s (152613b) e 254/232 s (3bb4477); mutation 431 → 237 s (152613b) e **788 → 419 s (3bb4477)** con 4 shard per modulo, identica (MEASURED); per mutazione circa 3,5–4 minuti (ESTIMATED) | **in CI**: vCPU del runner e GUI con display (NOT VERIFIED); confronto dei nodeid eseguiti con la raccolta completa | rimuovere `-n`; `--jobs 1` | wall CI 25–30 → circa 10–12 minuti (ESTIMATED; poi il limite diventa Grok, ~9 minuti) |
| Job FULL CERTIFICATION su `main` (seriale + parallelo ripetuto + mutation seriale) | **PFC-083** (merge OWNER) | — | — | — | chiude la lacuna post-merge |

**Verifiche obbligatorie del mandato (§6):**
1. **Workflow duplicati:** `ci-master-gate` → `ci-dynamic-intelligent` annidato, più la sua esecuzione autonoma. `merge-simulation-hard` gira 3 volte. 12 workflow partono sia su `push` sia su `pull_request`. 10+3 ultra-check eseguono ciascuno il backstop completo (VERIFIED).
2. **Esecuzioni eliminabili senza perdita di copertura:** 16 backstop su 17 eseguono lo stesso comando (`pytest -q`, lo stesso insieme di test) sullo stesso SHA. Restano i test focalizzati di modulo e i guardrail runner (VERIFIED dal contenuto dei workflow).
3. **`needs`:** oggi i job di `ci-master-gate` non hanno `needs`. Il gate è Merge Readiness, che attende tutti i check del head (VERIFIED). Con `needs: backstop`, un backstop FAILURE resta BAD e blocca. I job dipendenti diventano SKIPPED, che è **OK_STATE** (`scripts/pr_flow_automation.py:31`).
4. **Falso PASS:** `SKIPPED` conta come OK. Se il backstop stesso fosse saltato (`if`, filtri sui percorsi) diventerebbe un falso verde. **PFC-002 deve aggiungere un test che vieti `if` e `paths` sul job di backstop.** Più robusto ancora, e da proporre all'owner: Merge Readiness esige per nome un check `full-suite` SUCCESS. È una modifica a `scripts/pr_*`, quindi a merge owner. `CANCELLED` è BAD (`:33`), quindi un run del head corrente cancellato per errore blocca, non promuove.
5. **Review e SHA:** marker per range `base...head`; la readiness legge `headRefOid` e lo `statusCheckRollup` del head (VERIFIED da codice). La review sul range finale non vede i test oltre il budget (VERIFIED su #537).
6. **`cancel-in-progress`:** oggi solo su Merge Readiness, quarantine e build Windows (VERIFIED). Va proposto con gruppo `${{ github.workflow }}-pr-${{ github.event.pull_request.number }}` solo per `pull_request`, mai per `push` su `main` né per i reviewer.
7. **xdist con SQLite, lock, processi, GUI:** 8 esecuzioni parallele su due SHA con esiti identici al seriale (MEASURED). Il test GUI fallisce **anche in seriale** per assenza di tkinter in questo ambiente, quindi il comportamento GUI e Xvfb in parallelo resta **NOT VERIFIED** ed è la ragione del marker `serial`.
8. **Catalogo mutation completo:** resta nel gate ufficiale.
9. **Rollback:** colonna dedicata della tabella.
10. **Protezione owner sui workflow:** PFC-002, PFC-003 e PFC-083 sono a merge owner (`MANUAL_AUTHORITY_PATTERNS`).

## E. Piano di esecuzione immediato (5 PR)

| Ordine | PR | Motivo | Prerequisiti | Merge |
|---|---|---|---|---|
| 1 | **PFC-001** test timing + #528 | solo test: rimuove instabilità che colorano di rosso i P0 finanziari; prerequisito del parallelo sicuro | nessuno | agente (§0.9), salvo riserva del mandato |
| 2 | **PFC-002** deduplica CI + M130 | −80% dei minuti, meno falsi rossi, backstop non saltabile | PFC-001 | **OWNER** |
| 3 | **PFC-003** CI parallela + mutation 1× | wall per push 25–30 → ~10–12 minuti (ESTIMATED) | PFC-002 | **OWNER** |
| 4 | **PFC-004** G7-a (P0) | riserva di stake e liability nel SimulationBroker; rimuove 4 xfail strict | nessuno tecnico | agente, salvo riserva owner per safety-critical |
| 5 | **PFC-005** G7-b (P0) | transizioni miste senza doppio conteggio | PFC-004 | agente, salvo riserva owner |

**Perché la CI va prima dei P0 (ESTIMATED):**
- **Oggi nessun P0 espone denaro reale:** LIVE è NO-GO e G7 riguarda la parità in SIM.
- **Costo:** anticipare tre PR (circa 3,1 ore di pipeline ciascuna, più due merge owner) ritarda G7 di circa 9 ore più l'attesa owner.
- **Beneficio sulla sola catena G7:** circa 3 PR × 6,7 push × 15–20 minuti risparmiati, cioè circa 5–7 ore.
- **Beneficio sull'intero piano:** circa 670 push × 15–20 minuti, cioè circa 170–220 ore.

**Salvaguardia:** se il merge owner di PFC-002 o PFC-003 non arriva entro 24 ore, si apre PFC-004. La serialità resta di una PR alla volta. Le PR CI non attivano alcuna funzione finanziaria.

## F. Registro dei rischi

| Tipo | Voce | Stato |
|---|---|---|
| P0 aperti | G7 (PFC-004/005/006), 29-f doppio conteggio (PFC-007), t80u con chiavi assenti (PFC-008), PF-FIN-01 MTM (PFC-009), CL19 customer_ref del cashout (PFC-010), STOP persistente (PFC-029), AUD-02/03 hedge incerto (PFC-034), PF-EXEC-05 dati stantii (PFC-037), F01 SL/TP (PFC-048) | tracciati in PR dedicate; nessuno FIXED |
| P1 nuovi da #537 | RST-EXEC (in PFC-006), OP-VIS (PFC-031), M154 drain del task AMBIGUOUS (PFC-033) | aperti |
| P2 | TTL-PERSIST (PFC-060) | aperto |
| Incompatibilità | `xdist_group` non funziona con worksteal (corretto in §B.3); `SKIPPED` = OK nella readiness (vincolo in PFC-002) | risolte nel piano |
| Non verificato | xdist in CI (vCPU, GUI con display); durata di Grok come nuovo collo di bottiglia; costo reale dei minuti (piano GitHub); tempi delle PR future (stime basate su n=2 per le PR grandi) | NOT VERIFIED |
| Lacuna di processo | nessuna suite completa su `main` dopo il merge (PFC-083); #536 ancora open | VERIFIED |
| Decisioni owner necessarie | 1) adottare la roadmap canonica (108); 2) anticipare PFC-001..003 con la salvaguardia delle 24 h; 3) se aggiungere a Merge Readiness il requisito di un check `full-suite` per nome (file `scripts/pr_*`, merge owner); 4) chiusura documentale di #536 | da decidere |

Nessuna nuova decisione economica: la rettifica del 18/10 (nessun cap fisso di default) e t80u per le chiavi obbligatorie restano invariate (PFC-008 = M019 + M020).

## G. Verdetto

- **Adottare la roadmap canonica: READY.**
  - 108 PR future (101 + 7);
  - 164 requisiti atomici unici;
  - ordinamento topologico senza cicli (asserzioni dello script);
  - P0 protetti;
  - nessun requisito finanziario perso, verificato per mappatura.
- **Iniziare l'ottimizzazione CI: READY per PFC-001** (solo test, rischio basso). **READY con condizioni** per PFC-002 e PFC-003:
  - merge owner;
  - push di prova che confronti l'insieme dei check;
  - controllo nodeid eseguiti = raccolta completa;
  - backstop non saltabile.
  
  xdist in CI con GUI resta NOT VERIFIED, ed è coperto dallo step seriale.
- **Gate A #506: NON PASS. LIVE: NO-GO.** Una CI verde non equivale a una certificazione.

**STOP:** attendo l'approvazione owner. Nessuna PR aperta, nessun registro modificato.


## C-bis. Inventario canonico completo (108 PR)

| ID | ex-ID | Titolo (requisiti atomici) | Origine #461 | Prio | Dipendenze | Gate | Merge | Stato |
|---|---|---|---|---|---|---|---|---|
| **PFC-001** | PF-022 esteso | `M025` #528: harness F026 deterministica (niente attese fisse)<br>`M158` Test timing: backoff a 0 in test_retry_does_not_stack_overflow (181 s), attese a condizione al posto di Event.wait(2), marker serial per GUI/Xvfb/processi/lock | PR34 | P1 | — | PR GATE | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-002** | PF-074a | `M156` CI-DEDUP: niente ci-dynamic annidato in ci-master-gate, un solo backstop completo (job senza if/paths), push solo su main per i 12 workflow doppi, cancel-in-progress per SHA superati delle PR<br>`M130` CI: collection/workflow_run e policy reviewer | PR50 | P1 | PFC-001 | PR GATE | OWNER | DIPENDENTE |
| **PFC-003** | PF-074b | `M157` CI-PARALLEL: backstop con xdist worksteal (+ step seriale per i test marcati serial), mutation 1 volta per push con --jobs, modalita' FAST incrementale solo locale | PR50 | P1 | PFC-002 | PR GATE | OWNER | DIPENDENTE |
| **PFC-004** | PF-001 | `M008` G7-a: SimulationBroker riserva/rilascia stake BACK e liability LAY; INSUFFICIENT_FUNDS | G7 #532 | P0 | — | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-005** | PF-002 | `M009` G7-b: transizioni matched/parziale/settlement/cashout senza doppio conteggio | G7 #532 | P0 | PFC-004 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-006** | PF-003 | `M010` G7-c + RST-EXEC + L05-F2 (restart): riavvio con ordine EXECUTABLE non abbinato | G7 #532 | P0 | PFC-005 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-007** | PF-004 | `M012` 29-f: gate fondi senza doppio conteggio delle riserve (parita' SIM/LIVE)<br>`M013` PF-FIN-02 residuo + T-F03/T-F06: funds gate che riconcilia ordini/liability/esposizione con dati exchange incompleti | PR29-f | P0 | PFC-005, PFC-006 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-008** | PF-005 | `M019` C3/t80u 1-2: chiavi pct e recovery assenti => blocco / nessun recovery (+F10 loader/GUI)<br>`M020` C1: nessun cap monetario fisso sopravvive (config.example + test di guardia sui default) | PR28-f | P0 | — | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-009** | PF-006 | `M085` PF-FIN-01: MTM corretto nel position ledger<br>`M088` PF-FIN-02: realizzato e hedge nel position ledger | PR43-a | P0 | — | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-010** | PF-007 | `M097` CL19 (P0) + AUD-04: customer_ref del cashout univoco e canonico<br>`M098` AUD-05: ledger del cashout | PR45-a | P0 | — | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-011** | PF-008 | `M011` C2: validita' saldo LIVE <=30 s monotona su ogni nuovo ingresso (auto-next compreso) | PR29-f | P1 | — | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-012** | PF-009 | `M014` L05-F2 (reset) + PF-C29-01.09: il reset non rilascia riserve vive | PR29-d | P1 | PFC-006 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-013** | PF-010 | `M016` PF-LOT-D-F: aggregazione per bet_id dei settlement dello stesso mercato | #521 | P1 | — | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-014** | PF-011 | `M021` F04: drawdown con perdita non realizzata<br>`M022` F05 + F014 + F11: bankroll non finito, stop di sessione senza baseline, scope warning-only | PR28-f | P1 | PFC-008 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-015** | PF-012 | `M030` Saldi, picco e drawdown separati per modalita'<br>`M031` Contatori/stato persistenti per modalita' + emergency snapshot | PR15 | P1 | PFC-008 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-016** | PF-013 | `M032` marketBaseRate LIVE + commission_pct solo verificata (t80u 3) | PR19 | P1 | — | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-017** | PF-014 | `M033` F1 + F5: policy commissione e dedupe settlement prima del publish<br>`M034` VOIDED e resettlement (OD12-08) | PR19 | P1 | PFC-016 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-018** | PF-015 | `M035` Poller settlement attivo quando configurato, riconciliazione al riavvio | F017 | P1 | PFC-017 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-019** | PF-016 | `M015` L05-F4 + L05-F5: auto-close LAY su liability; FAILED post-invio senza table_id non lascia ACTIVE | PR29-e | P2 | — | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-020** | PF-017 | `M017` Conferma step del cycle executor legata al verdetto dell'engine | #519 | P2 | — | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-021** | PF-018 | `M018` Book SIM malformato = blocco pre-invio | #520 | P2 | — | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-022** | PF-019 | `M004` L07-F1: recovery fallita all'avvio => nessun ACTIVE<br>`M006` PF-ALL-F004: consumatore di RECOVER_ORDER | PR31-a | P0 | — | PR GATE | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-023** | PF-020 | `M002` L08-F3: fencing di riconciliazione mai fail-open<br>`M003` L08-F2: fencing token persistito e confrontato nel SQL | PR32-a | P1 | — | PR GATE | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-024** | PF-021 | `M023` Migrazioni DB versionate (schema_version, additive, rollback)<br>`M024` Hard-verify del delta c8fcaa3 (#446) | PR33 | P1 | — | PR GATE | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-025** | PF-023 | `M028` Dipendenza cryptography vincolata + primitiva AEAD (merge owner, manifest) | PR35 | P1 | — | PR GATE | OWNER | ESEGUIBILE |
| **PFC-026** | PF-024 | `M029` Migrazione non distruttiva dei segreti + checklist ACL Windows | PR35 | P1 | PFC-025 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-027** | PF-025 | `M047` Login SIM con App Key Delayed (P31)<br>`M048` Logout lato server (#343) | PR11-a+PR16 | P1 | — | PR GATE | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-028** | PF-026 | `M001` Contratto SIM/HYBRID_REAL_EXECUTION/LIVE + stake EXPLICIT/FIXED/MM | DOC-MODES | P2 | — | PR GATE | OWNER | ESEGUIBILE |
| **PFC-029** | PF-027 | `M026` Crash drill su SQLite reale (PF-C28-01..03, F4 durabilita')<br>`M027` STOP persistente e riavvio (OD12-07, PF-EXEC-06, F07 E2E) | PR34 | P0 | PFC-001, PFC-006, PFC-024 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-030** | PF-028 | `M005` Decision log di riconciliazione persistito<br>`M007` AUD-01: ordini ghost nei cap di mercato/evento | PR31-a | P1 | PFC-022 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-031** | PF-029 | `M045` OP-VIS + F022: segnalazione operatore (posizioni ricostruite, ghost, startup BLOCKED; campo stop in get_status) | PR47 | P1 | PFC-006 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-032** | PF-030 | `M065` L05-F6: OrderManager cablato o percorso latente ritirato<br>`M067` F003: produttore degli eventi del ciclo ordine | PR30-a | P1 | — | PR GATE | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-033** | PF-031 + M154 | `M066` FSM replace con AMBIGUOUS senza retry cieco<br>`M154` STOP drena anche il task dell'executor ancora vivo dopo AMBIGUOUS (limite dichiarato #537) | PR30-a | P1 | PFC-032 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-034** | PF-032 | `M099` AUD-03 + AUD-02: esito incerto dell'hedge = AMBIGUOUS + settlement senza betId | PR45-b | P0 | PFC-010, PFC-033 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-035** | PF-033 | `M068` F011 + F025: instructionReports top-level; SUCCESS senza betId => AMBIGUOUS<br>`M069` customer_ref di manual_bet + parita' SIM LAPSED | PR30-a | P1 | PFC-032 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-036** | PF-034 | `M100` #516: un solo cashout attivo per posizione<br>`M101` F06: fencing cashout per ambito evento<br>`M102` Parita' GUI/headless del cashout (test) | PR45-b | P1 | PFC-034 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-037** | PF-035 | `M049` Freshness/timestamp Market Stream; dati stantii => nessun ingresso (PF-EXEC-05) | PR11-b | P0 | PFC-027 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-038** | PF-036 | `M050` Reconnect, heartbeat, clock<br>`M051` Endpoint Italia (.it) e configurazione feed | PR11-b | P1 | PFC-037 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-039** | PF-037 | `M060` Order Stream: connessione, auth, heartbeat, reconnect (trasporto condiviso) | ORDER-STREAM-a | P1 | PFC-038 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-040** | PF-038 | `M061` Order Stream: cache ordini (oc/orc/uo), clk/initialClk<br>`M062` Order Stream: cablaggio runtime in sola lettura | ORDER-STREAM-a | P1 | PFC-039 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-041** | PF-039 | `M063` Matrice autorita' REST/stream per fill e cancel<br>`M064` Divergenza/disconnessione stream => riconciliazione fail-closed | ORDER-STREAM-b/RECON-02 | P0 | PFC-022, PFC-040 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-042** | PF-040 | `M106` Resolver neutro per nome cablato<br>`M107` Catalogo incrementale (delta, no-wipe) | PR08 | P1 | — | PR GATE | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-043** | PF-041 | `M052` F7 + F3: livello svuotato non abbina l'intero ordine<br>`M053` F9: ordine a riposo si riabbina col book | PR11-c | P2 | PFC-005 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-044** | PF-042 | `M054` listEventTypes/Competitions/Countries/MarketTypes/Venues/TimeRanges | REST-DISC-a | P2 | — | PR GATE | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-045** | PF-043 | `M055` listRunnerBook<br>`M056` listMarketProfitAndLoss (fonte P/L autorevole) | REST-DISC-b | P2 | PFC-044 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-046** | PF-044 | `M057` getAccountDetails + listCurrencyRates<br>`M058` getAccountStatement paginato | NEW-ACCOUNT | P2 | — | PR GATE | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-047** | PF-045 | `M059` updateOrders via autorita' ordini | NEW-UPDATEORDERS | P2 | PFC-033 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-048** | PF-046 | `M081` F01: direzione SL/TP e prezzo di riferimento<br>`M082` SL% su perdita netta liquidabile (U15HT) | PR24 | P0 | PFC-009 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-049** | PF-047 | `M036` F03/F032: EXPANSION su bankroll_start persistito e profitti consolidati | PR22 | P1 | PFC-017 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-050** | PF-048 | `M070` R1-R4: preflight tutto-o-niente del lotto + riserva esposizione (corsia dormiente) | PR30-b | P1 | PFC-033 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-051** | PF-049 | `M071` Cablaggio batch_manager + table_id dei lotti + finalize_terminal_batch<br>`M074` F018 per lotto: un tavolo per lotto al rebuild | PR30-b | P1 | PFC-006, PFC-050 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-052** | PF-050 | `M072` PF-EXEC-02: lotto parziale ribilanciato/coperto/sospeso<br>`M073` PF-EXEC-01: ripuntare il solo residuo dopo cancel confermato | PR30-b | P1 | PFC-051 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-053** | PF-051 | `M075` Publisher Dutching e budget equo<br>`M076` CASHOUT Esegui per Dutching<br>`M077` Suite E2E 72 casi #511 (sola prova) | PR09 | P1 | PFC-009, PFC-051 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-054** | PF-052 | `M078` Attivazione corsia Dutching (rimozione dormienza) solo con gate PASS | PR09 | P1 | PFC-053 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-055** | PF-053 | `M079` Stake EXPLICIT/FIXED/MM cablato (esclusivi, default da config, rifiuto se non valido)<br>`M080` Precedenze messaggio/GUI/MM e formula MM con esempi numerici | PR12-a+PR23 | P1 | PFC-008 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-056** | PF-054 | `M086` Miglior prezzo dinamico solo sull'unmatched con limiti verificabili<br>`M087` Uscite protettive non ritardate (PF-EXEC-03) | PR38-b | P1 | PFC-033, PFC-048 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-057** | PF-055 | `M089` P35: intento durevole (attesa, nessuna sostituzione, scadenza a chiusura mercato) | PR25 | P1 | PFC-009, PFC-048 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-058** | PF-056 | `M090` Trailing persistente<br>`M091` Auto-cashout MM | PR25 | P1 | PFC-057 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-059** | PF-057 | `M092` Cashout per target<br>`M093` Netting BACK/LAY | PR43-b | P1 | PFC-058 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-060** | PF-058 | `M083` F002: registro TTL DIRECT popolato (OFF di default)<br>`M084` TTL-PERSIST: registro TTL persistito al riavvio | PR38-a | P2 | PFC-024 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-061** | PF-059 | `M094` Void = hedge (P39) | PR43-b | P2 | PFC-059 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-062** | PF-060 | `M095` Cashout fraction/percentage (DEC-C44-03)<br>`M096` #517: minimo effettivo verificato al posto del floor 0,10 | PR44 | P1 | PFC-061 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-063** | PF-061 | `M039` F009: escalation anomalie con azione reale<br>`M040` F016: SafeMode e kill switch alimentati | PR20 | P1 | — | PR GATE | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-064** | PF-062 | `M041` F007-a: market_validator cablato nel percorso ordini<br>`M042` F007-b: auto_throttle cablato o ritirato con prova di ridondanza<br>`M043` F007-c: ai_guardrail cablato o ritirato con prova | F007 | P1 | PFC-063 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-065** | PF-063 | `M103` Control API: server locale, auth, handshake e versione<br>`M104` Correlation id request_id->operation_id->customer_ref + audit<br>`M105` Ritiro di betfair_market_api.py legacy | PF-API-0+PR36 | P1 | PFC-022, PFC-026 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-066** | PF-064 | `M108` Route lettura: stato, posizioni, ordini<br>`M109` Route lettura: mercati, resolver, fondi<br>`M110` Route lettura: Market P/L e stato Stream (OWN-MCP-02F) | PF-API-1 | P1 | PFC-040, PFC-042, PFC-045, PFC-065 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-067** | PF-065 | `M111` place_order (EXPLICIT/FIXED/MM, idempotente)<br>`M112` cancel/replace/update | PF-API-cmd-a | P1 | PFC-047, PFC-055, PFC-066 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-068** | PF-066 | `M113` Preview senza effetti (PR37)<br>`M114` STOP/resume con barriera fisica | PF-API-cmd-b+PR37 | P0 | PFC-067 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-069** | PF-067 | `M115` reconcile e cashout via API | PF-API-cmd-b+PR37 | P1 | PFC-036, PFC-068 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-070** | PF-068 | `M116` MANUAL a doppia conferma (OWN-002), principal automation distinto | PR12-b | P1 | PFC-069 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-071** | PF-069 | `M037` Shutdown una volta su errore main + HeadlessApp.stop ferma EventBus (F021) | PR17 | P2 | — | PR GATE | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-072** | PF-070 | `M038` F010: ordinamento EventBus e prova drain/backpressure | PR46 | P2 | PFC-071 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-073** | PF-071 | `M044` F005/F006: watchdog con liveness reale e heartbeat corretto<br>`M046` OD12-11: storico verificabile | PR47 | P2 | PFC-031, PFC-072 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-074** | PF-072 | `M123` Storico: writer e coordinator LIVE+SIM<br>`M124` Storico: pagine GUI | PR14 | P2 | PFC-015 | PR GATE, Xvfb+screenshot+handoff | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-075** | PF-073 | `M129` CI: autorizzazioni da fonte fidata + merge markers (#345) | PR49 | P2 | — | PR GATE | OWNER | ESEGUIBILE |
| **PFC-076** | PF-075 | `M133` Enforcement esclusione auto-merge (identita' agente, ruleset) | #472 | P2 | PFC-075 | PR GATE | OWNER | DIPENDENTE |
| **PFC-077** | PF-076 | `M135` Validatore tracciabilita' exit 0 e denominatore | GateA-a | P2 | — | PR GATE | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-078** | PF-077 | `M136` Manifest requisiti core<br>`M137` Tracciabilita' Roserpina multi-tavolo (#510) | GateA-b | P2 | PFC-077 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-079** | PF-078 | `M151` Parser neutrale deterministico (senza Telegram)<br>`M152` PF-CAP-08/13/18: nessuno stake o quota inventati | PR06 | P2 | — | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-080** | PF-079 | `M117` HYBRID_REAL_EXECUTION: identita' e stato distinti, DISABILITATA | HYBRID | P3 | PFC-015, PFC-070 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-081** | PF-080 | `M127` Ritiro percorsi XTrader legacy (callgraph)<br>`M128` F015/F020: codice finanziario duplicato o morto | PR48 | P3 | PFC-032 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-082** | PF-081 | `M131` Tooling pfckpt/pf_trace | #518 | P3 | — | PR GATE | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-083** | PF-082 | `M132` Strumenti invarianti G1-G7 (manifest/workflow a merge owner) | FORMAL-VERIF | P3 | PFC-082 | PR GATE | OWNER | DIPENDENTE |
| **PFC-084** | PF-083 | `M134` Motore di sblocco autonomo | #515 | P3 | PFC-076 | PR GATE | OWNER | DIPENDENTE |
| **PFC-085** | PF-084 | `M153` Rimozione parser regex dopo test di equivalenza | PR10 | P3 | PFC-079 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-086** | PF-085 | `M118` Catalogo GUI: contratto worker/on_success (#415) | PR07 | P2 | — | PR GATE, Xvfb+screenshot+handoff | agente (§0.9) — owner se il mandato lo riserva | ESEGUIBILE |
| **PFC-087** | PF-086 | `M119` GUI: scrollbar e DPI comuni | PR05 | P2 | PFC-086 | PR GATE, Xvfb+screenshot+handoff | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-088** | PF-087 | `M120` GUI: controlli safety visibili per tab (57/129) | PR05 | P2 | PFC-087 | PR GATE, Xvfb+screenshot+handoff | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-089** | PF-088 | `M121` Pannello osservabilita' unica<br>`M122` Safe Mode nella GUI + console readiness (#359) | PR13 | P2 | PFC-031, PFC-088 | PR GATE, Xvfb+screenshot+handoff | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-090** | PF-089 | `M125` Configurazione GUI dei 41 parametri Roserpina<br>`M126` Simulazione combinatoria Roserpina-41 (test) | #511 | P2 | PFC-055, PFC-088 | PR GATE, test FIN integrati, Xvfb+screenshot+handoff | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-091** | PF-090 | `M138` Backup SQLite cifrato<br>`M139` Scheduler e retention backup (12 h, 2 copie) | PR51 | P1 | PFC-026 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-092** | PF-091 | `M140` Restore drill isolato; errori disco/WAL/permessi | PR52 | P1 | PFC-091 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-093** | PF-092 | `M141` Pacchetto Linux (AppImage) + dipendenze dichiarate (F012) | PR53 | P1 | PFC-092 | PR GATE | OWNER | DIPENDENTE |
| **PFC-094** | PF-093 | `M142` Pacchetto Windows (exe) | PR53 | P1 | PFC-093 | PR GATE | OWNER | DIPENDENTE |
| **PFC-095** | PF-094 | `M143` Supervisore H24 Linux (systemd) (F008)<br>`M144` Supervisore Windows (NSSM) + parita' segnali | PR54 | P1 | PFC-093, PFC-094 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-096** | PF-095 | `M145` VERSION, pipeline Release, manifest verificabile | PR55 | P1 | PFC-095 | PR GATE | OWNER | DIPENDENTE |
| **PFC-097** | PF-096 | `M149` Accettazione whole-wiring sul runtime vero | PR59 | P0 | PFC-096 | PR GATE, test FIN integrati | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-098** | PF-097 | `M150` Sweep #380 e dossier di collaudo | PR60 | P1 | PFC-097 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-099** | PF-098 | `M146` Version checker, download con hash | PR56 | P3 | PFC-096 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-100** | PF-099 | `M147` Safe-state e updater separato | PR57 | P3 | PFC-099 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| **PFC-101** | PF-100 | `M148` Sostituzione, health, rollback | PR58 | P3 | PFC-100 | PR GATE | agente (§0.9) — owner se il mandato lo riserva | DIPENDENTE |
| PFC-S01 | — | Chat ID per-bot e due trasporti Telegram | PR18 | — | — | — | OWNER | SOSPESA (OWN-004/deferred) |
| PFC-S02 | — | Setup Bot API | PR21 | — | — | — | OWNER | SOSPESA (OWN-004/deferred) |
| PFC-S03 | — | Master/slave | PR39 | — | — | — | OWNER | SOSPESA (OWN-004/deferred) |
| PFC-S04 | — | Registry follower | PR40 | — | — | — | OWNER | SOSPESA (OWN-004/deferred) |
| PFC-S05 | — | Stake follower | PR41 | — | — | — | OWNER | SOSPESA (OWN-004/deferred) |
| PFC-S06 | — | Replica azioni follower | PR42 | — | — | — | OWNER | SOSPESA (OWN-004/deferred) |
| PFC-S07 | — | Ledger PR-flow su VPS | #497 | — | — | — | OWNER | SOSPESA (OWN-004/deferred) |

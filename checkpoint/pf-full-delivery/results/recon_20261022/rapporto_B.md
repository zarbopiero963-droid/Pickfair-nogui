# Rapporto B — Riconciliazione globale della roadmap (#461) su main `152613b`

Mandato: PF-POST-PR29C-MERGE-AND-RECONCILIATION-01 (owner, 22/10/2026), sezioni 5–6. Fase in sola lettura: nessuna modifica al codice di produzione, nessun merge, nessun LIVE. Baseline verificata: main `152613b30fb0719682d71ae6653f984079a48262` (squash di #535), CI 14/14, checkpoint seq 59 `6b4f1a2` DURABLE.

Fonti lette per intero: #461 (corpo + 224 commenti), #426 (corpo + 132), #453 (corpo + 101), #506 (corpo + 447), #351 (corpo + 84), 22 PR mergiate dal 1° ottobre con SHA di merge ricavati dai commit su main. Evidenza su main: lettura mirata del codice e **5319 test eseguiti sul checkout di `152613b`** (3875 + 1444), **0 falliti**, 4 xfail attesi (G7).

Criterio: una scheda è COMPLETATA solo con codice presente e cablato **e** test eseguiti con esito osservato. "PR mergiata" da sola non basta.

## 1. Stato delle 60 schede (PR01–PR60) e degli inserti

| Stato verificato | Schede numerate (60) | Inserti e voci di sequenza (20) | Totale (80) |
|---|---|---|---|
| COMPLETATO_CON_EVIDENZA | 6 | 13 | 19 |
| PARZIALE | 36 | 3 | 39 |
| MANCANTE | 10 | 3 | 13 |
| BLOCCATO | 8 | 0 | 8 |
| NON_VERIFICABILE | 0 | 1 (MCP-01a, repo esterno) | 1 |

Confronto con lo stato **dichiarato** in #461 (60 numerate): MERGED 6, PARZIALE 3, NON INIZIATO/PENDING 42, SOSPESO 6, DEFERRED 3. La differenza principale: **28 schede dichiarate "non iniziate" hanno già codice su main**, quasi sempre non cablato o incompleto (vedi §3).

### 1.1 Completate con evidenza (19)
PR01, PR02, PR03, PR04, PR04-bis, PR04-ter, PR04-quater, PR26-0/F001, PR26-a, PR26, PR27, PR28-slice1 (#502), PR28-b (#503), PR28-c (#504 = `028cee9`), PR28-Microstake (#512), PR28-d (#522), PR29-a (#533), PR29-b (#534), PR29-c (#535).
Residui che restano **di certificazione** (non di codice): verbali #351 NOT_RUN (T03/T09/T13/T14/T16/T18/T23/T30/T31), prova visiva GUI reale, login SIM con App Key Delayed (rinviato a PR11).

### 1.2 Parziali (39) — cosa manca, in sintesi
- **PR28 (ombrello)**: P06 cap per strategia DEFERRED; crash drill PF-C28-01..03 NOT VERIFIED (PR34); G7 #532.
- **PR28-e** (#531): gate fondi presente e testato (43), ma parità SIM/LIVE non certificata: `test_g7_sim_funds_reservation.py` 4 xfail (il SimulationBroker non riserva fondi); 29-f doppio conteggio riserve aperto.
- **PR29 (ombrello)**: 29-f aperto; T-F03/T-F06 con due runtime reali; mitigazione `.runtime.lock` (#536).
- **PR06/PR08/PR10 (parser)**: `services/delimiter_parser.py` e `services/deterministic_resolver.py` esistono ma sono orfani; il parser regex resta attivo (PR10 MANCANTE); trasporto Telegram sospeso (OWN-004).
- **PR07/PR13/PR14/PR17 (GUI e ciclo di vita)**: worker asincrono già usato (SYNC_CATALOG); safe mode e tab storico presenti; shutdown idempotente; mancano contratto formale, osservabilità unica, writer+coordinator dello storico, test del percorso errore→shutdown.
- **PR11 (Market Stream)**: `StreamingFeed` cablato (29 test); mancano login SIM Delayed, freshness per scheda, feed Italy.
- **PR12 (stake mode)**: `stake_mode.py` (MM/MASTER/FIXED, 19 test) non cablato nel runtime; manca MANUAL con preview via MCP (OWN-002).
- **PR15**: nessuna separazione mode-scoped SIM/LIVE di saldi e picco drawdown.
- **PR16**: `client.logout()` chiamato; prova server-side solo LIVE.
- **PR19/PR20/PR23/PR24**: commissioni SIM, hook escalation, formula MM, monitor SL/TP presenti; mancano marketBaseRate LIVE (OD-PF-COMMISSION-RATE-01), azione effettiva dell'escalation, precedenze messaggio/GUI/MM, SL% su perdita netta liquidabile.
- **PR22**: baseline EXPANSION già persistita (F032 via #512, 54 test); scheda mai chiusa formalmente.
- **PR30 (Dutching/lifecycle)**: FSM AMBIGUOUS reale (`order_manager.py`, `trading_engine.py`, #526), `RetryPolicy`, `replace_order`; **non cablato**: `OrderManager` non istanziato dal runtime (L05-F6), corsie Dutching dormienti (guardrail #523); mancano PF-EXEC-02, R1–R4 (#529, domanda owner aperta), batch manager, updateOrders.
- **PR31 (riconciliazione)**: `reconciliation_engine.py` + `state_recovery.py` cablati nello start; manca il decision log persistito e il blocco di ACTIVE su eccezione di recovery (oggi solo loggata, `runtime_controller.py` ~2630-2638); OD-PF-RECON-02 decisa ma non applicata; Order Stream assente.
- **PR32 (esclusività runtime)**: lock OS (PR29-c) + fencing token **gated da `enable_fencing_token`, fail-open se False** + lock in-process; nessun lease/registro cross-process; rischio `.runtime.lock` (#536).
- **PR33/PR34**: saga/outbox/migrazioni ad hoc esistono; prova SQLite a due processi OS già presente (21 test) ⇒ BLOCKED_ENV in parte superato; crash drill durevole non eseguito.
- **PR35**: `core/secret_cipher.py` è XOR-shake256, **non AEAD** (dichiarato in `database.py`); migrazione app key non distruttiva già fatta.
- **PR37/PR38**: `direct_best_price.py` e `direct_unmatched_ttl.py` presenti e testati (78) ma dormienti (flag OFF, registro TTL mai popolato); PF-BEST-EXECUTION-DYNAMIC-01 e PF-OD12-04 decise, non implementate.
- **PR43–PR45 (cashout)**: catena completa e cablata in headless e GUI (147 unit PASS); mancano target/fraction/netting/MTM; **PF-FIN-01/02 (MTM e realizzato errati nel position ledger) P1 aperto**.
- **PR46/PR47**: EventBus con drain/backpressure e observability (metrics/incidents/alerts/correlation) cablati; manca correlation id della Control API.
- **PR48/PR49/PR50/PR52/PR53/PR55**: callgraph, marker di merge (già usato da 3 workflow), orchestratori P16 (#527), build/release su tag presenti; build non eseguita in questo ambiente; dossier e parità OS NOT_RUN.
- **NEW-REST-DISCOVERY / NEW-ACCOUNT**: parziali (A08 #506: 17 Betting + 4 Account endpoint aperti).

### 1.3 Mancanti (13)
PR05 (verifica visiva GUI 57/129 controlli), PR10 (parser neutrale al posto della regex), PR25 (trailing/persistenza/auto-cashout MM), PR36 (Control API client GUI), PR51 (backup automatico cifrato), PR54 (supervisore H24), PR56/PR57/PR58 (updater, DEFERRED prima LIVE), PR60 (dossier), **PF-API-0/1/commands (Control API: `betfair_market_api.py` è legacy, senza auth, non cablata)**, NEW-ORDER-STREAM, NEW-UPDATEORDERS.

### 1.4 Bloccate (8)
PR09 (BLOCKS_DUTCHING_ACTIVATION, dormienza provata da `test_dutching_lane_dormant_523.py`), PR18 e PR21 (OWN-004 Telegram), PR39–PR42 (sospese OWN-004 a cascata da PR38), PR59 (VPS reali #351, Gate A NON PASS, G7, PR30; conflitto corpo #461 "PR58→PR59" vs OWN-003 Caso B).

## 2. Registri collegati
- **#426 decisioni owner**: 124 voci indicizzate. **Conflitti da riconciliare** (decisione owner richiesta, §7): drawdown €10 di campagna vs "nessun cap fisso"; TTL saldo 10 s (OD-PF-FUNDS-02) vs 30 s (THRESHOLD-01); `max_stake_abs` assente = blocco (t80u) vs None = non attivo (19/10); cap del fix loop (CLAUDE.md/#530 vs PF-QA-AUTONOMOUS-CONTINUATION-01, oggi 10 cicli); merge autonomo dei workflow (c.6286967995 vs CLAUDE.md: a merge owner); 4 nuove schede fuori dal denominatore 60.
- **#453 finding**: 97 record (58 OPEN/NOT VERIFIED, 31 FIXED con PR, 6 PARZIALI, 2 OBS); corpo mai ricontato dal 13/09; PF-C29-01 .11/.18 corretti in #535 ma non ancora registrati; nuovo #536 collegato.
- **#506 Gate A**: A01–A13 tutti aperti, NON PASS; validatore exit 1, denominatore `null` (M14: 307/327 parent senza child-set), D03 1035/1062 aperti.
- **#351 certificazione**: NO-GO, campagna non eseguita, tutti i gate NOT_RUN/BLOCKED; #536 aggiunto come prerequisito LIVE.

## 3. Lavoro realmente completato vs dichiarato
- Completato con evidenza: 19 voci (tutte le PR mergiate dal 1° ottobre tranne PR28-e, declassata a PARZIALE per G7).
- **Sopravvalutato in #461**: PR02 e PR28-e (direzioni opposte: PR02 è completa, PR28-e no).
- **Sottovalutato in #461**: 28 schede "non iniziate" con codice già su main (PR06, 07, 08, 11, 12, 13, 14, 15, 16, 17, 19, 20, 22, 23, 24, 30, 31, 32, 33, 34, 35, 37, 38, 43–50, 52, 53, 55). Per la maggior parte il codice esiste ma **non è cablato** (parser, resolver, stake_mode, OrderManager, best price/TTL) o è incompleto (AEAD, decision log, mode-scoped).

## 4. Aree trasversali (stato reale su main)
| Area | Stato | Evidenza / mancanze |
|---|---|---|
| Roserpina multi-tavolo e recovery | PARZIALE | 67+4+12+66 test PASS; #510 non certificato; recovery per tavolo con ledger exactly-once (PR29-c); reset di ciclo con più processi = #536 |
| SIM e parità SIM/LIVE | BLOCCATO su G7 | il SimulationBroker non riserva fondi (4 xfail strict); gate fondi LIVE legge il conto exchange |
| Cashout / hedge / Dutching | PARZIALE | cashout cablato (147 unit); Dutching dormiente (#523); PF-EXEC-02 e R1–R4 aperti; PF-FIN-01/02 P1 |
| Betfair REST / Market Stream / Order Stream | PARZIALE / PRESENTE / ASSENTE | discovery A08 aperta (17+4 endpoint); `StreamingFeed` cablato; Order Stream e updateOrders assenti |
| GUI Windows/Linux | PASS sotto Xvfb | parità OS NOT_RUN; build not run in this environment |
| Control API e coordinamento MCP | MANCANTE | PF-API-0/1 assenti; `betfair_market_api.py` legacy non cablata; MCP-01a in repo esterno (MCP_MUTATION_EXPOSURE=BLOCKED_UNTIL_CORE_AUTHORITY_READY) |
| Protezione del runtime lock | PARZIALE, blocker LIVE | lock OS su `realpath(db).runtime.lock`, nessuna verifica dell'inode, nessun PID/fencing nel file; file cancellato ⇒ secondo runtime parte senza `RUNTIME_START_REFUSED`; nessun test del caso; fencing token DB gated e fail-open se disattivo → #536 |

## 5. Dipendenze e ordine di integrazione
1. **Fondazioni senza dipendenze** (possono partire da `152613b`): #536 lock/fencing (PR32-slice); PR31 hardening (decision log, blocco ACTIVE su eccezione); PR35 AEAD; PR11 login Delayed + freshness; PR19 marketBaseRate; PR15 mode-scoped; PR22 chiusura formale; PR49/PR50 governance; PR17 test shutdown.
2. **Contratto Control API (PF-API-0/1)**: prerequisito seriale di PR36, PR37 (preview), PR45, PR47 (correlation id), MCP-01a (mutazioni reali), PR12 MANUAL.
3. **REST discovery (NEW-REST-DISCOVERY, A08)**: prerequisito di Order Stream e updateOrders, che sono prerequisiti di PR30/PR31 completi.
4. **PR30 Dutching**: dopo PF-EXEC-02, R1–R4 (#529), batch manager, updateOrders; sblocca PR09, PR45.
5. **PR38 best price/TTL**: PF-OD12-04 decisa; attivazione dopo PR37 preview e PR30 (riprezzamento solo dell'unmatched).
6. **PR43/PR44 cashout avanzato**: dopo PF-FIN-01/02 (P1) e PR25.
7. **Coda LIVE**: PR51 (backup, dopo PR35) → PR54 → PR55 → PR56–58 (DEFERRED) → PR59 (VPS, Gate A, G7, #536) → PR60.
8. **Sospese**: PR18, PR21, PR39–PR42 (OWN-004).

## 6. Roadmap aggiornata (proposta, da ratificare)
- **Lotto R1 (fondazioni, indipendenti)**: #536, PR31-h, PR35, PR11-a, PR19-a, PR15, PR22 (chiusura), PR17-t, PR49/50.
- **Lotto R2 (contratti)**: PF-API-0/1 + route table MCP; NEW-REST-DISCOVERY (A08); OD-PF-RECON-02 applicata.
- **Lotto R3 (integrazione)**: Order Stream, updateOrders, PR30 (con PF-EXEC-02 e R1–R4), PR09, PR12 MANUAL, PR36/37/38.
- **Lotto R4 (prodotto)**: PR43–45, PR25, PR46/47, PR13/14, PR06/08/10 (se OWN-004 riaperta).
- **Lotto R5 (LIVE)**: PR51, PR54, PR55, PR56–58, G7, #351 campagna, PR59/60.

## 7. Decisioni owner ancora necessarie
1. **Serialità delle PR**: CLAUDE.md impone una sola PR attiva per repository. La Parallel Factory richiede o una deroga esplicita (più PR aperte, integrazione seriale) o il modello "sviluppo parallelo su branch, apertura PR seriale" (§8). Senza decisione, la Factory non è eseguibile.
2. **#536 / OWNER-MULTIPROCESS-03**: mandato per la mitigazione (lease nel DB + verifica inode + fencing obbligatorio); confermare che `enable_fencing_token` non possa restare fail-open in LIVE.
3. **R1–R4 (#529) e PF-EXEC-02** per PR30.
4. **OD-PF-RECON-02**: ordine di applicazione (prima o dopo Order Stream).
5. **Conflitti di decisione** elencati in §2 (#426): drawdown €10, TTL saldo, `max_stake_abs`, cap fix loop, merge autonomo workflow.
6. **PR59**: dipendenza da PR58 (corpo #461) o Caso B (OWN-003).
7. **PR35 AEAD**: introduce una dipendenza crittografica (manifest = merge owner, supply chain).
8. **Denominatore**: numerare le 4 nuove schede (Order Stream, REST discovery, Account, updateOrders) dentro o fuori le 60.
9. **OWN-004**: perimetro della sospensione Telegram rispetto a PR06/PR10 (parser senza trasporto).
10. **#453/#506**: autorizzare l'aggiornamento dei registri con PF-C29-01 .11/.18 e con la riconta del corpo.

## 8. Piano Parallel Development Factory (PF-POST-PR29C-PARALLEL-FACTORY-01 — PENDING, non avviato)
- **Attività residue reali**: 61 (39 parziali + 13 mancanti + 8 bloccate + 1 non verificabile); di cui eseguibili senza decisione owner: 9 (Lotto R1); con decisione: 14 (R2/R3); bloccate da gate esterni o sospensioni: 38.
- **PR indipendenti sviluppabili in parallelo (prima ondata, max 3 sessioni contemporanee)**:
  - W1 `#536` lock/lease/fencing — file: `core/runtime_controller.py` (solo sezione lifecycle/lock), `database.py` (tabella lease), `tests/acceptance/test_issue461_pr29c_settlement_processes_and_schema.py`, docs. Safety-critical, review completa.
  - W2 `PR35` AEAD — file: `core/secret_cipher.py`, `database.py` (sezione segreti), test unit; manifest a merge owner.
  - W3 `PR11-a` login Delayed + freshness — file: `services/betfair_service.py`, `core/streaming_feed*.py`, test; nessuna sovrapposizione con W1/W2.
  - Seconda ondata (dopo merge di W1): `PR31-h` (`core/reconciliation_engine.py`, start section di `runtime_controller.py`), `PR19-a` (`core/pnl_engine.py` commissioni), `PR15` (chiavi settings mode-scoped).
- **PR dipendenti**: PF-API-0/1 (seriale, apre PR36/37/45/47), REST discovery (apre Order Stream/updateOrders), PR30 (dopo decisioni), PR38 (dopo PR37/PR30), PR43/44 (dopo PF-FIN-01/02).
- **Branch e SHA di partenza**: `main @ 152613b30fb0719682d71ae6653f984079a48262`; un branch per attività `lots/pf-<scheda>`; task key registrata in `.guardrails/allowed_scope.json` nello stesso PR con `files` = diff.
- **Confini dei file**: ogni worker dichiara `files_allowed` disgiunti; `core/runtime_controller.py` assegnato a **un solo** worker per ondata; `.github/workflows/*`, `scripts/pr_*`, CLAUDE/AGENTS/spec esclusi (merge owner).
- **Rebase e integrazione seriale**: i worker sviluppano in parallelo; le PR si aprono una alla volta nell'ordine W1→W2→W3; dopo ogni merge gli altri branch fanno `rebase` su main e rieseguono guard/full-diff, regressione completa e mutation prima della propria apertura. Nessun force-push su branch altrui.
- **Budget AI**: per PR ≈ 1 push iniziale + ≤3 cicli di fix attesi (cap 10) × (Sol ≈ $0.05–0.09 + Grok ≈ $0.04–0.14) ≈ $0.4–0.9/PR in review; sessioni: 1 orchestratore + max 3 worker contemporanei; stop immediato per crediti esauriti su Sol/Grok.
- **Test e review richiesti per ogni PR**: test RED→GREEN, mutazioni nel catalogo (KILLED), `py_compile`, regressione completa, guard + full-diff, Sol e Grok senza bloccanti sul head finale, Codex se disponibile, thread risolti solo a check conclusi, hard verify, docs nello stesso PR; merge solo su autorizzazione owner per le PR safety-critical finché la Factory non è ratificata.
- **Non avviato**: nessuna sessione parallela è stata creata.

## 9. Artefatti
`scratchpad/recon/`: `schede_461.json`, `decisioni_426.json`, `finding_453.json`, `gate_a_506.json`, `cert_351.json`, `pr_merged.json`, `evidenza_main_A.json/.md`, `evidenza_main_B.json/.md`, `note_agente1.md`, `note_agente2.md`, `rapporto_B.md` (questo file). Copiati nel checkpoint `results/recon_20261022/`.

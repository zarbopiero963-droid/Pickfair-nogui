# Changelog

All notable changes to Pickfair are documented here.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

### Fixed

- **PR #512 ciclo 17 (PF-QA-AUTONOMOUS-CONTINUATION-01):** bloccante GPT-6.1 Sol su `9f0d6ec`. La riconciliazione
  sull'exchange vale ora per ogni settlement non SIM per mercato (anche chiusura di posizione e payload senza
  `event_key`), non solo per `cleared:`; senza verifica possibile nessun rilascio per mercato. Test esistenti che
  rilasciavano con payload senza chiave e senza exchange (`tests/f026/test_f026_units.py`, due test c14) usano ora la
  chiave SIM per mercato che intendevano. Test `test_c17_*`.
- **PR #512 ciclo 16 (PF-QA-AUTONOMOUS-CONTINUATION-01):** bloccante GPT-6.1 Sol su `23bdbfe` (filesystem in sola
  lettura: falliscono sia il registro hedge sia la sentinella). `_market_has_foreign_cleared_legs`: prima di un
  rilascio per mercato da settlement cleared per bet si interroga l'exchange (`listClearedOrders` per mercato); bet
  regolate non del bot, lettura fallita o risposta vuota/malformata => nessun rilascio (fail-closed, nessuna
  dipendenza da scritture locali). Test `test_c16_*` in `tests/test_pr512_codex_034af59.py`.
- **PR #512 ciclo 15 (PF-QA-AUTONOMOUS-CONTINUATION-01):** bloccante GPT-6.1 Sol su `180ffa3`. Se la scrittura del
  registro `roserpina.hedged_markets` falliva, il fail-closed restava in RAM e dopo un riavvio il registro leggibile
  (senza quel mercato) era trattato come affidabile. Ora si scrive anche una sentinella durevole accanto al DB
  (`<db_path>.hedged_markets.unreliable`, fsync); se presente all'avvio il registro e' inaffidabile finche'
  l'operatore non la rimuove dopo la riconciliazione. Test `test_c15_*` in `tests/test_pr512_codex_034af59.py`.
- **PR #512 ciclo 14 (PF-QA-AUTONOMOUS-CONTINUATION-01 / PF-PR-SEQUENCE-RECOVERY-01, owner 19/10/2026):** la verifica
  indipendente dei rinvii ha classificato BLOCKING il residuo di r4303297587. Con posizione singola + hedge di cashout
  (che passa da OrderRouter senza riga in `orders`) il settlement per bet senza `table_id` di una sola gamba liberava
  il tavolo con il P&L di quella gamba, falsando la perdita di recovery e lo stake successivo. Ora ogni esito di
  cashout (`CASHOUT_SUCCESS`/`CASHOUT_FAILED` con mercato) marca il mercato nel registro durevole
  `roserpina.hedged_markets` (lock, 365 giorni) e `_single_bet_position` rifiuta il rilascio per un settlement PER
  BET su quel mercato, o con registro illeggibile/non persistito (fail-closed); il settlement SIM per mercato (netto
  di tutte le gambe) continua a rilasciare. Corretta
  anche la nota del ciclo 10 qui sotto («limite invariato rispetto a main»: falsa per l'effetto sulla recovery). Test
  RED->GREEN in `tests/test_pr512_codex_034af59.py` (`test_c14_*`). Rilievi Codex su `6138254` corretti nello stesso
  ciclo: (a) `core/liquidity.single_entry_liquidity` richiede stato esplicito OPEN/ACTIVE, una risposta senza stato
  non e' liquidita' osservata (r4312719713); (b) `MicrostakeCoordinator.execute`: ordine gia' presente con lo stesso
  `customer_ref` => AMBIGUOUS con riserva trattenuta, mai FAILED, e `recover()` non adotta mai quell'ordine
  (r4312719704); (c) `pfckpt.capture_code_patch` non dereferenzia mai i symlink non tracciati: registra solo il testo
  del link (`skipped_symlinks`), il ripristino lo ricrea (r4312719746, sicurezza). Test in
  `tests/agent_checkpoint/test_pfckpt_codex_034af59.py::test_c14_*`.
- **PR #512 ciclo 13 (PF-QA-AUTONOMOUS-CONTINUATION-01, owner 19/10/2026, #426):** rilievi Codex su `4231de9`.
  (1) `TableManager.restore_recovery_state`: una perdita di recovery persistita negativa e' stato illeggibile
  (avvio in PAUSED), non piu' un debito cancellato in silenzio; lo zero resta ammesso (r4312360615). (2)
  `TradingEngine._resolve_executor_future`: un `CancelledError` sollevato dalla callable gia' partita e' AMBIGUOUS,
  non pre-invio; solo un Future davvero cancellato resta pre-invio (r4312360608). (3) `BetfairClient.get_current_orders`
  e `BetfairService._well_formed_current_orders`: `currentOrders` assente/null e' `CURRENT_ORDERS_MALFORMED`, mai
  "nessun ordine" per la ricostruzione F018; la lista vuota esplicita resta il caso legittimo (r4312360620). Il test
  legacy che codificava il fail-open (`test_get_current_orders_empty_result_returns_empty_list`) usa ora una lista
  vuota esplicita. Test RED->GREEN in `tests/test_pr512_codex_034af59.py` (`test_c13_*`).
- **PR #512 ciclo 12 (decisioni owner del 19/10/2026: autorizzazione straordinaria del ciclo 12 e
  PF-QA-AUTONOMOUS-CONTINUATION-01, #426):** P1 Codex su `3d3f5cc`. (1) `NativeSimMicrostakeExecutor.execute`: un
  errore ordinario del DB (non `SagaStoreError`) dopo un'operazione exchange diventa AMBIGUOUS
  (`saga_store_write_failed_after_exchange_call`), con riserva trattenuta e dedupe consumato; prima finiva FAILED
  (PERMANENT) con lo stub vivo sul broker e il tavolo libero (r4311806874). (2) Emergency stop: la provenienza dei
  settlement si persiste insieme al flag e si ripristina al riavvio; assente o non valida => `UNKNOWN`, trattata come
  LIVE (r4311806886). (3) Riesame indipendente: in `SimulationExchangePort.place` un errore di lettura DOPO
  l'accettazione dell'ordine e' una risposta persa (`LostResponse`, riconciliazione per `customer_ref`), non un errore
  generico che rilasciava la riserva chiudendo FAILED con lo stub vivo. Test RED->GREEN in
  `tests/test_pr512_codex_034af59.py` (`test_c12_*`, compresa la dedupe durevole dopo un nuovo engine).
- **PR #512 ciclo 10 (deroga owner PF-PR512-CYCLES-08-10-AUTHORIZATION, ultimo ciclo):** rilievi Codex su `b918a6d`.
  (1) il reset trattiene OGNI tavolo con esito ignoto dal registro per tavolo, anche a mercato regolato (r4308704060);
  (2) stato di recovery non persistito => nessun nuovo ingresso su ogni percorso (`recovery_state_not_persisted`, via
  `_loss_block_reason`) con nuovo tentativo di scrittura a ogni ammissione, che scioglie il blocco appena il disco
  torna (r4308704062); (3) all'avvio le riserve transitorie delle saghe pendenti (`reservation_held`, assente =
  tenuta) sono ripristinate prima di snapshot e recovery, cosi' lo stub pendente occupa il cap (r4308704071);
  (4) `pf_trace`: solo un'evidenza PASS copre un test dichiarato (r4308704068); (5) `pfckpt`: `STATE.json` JSON
  valido ma non oggetto e' un checkpoint invalido strutturato (`STATE_JSON_INVALID`), saltato da
  `last_valid_checkpoint` (r4308704074). Test RED->GREEN in `tests/test_pr512_codex_034af59.py`,
  `tests/traceability/test_pf_trace_codex_034af59.py`, `tests/agent_checkpoint/test_pfckpt_codex_034af59.py`.
  Verifica indipendente dei thread rinviati (decisione owner opzione 2): due non separabili corretti qui. (a)
  Settlement per bet senza `table_id`: il tavolo si libera per mercato solo se la posizione e' a selezione singola con
  UN solo ordine del bot sul mercato/evento che possa essere stato piazzato (`_single_bet_position`; esclusi solo
  FAILED/DENIED/DUPLICATE_BLOCKED); Dutching, piu' ordini persistiti o DB illeggibile => nessun rilascio, tavolo
  trattenuto, mercato marcato regolato. Limite noto (corretto nel ciclo 14, vedi sopra): un hedge di cashout passa da
  OrderRouter senza riga in `orders` e non era visto (lotti D-D/D-F) (prima il P&L della prima gamba diventava la perdita di
  recovery del tavolo; aggregazione per `bet_id` = lotto D-F, r4303297587). (b) Dossier dei 41 parametri: nessun
  clamp dello stake minimo, rifiuto esplicito (r4306544234). Test CI robusto al clone shallow; `pfckpt` rifiuta
  CURRENT/TASK_STATE non oggetto; cinque database finti di test con impostazioni in memoria (estensione di scope
  dichiarata). Rollback microstake con stub abbinato anche in parte prima della cancellazione => AMBIGUOUS
  `rollback_matched` con riserva tenuta, mai ROLLED_BACK/FAILED (riesame indipendente).
- **PR #512 ciclo 11 (autorizzazione owner straordinaria del 19/10/2026, #426; ultimo ciclo, il 12 non e' autorizzato):** bloccanti di Grok su `9ab753a`. (1) Settlement per bet
  senza `table_id`: conta ogni ordine del bot sullo stesso mercato, qualunque selezione; il tavolo si libera solo se
  l'unico ordine del mercato e' il suo (prima il P&L di un'altra selezione poteva diventare la perdita di recovery del
  tavolo, confermato su sqlite reale). (2) Recovery delle riserve microstake: importo transitorio ignoto => liability
  dello stub ricalcolata da lato e prezzo persistiti, o riserva illimitata; mai il solo stub da 2 EUR. Rilievi Codex su `9ab753a`
  confermati dalla verifica indipendente: (3) rollback deciso con stub gia' abbinato in parte => il recover cancella
  comunque il residuo, poi AMBIGUOUS `rollback_matched` (r4309039350); (4) F028 libera il tavolo dell'ORDINE
  (`table_id` persistito con verifica dell'evento), mai quello di un altro ordine vivo sullo stesso evento; senza
  `table_id` e con piu' tavoli nessun rilascio (r4309039372); (5) Dutching: book assente con guard in blocco =>
  `LIQUIDITY_UNKNOWN` per ogni gamba (r4309039358). Trasferiti: r4309039345 -> #521, r4309039363 -> #516,
  r4309039377 -> #518. Test di integrazione `test_fail_open_when_book_missing_block_mode` (codificava il fail-open)
  sostituito da `test_unknown_liquidity_when_book_missing_blocks_in_block_mode`; dossier, docstring e
  `docs/roserpina_bankroll_equity.md` (F028 per `table_id`) allineati.
- **PR #512 ciclo 9 (deroga owner PF-PR512-CYCLES-08-10-AUTHORIZATION):** (1) readback del checkpoint con tutti i branch
  remoti; evidenza su albero non pubblicato ammessa solo se dichiarata (`unpublished_tree` + `sha_note`); (2) bloccante Sol:
  cancellazione del rollback fallita => risultato AMBIGUOUS con riserva tenuta ma fase persistita ROLLBACK_PENDING, il
  recover successivo ritenta solo la cancellazione (mai replace); (3) nessun secondo ordine su un evento con tavolo gia'
  occupato (`evento_gia_in_posizione`) e chiavi anti-duplicato trattenute/ricostruite esenti dal TTL (Codex r4307328207,
  difetto preesistente su main ristretto); (4) baseline F032, stato di recovery e marker del reset in una sola
  transazione (r4307328199); (5) saga ROLLED_BACK riconciliata all'avvio, non pendente (r4307328204); (6) `pf_trace`:
  un test dichiarato e' coperto solo da un'evidenza che lo nomina, mai per implicito (r4307328211); aggiunta l'evidenza
  reale mancante per `tests/test_pr512_codex_findings.py` (PF-MS-01/02/05, append-only). Test RED->GREEN in
  `tests/test_pr512_codex_034af59.py`, `tests/traceability/test_pf_trace_codex_034af59.py`,
  `tests/agent_checkpoint/test_pfckpt_codex_034af59.py`.
  Riesame indipendente del ciclo 9 (prima del push): (a) l'intento di rollback della saga e' un flag durevole
  `rollback_intent`, che un recover senza ordine visibile non declassa ad AMBIGUOUS (prima il recover successivo
  riduceva e riprezzava l'ordine da cancellare); un residuo gia' cancellato con snapshot SIM non persistito si chiude
  ROLLED_BACK al giro dopo, senza seconda cancellazione; (b) `DuplicationGuard.unpin`: una chiave fissata al riavvio
  torna alla scadenza per TTL quando F028 libera il tavolo (prima bloccava l'evento fino al riavvio); (c) un reset di
  ciclo non persistito resta in sospeso e viaggia con la scrittura successiva dello stato tavoli; (d) `pf_trace`: gli
  argomenti delle opzioni non sono bersagli, i comandi che filtrano i test (-k, -m, --deselect, --ignore, --lf,
  --collect-only) o che non invocano pytest non coprono nulla senza `tests` esplicito; (e) `unpublished_tree` esige
  comunque uno SHA ben formato; (f) una saga pendente nello store ma assente dai risultati del recover resta pendente.
  Secondo riesame indipendente (prima del push): un reset non persistito e' abbandonato e dichiarato
  (`CYCLE_RESET_ABANDONED`) quando lo stato si ricarica dal disco (START nello stesso processo), mai applicato alle
  perdite del ciclo vecchio; sul broker SIM reale, dove l'ordine cancellato sparisce dagli ordini correnti, il rollback
  deciso si chiude leggendo per bet id la riga terminale senza residuo (un ordine ancora vivo resta pendente);
  `pf_trace` riconosce i filtri nei gruppi di opzioni corte (`-qk`, `-vm`), le opzioni che non eseguono test
  (`--setup-plan`, `--fixtures`, ...) e `pytest` solo in posizione di comando; ogni SHA di test del checkpoint deve
  essere esadecimale (mai `HEAD`/`main`); con store illeggibile dopo il recover si riportano tutti i ref pendenti.
  Terzo riesame: fixture di `tests/test_pr512_review_cycle6.py` allineata a `acquire(..., pin=True)`; test sulle righe
  terminali con abbinato trovate per bet id (sempre AMBIGUOUS); `pf_trace` non deriva copertura da `|| true`,
  `PYTEST_ADDOPTS`, `-o addopts` e `--cache-show`.
  Nota di comportamento: il rifiuto `evento_gia_in_posizione` (un solo ordine per evento in posizione) vale con
  `anti_duplication_enabled=true` (default), come estensione del dedupe oltre il TTL. Con l'anti-duplicazione
  disattivata dall'utente restano OBBLIGATORI il rifiuto su una posizione con esito incerto (AMBIGUOUS, registrata
  per tavolo: l'esito certo di un altro ordine sullo stesso evento non la cancella) o con chiave fissata
  (`DuplicationGuard.is_pinned`: ricostruita al riavvio da F018, o trattenuta dal reset se la chiave era attiva) e il
  dedupe dell'engine per customer_ref sulla riconsegna dello stesso messaggio (mandato owner di chiusura #512). Un
  nuovo segnale su una posizione regolare e nota, nello stesso processo, segue il comportamento precedente; dopo un
  riavvio la posizione ricostruita e' fissata e blocca (prudenziale).
- **PR #512 — patch preparate per i 10 rilievi Codex su 034af59 (in attesa di deroga ciclo 8).**
  (1) `services/betfair_service.py`: riga di `currentOrders` senza `betId`/`marketId` ⇒
  `CURRENT_ORDERS_MALFORMED` ⇒ F018 in PAUSED, mai "posizione chiusa"; (2) `core/microstake_sim_port.py`:
  snapshot finale del broker SIM non persistito ⇒ saga AMBIGUOUS con riserva tenuta, mai SUCCESS/ACTIVE,
  `recover_pending()` ritenta lo snapshot prima di promuovere; (3) `controllers/dutching_controller.py`:
  liquidità ignota con guard armato ⇒ shortfall `LIQUIDITY_UNKNOWN` (blocco); (4) `core/microstake_safe.py`:
  `recover()` su ROLLBACK_PENDING cancella il residuo (ROLLED_BACK / AMBIGUOUS), mai replace;
  (5) `core/runtime_controller.py`: `reset_cycle`/auto-reset con persistenza fallita ⇒
  `_recovery_persist_failed`, evento `RECOVERY_STATE_PERSIST_FAILED`, `persisted: false`; (6) stato di
  recovery illeggibile al riavvio ⇒ PAUSED (`RECOVERY_STATE_UNREADABLE`), `resume()` non lo scavalca,
  nessuna sovrascrittura; (7) docstring di `core/microstake_safe.py` veritiera; (8) `pf_trace`: l'ultima
  evidenza deve essere PASS; (9) `pfckpt.verify`: ogni `tests_executed[].sha` risolto come `code.sha`;
  (10) `core/money_management.py`: crescita ignota ⇒ NORMAL anche con soglia ≤ 0. Test:
  `tests/test_pr512_codex_034af59.py`, `tests/traceability/test_pf_trace_codex_034af59.py`,
  `tests/agent_checkpoint/test_pfckpt_codex_034af59.py` (tutti RED prima della patch).
- **PR #512 ciclo 7 (deroga owner PF-PR512-CYCLE7-AUTHORIZATION, tre P1 Codex su f62f0e3):** (1) riga pendente senza
  lato persistito => non attribuita, mai ricostruita come BACK (la liability di un LAY sarebbe sottostimata;
  r4306544229); (2) `reset_cycle()` non azzera il broker SIM finche' un tavolo resta trattenuto (esito
  `sim_reset_skipped`; r4306544236); (3) baseline di espansione piu' vecchia dell'ultimo reset di ciclo durevole
  (marker `roserpina.cycle_reset_state` scritto nella stessa transazione dello stato tavoli RSR-12) rifiutata al
  riavvio: nessuna espansione da una baseline mai ribasata (r4306544231). Riesame indipendente del ciclo: baseline e
  marker dello stesso reset portano lo stesso istante (la baseline fissata da `reset_cycle`/auto-reset sopravvive al
  riavvio), un marker illeggibile vale come reset ignoto e un `set_at_ts` non finito (NaN/inf) nella baseline non
  scavalca mai il confronto (nessuna espansione, fail-closed). Test RED->GREEN in
  `tests/test_pr512_codex_f62f0e3.py`; doc `docs/roserpina_bankroll_equity.md` (F032).
- **PR #512 ciclo 6 (deroga owner PF-DECISIONS-DA-DJ-20261018, D-A=A1):** (1) `MicrostakeCoordinator.execute` conserva
  la riserva transitoria **preesistente** quando `store.load` fallisce su un retry dello stesso `customer_ref`
  (rilascio solo di una riserva sicuramente nuova; Sol c.6242912622); (2) costruzione e rebinding dell'esecutore
  Microstake serializzati da un lock dedicato in `TradingEngine` (due chiamate concorrenti => un solo esecutore,
  una sola mappa di riserve, un solo store; Codex r4304867227); (3) decisione D-C: `BetfairClient.get_current_orders`
  e `BetfairService.list_current_orders` sollevano `CURRENT_ORDERS_MALFORMED` su contenitore non-lista o riga non-dict
  invece di filtrare a vuoto, cosi' la ricostruzione all'avvio finisce in `EXCHANGE_ORDERS_UNAVAILABLE` e non chiude
  posizioni SUBMITTED vive (Codex r4304867229). Test RED→GREEN in `tests/test_pr512_review_cycle6.py`.
- **PF-PR-RECOVERY-DECISIONS-02 (owner 17/10, lotto unico sui sei finding triagiati di PR #512):** `resume()` ritenta
  anche la recovery delle saghe Microstake (non solo F018) e resta PAUSED finché entrambe non riescono; stake con più
  di due decimali rifiutato prima dell'invio (`invalid_size_precision`) e residuo del broker diverso dallo stake
  richiesto ⇒ AMBIGUOUS `unexpected_residual_size`; ladder del book malformata ⇒ liquidità ignota (RiskGate nega),
  mai un'eccezione dopo tavolo e dedupe acquisiti; `persist_snapshot` del broker SIM scritto sotto lock e fallimento
  dichiarato nel risultato (`snapshot_persisted`); `pre_send` calcolato per `customer_ref`; scan segreti del
  checkpoint esteso ai sorgenti Python (esente solo `pfckpt.py`); `pf_trace` verifica che ogni SHA di evidenza sia un
  commit reale (clone shallow ⇒ warning, `--strict` lo rende errore). Test: `tests/test_pr512_recovery_fixes.py`.
- **PR #512 ciclo 4 (rilievi Codex su cad07e1/21aab95):** esecutore Microstake in cache legato al broker SIM e alla
  modalità correnti (ricostruito dopo stop/start, mai l'adapter SIM in LIVE); fondi disponibili non leggibili o
  incompleti ⇒ cap transitorio non disponibile; riserva transitoria rilasciata se lo store della saga fallisce prima
  dell'invio; cashout AMBIGUOUS conserva il `matched` noto; nessun nuovo hedge nativo su una posizione con saga
  pendente (`MICROSTAKE_PENDING_SAME_POSITION`); valori non finiti nella ladder ⇒ liquidità ignota; perdita di
  recovery persistita non finita ⇒ stato illeggibile; `pfckpt` cattura i percorsi con spazi (`--porcelain -z`).
  Test: `tests/test_pr512_review_cycle4.py`.
- **Decisione owner 2 (PF-PR-RECOVERY-DECISIONS-02):** `RoserpinaMoneyManagement.calculate` non ridimensiona più lo
  stake al tetto del singolo ordine: sopra `max_single_bet` rifiuto esplicito `supera_max_single_bet` con lo stake
  richiesto (anche per i passi di recovery). Riserva transitoria di una saga Microstake già avviata conservata anche se
  `recover()` fallisce (rilievi Sol/Grok su 464f347). Test: `tests/core/test_money_management_decision2_20261017.py`.

### Added
- **Microstake nativo automatico (PF-MICROSTAKE-NATIVE-INTEGRATION-02, decisioni owner PF-MICROSTAKE-NATIVE-AUTO-01 e
  PF-MICROSTAKE-OWNER-RECTIFICATION-20261017):** ogni importo sotto il minimo exchange (2 EUR) passa dal Microstake
  nativo — saga PLACE stub 2 EUR a prezzo passivo → `sizeReduction` al residuo esatto → REPLACE solo se serve (nuovo
  betId persistito) — sul `SimulationBroker` reale (`core/microstake_sim_port.py`), senza flag; LIVE senza adapter
  resta fail-closed. Vale per Quick Bet, Dutching, Roserpina e per l'hedge del cashout (`OrderRouter`). Esposizione
  temporanea dello stub entro fondi disponibili e headroom di esposizione; saga nel DB (`microstake.sagas`),
  recovery all'avvio prima di F018 (PAUSED se pendente). `SimulationBroker.cancel_orders` supporta `sizeReduction`.
  Docs: `docs/microstake_native.md`.

- **Continuità multi-agente (PF-AGENT-CONTINUITY-01):** `pfckpt` aggiunge `CURRENT.json`, `TASK_STATE.json`
  (puntatori verificati contro STATE.json), `AGENT_LEASE.json` (lease con scadenza e fencing token letto dal
  checkpoint remoto: `save --agent` obbligatorio, `LEASE_HELD` se tenuto da altri, `--release` per l'handoff), cattura
  e ripristino della patch del codice non committato (`--code-repo`, `restore-code`), comando `recover` (ultimo
  checkpoint valido, remoto intatto) e rilevazione della divergenza del branch di codice in `bootstrap`.

- **Riconciliazione delle issue aperte (PF-ISSUE-RECONCILIATION-01):** `docs/issue_reconciliation/` (csv/json/md):
  60 issue aperte al 17/10/2026 classificate con motivazione ed evidenza, deduplicate per scheda #461.

### Fixed
- **Rilievi Codex su PR #512 (commit 8ffb4f3):** riserva transitoria del Microstake rilasciata all'attivazione; blocco
  pre-invio del Microstake = errore pre-invio tipizzato (intento dedupe rilasciato, retry non duplicato); snapshot del
  broker SIM persistito dopo ogni operazione della saga; ricostruzione F018: righe pendenti senza attribuzione =>
  avvio non ok, chiavi dedupe dei tavoli gia' occupati riacquisite, betId annidato letto; RSR-12: tavolo surplus con
  perdita ricreato al riavvio, write failure dello stato di recovery fail-closed.

### Changed
- **Money Management (rettifica owner 17/10/2026):** lo stake calcolato non viene più alzato in silenzio al
  `min_stake`: sotto il minimo configurato la decisione è rifiutata con `stake_sotto_min_stake` e lo stake richiesto
  in chiaro. Test F001/side-parity del trasporto diretto portati sopra il minimo exchange (il percorso diretto non
  riceve più importi sotto 2 EUR).
- **Checkpoint multi-agente `pf-checkpoint/3`** (`tools/agent_checkpoint/pfckpt.py`, `docs/agent_checkpoint_protocol.md`):
  verifica sul tree committato, salvataggio atomico con concorrenza ottimistica, ripresa idempotente dopo crash,
  readback da clone pulito, bootstrap da zero; ripara l'incidente INC-b45f528 (artefatti `*.log` esclusi dal
  `.gitignore`). Regole per tutti gli agenti in AGENTS.md/CLAUDE.md. Nessun effetto sul runtime di trading.

### Fixed
- **F026 (v2):** Roserpina distingue saldo disponibile, fondi impegnati ed equità di cassa del conto
  (`RuntimeController._account_from_funds`, soli campi exchange). L'auto-reset, che ribasa il picco e libera
  risorse, si decide sul regolato e non sul solo disponibile: le puntate aperte non sono più perdite. Il reset
  trattiene tavoli e chiavi delle posizioni non regolate. La base di stake e dei limiti percentuali **non cambia**
  (resta il disponibile). Vedi `docs/roserpina_bankroll_equity.md`.
- **F2:** un settlement senza `table_id` (poller SIM/cleared) libera il tavolo del suo mercato, solo se univoco e
  con esito certo.
- **F018:** al riavvio le posizioni aperte vengono ricostruite dal DB e dagli ordini correnti dell'exchange (tavoli,
  esposizione, chiavi anti-duplicato, riserve incerte). Con exchange illeggibile o tavoli insufficienti il runtime
  parte in PAUSED e `resume()` rifiuta finché la riconciliazione non è completa.
- **F032:** l'espansione dello stake Roserpina si raggiunge davvero: la crescita si misura sull'equità di cassa
  regolata rispetto a una baseline persistente del ciclo (non più sul picco che segue il saldo); dati incerti ⇒
  nessuna espansione.
- **RSR-12:** lo stato dei tavoli di recovery è persistente e sopravvive al riavvio; il limite conta anche i tavoli
  in recovery senza ordini.
- **F027:** i settlement confermati si contabilizzano anche in PAUSED, LOCKDOWN ed emergency stop (nessun nuovo
  ordine dalla riconciliazione; dopo un emergency stop in LIVE il registro SIM non viene usato e l'incertezza si
  segnala).
- **F028:** un ordine d'ingresso con stato finale confermato dal broker e nessun abbinato libera tavolo e riserva;
  gli esiti incerti non vengono mai liberati.
- **F031:** nel broker SIM un ordine parzialmente abbinato con residuo annullato resta fra gli ordini correnti
  (parità con Betfair): F018 lo ricostruisce al riavvio.
- **F030:** il Liquidity Guard protegge anche gli ingressi singoli BACK/LAY (liquidità eseguibile al prezzo, lato
  corretto del book, `core/liquidity.py`); un book assente o non attendibile nega l'ingresso anche in solo-avviso.
  Il Book Block resta sul dutching: per una singola selezione non esiste un book percentuale significativo.
- **Stop di esposizione:** un ordine candidato che raggiungerebbe `max_exposure_stop` si rifiuta senza armare un
  RISK_STOP persistente; lo stop scatta quando è l'esposizione reale a raggiungere il limite.
- **GUI Roserpina:** etichette Max Win e Liquidity riallineate al comportamento (Max Win resta un blocco hard sul
  percorso ordine); checkbox su quattro righe, prima gli ultimi uscivano dalla finestra.
- **F029:** `TableManager.allocate` non riassegna più un tavolo in recovery già occupato da una posizione aperta,
  né un tavolo oltre `table_count`. Prima, con recovery attivo e dopo una perdita, un secondo ingresso riceveva
  lo stesso tavolo e ne sovrascriveva la posizione: esposizione persa dal totale dei cap e due posizioni su un
  tavolo. Trovato dalla campagna di simulazione Roserpina (con un solo tavolo), reso raggiungibile da F2.

### Security
- Secrets (`password`, `api_hash`, `session_string`, etc.) now encrypted at
  rest in SQLite using a stdlib-only XOF stream cipher (`hashlib.shake_256`).
  Wire format: `enc:v1:<base64(nonce_16 + ciphertext)>`.
- Sensitive fields are redacted in all Telegram alert text and log output via
  `observability/sanitizers.py` with dot-notation key support
  (`telegram.api_hash`, `betfair.password`, …).
- `.gitignore` now covers `*.pem`, `*.key`, `.pickfair/`, `.env.*` and common
  secret file patterns.
- AI review workflows (the five `pr-review-*.yml`): when a secret key starts
  the line (`.env`, YAML, INI, properties, `export`/`set`, list items, comments
  with `#`, `//`, `;`, `!`, `--`, `/*` or `<!--`), an
  unquoted value is now redacted up to the end of the line, spaces included,
  before the diff reaches the provider and before the review is published. It
  used to stop at the first space, so the rest of the value leaked. Mid-line
  matches (arguments, comparisons, signatures) and code-shaped values at the
  start of a statement (calls, subscripts, built-in type annotations,
  arguments) keep the previous rule, so the reviewer still sees the code.
  Arithmetic operators do not count as code, since encoded secrets and
  passphrases contain them; `==`, `:=` and `=>` are not separators, so
  comparisons stay visible.
  Across the whole repo, 18 of 219,588 lines are now redacted more than before
  (comments, examples, test paths joined with `/`), none of them production
  code. Quoted values and GitHub Actions expressions are unchanged.
  (DECISIONE-426 P14, #479)

### Added
- `core/duplication_guard.py`: concrete `is_duplicate()` / `register()` two-phase
  interface (non-atomic check + register) alongside existing atomic `acquire()`.
- `core/runtime_controller.py`: `emergency_stop()` fail-closed implementation —
  cancels all live orders grouped by market, blocks further signals, publishes
  `EMERGENCY_STOP_TRIGGERED`; `reset_emergency()` unblocks.
- `services/betfair_service.py`: `handle_session_expiry()` with bounded re-auth
  (max 1 attempt); `is_live_usable()` gate; `LIVE_BLOCKED_SESSION_INVALID`
  error when session invalid and live requested.
- `core/reconciliation_engine.py`: concrete fencing token implementation —
  `_next_fencing_token()`, `get_active_fencing_token()`,
  `assert_fencing_ownership()`.
- `circuit_breaker.py`: `record_success()` public method.
- `betfair_client.py`: `_api_breaker` (CircuitBreaker, 5 failures / 60 s)
  protecting all JSON-RPC calls; SESSION_EXPIRED does not trip the breaker.
- `core/trading_engine.py`: `_order_submission_breaker` (CircuitBreaker,
  3 failures / 120 s) protecting the live order submission path.
- `core/type_helpers.py`: canonical `safe_float()`, `safe_int()`, `safe_side()`
  — replaces copy-pasted helpers across 7 modules.
- `core/reconciliation_types.py`: extracted public types/enums from
  `reconciliation_engine.py` for focused module concerns.
- `database_schema.py`: extracted DDL from `Database._init_db()`.
- `requirements-dev.txt`, expanded `requirements-test.txt`.
- `pyproject.toml` packaging metadata.

### Changed
- AI review workflow for Sol (`pr-review-openrouter-gpt56-sol.yml`, file name
  kept): the per-push reviewer moves from `openai/gpt-5.6-sol` to
  `openai/gpt-6.1-sol` on OpenRouter, with reasoning effort `max` instead of
  `high` — the highest level the model offers, checked against its OpenRouter
  metadata. The effort whitelist of this workflow now admits `xhigh` and `max`
  (without it `max` would silently fall back to `high`); the output ceiling
  rises from 25,000 to 64,000 tokens (model maximum 128,000), each request may
  wait 1800 s instead of 100 s and the job timeout rises from 12 to 105 minutes.
  The cost estimate now uses the OpenRouter list price, $2/M input and $10/M
  output: the previous 5.00/30.00 overstated the reported cost about threefold.
  New tests pin the model id, tie the widened whitelist to that id, require a
  higher ceiling and a longer wait at `xhigh`/`max`, and check that the
  failed-call heading is recognised by the done-marker guard. (Owner decision,
  07-10-2026)
- Merge readiness gate (`pr-merge-readiness.yml`): the wait for the head's
  checks rises from 900 s to 6600 s and the job timeout from 25 to 120 minutes.
  The budget must cover the slowest review job (Sol, 105 minutes) plus its
  start-up delay: with 900 s a valid but slow review left the gate red, which
  could already happen with Grok's 20-minute job. The gate still exits as soon
  as the checks are settled, and still fails when the budget runs out. A test
  ties the budget to the five review workflows' timeouts. The worst-case test
  of the review workflows now also counts a GitHub call written over several
  lines: there are seven calls per workflow, not six, and every call must be
  recognised or the test fails.
- AI review workflow for Grok 4.7 (`pr-review-xai-grok46.yml`): follow-up owner
  decision on 07-10-2026 raises the per-attempt wait from 240 s to **400 s**
  and the job timeout from 20 to **30 minutes**, giving large diffs more time
  to finish instead of losing paid attempts. The request is not streamed, so
  the answer arrives only after the reasoning. The earlier 100→240 s change
  remains historical context; waiting longer adds no provider cost because
  billing is per generated token.
  A new test checks that, for all five reviewers, exactly three attempts plus
  backoff, the GitHub calls at their timeout and runner start-up fit in the
  job timeout. (DECISIONE-426 P24)
- `requirements.txt`: removed `pytest` / `pytest-asyncio` (test deps only).
- CI `unit.yml`, `failure.yml`, `net.yml`: install via `requirements-test.txt`
  instead of `requirements.txt + pip install pytest`.
- `pytest.ini`: registered missing markers (`reconciliation`, `safety`,
  `observability`, `mutation`).
- GitHub Actions: removed `|| true` from `pip install` steps (fail-closed CI).
- `core/reconciliation_engine.py`: -325 lines (-15%) after type extraction.
- `database.py`: -262 lines (-19%) after schema extraction.

### Fixed
- Order outcome with the production `ExecutorManager` (PF-ALL-F001, #453): the
  `TradingEngine` now waits for and classifies the `Future` instead of treating
  it as the broker response. Rejections are `FAILED`; timeouts after the task
  started, lost responses, `order_unknown`, mixed or malformed responses are
  `AMBIGUOUS` with reconciliation; a task still queued when the wait expires is
  cancelled and reported as pre-send `FAILED`. A top-level `SUCCESS` whose
  instruction reports are all `FAILURE` is a rejection. No order is recorded as
  `SUBMITTED` without the real broker response.
- Table release on terminal events (PF-ALL-F023): engine bus events carry
  `table_id` and `market_id`; a confirmed failure frees the table only if it still
  holds that order's reservation (`TableManager.force_unlock_if_event`,
  idempotent, lock-protected).
- `QUICK_BET_AMBIGUOUS` no longer releases the table, the exposure or the
  anti-duplicate key (PF-ALL-F024): the risk is held until reconciliation, also
  across `ROSERPINA_AUTO_RESET` and `reset_cycle` (`TableManager.reset_all` and
  `DuplicationGuard.clear` keep the uncertain reservations).
  See `docs/trading_engine_executor_contract.md`.
- RISK_STOP on owner stop limits, separate from EMERGENCY (#461 PR28-c,
  PKG-P24-B: P38 and the P37 exposure stop).
  - Before, reaching the session-loss limit only blocked new entries: the bot's
    unmatched orders stayed in the market and its open positions were not
    closed. There was no total-exposure stop at all.
  - **RISK_STOP** (`RuntimeController.risk_stop`, rules in `core/risk_stop.py`):
    blocks new entries (signals, auto-next, resume) but not cashout; cancels the
    bot's unmatched orders and attempts to close its positions by routing the
    existing `CASHOUT_ALL` through the CashoutRouter (bot orders only, the
    cashout's own gates, blocked or uncertain outcomes reported by the router).
    It publishes `RISK_STOP_TRIGGERED` and is idempotent. If EMERGENCY is
    already active (the strongest barrier, cancel-all + LOCKDOWN) no cashout is
    routed. PAUSE is unchanged (blocks new entries only).
  - The stop is persisted (`risk_stop_state` setting) together with the state
    change, under the same lock, as a versioned record `{active, reason, seq}`
    (the PR28-b model). If the db cannot be written, the same version goes to a
    marker file next to the db (`<db>.risk_stop_pending.json`). At restart the
    readable copy with the higher `seq` wins; an equal or unknown `seq`, or an
    unreadable copy, gives the conservative result (stop active).
  - Reset is a fail-closed two-phase commit. An independent
    `risk_stop_reset_guard` plus `<db>.risk_stop_reset_guard.json` is armed
    BEFORE any `active=false` write and remains active through persistence and
    readback. Only after the inactive state is verified is the DB guard cleared
    and the guard marker removed last. Any failure before that commit leaves a
    durable guard, so a rejected/incomplete reset cannot reopen after restart.
    Only if neither normal stop store can be written during activation does the
    stop hold for the running process alone (KNOWN_LIMITATION, as in PR28-b).
  - Neither a restart nor `start()` clears the stop: tables live in memory
    (exposure reads 0 after a restart) and the session loss restarts, so
    neither can prove the positions are closed. Only an explicit
    `reset_risk_stop()` clears it, after a full recheck: no emergency, daily
    loss not breached, limits valid, session loss below its limit and exposure
    below the exposure stop. While the stop stays on, `start()` re-attempts the
    `CASHOUT_ALL` (for example after a restart).
  - **Triggers:** session loss reached (`max_session_loss`, PR28-b) and the new
    `max_exposure_stop` (EUR, owner limit with no default, GUI/loader/save). It
    is checked before submission on the total exposure including the order; a
    misconfigured value (unreadable or <= 0) blocks entries without closing
    anything. No fixed amount belongs to STOP/cashout (#426, 06/10).
- Durable daily loss and session-loss stop (#461 PR28-b, PKG-P24-B: F4, P37).
  - **F4:** the day's realized loss is saved (`daily_loss_state` setting) and
    rebuilt at restart on the same UTC day, before any new risk. Before, a
    restart zeroed it: EUR 8 lost, restart, another EUR 8 = 16 against
    `max_daily_loss` 10 with no stop. An unreadable saved state, or an invalid
    or future saved day, counts as a breach (fail-closed); a past UTC day
    starts from zero. If a save fails, new risk is refused until a save
    succeeds and the state goes to a marker file next to the db
    (`<db>.daily_loss_pending.json`). Every record (db and marker) carries
    the UTC day and a monotonic sequence number `seq`. At restart both are
    read: the higher `seq` wins; with an equal or unknown `seq` the larger
    loss wins, and a breach in either always counts. A leftover marker
    (failed remove, crash between save and remove) has a lower `seq` and can
    never lower the loss. Known limitation (declared): if neither the db nor
    the marker can be written, the block holds for the session only; a
    restart in that condition cannot rebuild the unsaved loss.
    Persistence lives in the new `core/daily_loss_store.py` (`db_path` may
    be a str or an `os.PathLike`).
  - `reset_cycle()` and the drawdown auto-reset no longer wipe the day's or the
    session's loss (they zero the desk `realized_pnl`; the baselines now shift
    with it).
  - **P37:** new owner limit `max_session_loss` (EUR, realized loss since the
    last `start()`), in GUI, loader and save. No default: empty = not set.
    Reached (with float tolerance) => new entries, auto-next and `resume()`
    are refused and `SESSION_LOSS_BREACH_TRIGGERED` is published once.
    Unreadable or <= 0 => entries refused. It is a risk limit, not a cashout
    parameter: exits/cashout keep their own gates (#426 decision 06/10).
- Owner limits measured on the order's risk (#461 PR28, PKG-P24-B, first slice).
  - **Single formula:** `core/validators.py` `order_exposure` gives BACK = stake
    and LAY = liability `stake * (price - 1)`. `exceeds_cap` holds at the exact
    threshold with float tolerance.
  - **LAY consumers:** money management (single, total and event caps), the A2
    `max_open_exposure` cap, the table exposure and auto-next all used the stake.
    A LAY of 1.5 at 21 (liability 30) counted as 1.5, and a LAY of 10 at 21
    (liability 200) passed the single cap of 180. A LAY over a cap is now
    rejected, with no resizing. RiskGate uses the same function.
  - **Exact threshold:** 0.1 + 0.2 against a cap of 0.3, or a LAY of 0.1 at 4
    against MAX_WIN 0.3, are no longer denied. A one-cent excess still is.
  - **`reload_config`** (GUI save while the bot is running): it no longer
    rebuilds the TableManager and the ReconciliationEngine. Before, tables,
    exposure and recovery memory went back to zero, so the aggregate caps
    restarted from 0. The table count adapts without dropping busy tables.
    `start()` keeps its fresh session.
  - **Auto-next:** it now applies the A2 cap and the real event exposure, and
    activates the table with the liability.
  - **Best price DIRECT:** if the price changes after the caps, a LAY whose
    liability grows is rejected.
- Fail-closed numeric validation for orders and risk (#461 PR27, PKG-P24-A).
  - A new shared source, `core/validators.py` (`order_market_id`,
    `order_selection_id`, `order_price`, `order_stake`, `finite_number`), uses
    the same `INVALID_*` codes as the client. The #488 client check is unchanged.
  - Engine: a present bool, NaN/inf, non-numeric value or non-integer selection
    id is now rejected at normalization (FAILED/INVALID_REQUEST). There is no
    `orders` row, no intent mark, no transport, and the key of an in-flight
    original is not released. Before, `selection_id=True` was placed on
    runner 1, `stake=True` was placed as 1 EUR, and 5678.9 was truncated.
  - Runtime signal: market/selection/price are checked before anti-duplication,
    tables and money management. A non-numeric selection used to raise with the
    event key still locked.
  - A2 cap: `max_open_exposure` that is configured but unreadable
    (NaN/inf/bool/text) now denies. NaN used to disable the cap.
  - Money management:
    - an invalid fixed stake from a signal, including `False`, is rejected
      (`stake_segnale_non_valido`). `0`/`None`/`""` stay the producer's
      "let MM decide" sentinel;
    - a non-finite exposure (`total_exposure()` = inf on a corrupted state,
      #449) is rejected instead of being counted as 0;
    - an unreadable `max_stake_abs` fails closed;
    - a bool recovery cap counts as a corrupted config (chase 0, #450).
  - OrderManager: bool and truncated ids or amounts are rejected before
    conversion.
- Single order authority and durable intent de-dup (#461 PR26, gate
  `LIVE_BLOCKED_UNTIL_PR26_DURABLE_DEDUPE`): the engine marks a
  `customer_ref` as consumed in the new additive SQLite table
  `consumed_order_intents`, committed right before every transport call
  (LIVE branch, declared SIM broker, `OrderManager`, fallback). The mark does
  not follow the order status: the same intent is `DUPLICATE_BLOCKED` after
  COMPLETED, FAILED after send (broker rejection), AMBIGUOUS, and after a
  restart, and two processes on the same file cannot both send (PRIMARY KEY).
  Before, de-dup only saw INFLIGHT/SUBMITTED rows and in-memory keys released
  on terminal outcomes. The mark is removed only with proof that nothing was
  sent: an engine pre-send error, or an argument error raised by the real
  `BetfairClient` or by the identity-proven `SimulationBroker`. Timeout and
  unknown outcomes stay AMBIGUOUS and go to reconciliation, never to a replay.
  STOP and every gate before the transport consume nothing. In declared LIVE a
  DB without the capability sends nothing (`DURABLE_DEDUPE_UNAVAILABLE`). A
  `DUPLICATE_BLOCKED` no longer releases the original's in-memory key.
- `TradingEngine` with `simulation_broker` set and a getter that does not
  return it: places through the broker's `place_bet` with the same side and
  signature adapter instead of failing with `NO_VALID_EXECUTION_PATH`
  (`SimulationBroker` has no `execute()`).
- `SimulationBroker` (F8, F10 of #453): an accepted unmatched order reports
  `SUCCESS` with `orderStatus` `EXECUTABLE`, not `FAILURE`; an invalid market,
  selection, price or size (NaN/Inf included) raises the same
  `INVALID_*` code as `BetfairClient.place_bet` before matching, storage or
  writes, and `place_orders` reports that leg as `FAILURE` with nothing stored.
- `OrderManager` (F14 of #453 and adapter signatures): the client pre-send
  codes are certain rejections (PERMANENT), not AMBIGUOUS; non-finite price or
  stake and an invalid or conflicting side are rejected before saga and client
  (a typo no longer becomes BACK); the `BetfairClient` envelope is read
  (`ok`/`result`/`order_unknown`), so a placed order is no longer FAILED and a
  lost response is AMBIGUOUS; cancel uses each broker's signature for one bet
  id and refuses an empty bet id (never cancel-all) and `size_reduction`.
- `SafetyLayer.validate_cashout_request` (F15 of #453) rejects NaN/Inf price
  and stake.
- Order identity for existing producers (#461 PR26-a): the runtime signal
  path, the post-settlement auto-trade, every dutching leg and
  `DutchingController.manual_bet` now publish `CMD_QUICK_BET` with a
  `customer_ref`. Before, the engine rejected all of them with
  `CUSTOMER_REF_REQUIRED`. `core/order_identity.py` gives every ref the
  Betfair-compliant shape `<prefix>-<28 hex>` (32 chars). Where each ref
  comes from:
  - an upstream ref is preserved, also when the mini-GUI nests it in
    `raw_signal`; a non-compliant one is mapped
    deterministically instead of being silently dropped by `BetfairClient`;
  - signal: derived from the message and its chat, without `received_at`
    or `event_key` (at any depth) and without the MM stake. The message has
    the same canonical form on the headless and mini-GUI paths (the innermost
    `raw_signal`), so the resolved price and the GUI stake are not part of the
    identity, while the effective SIM/LIVE mode is. A redelivery after
    reconnect or restart, even on the other entrypoint, keeps the same ref,
    and the engine blocks it;
  - auto-trade: derived from the triggering settlement;
  - dutching leg: derived from the request inputs (market, total stake,
    sorted selections) plus the leg itself, not from the computed stakes, so
    neither the order of the selections nor the order-sensitive rounding of
    equal odds matters. The ref is also stored in
    `dutching_batch_legs`;
  - `manual_bet` without an upstream ref: a fresh operation ref per call.
  The `RiskMiddleware` REQ_QUICK_BET forward keeps the caller's ref and never
  invents one. The engine gate is unchanged, and the Telegram REQ_QUICK_BET
  compat fallback (no runtime gate) stays rejected.
- Betfair credentials (#461 PR04-quater, DEC-426-P28–P32): separate encrypted
  Delayed and Live App Keys; the legacy key migrates only to Delayed. LIVE
  requires the explicit Live key, and the real client never uses Delayed.
  Settings include certificate/private-key file pickers and validate readable
  PEM files, permissions and the matching TLS pair before an atomic save.
  An empty password preserves the saved password; a replacement clears the
  input after saving. App Keys are masked in the registry and redacted from
  client errors and diagnostics. SIM remains offline; its Delayed feed belongs
  to PR11, and the authenticated encryption upgrade remains in PR35.
  LIVE readiness reads canonical settings without triggering migration and
  reports actionable missing/unavailable-key remedies in registry/headless.
  The legacy credentials writer updates Delayed after migration while keeping
  Live/password unchanged. Colliding redaction masks use a neutral ASCII
  character so strict Windows CP1252 log sinks retain login diagnostics.
  A read-only Live-key status distinguishes absent values from undecodable or
  unsupported ciphertext, retaining the stored value and directing master-key
  recovery. The existing unauthenticated enc:v1 format is unchanged; integrity
  verification remains in PR35.
  Empty GUI saves preserve unreadable Delayed/Live or unmigrated legacy App
  Keys; explicit replacements and clearing readable keys remain supported.
  Registry configuration reports never migrate, including on read-only legacy
  storage, and unsupported legacy enc formats remain retryable after recovery.
  Volatile unreadable-key provenance also protects an old empty form when
  another writer restores or replaces the credential before that form saves.
  Registry entries and presence checklist direct unreadable credentials to
  recovery; expired matching certificates are rejected before persistence.
  Load values and unreadable provenance share a transaction snapshot;
  certificate validation also rejects a future notBefore date.
  The client also blocks a future certificate swapped in after save with the
  trusted CERT_NOT_YET_VALID diagnostic before any login HTTP request.
  Every credential echo is redacted, including short/concatenated values and
  login passwords in client diagnostics. Local login codes are separate from
  provider text; session expiry is classified before redaction.
  Certificate diagnostics remain actionable; unreadable legacy credentials
  do not finalize migration, and original ciphertext remains recoverable.
  LIVE readiness/deploy also require the Live key; SIM remains independent.
  Short session tokens and colliding redaction markers are covered.
- Live client, non-finite price or size (F11 in #453, DECISIONE-426 P27):
  `BetfairClient.place_bet` now rejects a price or a size that is NaN or
  infinite with `INVALID_PRICE` / `INVALID_SIZE` before building the request,
  as `replace_orders` already did. Before, `float()` accepted them and
  `nan <= 1.0` is false, so the `placeOrders` body went out with
  `"price": NaN` or `"size": Infinity`; Betfair would have refused a body
  that is not valid JSON, but the check belongs to the client. In production
  the cashout chain reaches the client through `OrderRouter.place`, where the
  SafetyLayer only checks `price <= 1` and `stake <= 0`: the client was the
  only barrier. `BetfairService.place_order` and `OrderManager.place_order`
  (which only checks `price <= 1.0`) had the same hole, but today nothing in
  production calls them. For the `TradingEngine` the rejection is a certain
  pre-send failure, not an ambiguous outcome.
- Book sides (F6 in #453, DECISIONE-426 P25): the simulation broker, the
  cashout router and the Telegram resolver now read the two ladders as Betfair
  defines them, the convention the owner chose for dutching in #383:
  `availableToBack` holds the prices you can back at now, `availableToLay` the
  prices you can lay at now.
  - SIM: a BACK matches on the best `availableToBack` when its price is at or
    below it, a LAY on the best `availableToLay` when its price is at or above
    it, both at the book price. Before, each side matched against the opposite
    ladder: paper results earned the spread instead of paying it, and a BACK at
    the really executable price stayed unmatched.
  - Cashout: a BACK position is closed with a LAY at the best `availableToLay`
    and a LAY position with a BACK at the best `availableToBack`; the L95 depth
    gate reads that same level. Before, in LIVE the hedge would have rested
    unmatched and the residual handler would have cancelled it.
  - Cashout in LIVE: the router now asks for the market book with prices
    (`include_prices=True`, `EX_BEST_OFFERS`). Without them Betfair returns no
    ladders, so every position was skipped with only a log warning and a
    `CASHOUT` from the chat closed nothing.
  - The "aggressive" BACK of the Telegram resolver (old Telegram tab of the
    GUI, reachable in SIM only) now takes the best `availableToBack`, the
    price that matches at once.
  - The SIM no longer matches an order with an invalid price (malformed,
    non-finite or at most 1.0), as the live client refuses it. Before F6 a LAY
    with such a price matched; the side fix alone would have moved the hole to
    the BACK side.
  - `SimulationMatchingEngine` and `SimulationOrderBook` (used by tests only)
    follow the same rules; `direct_best_price` docstrings no longer call the
    executable price "defensive" (its code was already right).
  May look like a regression: SIM results are less optimistic, since the
  spread is now paid, and a SIM BACK priced above the best back now rests
  unmatched instead of matching.
- Cashout in the GUI (#461 PR04): the GUI process now builds the same cashout
  execution chain as the headless app (request bridge with duplicate filter,
  executor with the real SafetyLayer, OrderRouter, residual handler), through
  one shared builder, `cashout_wiring.cabla_catena_cashout`, used by both
  entrypoints. Before, a `CASHOUT` / `CASHOUT ALL` message received by the
  GUI was rejected with `cashout_chain_not_wired`, and the cashout request of
  the auto-close fell on a bus with no listener. Now it goes through the same
  gates as headless (emergency stop, runtime active, session and deploy gate
  in LIVE) and, in LIVE, places a real hedge. The GUI `Log` shows
  `CASHOUT_SUCCESS` and `CASHOUT_FAILED`, and `Storico Bet` and `Risk Desk`
  refresh on both. The GUI still has no manual cashout control. The Telegram
  notification of a cashout residual moved from `headless_main` to
  `cashout_wiring` unchanged, and both entrypoints use it.
  Closing the GUI window now stops the runtime first, as the headless app
  does, and `RuntimeController.stop` marks the runtime stopped before tearing
  down Telegram and Betfair, in both entrypoints. A command or a delayed
  cashout (auto-green grace) arriving during that teardown is rejected
  instead of being placed while the Betfair service is still connected.
  If stopping Telegram fails, `stop` now still disconnects Betfair before
  raising the error; before, the Betfair disconnect was skipped.
- Simulation settlement (#461 PR03): a SIM market is now settled when its
  CLOSED market book reaches the simulation broker. Before, nothing settled
  a SIM market: stakes and liabilities stayed locked forever and the runtime
  never saw a SIM result. Each matched bet is settled with exchange
  semantics (`core.pnl_engine.exchange_settled_gross_pnl`), selections are
  aggregated per market before the 4.5% policy commission, which applies
  once to the market net. The balance is credited with the net minus what
  the fills already moved (stakes, liabilities, realized close-outs), so a
  stake is no longer counted twice, and the market's position ledgers are
  closed. Duplicate CLOSED books, replays and restarts never settle twice,
  and an order placed on an already settled SIM market lapses unmatched.
  With `settlement.poll_enabled` on (default still off) the settlement
  poller now runs in SIM too and delivers each settled market once to the
  runtime cycle (realized PnL, daily loss, bankroll sync, checkpoints),
  also after a restart between the SIM state save and the cycle update.
  Runners without a terminal status, removed runners with bets on other
  runners, possible dead heats (several winners without a declared number
  of winners, or more than declared) and ledgers inconsistent with the
  orders leave the positions open. If handing the settlement to the event
  bus fails, the next poll round delivers it again without re-applying it;
  a delivery in progress is never repeated by a concurrent call. In production SIM still receives no market books until
  the SIM feed arrives (#461 PR11).
- `SimulationBroker.place_bet` now requires BACK or LAY and normalizes case
  and surrounding spaces before recording a PAPER order. Missing, malformed,
  or non-string sides raise `INVALID_SIDE` without changing simulated orders
  or funds. `place_orders` rejects just the invalid instruction without an
  implicit BACK; empty `side` permits a valid `bet_type`, while conflicting
  non-empty aliases and non-string aliases fail. Independent valid batch
  instructions still run. A regression test exercises the actual PAPER
  OrderRouter, OrderManager and CashoutExecutor against the broker.
  This aligns the simulation boundary with the live client (#426 P15).
  The engine's PAPER path now checks the two side aliases (`bet_type` and
  `side`) the same way as the LIVE branch before calling the simulation
  broker: discordant aliases, such as `bet_type=BACK` with `side=LAY`, fail
  with `INVALID_SIDE` instead of recording a `bet_type` order (#426 P17).
  A non-string side alias now fails with `INVALID_SIDE` on both paths, even
  when `str()` would turn it into BACK or LAY; on the LIVE branch such a
  value had been converted and sent since #478 (#426 P20).
- `betfair_client.py`: `BetfairClient.place_bet` now rejects a side outside
  BACK/LAY (checked after strip/upper; empty, `None` and non-string values
  included) with `RuntimeError("INVALID_SIDE")` before building the request,
  so nothing is sent. It used to turn any such value into BACK and report
  `ok=True`: a typo or a missing side became a real BACK bet. Callers that
  relied on the implicit BACK now get the error instead. `TradingEngine`
  treats `INVALID_SIDE` from the real client as a pre-send error (FAILED, not
  AMBIGUOUS). `_safe_side` stays permissive for `calculate_cashout`.
  (DECISIONE-426 P13, #480)

# Piano micro-PR: (orig_n, orig_id, [ (micro_id, titolo, scope/file, dipendenze, test minimi, prio, size, kind) ])
# size: S (<8k char prod), M (8-18k), L (>18k, eccezione atomica motivata)
# kind: FIN = finanziario (test integrati obbligatori), SAFE, BF (Betfair), GOV (merge owner), GUI, DOC, OPS
# prio: P0..P3
P = [
(1,"DOC-MODES",[
 ("M001","Contratto SIM/HYBRID_REAL_EXECUTION/LIVE + stake EXPLICIT/FIXED/MM","docs/mcp_operational_contract.md, CLAUDE.md, test governance","#537","test coerenza policy","P2","S","GOV")]),
(2,"PR32-a",[
 ("M002","L08-F3: fencing di riconciliazione mai fail-open","core/reconciliation_engine.py","#537","RED con enable_fencing_token=False; mutation","P1","S","SAFE"),
 ("M003","L08-F2: fencing token persistito e confrontato nel SQL","core/reconciliation_engine.py, database.py","M002","token superato rifiutato in transazione; riavvio monotono","P1","M","SAFE")]),
(3,"PR31-a",[
 ("M004","L07-F1: recovery fallita all'avvio => nessun ACTIVE","core/runtime_controller.py (start)","#537","eccezione recovery => PAUSED/BLOCKED, nessun ordine","P0","S","SAFE"),
 ("M005","Decision log di riconciliazione persistito","database.py, core/reconciliation_engine.py","M004","decision log dopo riavvio; schema additivo","P2","M","SAFE"),
 ("M006","PF-ALL-F004: consumatore di RECOVER_ORDER","core/runtime_controller.py, core/state_recovery.py","M004","evento consumato, nessun replay cieco","P1","S","SAFE"),
 ("M007","AUD-01: ordini ghost nei cap di mercato/evento","core/reconciliation_engine.py, core/risk_gate.py","M004","ghost conteggiato in totale, mercato, evento","P1","S","FIN")]),
(4,"G7 #532",[
 ("M008","G7-a: SimulationBroker riserva/rilascia stake BACK e liability LAY; INSUFFICIENT_FUNDS","simulation_broker.py","#537","4 xfail strict -> PASS; place/cancel/reject","P0","M","FIN"),
 ("M009","G7-b: transizioni matched/parziale/settlement/cashout senza doppio conteggio","simulation_broker.py","M008","oracle fondi su sequenze miste; multi-tavolo","P0","M","FIN"),
 ("M010","G7-c + RST-EXEC + L05-F2 (restart): riavvio con ordine EXECUTABLE non abbinato","core/runtime_controller.py (_rebuild_open_positions, at=-inf), core/table_manager.py","M009","test integrato: esposizione, riserva tavolo, giro F028, funds gate dopo il rebuild","P0","M","FIN")]),
(5,"PR29-f",[
 ("M011","C2: validita' saldo LIVE <=30 s monotona su ogni nuovo ingresso (auto-next compreso)","core/runtime_controller.py (_entry_funds_reason, snapshot)","#537","29,999 ammesso / 30,001 rifiutato; orologio di sistema spostato; distinto da TTL tombstone 120 s","P1","S","FIN"),
 ("M012","29-f: gate fondi senza doppio conteggio delle riserve (parita' SIM/LIVE)","core/runtime_controller.py, core/table_manager.py","M009","riserve interne vs available; xfail G7 gate -> PASS","P0","M","FIN"),
 ("M013","PF-FIN-02 residuo + T-F03/T-F06: funds gate che riconcilia ordini/liability/esposizione con dati exchange incompleti","core/runtime_controller.py, services/betfair_service.py","M012,M010","dati incompleti => blocco; due processi","P1","M","FIN")]),
(6,"PR29-d",[
 ("M014","L05-F2 (reset) + PF-C29-01.09: il reset non rilascia riserve vive","core/table_manager.py, core/runtime_controller.py (reset_cycle)","M010","reset con ordine vivo => riserva mantenuta","P1","S","FIN")]),
(7,"PR29-e",[
 ("M015","L05-F4 + L05-F5: auto-close LAY su liability; FAILED post-invio senza table_id non lascia ACTIVE","core/pnl_engine.py, core/table_manager.py","#537","oracle liability LAY; tavolo liberato","P2","S","FIN")]),
(8,"#521",[("M016","PF-LOT-D-F: aggregazione per bet_id dei settlement dello stesso mercato","core/runtime_controller.py (settlement)","#537","piu' bet stesso mercato, un solo rilascio corretto","P1","S","FIN")]),
(9,"#519",[("M017","Conferma step del cycle executor legata al verdetto dell'engine","core/money_management.py","#537","nessuna conferma senza verdetto","P2","S","FIN")]),
(10,"#520",[("M018","Book SIM malformato = blocco pre-invio","core/microstake_sim_port.py, simulation_broker.py","#537","book malformato => rifiuto senza effetti","P2","S","FIN")]),
(11,"PR28-f",[
 ("M019","C3/t80u 1-2: chiavi pct e recovery assenti => blocco / nessun recovery (+F10 loader/GUI)","services/setting_service.py, core/system_state.py, mini_gui.py (solo default)","#537","chiave assente => blocco; max_stake_abs assente resta non attivo","P0","M","FIN"),
 ("M020","C1: nessun cap monetario fisso sopravvive (config.example + test di guardia sui default)","config.example.json, tests/guardrails","M019","scansione default runtime/loader/GUI/esempio","P1","S","FIN"),
 ("M021","F04: drawdown con perdita non realizzata","core/risk_desk.py, core/loss_limits.py","M019","oracle drawdown con esposizione aperta","P1","S","FIN"),
 ("M022","F05 + F014 + F11: bankroll non finito, stop di sessione senza baseline, scope warning-only","core/risk_desk.py, core/loss_limits.py, core/risk_gate.py","M019","input NaN/inf/assente => blocco","P1","S","FIN")]),
(12,"PR33",[
 ("M023","Migrazioni DB versionate (schema_version, additive, rollback)","database_schema.py, database.py","#537","DB vecchio migra; rollback su errore","P1","M","SAFE"),
 ("M024","Hard-verify del delta c8fcaa3 (#446)","tests only","M023","atomicita' outbox/saga","P2","S","SAFE")]),
(13,"PR34",[
 ("M025","#528: harness F026 deterministica (niente attese fisse)","tests/f026","#537","100 run senza flake","P1","S","OPS"),("M158","Test timing: backoff a 0 in test_retry_does_not_stack_overflow (181 s), attese a condizione al posto di Event.wait(2), marker serial per GUI/Xvfb/processi/lock","tests/, pytest.ini","#537","RED->GREEN; suite seriale e -n 8 worksteal con insieme di esiti identico","P1","S","OPS"),
 ("M026","Crash drill su SQLite reale (PF-C28-01..03, F4 durabilita')","tests + core/state_recovery.py","M023,M025","kill fra write e commit","P1","M","SAFE"),
 ("M027","STOP persistente e riavvio (OD12-07, PF-EXEC-06, F07 E2E)","core/runtime_controller.py, core/risk_stop.py","M026,M010","STOP sopravvive al riavvio; ripresa solo se coerente","P0","M","SAFE")]),
(14,"PR35",[
 ("M028","Dipendenza cryptography vincolata + primitiva AEAD (merge owner, manifest)","requirements*, core/secret_cipher.py","#537","bitflip/chiave errata rifiutati","P1","S","GOV"),
 ("M029","Migrazione non distruttiva dei segreti + checklist ACL Windows","database.py (segreti), core/secret_cipher.py","M028","migrazione e rollback; MANUAL_ONLY Windows","P1","M","SAFE")]),
(15,"PR15",[
 ("M030","Saldi, picco e drawdown separati per modalita'","core/risk_desk.py, database.py","M019","picco SIM non contamina LIVE","P1","M","FIN"),
 ("M031","Contatori/stato persistenti per modalita' + emergency snapshot","core/runtime_controller.py, database.py","M030","switch modalita' senza ereditare bankroll","P1","M","FIN")]),
(16,"PR19",[
 ("M032","marketBaseRate LIVE + commission_pct solo verificata (t80u 3)","betfair_client.py, core/pnl_engine.py","#537","commissione stimata non aumenta rischio","P1","M","FIN"),
 ("M033","F1 + F5: policy commissione e dedupe settlement prima del publish","core/pnl_engine.py, core/runtime_controller.py","M032","vincita dopo perdita stesso mercato","P1","S","FIN"),
 ("M034","VOIDED e resettlement (OD12-08)","core/runtime_controller.py (settlement)","M033","void dopo settlement, rettifica","P1","M","FIN")]),
(17,"F017",[("M035","Poller settlement attivo quando configurato, riconciliazione al riavvio","trading_config.py, core/runtime_controller.py","M034","mai liberare fondi senza esito autorevole","P1","S","FIN")]),
(18,"PR22",[("M036","F03/F032: EXPANSION su bankroll_start persistito e profitti consolidati","core/money_management.py","M034","RSR-08/09, timing settlement","P1","M","FIN")]),
(19,"PR17",[("M037","Shutdown una volta su errore main + HeadlessApp.stop ferma EventBus (F021)","headless_main.py, shutdown_manager.py","#537","errore main => teardown unico","P2","S","SAFE")]),
(20,"PR46",[("M038","F010: ordinamento EventBus e prova drain/backpressure","core/event_bus.py","M037","ordine sotto carico","P2","S","SAFE")]),
(21,"PR20",[
 ("M039","F009: escalation anomalie con azione reale","safe_mode_manager.py, observability/","#537","anomalia => pausa/stop","P1","S","SAFE"),
 ("M040","F016: SafeMode e kill switch alimentati","safe_mode_manager.py, core/runtime_controller.py","M039","trigger => blocco ingressi","P1","S","SAFE")]),
(22,"F007",[
 ("M041","F007-a: market_validator cablato nel percorso ordini","market_validator.py, core/runtime_controller.py","M040","mercato non valido => blocco","P1","S","SAFE"),
 ("M042","F007-b: auto_throttle cablato o ritirato con prova di ridondanza","auto_throttle.py","M041","burst => throttle","P2","S","SAFE"),
 ("M043","F007-c: ai_guardrail cablato o ritirato con prova","ai/ai_guardrail.py","M041","auto-entry mai fail-open","P2","S","SAFE")]),
(23,"PR47",[
 ("M044","F005/F006: watchdog con liveness reale e heartbeat corretto","observability/","M038","liveness falsa => alert","P2","S","SAFE"),
 ("M045","OP-VIS + F022: segnalazione operatore (posizioni ricostruite, ghost, startup BLOCKED; campo stop in get_status)","core/runtime_controller.py, observability/","M010","evento consumato e visibile","P1","S","SAFE"),
 ("M046","OD12-11: storico verificabile","database.py, observability/","M045","ricostruzione audit","P2","M","SAFE")]),
(24,"PR11-a+PR16",[
 ("M047","Login SIM con App Key Delayed (P31)","services/betfair_service.py, betfair_client.py","#537","Delayed mai per ordini reali","P1","S","BF"),
 ("M048","Logout lato server (#343)","betfair_client.py","M047","logout chiama endpoint; MANUAL_ONLY reale","P3","S","BF")]),
(25,"PR11-b",[
 ("M049","Freshness/timestamp Market Stream; dati stantii => nessun ingresso (PF-EXEC-05)","services/streaming_feed.py, core/runtime_controller.py","M047","stale => no entry, no reprice","P0","M","BF"),
 ("M050","Reconnect, heartbeat, clock","services/streaming_feed.py","M049","disconnessione => riconciliazione","P1","M","BF"),
 ("M051","Endpoint Italia (.it) e configurazione feed","services/betfair_service.py","M050","endpoint corretto per account IT","P1","S","BF")]),
(26,"PR11-c",[
 ("M052","F7 + F3: livello svuotato non abbina l'intero ordine","core/simulation_matching_engine.py","M009","size residua corretta","P2","S","FIN"),
 ("M053","F9: ordine a riposo si riabbina col book","core/simulation_matching_engine.py","M052","riabbinamento","P2","S","FIN")]),
(27,"REST-DISC-a",[("M054","listEventTypes/Competitions/Countries/MarketTypes/Venues/TimeRanges","betfair_client.py","#537","fixture payload reali","P2","M","BF")]),
(28,"REST-DISC-b",[
 ("M055","listRunnerBook","betfair_client.py","M054","schema/errori","P2","S","BF"),
 ("M056","listMarketProfitAndLoss (fonte P/L autorevole)","betfair_client.py, core/pnl_engine.py","M055","confronto P/L","P2","S","BF")]),
(29,"NEW-ACCOUNT",[
 ("M057","getAccountDetails + listCurrencyRates","betfair_client.py","#537","valuta/conversione","P2","S","BF"),
 ("M058","getAccountStatement paginato","betfair_client.py","M057","paginazione","P3","S","BF")]),
(30,"NEW-UPDATEORDERS",[("M059","updateOrders via autorita' ordini","betfair_client.py, core/trading_engine.py","M066","nessun wrapper raw; AMBIGUOUS","P2","S","BF")]),
(31,"ORDER-STREAM-a",[
 ("M060","Order Stream: connessione, auth, heartbeat, reconnect (trasporto condiviso)","services/order_stream.py (nuovo)","M050","fixture stream","P1","M","BF"),
 ("M061","Order Stream: cache ordini (oc/orc/uo), clk/initialClk","services/order_stream.py","M060","image+delta","P1","M","BF"),
 ("M062","Order Stream: cablaggio runtime in sola lettura","core/runtime_controller.py","M061","eventi senza autorita'","P1","S","BF")]),
(32,"ORDER-STREAM-b/RECON-02",[
 ("M063","Matrice autorita' REST/stream per fill e cancel","core/reconciliation_engine.py","M062,M004","stream primario + verifica REST","P1","M","FIN"),
 ("M064","Divergenza/disconnessione stream => riconciliazione fail-closed","core/reconciliation_engine.py","M063","divergenza => blocco","P0","M","FIN")]),
(33,"PR30-a",[
 ("M065","L05-F6: OrderManager cablato o percorso latente ritirato","order_manager.py, core/trading_engine.py","#537","nessun percorso morto raggiungibile","P1","M","SAFE"),
 ("M066","FSM replace con AMBIGUOUS senza retry cieco","order_manager.py, core/trading_engine.py","M065","response lost","P1","M","FIN"),("M154","STOP drena anche il task dell'executor ancora vivo dopo AMBIGUOUS (limite dichiarato #537)","core/runtime_controller.py, executor_manager.py","M065","STOP con task AMBIGUOUS vivo => attesa o trattenuta dell'autorita'","P1","S","SAFE"),
 ("M067","F003: produttore degli eventi del ciclo ordine","core/trading_engine.py","M065","eventi emessi","P2","S","SAFE"),
 ("M068","F011 + F025: instructionReports top-level; SUCCESS senza betId => AMBIGUOUS","betfair_client.py, core/trading_engine.py","M065","payload reali","P1","S","FIN"),
 ("M069","customer_ref di manual_bet + parita' SIM LAPSED","core/trading_engine.py, simulation_broker.py","M065","ref stabile; LAPSED","P2","S","FIN")]),
(34,"PR30-b",[
 ("M070","R1-R4: preflight tutto-o-niente del lotto + riserva esposizione (corsia dormiente)","core/dutching_batch_manager.py, controllers/dutching_controller.py","M066","una gamba negata => lotto invalidato","P1","M","FIN"),
 ("M071","Cablaggio batch_manager + table_id dei lotti + finalize_terminal_batch","core/dutching_batch_manager.py, core/runtime_controller.py","M070","lotto tracciato per gamba","P1","M","FIN"),
 ("M072","PF-EXEC-02: lotto parziale ribilanciato/coperto/sospeso","core/dutching_batch_manager.py","M071","parziali/incerti, nessun hedge non verificato","P1","M","FIN"),
 ("M073","PF-EXEC-01: ripuntare il solo residuo dopo cancel confermato","core/dutching_batch_manager.py","M072","cancel ignoto => nessun riposizionamento","P1","S","FIN"),
 ("M074","F018 per lotto: un tavolo per lotto al rebuild","core/runtime_controller.py","M071,M010","rebuild lotto","P2","S","FIN")]),
(35,"PR09",[
 ("M075","Publisher Dutching e budget equo","dutching.py, controllers/dutching_controller.py","M074","distribuzione stake numerica","P2","M","FIN"),
 ("M076","CASHOUT Esegui per Dutching","cashout_router.py, controllers/dutching_controller.py","M075,M085","cashout lotto","P2","S","FIN"),
 ("M077","Suite E2E 72 casi #511 (sola prova)","tests","M076","72 casi","P1","M","FIN"),
 ("M078","Attivazione corsia Dutching (rimozione dormienza) solo con gate PASS","controllers/dutching_controller.py","M077","gate PASS prima dell'attivazione","P1","S","FIN")]),
(36,"PR12-a+PR23",[
 ("M079","Stake EXPLICIT/FIXED/MM cablato (esclusivi, default da config, rifiuto se non valido)","stake_mode.py, core/runtime_controller.py","M019","tre modi + rifiuto","P1","M","FIN"),
 ("M080","Precedenze messaggio/GUI/MM e formula MM con esempi numerici","core/money_management.py, docs","M079","esempi numerici verificati","P1","S","FIN")]),
(37,"PR24",[
 ("M081","F01: direzione SL/TP e prezzo di riferimento","core/pnl_engine.py","M085","BACK/LAY direzione","P0","S","FIN"),
 ("M082","SL% su perdita netta liquidabile (U15HT)","core/pnl_engine.py","M081","oracle U15HT","P1","S","FIN")]),
(38,"PR38-a",[
 ("M083","F002: registro TTL DIRECT popolato (OFF di default)","direct_unmatched_ttl.py, core/runtime_controller.py","#537","solo ordini DIRECT noti","P2","S","FIN"),
 ("M084","TTL-PERSIST: registro TTL persistito al riavvio","database.py, core/runtime_controller.py","M083,M023","riavvio => TTL ripreso","P2","S","FIN")]),
(39,"PR38-b",[
 ("M086","Miglior prezzo dinamico solo sull'unmatched con limiti verificabili","direct_best_price.py, core/runtime_controller.py","M066,M081","limiti inseguimento","P2","M","FIN"),
 ("M087","Uscite protettive non ritardate (PF-EXEC-03)","core/runtime_controller.py","M086","uscita senza inseguimento","P1","S","FIN")]),
(40,"PR43-a",[
 ("M085","PF-FIN-01: MTM corretto nel position ledger","core/position_ledger.py","#537","oracle MTM BACK/LAY","P0","S","FIN"),
 ("M088","PF-FIN-02: realizzato e hedge nel position ledger","core/position_ledger.py","M085","oracle realizzato","P0","S","FIN")]),
(41,"PR25",[
 ("M089","P35: intento durevole (attesa, nessuna sostituzione, scadenza a chiusura mercato)","core/pnl_engine.py, database.py","M082,M088","riavvio conserva intento","P1","M","FIN"),
 ("M090","Trailing persistente","core/pnl_engine.py","M089","trailing dopo riavvio","P1","S","FIN"),
 ("M091","Auto-cashout MM","core/pnl_engine.py, cashout_router.py","M090","cashout automatico se abilitato","P1","S","FIN")]),
(42,"PR43-b",[
 ("M092","Cashout per target","cashout_resolver.py","M091","estranei invariati","P1","S","FIN"),
 ("M093","Netting BACK/LAY","cashout_resolver.py, cashout_executor.py","M092","vettore per target","P1","M","FIN"),
 ("M094","Void = hedge (P39)","cashout_executor.py","M093","risultato azzerato sulla selezione","P2","S","FIN")]),
(43,"PR44",[
 ("M095","Cashout fraction/percentage (DEC-C44-03)","cashout_residual_handler.py","M094","soglia+frazione, nessun arrotondamento che aumenta rischio","P1","M","FIN"),
 ("M096","#517: minimo effettivo verificato al posto del floor 0,10","core/microstake_safe.py","M095","minimo exchange","P2","S","FIN")]),
(44,"PR45-a",[
 ("M097","CL19 (P0) + AUD-04: customer_ref del cashout univoco e canonico","cashout_router.py, cashout_wiring.py","#537","due cashout => ref distinti","P0","S","FIN"),
 ("M098","AUD-05: ledger del cashout","cashout_*.py, database.py","M097","tracciabilita'","P1","S","FIN")]),
(45,"PR45-b",[
 ("M099","AUD-03 + AUD-02: esito incerto dell'hedge = AMBIGUOUS + settlement senza betId","cashout_executor.py, core/runtime_controller.py","M098,M066","nessun secondo hedge cieco","P0","M","FIN"),
 ("M100","#516: un solo cashout attivo per posizione","cashout_router.py","M099","secondo comando rifiutato","P1","S","FIN"),
 ("M101","F06: fencing cashout per ambito evento","cashout_router.py","M100","scope evento","P1","S","FIN"),
 ("M102","Parita' GUI/headless del cashout (test)","tests","M101","stesso esito","P2","S","FIN")]),
(46,"PF-API-0+PR36",[
 ("M103","Control API: server locale, auth, handshake e versione","control_api/ (nuovo)","M029,M004","auth negata; versione","P1","M","SAFE"),
 ("M104","Correlation id request_id->operation_id->customer_ref + audit","control_api/, core/trading_engine.py","M103","catena ricostruibile","P1","S","SAFE"),
 ("M105","Ritiro di betfair_market_api.py legacy","betfair_market_api.py","M103","nessun consumatore","P2","S","SAFE")]),
(47,"PR08",[
 ("M106","Resolver neutro per nome cablato","services/deterministic_resolver.py","#537","nome ambiguo => rifiuto","P1","S","BF"),
 ("M107","Catalogo incrementale (delta, no-wipe)","services/catalog_sync_service.py","M106","delta/upsert","P2","M","BF")]),
(48,"PF-API-1",[
 ("M108","Route lettura: stato, posizioni, ordini","control_api/","M104","schema stabile","P1","M","SAFE"),
 ("M109","Route lettura: mercati, resolver, fondi","control_api/","M108,M107","schema","P1","S","SAFE"),
 ("M110","Route lettura: Market P/L e stato Stream (OWN-MCP-02F)","control_api/","M109,M056,M062","fonte core","P2","S","SAFE")]),
(49,"PF-API-cmd-a",[
 ("M111","place_order (EXPLICIT/FIXED/MM, idempotente)","control_api/","M110,M079","request_id duplicato => un ordine","P1","M","FIN"),
 ("M112","cancel/replace/update","control_api/","M111,M059","AMBIGUOUS","P1","M","FIN")]),
(50,"PF-API-cmd-b+PR37",[
 ("M113","Preview senza effetti (PR37)","control_api/, core/runtime_controller.py","M112","zero effetti","P1","S","FIN"),
 ("M114","STOP/resume con barriera fisica","control_api/, core/risk_stop.py","M113","zero nuove entrate","P0","S","FIN"),
 ("M115","reconcile e cashout via API","control_api/","M114,M102","stessa catena","P1","M","FIN")]),
(51,"PR12-b",[("M116","MANUAL a doppia conferma (OWN-002), principal automation distinto","control_api/","M115","conferma singola => rifiuto","P1","M","FIN")]),
(52,"HYBRID",[("M117","HYBRID_REAL_EXECUTION: identita' e stato distinti, DISABILITATA","core/runtime_controller.py, control_api/","M116,M031","attivazione impossibile senza autorizzazione","P3","M","SAFE")]),
(53,"PR07",[("M118","Catalogo GUI: contratto worker/on_success (#415)","mini_gui.py","#537","test GUI + screenshot Xvfb","P2","S","GUI")]),
(54,"PR05",[
 ("M119","GUI: scrollbar e DPI comuni","mini_gui.py, ui_panels/","M118","1280x820 tutti visibili (framework)","P2","M","GUI"),
 ("M120","GUI: controlli safety visibili per tab (57/129)","mini_gui.py, ui_panels/","M119","129/129 visibili, screenshot","P2","M","GUI")]),
(55,"PR13",[
 ("M121","Pannello osservabilita' unica","ui_panels/","M045,M120","stato coerente col runtime","P2","M","GUI"),
 ("M122","Safe Mode nella GUI + console readiness (#359)","mini_gui.py","M121","indicatori","P2","S","GUI")]),
(56,"PR14",[
 ("M123","Storico: writer e coordinator LIVE+SIM","database.py, core/","M031","separazione modalita'","P2","M","SAFE"),
 ("M124","Storico: pagine GUI","ui_panels/","M123","paginazione","P3","M","GUI")]),
(57,"#511",[
 ("M125","Configurazione GUI dei 41 parametri Roserpina","mini_gui.py","M079,M120","validazione campi","P2","M","GUI"),
 ("M126","Simulazione combinatoria Roserpina-41 (test)","tests","M125","copertura combinatoria","P2","M","FIN")]),
(58,"PR48",[
 ("M127","Ritiro percorsi XTrader legacy (callgraph)","moduli legacy","M065","nessun consumatore","P3","M","SAFE"),
 ("M128","F015/F020: codice finanziario duplicato o morto","moduli","M127","callgraph","P3","M","SAFE")]),
(59,"PR49",[("M129","CI: autorizzazioni da fonte fidata + merge markers (#345)","workflow","#537","test guard","P2","S","GOV")]),
(60,"PR50",[("M156","CI-DEDUP: niente ci-dynamic annidato in ci-master-gate, un solo backstop completo (job senza if/paths), push solo su main per i 12 workflow doppi, cancel-in-progress per SHA superati delle PR","workflow, tests/testsuite/test_false_green_semantics.py, tests/guardrails/test_orchestratori_ci_525.py","M158","stessi check prodotti su push di prova; backstop non saltabile; nessun job obbligatorio perso","P1","M","GOV"),("M157","CI-PARALLEL: backstop con xdist worksteal (+ step seriale per i test marcati serial), mutation 1 volta per push con --jobs, modalita' FAST incrementale solo locale","workflow, scripts/run_mutation_guardrails.py","M156","3 run CI parallelo vs seriale: stesso insieme di nodeid ed esiti; mutation parallela identica alla seriale","P1","M","GOV"),("M130","CI: collection/workflow_run e policy reviewer","workflow","#537","collection","P2","S","GOV")]),
(61,"#518",[("M131","Tooling pfckpt/pf_trace","tools/","#537","test tooling","P3","S","OPS")]),
(62,"FORMAL-VERIF",[("M132","Strumenti invarianti G1-G7 (manifest/workflow a merge owner)","tools, workflow","M131","NOT_RUN mai PASS","P3","M","GOV")]),
(63,"#472",[("M133","Enforcement esclusione auto-merge (identita' agente, ruleset)","scripts/guardrail_check.py, ops","M129","path protetti mai auto-merge","P2","S","GOV")]),
(64,"#515",[("M134","Motore di sblocco autonomo","scripts","M133","policy test","P3","M","GOV")]),
(65,"GateA-a",[("M135","Validatore tracciabilita' exit 0 e denominatore","tools/traceability","#537","validatore","P2","S","DOC")]),
(66,"GateA-b",[
 ("M136","Manifest requisiti core","docs/traceability","M135","zero orfani","P2","M","DOC"),
 ("M137","Tracciabilita' Roserpina multi-tavolo (#510)","docs/traceability","M136","41x10","P2","M","DOC")]),
(67,"PR51",[
 ("M138","Backup SQLite cifrato","scripts, core","M029","restore verificato","P1","M","OPS"),
 ("M139","Scheduler e retention backup (12 h, 2 copie)","core","M138","retention","P1","S","OPS")]),
(68,"PR52",[("M140","Restore drill isolato; errori disco/WAL/permessi","scripts/db_restore_validate.py","M139","drill","P1","M","OPS")]),
(69,"PR53",[
 ("M141","Pacchetto Linux (AppImage) + dipendenze dichiarate (F012)","packaging/, requirements","M140","build CI","P1","M","GOV"),
 ("M142","Pacchetto Windows (exe)","pickfair.spec, *.bat","M141","build CI","P1","M","GOV")]),
(70,"PR54",[
 ("M143","Supervisore H24 Linux (systemd) (F008)","packaging/","M141","restart","P1","S","OPS"),
 ("M144","Supervisore Windows (NSSM) + parita' segnali","packaging/","M142,M143","restart Windows MANUAL","P1","S","OPS")]),
(71,"PR55",[("M145","VERSION, pipeline Release, manifest verificabile","workflow, VERSION","M144","hash","P1","M","GOV")]),
(72,"PR56",[("M146","Version checker, download con hash","core","M145","hash errato => rifiuto","P3","M","OPS")]),
(73,"PR57",[("M147","Safe-state e updater separato","updater/","M146","barriera","P3","M","OPS")]),
(74,"PR58",[("M148","Sostituzione, health, rollback","updater/","M147","rollback","P3","M","OPS")]),
(75,"PR59",[("M149","Accettazione whole-wiring sul runtime vero","tests/acceptance","M145 + tutte le P0/P1","whole-wiring","P0","M","FIN")]),
(76,"PR60",[("M150","Sweep #380 e dossier di collaudo","mini_gui.py, docs","M149","dossier","P1","M","DOC")]),
(77,"PR06",[
 ("M151","Parser neutrale deterministico (senza Telegram)","services/delimiter_parser.py","#537","formati, virgola/punto","P3","M","SAFE"),
 ("M152","PF-CAP-08/13/18: nessuno stake o quota inventati","telegram_listener.py (codice, trasporto sospeso)","M151","campi mancanti => scarto","P2","S","FIN")]),
(78,"PR10",[("M153","Rimozione parser regex dopo test di equivalenza","core/parser.py","M152","equivalenza","P3","M","SAFE")]),
(79,"PR18",[("S01","Chat ID per-bot e due trasporti Telegram","SOSPESA","-","-","-","-","SUSP")]),
(80,"PR21",[("S02","Setup Bot API","SOSPESA","-","-","-","-","SUSP")]),
(81,"PR39",[("S03","Master/slave","SOSPESA","-","-","-","-","SUSP")]),
(82,"PR40",[("S04","Registry follower","SOSPESA","-","-","-","-","SUSP")]),
(83,"PR41",[("S05","Stake follower","SOSPESA","-","-","-","-","SUSP")]),
(84,"PR42",[("S06","Replica azioni follower","SOSPESA","-","-","-","-","SUSP")]),
(85,"#497",[("S07","Ledger PR-flow su VPS","DEFERRED","-","-","-","-","SUSP")]),
]

# PF-MICRO-PR-OPTIMIZATION-01 — Analisi della granularità delle PR residue di Pickfair

Mandato owner del 22/10/2026. Lavoro in sola lettura: nessuna modifica a issue, roadmap, codice, branch o PR; nessun commit, push, merge o deploy. Dati estratti da GitHub il 22/10/2026 verso le 16:20Z. Main di riferimento `152613b`.

Legenda: **[M]** dato misurato · **[S]** stima derivata da dati misurati · **[I]** ipotesi dichiarata.

## A. Stato reale

| Voce | Valore | Fonte |
|---|---|---|
| PR mergiate da #462 a #535 | **45** [M] | `git log 5d6d501..152613b` |
| PR aperte | **1** [M]: #537, head `2a5a6c2`, ciclo di fix 5/10, NOT READY | GitHub, #453 c.6345531244 |
| PR future (piano dopo #537) | **85** [M]: 78 eseguibili e 7 sospese | piano del 22/10, CLOSURE-05 (invariato) |

I quattro finding registrati oggi (#453 c.6345531244) entrano in PR già previste, senza crearne di nuove:
- **G7 (P0)** → PF-001/002/003;
- **RST-EXEC (P1)** → PF-003;
- **OP-VIS (P1)** → PF-029;
- **TTL-PERSIST (P2)** → PF-058.

## Dati storici misurati (17 PR mergiate, #490–#535)

| PR | Caratteri di produzione nel diff [M] | Round di review Sol/Grok [M] | Cicli di fix [M] | Ore aperta [M] | Costo dei revisori [M] |
|---|---|---|---|---|---|
| #498 | 17 351 | 5/4 | 4/5 | 8,3 | $0,87 |
| #500 | 34 651 | 2/2 | 1/5 | 0,8 | $0,16 |
| #501 | 16 573 | 2/2 | 1/5 | 0,4 | $0,23 |
| #502 | 18 289 | 1/1 | 0 | 0,1 | $0,23 |
| #503 | 22 511 | 5/5 | 4/5 | 1,2 | $0,66 |
| #504 | 25 617 | 6/4 | 5/5, esaurito | 9,8 | $0,80 |
| #512 | ~200 000 | 21/21 | 17, con deroghe owner | 45,5 | $2,78 |
| #522 | 147 531 | 6/6 | 6, con deroga | 6,8 | $0,80 |
| #526 | 6 724 | 1/1 | — | 7,3 (notte) | $0,12 |
| #529 | 52 962 | 15/15 | 7, con deroga | 6,9 | $2,15 |
| #531 | 22 382 | 7/7 | 3/10 | 2,1 | $1,03 |
| #533 | 15 305 | 8/8 | 7/10 | 4,8 | $1,21 |
| #534 | 8 684 | 5/5 | 4/10 | 2,3 | $0,74 |
| #535 | 97 241 | 9/9 | 8/10 | 14,0 | $1,50 |
| #537 (aperta) | 46 006 al primo push | 5/5 | 5/10 | >4,5 | $0,97 finora |

Altri dati misurati:
- **Totale delle 17 PR mergiate:** 125,4 ore aperte, $15,15 di revisori, 216 round. In media $0,070 per singola review, quindi circa **$0,14 per round** (Sol + Grok).
- **Budget di patch dei revisori:** Grok vede al massimo 18 000 caratteri per push e 5 000 per file; Sol 24 000 e 7 000.
- **CI per ogni round della PR:** `ci-master-gate` 25,3 min (mediana), `PR Merge Readiness` 27,0 min, Grok 9,2 min, Sol 2,8 min. Il pavimento di un round è quindi **circa 27–30 minuti**.
- **Rapporto fra calendario e ore aperte (19–22/10):** 9 PR in 66,6 ore di calendario, contro 48,6 ore aperte → **1,37**. Le pause includono implementazione, attese owner e notti.
- **Codex:** ha aperto 181 thread inline su 181, ed è arrivato 24 volte al limite d'uso. Il revisore che trova di più è anche il meno disponibile, e la sua disponibilità non dipende dalla dimensione della PR.
- **`core/runtime_controller.py` ha 7 821 righe.** Quasi ogni PR finanziaria lo tocca. Una modifica oltre circa 100 righe in quel file supera il tetto per file di Grok anche in una PR piccola.

### Cosa dicono i dati
1. **Le PR piccole non scendono sotto circa 5 round.** Nell'era attuale (cap 10, mutation obbligatoria):
   - PR fino a 25 000 caratteri (#531, #533, #534): **6,7 round, 3,1 ore, $0,99** in media;
   - PR oltre 45 000 caratteri (#529, #535): **12 round, 10,5 ore, $1,82**.
2. **Per carattere, le PR grandi costano meno round.** Spezzarle aumenta i round totali. Il loro problema è un altro: copertura di review parziale ed esaurimento dei cicli. #512 ha richiesto 17 cicli con deroghe owner; #529 e #522 sono andate oltre il tetto con deroga; #535 si è fermata a 8/10.
3. **Sotto i 25 000 caratteri il tempo per PR non cala più.** Per questo 150–200 PR aggiungono pavimenti di round senza ridurre i finding.

## Caso di studio — #537

- **Primo push:** 46 006 caratteri di produzione, oltre il doppio del budget di Grok e quasi il doppio di quello di Sol. Entrambi hanno saltato `database.py` e i test.
  - Sol ha scritto «NON VERIFICABILE» nei round 1–3 e ha trovato i suoi due bloccanti solo al round 4, quando il push era di 13 000 caratteri e lo vedeva per intero.
  - I push successivi (21k, 19,6k, 13k, 8,7k, 6,5k) sono stati visti sempre di più.
- **Finding:** 30 indipendenti finora (Codex 17, Grok 11, Sol 2), più un difetto reale trovato dalla CI (`F032_01`, BEGIN differito).
- **Cicli:** 5 su 10.
- **Natura dei finding:** quasi tutti sono semantica di concorrenza (drain allo STOP, rinnovo concorrente, upgrade della transazione, hedge in volo). Dipendono dal design e sarebbero emersi anche in PR più piccole. Non sono colpa della dimensione.
- **Cosa doveva restare atomico:** lock OS + identità del file + lease + fence nel `Database` + rifiuto allo START. Una lease senza fence, o un fence senza lease, non è sicuro.
- **Cosa si poteva separare:** le verifiche ai confini finanziari (choke point dell'engine, esecutore e cancel del cashout, TTL, emergency stop, drain), in una seconda PR dipendente.
- **Effetto stimato di due PR da circa 23k ciascuna [S]:** tempo totale circa uguale (2 × ~6 round), costo circa +40%, ma revisori in grado di vedere quasi tutto fin dal primo push.
- **Conclusione:** la dimensione giusta era **2 PR**, non 4 o più. Spezzare oltre avrebbe moltiplicato i pavimenti di round senza ridurre i finding di concorrenza.

## B. Confronto quantitativo

Modello [S] costruito sui dati dell'era attuale:
- PR fino a ~25k caratteri: 3,1 ore aperte, 6,7 round, $0,99;
- PR oltre ~35–45k: 10,5 ore, 12 round, $1,82;
- calendario = ore aperte × 1,37.

Nel piano attuale le PR grandi sono le 19 che lo scenario B divide [S].

| Indicatore | A: 86 PR (85 future + #537) | B: 108 (107 future + #537) — **raccomandato** | C: 161 (160 future + #537) | C+: ~200 |
|---|---|---|---|---|
| PR eseguibili future | 78 (59 medie + 19 grandi) | 100 medie | 153 piccole o medie | ~193 |
| Complessità media (produzione per PR) | ~17k, con 19 PR da 35–60k | ~13k, massimo ~31k | ~8,7k | ~6,9k, con split artificiali |
| Round di review stimati | ~623 | ~670 | ~1 025 | ~1 290 |
| Ore aperte totali | ~382 | ~310 | ~474 | ~600 |
| Tempo di calendario | ~524 h (~22 giorni) | ~425 h (**~18 giorni**) | ~650 h (~27 giorni) | ~820 h (~34 giorni) |
| Costo revisori (Sol + Grok) | ~$93 | ~$99 | ~$151 | ~$190 |
| Rischio tecnico | **Alto**: 19 PR con primo push oltre il budget, rischio di esaurire il cap 10 | **Medio-basso** | Medio: ~150 rebase su `runtime_controller.py`, catena lunga | Alto: stati intermedi incoerenti |
| Rischio finanziario | **Alto**: percorsi di denaro rivisti in parte | **Basso**: ogni PR FIN completa, con test integrati | Medio: alcuni invarianti si dividono fra PR | Alto: riserva separata dal rilascio, fence separato dalla lease |

Lo scenario C+ a circa 200 PR richiederebbe split artificiali: PR di soli test, docs separate, una mezza transizione finanziaria per PR. Violano i criteri 2, 4 e 5 del mandato.

## E. Verdetto

- **Numero ottimale di PR residue: 107** dopo #537, cioè **100 eseguibili + 7 sospese**. Contando anche #537, **108**.
- **Da dividere: 19 delle 85 PR**, quelle che prevedibilmente superano 35–45k caratteri di produzione o mettono insieme rischi indipendenti. Ne nascono 41 PR.
  - Elenco: PR31-a, G7, PR29-f, PR28-f, PR34, PR35 (la dipendenza va separata perché è a merge owner), PR19, PR47, PR11-b, Order Stream, PR30-a, PR30-b, PR09, PR25, PR43-b, PR45-b, PF-API-cmd-b, PR05, PR53.
- **Da lasciare invariate: 66** (59 eseguibili + 7 sospese).
- **Guadagno di tempo plausibile:** circa **−4 giorni (−19%)** rispetto ad A nello scenario centrale, con forchetta da 0% a −20%.
- **Costo:** circa **+$6 (+6%)** di revisori rispetto ad A. Rispetto a C si risparmiano circa **$52** e circa **9 giorni**.

### Regole operative della scomposizione
1. **Taglio:** un obiettivo per PR, con un primo push sotto i ~25k caratteri di produzione.
2. **Sempre in una sola PR:**
   - riserva e rilascio;
   - lease e fence;
   - transizione e test integrato;
   - attivazione insieme ai gate.
3. **Dipendenze e manifest in PR minime e isolate:** l'etichetta `manual-review-required` resta così su un diff piccolo.
4. **Su `runtime_controller.py` al massimo circa 5k caratteri per PR**, estraendo moduli nuovi come ha fatto #537 con `runtime_authority.py`.
5. **Corsie dormienti:** Dutching, HYBRID e TTL DIRECT si attivano solo nell'ultima PR della loro catena, con i gate PASS.

### Principali rischi
- Il modello delle PR grandi poggia su **n = 2** (#529, #535). #537 è ancora aperta.
- La dimensione per PR del piano futuro è **stimata**: S ≈ 5k e M ≈ 13k caratteri per requisito.
- Le ore «aperte» includono attese owner e notti. Con merge sempre manuale dell'owner, ogni PR in più aggiunge una attesa, a sfavore degli scenari C e C+.
- La disponibilità di Codex, il revisore che trova di più, è imprevedibile.

### Condizioni che invaliderebbero la stima
1. **Se una PR grande costa circa 7 ore invece di 10,5**, A e B si equivalgono sul tempo. B resta preferibile per la copertura di review e il rischio sul cap.
2. **Se i revisori ricevessero budget di patch più alti** (decisione owner sui workflow), le PR grandi perderebbero lo svantaggio di copertura e A tornerebbe competitivo.
3. **Se l'owner autorizzasse sviluppo parallelo con apertura seriale** (Factory), il collo di bottiglia diventerebbe la CI e la review, e il guadagno di B aumenterebbe.
4. **Nuovi finding P0/P1** aggiungerebbero PR in qualsiasi scenario.

## C. Mapping completo — scenario B raccomandato (85 PR originali → 107 PR, di cui 100 eseguibili + 7 sospese)

Ogni requisito atomico (ID `Mnnn`) compare **una sola volta**. Le colonne «Dipendenze» usano gli ID della sequenza `PF-nnn` (sezione D). Dimensione stimata = caratteri di diff di produzione (S≈5k, M≈13k per requisito; **stima**, non misura). ⓜ = merge owner. FIN = test finanziari integrati obbligatori.

| # orig | PR originale | Nuove PR | Requisiti atomici (scope) | File principali | Dipendenze | Test minimi | Priorità | Dim. stim. |
|---|---|---|---|---|---|---|---|---|
| 1 | DOC-MODES | **PF-026** ⓜ | `M001` Contratto SIM/HYBRID_REAL_EXECUTION/LIVE + stake EXPLICIT/FIXED/MM | docs/mcp_operational_contract.md, CLAUDE.md, test governance | #537 | test coerenza policy | P2 | ~5k |
| 2 | PR32-a | **PF-020** | `M002` L08-F3: fencing di riconciliazione mai fail-open<br>`M003` L08-F2: fencing token persistito e confrontato nel SQL | core/reconciliation_engine.py; core/reconciliation_engine.py, database.py | #537 | RED con enable_fencing_token=False; mutation; token superato rifiutato in transazione; riavvio monotono | P1 | ~18k |
| 3 | PR31-a | **PF-019** | `M004` L07-F1: recovery fallita all'avvio => nessun ACTIVE<br>`M006` PF-ALL-F004: consumatore di RECOVER_ORDER | core/runtime_controller.py (start); core/runtime_controller.py, core/state_recovery.py | #537 | eccezione recovery => PAUSED/BLOCKED, nessun ordine; evento consumato, nessun replay cieco | P0 | ~10k |
| 3 | PR31-a | **PF-028** FIN | `M005` Decision log di riconciliazione persistito<br>`M007` AUD-01: ordini ghost nei cap di mercato/evento | database.py, core/reconciliation_engine.py; core/reconciliation_engine.py, core/risk_gate.py | PF-019 | decision log dopo riavvio; schema additivo; ghost conteggiato in totale, mercato, evento | P1 | ~18k |
| 4 | G7 #532 | **PF-001** FIN | `M008` G7-a: SimulationBroker riserva/rilascia stake BACK e liability LAY; INSUFFICIENT_FUNDS | simulation_broker.py | #537 | 4 xfail strict -> PASS; place/cancel/reject | P0 | ~13k |
| 4 | G7 #532 | **PF-002** FIN | `M009` G7-b: transizioni matched/parziale/settlement/cashout senza doppio conteggio | simulation_broker.py | PF-001 | oracle fondi su sequenze miste; multi-tavolo | P0 | ~13k |
| 4 | G7 #532 | **PF-003** FIN | `M010` G7-c + RST-EXEC + L05-F2 (restart): riavvio con ordine EXECUTABLE non abbinato | core/runtime_controller.py (_rebuild_open_positions, at=-inf), core/table_manager.py | PF-002 | test integrato: esposizione, riserva tavolo, giro F028, funds gate dopo il rebuild | P0 | ~13k |
| 5 | PR29-f | **PF-008** FIN | `M011` C2: validita' saldo LIVE <=30 s monotona su ogni nuovo ingresso (auto-next compreso) | core/runtime_controller.py (_entry_funds_reason, snapshot) | #537 | 29,999 ammesso / 30,001 rifiutato; orologio di sistema spostato; distinto da TTL tombstone 120 s | P1 | ~5k |
| 5 | PR29-f | **PF-004** FIN | `M012` 29-f: gate fondi senza doppio conteggio delle riserve (parita' SIM/LIVE)<br>`M013` PF-FIN-02 residuo + T-F03/T-F06: funds gate che riconcilia ordini/liability/esposizione con dati exchange incompleti | core/runtime_controller.py, core/table_manager.py; core/runtime_controller.py, services/betfair_service.py | PF-002, PF-003 | riserve interne vs available; xfail G7 gate -> PASS; dati incompleti => blocco; due processi | P0 | ~26k |
| 6 | PR29-d | **PF-009** FIN | `M014` L05-F2 (reset) + PF-C29-01.09: il reset non rilascia riserve vive | core/table_manager.py, core/runtime_controller.py (reset_cycle) | PF-003 | reset con ordine vivo => riserva mantenuta | P1 | ~5k |
| 7 | PR29-e | **PF-016** FIN | `M015` L05-F4 + L05-F5: auto-close LAY su liability; FAILED post-invio senza table_id non lascia ACTIVE | core/pnl_engine.py, core/table_manager.py | #537 | oracle liability LAY; tavolo liberato | P2 | ~5k |
| 8 | #521 | **PF-010** FIN | `M016` PF-LOT-D-F: aggregazione per bet_id dei settlement dello stesso mercato | core/runtime_controller.py (settlement) | #537 | piu' bet stesso mercato, un solo rilascio corretto | P1 | ~5k |
| 9 | #519 | **PF-017** FIN | `M017` Conferma step del cycle executor legata al verdetto dell'engine | core/money_management.py | #537 | nessuna conferma senza verdetto | P2 | ~5k |
| 10 | #520 | **PF-018** FIN | `M018` Book SIM malformato = blocco pre-invio | core/microstake_sim_port.py, simulation_broker.py | #537 | book malformato => rifiuto senza effetti | P2 | ~5k |
| 11 | PR28-f | **PF-005** FIN | `M019` C3/t80u 1-2: chiavi pct e recovery assenti => blocco / nessun recovery (+F10 loader/GUI)<br>`M020` C1: nessun cap monetario fisso sopravvive (config.example + test di guardia sui default) | services/setting_service.py, core/system_state.py, mini_gui.py (solo default); config.example.json, tests/guardrails | #537 | chiave assente => blocco; max_stake_abs assente resta non attivo; scansione default runtime/loader/GUI/esempio | P0 | ~18k |
| 11 | PR28-f | **PF-011** FIN | `M021` F04: drawdown con perdita non realizzata<br>`M022` F05 + F014 + F11: bankroll non finito, stop di sessione senza baseline, scope warning-only | core/risk_desk.py, core/loss_limits.py; core/risk_desk.py, core/loss_limits.py, core/risk_gate.py | PF-005 | oracle drawdown con esposizione aperta; input NaN/inf/assente => blocco | P1 | ~10k |
| 12 | PR33 | **PF-021** | `M023` Migrazioni DB versionate (schema_version, additive, rollback)<br>`M024` Hard-verify del delta c8fcaa3 (#446) | database_schema.py, database.py; tests only | #537 | DB vecchio migra; rollback su errore; atomicita' outbox/saga | P1 | ~18k |
| 13 | PR34 | **PF-022** | `M025` #528: harness F026 deterministica (niente attese fisse) | tests/f026 | #537 | 100 run senza flake | P1 | ~5k |
| 13 | PR34 | **PF-027** | `M026` Crash drill su SQLite reale (PF-C28-01..03, F4 durabilita')<br>`M027` STOP persistente e riavvio (OD12-07, PF-EXEC-06, F07 E2E) | tests + core/state_recovery.py; core/runtime_controller.py, core/risk_stop.py | PF-003, PF-021, PF-022 | kill fra write e commit; STOP sopravvive al riavvio; ripresa solo se coerente | P0 | ~26k |
| 14 | PR35 | **PF-023** ⓜ | `M028` Dipendenza cryptography vincolata + primitiva AEAD (merge owner, manifest) | requirements*, core/secret_cipher.py | #537 | bitflip/chiave errata rifiutati | P1 | ~5k |
| 14 | PR35 | **PF-024** | `M029` Migrazione non distruttiva dei segreti + checklist ACL Windows | database.py (segreti), core/secret_cipher.py | PF-023 | migrazione e rollback; MANUAL_ONLY Windows | P1 | ~13k |
| 15 | PR15 | **PF-012** FIN | `M030` Saldi, picco e drawdown separati per modalita'<br>`M031` Contatori/stato persistenti per modalita' + emergency snapshot | core/risk_desk.py, database.py; core/runtime_controller.py, database.py | PF-005 | picco SIM non contamina LIVE; switch modalita' senza ereditare bankroll | P1 | ~26k |
| 16 | PR19 | **PF-013** FIN | `M032` marketBaseRate LIVE + commission_pct solo verificata (t80u 3) | betfair_client.py, core/pnl_engine.py | #537 | commissione stimata non aumenta rischio | P1 | ~13k |
| 16 | PR19 | **PF-014** FIN | `M033` F1 + F5: policy commissione e dedupe settlement prima del publish<br>`M034` VOIDED e resettlement (OD12-08) | core/pnl_engine.py, core/runtime_controller.py; core/runtime_controller.py (settlement) | PF-013 | vincita dopo perdita stesso mercato; void dopo settlement, rettifica | P1 | ~18k |
| 17 | F017 | **PF-015** FIN | `M035` Poller settlement attivo quando configurato, riconciliazione al riavvio | trading_config.py, core/runtime_controller.py | PF-014 | mai liberare fondi senza esito autorevole | P1 | ~5k |
| 18 | PR22 | **PF-047** FIN | `M036` F03/F032: EXPANSION su bankroll_start persistito e profitti consolidati | core/money_management.py | PF-014 | RSR-08/09, timing settlement | P1 | ~13k |
| 19 | PR17 | **PF-069** | `M037` Shutdown una volta su errore main + HeadlessApp.stop ferma EventBus (F021) | headless_main.py, shutdown_manager.py | #537 | errore main => teardown unico | P2 | ~5k |
| 20 | PR46 | **PF-070** | `M038` F010: ordinamento EventBus e prova drain/backpressure | core/event_bus.py | PF-069 | ordine sotto carico | P2 | ~5k |
| 21 | PR20 | **PF-061** | `M039` F009: escalation anomalie con azione reale<br>`M040` F016: SafeMode e kill switch alimentati | safe_mode_manager.py, observability/; safe_mode_manager.py, core/runtime_controller.py | #537 | anomalia => pausa/stop; trigger => blocco ingressi | P1 | ~10k |
| 22 | F007 | **PF-062** | `M041` F007-a: market_validator cablato nel percorso ordini<br>`M042` F007-b: auto_throttle cablato o ritirato con prova di ridondanza<br>`M043` F007-c: ai_guardrail cablato o ritirato con prova | market_validator.py, core/runtime_controller.py; auto_throttle.py; ai/ai_guardrail.py | PF-061 | mercato non valido => blocco; burst => throttle; auto-entry mai fail-open | P1 | ~15k |
| 23 | PR47 | **PF-029** | `M045` OP-VIS + F022: segnalazione operatore (posizioni ricostruite, ghost, startup BLOCKED; campo stop in get_status) | core/runtime_controller.py, observability/ | PF-003 | evento consumato e visibile | P1 | ~5k |
| 23 | PR47 | **PF-071** | `M044` F005/F006: watchdog con liveness reale e heartbeat corretto<br>`M046` OD12-11: storico verificabile | observability/; database.py, observability/ | PF-029, PF-070 | liveness falsa => alert; ricostruzione audit | P2 | ~18k |
| 24 | PR11-a+PR16 | **PF-025** | `M047` Login SIM con App Key Delayed (P31)<br>`M048` Logout lato server (#343) | services/betfair_service.py, betfair_client.py; betfair_client.py | #537 | Delayed mai per ordini reali; logout chiama endpoint; MANUAL_ONLY reale | P1 | ~10k |
| 25 | PR11-b | **PF-035** | `M049` Freshness/timestamp Market Stream; dati stantii => nessun ingresso (PF-EXEC-05) | services/streaming_feed.py, core/runtime_controller.py | PF-025 | stale => no entry, no reprice | P0 | ~13k |
| 25 | PR11-b | **PF-036** | `M050` Reconnect, heartbeat, clock<br>`M051` Endpoint Italia (.it) e configurazione feed | services/streaming_feed.py; services/betfair_service.py | PF-035 | disconnessione => riconciliazione; endpoint corretto per account IT | P1 | ~18k |
| 26 | PR11-c | **PF-041** FIN | `M052` F7 + F3: livello svuotato non abbina l'intero ordine<br>`M053` F9: ordine a riposo si riabbina col book | core/simulation_matching_engine.py | PF-002 | size residua corretta; riabbinamento | P2 | ~10k |
| 27 | REST-DISC-a | **PF-042** | `M054` listEventTypes/Competitions/Countries/MarketTypes/Venues/TimeRanges | betfair_client.py | #537 | fixture payload reali | P2 | ~13k |
| 28 | REST-DISC-b | **PF-043** | `M055` listRunnerBook<br>`M056` listMarketProfitAndLoss (fonte P/L autorevole) | betfair_client.py; betfair_client.py, core/pnl_engine.py | PF-042 | schema/errori; confronto P/L | P2 | ~10k |
| 29 | NEW-ACCOUNT | **PF-044** | `M057` getAccountDetails + listCurrencyRates<br>`M058` getAccountStatement paginato | betfair_client.py | #537 | valuta/conversione; paginazione | P2 | ~10k |
| 30 | NEW-UPDATEORDERS | **PF-045** | `M059` updateOrders via autorita' ordini | betfair_client.py, core/trading_engine.py | PF-031 | nessun wrapper raw; AMBIGUOUS | P2 | ~5k |
| 31 | ORDER-STREAM-a | **PF-037** | `M060` Order Stream: connessione, auth, heartbeat, reconnect (trasporto condiviso) | services/order_stream.py (nuovo) | PF-036 | fixture stream | P1 | ~13k |
| 31 | ORDER-STREAM-a | **PF-038** | `M061` Order Stream: cache ordini (oc/orc/uo), clk/initialClk<br>`M062` Order Stream: cablaggio runtime in sola lettura | services/order_stream.py; core/runtime_controller.py | PF-037 | image+delta; eventi senza autorita' | P1 | ~18k |
| 32 | ORDER-STREAM-b/RECON-02 | **PF-039** FIN | `M063` Matrice autorita' REST/stream per fill e cancel<br>`M064` Divergenza/disconnessione stream => riconciliazione fail-closed | core/reconciliation_engine.py | PF-019, PF-038 | stream primario + verifica REST; divergenza => blocco | P0 | ~26k |
| 33 | PR30-a | **PF-030** | `M065` L05-F6: OrderManager cablato o percorso latente ritirato<br>`M067` F003: produttore degli eventi del ciclo ordine | order_manager.py, core/trading_engine.py; core/trading_engine.py | #537 | nessun percorso morto raggiungibile; eventi emessi | P1 | ~18k |
| 33 | PR30-a | **PF-031** FIN | `M066` FSM replace con AMBIGUOUS senza retry cieco | order_manager.py, core/trading_engine.py | PF-030 | response lost | P1 | ~13k |
| 33 | PR30-a | **PF-033** FIN | `M068` F011 + F025: instructionReports top-level; SUCCESS senza betId => AMBIGUOUS<br>`M069` customer_ref di manual_bet + parita' SIM LAPSED | betfair_client.py, core/trading_engine.py; core/trading_engine.py, simulation_broker.py | PF-030 | payload reali; ref stabile; LAPSED | P1 | ~10k |
| 34 | PR30-b | **PF-048** FIN | `M070` R1-R4: preflight tutto-o-niente del lotto + riserva esposizione (corsia dormiente) | core/dutching_batch_manager.py, controllers/dutching_controller.py | PF-031 | una gamba negata => lotto invalidato | P1 | ~13k |
| 34 | PR30-b | **PF-049** FIN | `M071` Cablaggio batch_manager + table_id dei lotti + finalize_terminal_batch<br>`M074` F018 per lotto: un tavolo per lotto al rebuild | core/dutching_batch_manager.py, core/runtime_controller.py; core/runtime_controller.py | PF-003, PF-048 | lotto tracciato per gamba; rebuild lotto | P1 | ~18k |
| 34 | PR30-b | **PF-050** FIN | `M072` PF-EXEC-02: lotto parziale ribilanciato/coperto/sospeso<br>`M073` PF-EXEC-01: ripuntare il solo residuo dopo cancel confermato | core/dutching_batch_manager.py | PF-049 | parziali/incerti, nessun hedge non verificato; cancel ignoto => nessun riposizionamento | P1 | ~18k |
| 35 | PR09 | **PF-051** FIN | `M075` Publisher Dutching e budget equo<br>`M076` CASHOUT Esegui per Dutching<br>`M077` Suite E2E 72 casi #511 (sola prova) | dutching.py, controllers/dutching_controller.py; cashout_router.py, controllers/dutching_controller.py; tests | PF-006, PF-049 | distribuzione stake numerica; cashout lotto; 72 casi | P1 | ~31k |
| 35 | PR09 | **PF-052** FIN | `M078` Attivazione corsia Dutching (rimozione dormienza) solo con gate PASS | controllers/dutching_controller.py | PF-051 | gate PASS prima dell'attivazione | P1 | ~5k |
| 36 | PR12-a+PR23 | **PF-053** FIN | `M079` Stake EXPLICIT/FIXED/MM cablato (esclusivi, default da config, rifiuto se non valido)<br>`M080` Precedenze messaggio/GUI/MM e formula MM con esempi numerici | stake_mode.py, core/runtime_controller.py; core/money_management.py, docs | PF-005 | tre modi + rifiuto; esempi numerici verificati | P1 | ~18k |
| 37 | PR24 | **PF-046** FIN | `M081` F01: direzione SL/TP e prezzo di riferimento<br>`M082` SL% su perdita netta liquidabile (U15HT) | core/pnl_engine.py | PF-006 | BACK/LAY direzione; oracle U15HT | P0 | ~10k |
| 38 | PR38-a | **PF-058** FIN | `M083` F002: registro TTL DIRECT popolato (OFF di default)<br>`M084` TTL-PERSIST: registro TTL persistito al riavvio | direct_unmatched_ttl.py, core/runtime_controller.py; database.py, core/runtime_controller.py | PF-021 | solo ordini DIRECT noti; riavvio => TTL ripreso | P2 | ~10k |
| 39 | PR38-b | **PF-054** FIN | `M086` Miglior prezzo dinamico solo sull'unmatched con limiti verificabili<br>`M087` Uscite protettive non ritardate (PF-EXEC-03) | direct_best_price.py, core/runtime_controller.py; core/runtime_controller.py | PF-031, PF-046 | limiti inseguimento; uscita senza inseguimento | P1 | ~18k |
| 40 | PR43-a | **PF-006** FIN | `M085` PF-FIN-01: MTM corretto nel position ledger<br>`M088` PF-FIN-02: realizzato e hedge nel position ledger | core/position_ledger.py | #537 | oracle MTM BACK/LAY; oracle realizzato | P0 | ~10k |
| 41 | PR25 | **PF-055** FIN | `M089` P35: intento durevole (attesa, nessuna sostituzione, scadenza a chiusura mercato) | core/pnl_engine.py, database.py | PF-006, PF-046 | riavvio conserva intento | P1 | ~13k |
| 41 | PR25 | **PF-056** FIN | `M090` Trailing persistente<br>`M091` Auto-cashout MM | core/pnl_engine.py; core/pnl_engine.py, cashout_router.py | PF-055 | trailing dopo riavvio; cashout automatico se abilitato | P1 | ~10k |
| 42 | PR43-b | **PF-057** FIN | `M092` Cashout per target<br>`M093` Netting BACK/LAY | cashout_resolver.py; cashout_resolver.py, cashout_executor.py | PF-056 | estranei invariati; vettore per target | P1 | ~18k |
| 42 | PR43-b | **PF-059** FIN | `M094` Void = hedge (P39) | cashout_executor.py | PF-057 | risultato azzerato sulla selezione | P2 | ~5k |
| 43 | PR44 | **PF-060** FIN | `M095` Cashout fraction/percentage (DEC-C44-03)<br>`M096` #517: minimo effettivo verificato al posto del floor 0,10 | cashout_residual_handler.py; core/microstake_safe.py | PF-059 | soglia+frazione, nessun arrotondamento che aumenta rischio; minimo exchange | P1 | ~18k |
| 44 | PR45-a | **PF-007** FIN | `M097` CL19 (P0) + AUD-04: customer_ref del cashout univoco e canonico<br>`M098` AUD-05: ledger del cashout | cashout_router.py, cashout_wiring.py; cashout_*.py, database.py | #537 | due cashout => ref distinti; tracciabilita' | P0 | ~10k |
| 45 | PR45-b | **PF-032** FIN | `M099` AUD-03 + AUD-02: esito incerto dell'hedge = AMBIGUOUS + settlement senza betId | cashout_executor.py, core/runtime_controller.py | PF-007, PF-031 | nessun secondo hedge cieco | P0 | ~13k |
| 45 | PR45-b | **PF-034** FIN | `M100` #516: un solo cashout attivo per posizione<br>`M101` F06: fencing cashout per ambito evento<br>`M102` Parita' GUI/headless del cashout (test) | cashout_router.py; tests | PF-032 | secondo comando rifiutato; scope evento; stesso esito | P1 | ~15k |
| 46 | PF-API-0+PR36 | **PF-063** | `M103` Control API: server locale, auth, handshake e versione<br>`M104` Correlation id request_id->operation_id->customer_ref + audit<br>`M105` Ritiro di betfair_market_api.py legacy | control_api/ (nuovo); control_api/, core/trading_engine.py; betfair_market_api.py | PF-019, PF-024 | auth negata; versione; catena ricostruibile; nessun consumatore | P1 | ~23k |
| 47 | PR08 | **PF-040** | `M106` Resolver neutro per nome cablato<br>`M107` Catalogo incrementale (delta, no-wipe) | services/deterministic_resolver.py; services/catalog_sync_service.py | #537 | nome ambiguo => rifiuto; delta/upsert | P1 | ~18k |
| 48 | PF-API-1 | **PF-064** | `M108` Route lettura: stato, posizioni, ordini<br>`M109` Route lettura: mercati, resolver, fondi<br>`M110` Route lettura: Market P/L e stato Stream (OWN-MCP-02F) | control_api/ | PF-038, PF-040, PF-043, PF-063 | schema stabile; schema; fonte core | P1 | ~23k |
| 49 | PF-API-cmd-a | **PF-065** FIN | `M111` place_order (EXPLICIT/FIXED/MM, idempotente)<br>`M112` cancel/replace/update | control_api/ | PF-045, PF-053, PF-064 | request_id duplicato => un ordine; AMBIGUOUS | P1 | ~26k |
| 50 | PF-API-cmd-b+PR37 | **PF-066** FIN | `M113` Preview senza effetti (PR37)<br>`M114` STOP/resume con barriera fisica | control_api/, core/runtime_controller.py; control_api/, core/risk_stop.py | PF-065 | zero effetti; zero nuove entrate | P0 | ~10k |
| 50 | PF-API-cmd-b+PR37 | **PF-067** FIN | `M115` reconcile e cashout via API | control_api/ | PF-034, PF-066 | stessa catena | P1 | ~13k |
| 51 | PR12-b | **PF-068** FIN | `M116` MANUAL a doppia conferma (OWN-002), principal automation distinto | control_api/ | PF-067 | conferma singola => rifiuto | P1 | ~13k |
| 52 | HYBRID | **PF-079** | `M117` HYBRID_REAL_EXECUTION: identita' e stato distinti, DISABILITATA | core/runtime_controller.py, control_api/ | PF-012, PF-068 | attivazione impossibile senza autorizzazione | P3 | ~13k |
| 53 | PR07 | **PF-085** | `M118` Catalogo GUI: contratto worker/on_success (#415) | mini_gui.py | #537 | test GUI + screenshot Xvfb | P2 | ~5k |
| 54 | PR05 | **PF-086** | `M119` GUI: scrollbar e DPI comuni | mini_gui.py, ui_panels/ | PF-085 | 1280x820 tutti visibili (framework) | P2 | ~13k |
| 54 | PR05 | **PF-087** | `M120` GUI: controlli safety visibili per tab (57/129) | mini_gui.py, ui_panels/ | PF-086 | 129/129 visibili, screenshot | P2 | ~13k |
| 55 | PR13 | **PF-088** | `M121` Pannello osservabilita' unica<br>`M122` Safe Mode nella GUI + console readiness (#359) | ui_panels/; mini_gui.py | PF-029, PF-087 | stato coerente col runtime; indicatori | P2 | ~18k |
| 56 | PR14 | **PF-072** | `M123` Storico: writer e coordinator LIVE+SIM<br>`M124` Storico: pagine GUI | database.py, core/; ui_panels/ | PF-012 | separazione modalita'; paginazione | P2 | ~26k |
| 57 | #511 | **PF-089** FIN | `M125` Configurazione GUI dei 41 parametri Roserpina<br>`M126` Simulazione combinatoria Roserpina-41 (test) | mini_gui.py; tests | PF-053, PF-087 | validazione campi; copertura combinatoria | P2 | ~26k |
| 58 | PR48 | **PF-080** | `M127` Ritiro percorsi XTrader legacy (callgraph)<br>`M128` F015/F020: codice finanziario duplicato o morto | moduli legacy; moduli | PF-030 | nessun consumatore; callgraph | P3 | ~26k |
| 59 | PR49 | **PF-073** ⓜ | `M129` CI: autorizzazioni da fonte fidata + merge markers (#345) | workflow | #537 | test guard | P2 | ~5k |
| 60 | PR50 | **PF-074** ⓜ | `M130` CI: collection/workflow_run e policy reviewer | workflow | PF-073 | collection | P2 | ~5k |
| 61 | #518 | **PF-081** | `M131` Tooling pfckpt/pf_trace | tools/ | #537 | test tooling | P3 | ~5k |
| 62 | FORMAL-VERIF | **PF-082** ⓜ | `M132` Strumenti invarianti G1-G7 (manifest/workflow a merge owner) | tools, workflow | PF-081 | NOT_RUN mai PASS | P3 | ~13k |
| 63 | #472 | **PF-075** ⓜ | `M133` Enforcement esclusione auto-merge (identita' agente, ruleset) | scripts/guardrail_check.py, ops | PF-073 | path protetti mai auto-merge | P2 | ~5k |
| 64 | #515 | **PF-083** ⓜ | `M134` Motore di sblocco autonomo | scripts | PF-075 | policy test | P3 | ~13k |
| 65 | GateA-a | **PF-076** | `M135` Validatore tracciabilita' exit 0 e denominatore | tools/traceability | #537 | validatore | P2 | ~5k |
| 66 | GateA-b | **PF-077** | `M136` Manifest requisiti core<br>`M137` Tracciabilita' Roserpina multi-tavolo (#510) | docs/traceability | PF-076 | zero orfani; 41x10 | P2 | ~26k |
| 67 | PR51 | **PF-090** | `M138` Backup SQLite cifrato<br>`M139` Scheduler e retention backup (12 h, 2 copie) | scripts, core; core | PF-024 | restore verificato; retention | P1 | ~18k |
| 68 | PR52 | **PF-091** | `M140` Restore drill isolato; errori disco/WAL/permessi | scripts/db_restore_validate.py | PF-090 | drill | P1 | ~13k |
| 69 | PR53 | **PF-092** ⓜ | `M141` Pacchetto Linux (AppImage) + dipendenze dichiarate (F012) | packaging/, requirements | PF-091 | build CI | P1 | ~13k |
| 69 | PR53 | **PF-093** ⓜ | `M142` Pacchetto Windows (exe) | pickfair.spec, *.bat | PF-092 | build CI | P1 | ~13k |
| 70 | PR54 | **PF-094** | `M143` Supervisore H24 Linux (systemd) (F008)<br>`M144` Supervisore Windows (NSSM) + parita' segnali | packaging/ | PF-092, PF-093 | restart; restart Windows MANUAL | P1 | ~10k |
| 71 | PR55 | **PF-095** ⓜ | `M145` VERSION, pipeline Release, manifest verificabile | workflow, VERSION | PF-094 | hash | P1 | ~13k |
| 72 | PR56 | **PF-098** | `M146` Version checker, download con hash | core | PF-095 | hash errato => rifiuto | P3 | ~13k |
| 73 | PR57 | **PF-099** | `M147` Safe-state e updater separato | updater/ | PF-098 | barriera | P3 | ~13k |
| 74 | PR58 | **PF-100** | `M148` Sostituzione, health, rollback | updater/ | PF-099 | rollback | P3 | ~13k |
| 75 | PR59 | **PF-096** FIN | `M149` Accettazione whole-wiring sul runtime vero | tests/acceptance | PF-095 | whole-wiring | P0 | ~13k |
| 76 | PR60 | **PF-097** | `M150` Sweep #380 e dossier di collaudo | mini_gui.py, docs | PF-096 | dossier | P1 | ~13k |
| 77 | PR06 | **PF-078** FIN | `M151` Parser neutrale deterministico (senza Telegram)<br>`M152` PF-CAP-08/13/18: nessuno stake o quota inventati | services/delimiter_parser.py; telegram_listener.py (codice, trasporto sospeso) | #537 | formati, virgola/punto; campi mancanti => scarto | P2 | ~18k |
| 78 | PR10 | **PF-084** | `M153` Rimozione parser regex dopo test di equivalenza | core/parser.py | PF-078 | equivalenza | P3 | ~13k |
| 79 | PR18 | PF-S01 | Chat ID per-bot e due trasporti Telegram | — | — | — | SOSPESA | — |
| 80 | PR21 | PF-S02 | Setup Bot API | — | — | — | SOSPESA | — |
| 81 | PR39 | PF-S03 | Master/slave | — | — | — | SOSPESA | — |
| 82 | PR40 | PF-S04 | Registry follower | — | — | — | SOSPESA | — |
| 83 | PR41 | PF-S05 | Stake follower | — | — | — | SOSPESA | — |
| 84 | PR42 | PF-S06 | Replica azioni follower | — | — | — | SOSPESA | — |
| 85 | #497 | PF-S07 | Ledger PR-flow su VPS | — | — | — | SOSPESA | — |

## D. Sequenza ottimizzata — 100 PR eseguibili, dalla prima alla certificazione

Ordine topologico delle dipendenze reali; a parità di dipendenze, priorità owner (1 sicurezza finanziaria/ledger → 2 sblocchi → 3 recovery/concorrenza/idempotenza → 4 Betfair → 5 Money Management → 6 funzioni operative → 7 GUI → 8 packaging/certificazione), poi P0→P3.

| Seq | PR | Titolo | Origine | Prio | Dip. | Note |
|---|---|---|---|---|---|---|
| 001 | PF-001 | G7-a: SimulationBroker riserva/rilascia stake BACK e liability LAY; INSUFFICIENT_FUNDS | G7 #532 | P0 | #537 | eseguibile subito, FIN integrati |
| 002 | PF-002 | G7-b: transizioni matched/parziale/settlement/cashout senza doppio conteggio | G7 #532 | P0 | PF-001 | FIN integrati |
| 003 | PF-003 | G7-c + RST-EXEC + L05-F2 (restart): riavvio con ordine EXECUTABLE non abbinato | G7 #532 | P0 | PF-002 | FIN integrati |
| 004 | PF-004 | 29-f: gate fondi senza doppio conteggio delle riserve (parita' SIM/LIVE) + PF-FIN-02 residuo + T-F03/T-F06: funds gate che riconcilia ordini/liabil… | PR29-f | P0 | PF-002, PF-003 | FIN integrati |
| 005 | PF-005 | C3/t80u 1-2: chiavi pct e recovery assenti => blocco / nessun recovery (+F10 loader/GUI) + C1: nessun cap monetario fisso sopravvive (config.exampl… | PR28-f | P0 | #537 | eseguibile subito, FIN integrati |
| 006 | PF-006 | PF-FIN-01: MTM corretto nel position ledger + PF-FIN-02: realizzato e hedge nel position ledger | PR43-a | P0 | #537 | eseguibile subito, FIN integrati |
| 007 | PF-007 | CL19 (P0) + AUD-04: customer_ref del cashout univoco e canonico + AUD-05: ledger del cashout | PR45-a | P0 | #537 | eseguibile subito, FIN integrati |
| 008 | PF-008 | C2: validita' saldo LIVE <=30 s monotona su ogni nuovo ingresso (auto-next compreso) | PR29-f | P1 | #537 | eseguibile subito, rapida, FIN integrati |
| 009 | PF-009 | L05-F2 (reset) + PF-C29-01.09: il reset non rilascia riserve vive | PR29-d | P1 | PF-003 | rapida, FIN integrati |
| 010 | PF-010 | PF-LOT-D-F: aggregazione per bet_id dei settlement dello stesso mercato | #521 | P1 | #537 | eseguibile subito, rapida, FIN integrati |
| 011 | PF-011 | F04: drawdown con perdita non realizzata + F05 + F014 + F11: bankroll non finito, stop di sessione senza baseline, scope warning-only | PR28-f | P1 | PF-005 | FIN integrati |
| 012 | PF-012 | Saldi, picco e drawdown separati per modalita' + Contatori/stato persistenti per modalita' + emergency snapshot | PR15 | P1 | PF-005 | FIN integrati |
| 013 | PF-013 | marketBaseRate LIVE + commission_pct solo verificata (t80u 3) | PR19 | P1 | #537 | eseguibile subito, FIN integrati |
| 014 | PF-014 | F1 + F5: policy commissione e dedupe settlement prima del publish + VOIDED e resettlement (OD12-08) | PR19 | P1 | PF-013 | FIN integrati |
| 015 | PF-015 | Poller settlement attivo quando configurato, riconciliazione al riavvio | F017 | P1 | PF-014 | rapida, FIN integrati |
| 016 | PF-016 | L05-F4 + L05-F5: auto-close LAY su liability; FAILED post-invio senza table_id non lascia ACTIVE | PR29-e | P2 | #537 | eseguibile subito, rapida, FIN integrati |
| 017 | PF-017 | Conferma step del cycle executor legata al verdetto dell'engine | #519 | P2 | #537 | eseguibile subito, rapida, FIN integrati |
| 018 | PF-018 | Book SIM malformato = blocco pre-invio | #520 | P2 | #537 | eseguibile subito, rapida, FIN integrati |
| 019 | PF-019 | L07-F1: recovery fallita all'avvio => nessun ACTIVE + PF-ALL-F004: consumatore di RECOVER_ORDER | PR31-a | P0 | #537 | eseguibile subito |
| 020 | PF-020 | L08-F3: fencing di riconciliazione mai fail-open + L08-F2: fencing token persistito e confrontato nel SQL | PR32-a | P1 | #537 | eseguibile subito |
| 021 | PF-021 | Migrazioni DB versionate (schema_version, additive, rollback) + Hard-verify del delta c8fcaa3 (#446) | PR33 | P1 | #537 | eseguibile subito |
| 022 | PF-022 | #528: harness F026 deterministica (niente attese fisse) | PR34 | P1 | #537 | eseguibile subito, rapida |
| 023 | PF-023 | Dipendenza cryptography vincolata + primitiva AEAD (merge owner, manifest) | PR35 | P1 | #537 | eseguibile subito, rapida, ⓜ |
| 024 | PF-024 | Migrazione non distruttiva dei segreti + checklist ACL Windows | PR35 | P1 | PF-023 |  |
| 025 | PF-025 | Login SIM con App Key Delayed (P31) + Logout lato server (#343) | PR11-a+PR16 | P1 | #537 | eseguibile subito |
| 026 | PF-026 | Contratto SIM/HYBRID_REAL_EXECUTION/LIVE + stake EXPLICIT/FIXED/MM | DOC-MODES | P2 | #537 | eseguibile subito, rapida, ⓜ |
| 027 | PF-027 | Crash drill su SQLite reale (PF-C28-01..03, F4 durabilita') + STOP persistente e riavvio (OD12-07, PF-EXEC-06, F07 E2E) | PR34 | P0 | PF-003, PF-021, PF-022 |  |
| 028 | PF-028 | Decision log di riconciliazione persistito + AUD-01: ordini ghost nei cap di mercato/evento | PR31-a | P1 | PF-019 | FIN integrati |
| 029 | PF-029 | OP-VIS + F022: segnalazione operatore (posizioni ricostruite, ghost, startup BLOCKED; campo stop in get_status) | PR47 | P1 | PF-003 | rapida |
| 030 | PF-030 | L05-F6: OrderManager cablato o percorso latente ritirato + F003: produttore degli eventi del ciclo ordine | PR30-a | P1 | #537 | eseguibile subito |
| 031 | PF-031 | FSM replace con AMBIGUOUS senza retry cieco | PR30-a | P1 | PF-030 | FIN integrati |
| 032 | PF-032 | AUD-03 + AUD-02: esito incerto dell'hedge = AMBIGUOUS + settlement senza betId | PR45-b | P0 | PF-007, PF-031 | FIN integrati |
| 033 | PF-033 | F011 + F025: instructionReports top-level; SUCCESS senza betId => AMBIGUOUS + customer_ref di manual_bet + parita' SIM LAPSED | PR30-a | P1 | PF-030 | FIN integrati |
| 034 | PF-034 | #516: un solo cashout attivo per posizione + F06: fencing cashout per ambito evento + Parita' GUI/headless del cashout (test) | PR45-b | P1 | PF-032 | FIN integrati |
| 035 | PF-035 | Freshness/timestamp Market Stream; dati stantii => nessun ingresso (PF-EXEC-05) | PR11-b | P0 | PF-025 |  |
| 036 | PF-036 | Reconnect, heartbeat, clock + Endpoint Italia (.it) e configurazione feed | PR11-b | P1 | PF-035 |  |
| 037 | PF-037 | Order Stream: connessione, auth, heartbeat, reconnect (trasporto condiviso) | ORDER-STREAM-a | P1 | PF-036 |  |
| 038 | PF-038 | Order Stream: cache ordini (oc/orc/uo), clk/initialClk + Order Stream: cablaggio runtime in sola lettura | ORDER-STREAM-a | P1 | PF-037 |  |
| 039 | PF-039 | Matrice autorita' REST/stream per fill e cancel + Divergenza/disconnessione stream => riconciliazione fail-closed | ORDER-STREAM-b/RECON-02 | P0 | PF-019, PF-038 | FIN integrati |
| 040 | PF-040 | Resolver neutro per nome cablato + Catalogo incrementale (delta, no-wipe) | PR08 | P1 | #537 | eseguibile subito |
| 041 | PF-041 | F7 + F3: livello svuotato non abbina l'intero ordine + F9: ordine a riposo si riabbina col book | PR11-c | P2 | PF-002 | FIN integrati |
| 042 | PF-042 | listEventTypes/Competitions/Countries/MarketTypes/Venues/TimeRanges | REST-DISC-a | P2 | #537 | eseguibile subito |
| 043 | PF-043 | listRunnerBook + listMarketProfitAndLoss (fonte P/L autorevole) | REST-DISC-b | P2 | PF-042 |  |
| 044 | PF-044 | getAccountDetails + listCurrencyRates + getAccountStatement paginato | NEW-ACCOUNT | P2 | #537 | eseguibile subito |
| 045 | PF-045 | updateOrders via autorita' ordini | NEW-UPDATEORDERS | P2 | PF-031 | rapida |
| 046 | PF-046 | F01: direzione SL/TP e prezzo di riferimento + SL% su perdita netta liquidabile (U15HT) | PR24 | P0 | PF-006 | FIN integrati |
| 047 | PF-047 | F03/F032: EXPANSION su bankroll_start persistito e profitti consolidati | PR22 | P1 | PF-014 | FIN integrati |
| 048 | PF-048 | R1-R4: preflight tutto-o-niente del lotto + riserva esposizione (corsia dormiente) | PR30-b | P1 | PF-031 | FIN integrati |
| 049 | PF-049 | Cablaggio batch_manager + table_id dei lotti + finalize_terminal_batch + F018 per lotto: un tavolo per lotto al rebuild | PR30-b | P1 | PF-003, PF-048 | FIN integrati |
| 050 | PF-050 | PF-EXEC-02: lotto parziale ribilanciato/coperto/sospeso + PF-EXEC-01: ripuntare il solo residuo dopo cancel confermato | PR30-b | P1 | PF-049 | FIN integrati |
| 051 | PF-051 | Publisher Dutching e budget equo + CASHOUT Esegui per Dutching + Suite E2E 72 casi #511 (sola prova) | PR09 | P1 | PF-006, PF-049 | FIN integrati |
| 052 | PF-052 | Attivazione corsia Dutching (rimozione dormienza) solo con gate PASS | PR09 | P1 | PF-051 | rapida, FIN integrati |
| 053 | PF-053 | Stake EXPLICIT/FIXED/MM cablato (esclusivi, default da config, rifiuto se non valido) + Precedenze messaggio/GUI/MM e formula MM con esempi numerici | PR12-a+PR23 | P1 | PF-005 | FIN integrati |
| 054 | PF-054 | Miglior prezzo dinamico solo sull'unmatched con limiti verificabili + Uscite protettive non ritardate (PF-EXEC-03) | PR38-b | P1 | PF-031, PF-046 | FIN integrati |
| 055 | PF-055 | P35: intento durevole (attesa, nessuna sostituzione, scadenza a chiusura mercato) | PR25 | P1 | PF-006, PF-046 | FIN integrati |
| 056 | PF-056 | Trailing persistente + Auto-cashout MM | PR25 | P1 | PF-055 | FIN integrati |
| 057 | PF-057 | Cashout per target + Netting BACK/LAY | PR43-b | P1 | PF-056 | FIN integrati |
| 058 | PF-058 | F002: registro TTL DIRECT popolato (OFF di default) + TTL-PERSIST: registro TTL persistito al riavvio | PR38-a | P2 | PF-021 | FIN integrati |
| 059 | PF-059 | Void = hedge (P39) | PR43-b | P2 | PF-057 | rapida, FIN integrati |
| 060 | PF-060 | Cashout fraction/percentage (DEC-C44-03) + #517: minimo effettivo verificato al posto del floor 0,10 | PR44 | P1 | PF-059 | FIN integrati |
| 061 | PF-061 | F009: escalation anomalie con azione reale + F016: SafeMode e kill switch alimentati | PR20 | P1 | #537 | eseguibile subito |
| 062 | PF-062 | F007-a: market_validator cablato nel percorso ordini + F007-b: auto_throttle cablato o ritirato con prova di ridondanza + F007-c: ai_guardrail cabl… | F007 | P1 | PF-061 |  |
| 063 | PF-063 | Control API: server locale, auth, handshake e versione + Correlation id request_id->operation_id->customer_ref + audit + Ritiro di betfair_market_a… | PF-API-0+PR36 | P1 | PF-019, PF-024 |  |
| 064 | PF-064 | Route lettura: stato, posizioni, ordini + Route lettura: mercati, resolver, fondi + Route lettura: Market P/L e stato Stream (OWN-MCP-02F) | PF-API-1 | P1 | PF-038, PF-040, PF-043, PF-063 |  |
| 065 | PF-065 | place_order (EXPLICIT/FIXED/MM, idempotente) + cancel/replace/update | PF-API-cmd-a | P1 | PF-045, PF-053, PF-064 | FIN integrati |
| 066 | PF-066 | Preview senza effetti (PR37) + STOP/resume con barriera fisica | PF-API-cmd-b+PR37 | P0 | PF-065 | FIN integrati |
| 067 | PF-067 | reconcile e cashout via API | PF-API-cmd-b+PR37 | P1 | PF-034, PF-066 | FIN integrati |
| 068 | PF-068 | MANUAL a doppia conferma (OWN-002), principal automation distinto | PR12-b | P1 | PF-067 | FIN integrati |
| 069 | PF-069 | Shutdown una volta su errore main + HeadlessApp.stop ferma EventBus (F021) | PR17 | P2 | #537 | eseguibile subito, rapida |
| 070 | PF-070 | F010: ordinamento EventBus e prova drain/backpressure | PR46 | P2 | PF-069 | rapida |
| 071 | PF-071 | F005/F006: watchdog con liveness reale e heartbeat corretto + OD12-11: storico verificabile | PR47 | P2 | PF-029, PF-070 |  |
| 072 | PF-072 | Storico: writer e coordinator LIVE+SIM + Storico: pagine GUI | PR14 | P2 | PF-012 |  |
| 073 | PF-073 | CI: autorizzazioni da fonte fidata + merge markers (#345) | PR49 | P2 | #537 | eseguibile subito, rapida, ⓜ |
| 074 | PF-074 | CI: collection/workflow_run e policy reviewer | PR50 | P2 | PF-073 | rapida, ⓜ |
| 075 | PF-075 | Enforcement esclusione auto-merge (identita' agente, ruleset) | #472 | P2 | PF-073 | rapida, ⓜ |
| 076 | PF-076 | Validatore tracciabilita' exit 0 e denominatore | GateA-a | P2 | #537 | eseguibile subito, rapida |
| 077 | PF-077 | Manifest requisiti core + Tracciabilita' Roserpina multi-tavolo (#510) | GateA-b | P2 | PF-076 |  |
| 078 | PF-078 | Parser neutrale deterministico (senza Telegram) + PF-CAP-08/13/18: nessuno stake o quota inventati | PR06 | P2 | #537 | eseguibile subito, FIN integrati |
| 079 | PF-079 | HYBRID_REAL_EXECUTION: identita' e stato distinti, DISABILITATA | HYBRID | P3 | PF-012, PF-068 |  |
| 080 | PF-080 | Ritiro percorsi XTrader legacy (callgraph) + F015/F020: codice finanziario duplicato o morto | PR48 | P3 | PF-030 |  |
| 081 | PF-081 | Tooling pfckpt/pf_trace | #518 | P3 | #537 | eseguibile subito, rapida |
| 082 | PF-082 | Strumenti invarianti G1-G7 (manifest/workflow a merge owner) | FORMAL-VERIF | P3 | PF-081 | ⓜ |
| 083 | PF-083 | Motore di sblocco autonomo | #515 | P3 | PF-075 | ⓜ |
| 084 | PF-084 | Rimozione parser regex dopo test di equivalenza | PR10 | P3 | PF-078 |  |
| 085 | PF-085 | Catalogo GUI: contratto worker/on_success (#415) | PR07 | P2 | #537 | eseguibile subito, rapida |
| 086 | PF-086 | GUI: scrollbar e DPI comuni | PR05 | P2 | PF-085 |  |
| 087 | PF-087 | GUI: controlli safety visibili per tab (57/129) | PR05 | P2 | PF-086 |  |
| 088 | PF-088 | Pannello osservabilita' unica + Safe Mode nella GUI + console readiness (#359) | PR13 | P2 | PF-029, PF-087 |  |
| 089 | PF-089 | Configurazione GUI dei 41 parametri Roserpina + Simulazione combinatoria Roserpina-41 (test) | #511 | P2 | PF-053, PF-087 | FIN integrati |
| 090 | PF-090 | Backup SQLite cifrato + Scheduler e retention backup (12 h, 2 copie) | PR51 | P1 | PF-024 |  |
| 091 | PF-091 | Restore drill isolato; errori disco/WAL/permessi | PR52 | P1 | PF-090 |  |
| 092 | PF-092 | Pacchetto Linux (AppImage) + dipendenze dichiarate (F012) | PR53 | P1 | PF-091 | ⓜ |
| 093 | PF-093 | Pacchetto Windows (exe) | PR53 | P1 | PF-092 | ⓜ |
| 094 | PF-094 | Supervisore H24 Linux (systemd) (F008) + Supervisore Windows (NSSM) + parita' segnali | PR54 | P1 | PF-092, PF-093 |  |
| 095 | PF-095 | VERSION, pipeline Release, manifest verificabile | PR55 | P1 | PF-094 | ⓜ |
| 096 | PF-096 | Accettazione whole-wiring sul runtime vero | PR59 | P0 | PF-095 | FIN integrati |
| 097 | PF-097 | Sweep #380 e dossier di collaudo | PR60 | P1 | PF-096 |  |
| 098 | PF-098 | Version checker, download con hash | PR56 | P3 | PF-095 |  |
| 099 | PF-099 | Safe-state e updater separato | PR57 | P3 | PF-098 |  |
| 100 | PF-100 | Sostituzione, health, rollback | PR58 | P3 | PF-099 |  |

## Appendice — scenario C (granularità massima sensata: 153 PR eseguibili + 7 sospese)

Ogni requisito `Mnnn` come PR autonoma, nello stesso ordine di priorità. Serve da confronto; **non raccomandato** (vedi sezione B/E).

| Seq | ID | Titolo | Origine | Dim. | Prio | Dip. |
|---|---|---|---|---|---|---|
| 1 | M008 | G7-a: SimulationBroker riserva/rilascia stake BACK e liability LAY; INSUFFICIENT_FUNDS | G7 #532 | M | P0 | #537 |
| 2 | M009 | G7-b: transizioni matched/parziale/settlement/cashout senza doppio conteggio | G7 #532 | M | P0 | M008 |
| 3 | M010 | G7-c + RST-EXEC + L05-F2 (restart): riavvio con ordine EXECUTABLE non abbinato | G7 #532 | M | P0 | M009 |
| 4 | M012 | 29-f: gate fondi senza doppio conteggio delle riserve (parita' SIM/LIVE) | PR29-f | M | P0 | M009 |
| 5 | M019 | C3/t80u 1-2: chiavi pct e recovery assenti => blocco / nessun recovery (+F10 loader/GUI) | PR28-f | M | P0 | #537 |
| 6 | M085 | PF-FIN-01: MTM corretto nel position ledger | PR43-a | S | P0 | #537 |
| 7 | M088 | PF-FIN-02: realizzato e hedge nel position ledger | PR43-a | S | P0 | M085 |
| 8 | M097 | CL19 (P0) + AUD-04: customer_ref del cashout univoco e canonico | PR45-a | S | P0 | #537 |
| 9 | M011 | C2: validita' saldo LIVE <=30 s monotona su ogni nuovo ingresso (auto-next compreso) | PR29-f | S | P1 | #537 |
| 10 | M013 | PF-FIN-02 residuo + T-F03/T-F06: funds gate che riconcilia ordini/liability/esposizione con dati exchange incompleti | PR29-f | M | P1 | M012, M010 |
| 11 | M014 | L05-F2 (reset) + PF-C29-01.09: il reset non rilascia riserve vive | PR29-d | S | P1 | M010 |
| 12 | M016 | PF-LOT-D-F: aggregazione per bet_id dei settlement dello stesso mercato | #521 | S | P1 | #537 |
| 13 | M020 | C1: nessun cap monetario fisso sopravvive (config.example + test di guardia sui default) | PR28-f | S | P1 | M019 |
| 14 | M021 | F04: drawdown con perdita non realizzata | PR28-f | S | P1 | M019 |
| 15 | M022 | F05 + F014 + F11: bankroll non finito, stop di sessione senza baseline, scope warning-only | PR28-f | S | P1 | M019 |
| 16 | M030 | Saldi, picco e drawdown separati per modalita' | PR15 | M | P1 | M019 |
| 17 | M031 | Contatori/stato persistenti per modalita' + emergency snapshot | PR15 | M | P1 | M030 |
| 18 | M032 | marketBaseRate LIVE + commission_pct solo verificata (t80u 3) | PR19 | M | P1 | #537 |
| 19 | M033 | F1 + F5: policy commissione e dedupe settlement prima del publish | PR19 | S | P1 | M032 |
| 20 | M034 | VOIDED e resettlement (OD12-08) | PR19 | M | P1 | M033 |
| 21 | M035 | Poller settlement attivo quando configurato, riconciliazione al riavvio | F017 | S | P1 | M034 |
| 22 | M098 | AUD-05: ledger del cashout | PR45-a | S | P1 | M097 |
| 23 | M015 | L05-F4 + L05-F5: auto-close LAY su liability; FAILED post-invio senza table_id non lascia ACTIVE | PR29-e | S | P2 | #537 |
| 24 | M017 | Conferma step del cycle executor legata al verdetto dell'engine | #519 | S | P2 | #537 |
| 25 | M018 | Book SIM malformato = blocco pre-invio | #520 | S | P2 | #537 |
| 26 | M004 | L07-F1: recovery fallita all'avvio => nessun ACTIVE | PR31-a | S | P0 | #537 |
| 27 | M002 | L08-F3: fencing di riconciliazione mai fail-open | PR32-a | S | P1 | #537 |
| 28 | M003 | L08-F2: fencing token persistito e confrontato nel SQL | PR32-a | M | P1 | M002 |
| 29 | M006 | PF-ALL-F004: consumatore di RECOVER_ORDER | PR31-a | S | P1 | M004 |
| 30 | M023 | Migrazioni DB versionate (schema_version, additive, rollback) | PR33 | M | P1 | #537 |
| 31 | M025 | #528: harness F026 deterministica (niente attese fisse) | PR34 | S | P1 | #537 |
| 32 | M028 | Dipendenza cryptography vincolata + primitiva AEAD (merge owner, manifest) | PR35 | S | P1 | #537 |
| 33 | M029 | Migrazione non distruttiva dei segreti + checklist ACL Windows | PR35 | M | P1 | M028 |
| 34 | M047 | Login SIM con App Key Delayed (P31) | PR11-a+PR16 | S | P1 | #537 |
| 35 | M001 | Contratto SIM/HYBRID_REAL_EXECUTION/LIVE + stake EXPLICIT/FIXED/MM | DOC-MODES | S | P2 | #537 |
| 36 | M024 | Hard-verify del delta c8fcaa3 (#446) | PR33 | S | P2 | M023 |
| 37 | M007 | AUD-01: ordini ghost nei cap di mercato/evento | PR31-a | S | P1 | M004 |
| 38 | M026 | Crash drill su SQLite reale (PF-C28-01..03, F4 durabilita') | PR34 | M | P1 | M023, M025 |
| 39 | M027 | STOP persistente e riavvio (OD12-07, PF-EXEC-06, F07 E2E) | PR34 | M | P0 | M026, M010 |
| 40 | M045 | OP-VIS + F022: segnalazione operatore (posizioni ricostruite, ghost, startup BLOCKED; campo stop in get_status) | PR47 | S | P1 | M010 |
| 41 | M065 | L05-F6: OrderManager cablato o percorso latente ritirato | PR30-a | M | P1 | #537 |
| 42 | M066 | FSM replace con AMBIGUOUS senza retry cieco | PR30-a | M | P1 | M065 |
| 43 | M099 | AUD-03 + AUD-02: esito incerto dell'hedge = AMBIGUOUS + settlement senza betId | PR45-b | M | P0 | M098, M066 |
| 44 | M068 | F011 + F025: instructionReports top-level; SUCCESS senza betId => AMBIGUOUS | PR30-a | S | P1 | M065 |
| 45 | M100 | #516: un solo cashout attivo per posizione | PR45-b | S | P1 | M099 |
| 46 | M101 | F06: fencing cashout per ambito evento | PR45-b | S | P1 | M100 |
| 47 | M005 | Decision log di riconciliazione persistito | PR31-a | M | P2 | M004 |
| 48 | M067 | F003: produttore degli eventi del ciclo ordine | PR30-a | S | P2 | M065 |
| 49 | M069 | customer_ref di manual_bet + parita' SIM LAPSED | PR30-a | S | P2 | M065 |
| 50 | M102 | Parita' GUI/headless del cashout (test) | PR45-b | S | P2 | M101 |
| 51 | M049 | Freshness/timestamp Market Stream; dati stantii => nessun ingresso (PF-EXEC-05) | PR11-b | M | P0 | M047 |
| 52 | M050 | Reconnect, heartbeat, clock | PR11-b | M | P1 | M049 |
| 53 | M051 | Endpoint Italia (.it) e configurazione feed | PR11-b | S | P1 | M050 |
| 54 | M060 | Order Stream: connessione, auth, heartbeat, reconnect (trasporto condiviso) | ORDER-STREAM-a | M | P1 | M050 |
| 55 | M061 | Order Stream: cache ordini (oc/orc/uo), clk/initialClk | ORDER-STREAM-a | M | P1 | M060 |
| 56 | M062 | Order Stream: cablaggio runtime in sola lettura | ORDER-STREAM-a | S | P1 | M061 |
| 57 | M063 | Matrice autorita' REST/stream per fill e cancel | ORDER-STREAM-b/RECON-02 | M | P1 | M062, M004 |
| 58 | M064 | Divergenza/disconnessione stream => riconciliazione fail-closed | ORDER-STREAM-b/RECON-02 | M | P0 | M063 |
| 59 | M106 | Resolver neutro per nome cablato | PR08 | S | P1 | #537 |
| 60 | M052 | F7 + F3: livello svuotato non abbina l'intero ordine | PR11-c | S | P2 | M009 |
| 61 | M053 | F9: ordine a riposo si riabbina col book | PR11-c | S | P2 | M052 |
| 62 | M054 | listEventTypes/Competitions/Countries/MarketTypes/Venues/TimeRanges | REST-DISC-a | M | P2 | #537 |
| 63 | M055 | listRunnerBook | REST-DISC-b | S | P2 | M054 |
| 64 | M056 | listMarketProfitAndLoss (fonte P/L autorevole) | REST-DISC-b | S | P2 | M055 |
| 65 | M057 | getAccountDetails + listCurrencyRates | NEW-ACCOUNT | S | P2 | #537 |
| 66 | M059 | updateOrders via autorita' ordini | NEW-UPDATEORDERS | S | P2 | M066 |
| 67 | M107 | Catalogo incrementale (delta, no-wipe) | PR08 | M | P2 | M106 |
| 68 | M048 | Logout lato server (#343) | PR11-a+PR16 | S | P3 | M047 |
| 69 | M058 | getAccountStatement paginato | NEW-ACCOUNT | S | P3 | M057 |
| 70 | M081 | F01: direzione SL/TP e prezzo di riferimento | PR24 | S | P0 | M085 |
| 71 | M036 | F03/F032: EXPANSION su bankroll_start persistito e profitti consolidati | PR22 | M | P1 | M034 |
| 72 | M070 | R1-R4: preflight tutto-o-niente del lotto + riserva esposizione (corsia dormiente) | PR30-b | M | P1 | M066 |
| 73 | M071 | Cablaggio batch_manager + table_id dei lotti + finalize_terminal_batch | PR30-b | M | P1 | M070 |
| 74 | M072 | PF-EXEC-02: lotto parziale ribilanciato/coperto/sospeso | PR30-b | M | P1 | M071 |
| 75 | M073 | PF-EXEC-01: ripuntare il solo residuo dopo cancel confermato | PR30-b | S | P1 | M072 |
| 76 | M079 | Stake EXPLICIT/FIXED/MM cablato (esclusivi, default da config, rifiuto se non valido) | PR12-a+PR23 | M | P1 | M019 |
| 77 | M080 | Precedenze messaggio/GUI/MM e formula MM con esempi numerici | PR12-a+PR23 | S | P1 | M079 |
| 78 | M082 | SL% su perdita netta liquidabile (U15HT) | PR24 | S | P1 | M081 |
| 79 | M089 | P35: intento durevole (attesa, nessuna sostituzione, scadenza a chiusura mercato) | PR25 | M | P1 | M082, M088 |
| 80 | M090 | Trailing persistente | PR25 | S | P1 | M089 |
| 81 | M091 | Auto-cashout MM | PR25 | S | P1 | M090 |
| 82 | M092 | Cashout per target | PR43-b | S | P1 | M091 |
| 83 | M093 | Netting BACK/LAY | PR43-b | M | P1 | M092 |
| 84 | M074 | F018 per lotto: un tavolo per lotto al rebuild | PR30-b | S | P2 | M071, M010 |
| 85 | M075 | Publisher Dutching e budget equo | PR09 | M | P2 | M074 |
| 86 | M076 | CASHOUT Esegui per Dutching | PR09 | S | P2 | M075, M085 |
| 87 | M077 | Suite E2E 72 casi #511 (sola prova) | PR09 | M | P1 | M076 |
| 88 | M078 | Attivazione corsia Dutching (rimozione dormienza) solo con gate PASS | PR09 | S | P1 | M077 |
| 89 | M083 | F002: registro TTL DIRECT popolato (OFF di default) | PR38-a | S | P2 | #537 |
| 90 | M084 | TTL-PERSIST: registro TTL persistito al riavvio | PR38-a | S | P2 | M083, M023 |
| 91 | M086 | Miglior prezzo dinamico solo sull'unmatched con limiti verificabili | PR38-b | M | P2 | M066, M081 |
| 92 | M087 | Uscite protettive non ritardate (PF-EXEC-03) | PR38-b | S | P1 | M086 |
| 93 | M094 | Void = hedge (P39) | PR43-b | S | P2 | M093 |
| 94 | M095 | Cashout fraction/percentage (DEC-C44-03) | PR44 | M | P1 | M094 |
| 95 | M096 | #517: minimo effettivo verificato al posto del floor 0,10 | PR44 | S | P2 | M095 |
| 96 | M039 | F009: escalation anomalie con azione reale | PR20 | S | P1 | #537 |
| 97 | M040 | F016: SafeMode e kill switch alimentati | PR20 | S | P1 | M039 |
| 98 | M041 | F007-a: market_validator cablato nel percorso ordini | F007 | S | P1 | M040 |
| 99 | M103 | Control API: server locale, auth, handshake e versione | PF-API-0+PR36 | M | P1 | M029, M004 |
| 100 | M104 | Correlation id request_id->operation_id->customer_ref + audit | PF-API-0+PR36 | S | P1 | M103 |
| 101 | M108 | Route lettura: stato, posizioni, ordini | PF-API-1 | M | P1 | M104 |
| 102 | M109 | Route lettura: mercati, resolver, fondi | PF-API-1 | S | P1 | M108, M107 |
| 103 | M037 | Shutdown una volta su errore main + HeadlessApp.stop ferma EventBus (F021) | PR17 | S | P2 | #537 |
| 104 | M038 | F010: ordinamento EventBus e prova drain/backpressure | PR46 | S | P2 | M037 |
| 105 | M042 | F007-b: auto_throttle cablato o ritirato con prova di ridondanza | F007 | S | P2 | M041 |
| 106 | M043 | F007-c: ai_guardrail cablato o ritirato con prova | F007 | S | P2 | M041 |
| 107 | M044 | F005/F006: watchdog con liveness reale e heartbeat corretto | PR47 | S | P2 | M038 |
| 108 | M046 | OD12-11: storico verificabile | PR47 | M | P2 | M045 |
| 109 | M105 | Ritiro di betfair_market_api.py legacy | PF-API-0+PR36 | S | P2 | M103 |
| 110 | M110 | Route lettura: Market P/L e stato Stream (OWN-MCP-02F) | PF-API-1 | S | P2 | M109, M056, M062 |
| 111 | M111 | place_order (EXPLICIT/FIXED/MM, idempotente) | PF-API-cmd-a | M | P1 | M110, M079 |
| 112 | M112 | cancel/replace/update | PF-API-cmd-a | M | P1 | M111, M059 |
| 113 | M113 | Preview senza effetti (PR37) | PF-API-cmd-b+PR37 | S | P1 | M112 |
| 114 | M114 | STOP/resume con barriera fisica | PF-API-cmd-b+PR37 | S | P0 | M113 |
| 115 | M115 | reconcile e cashout via API | PF-API-cmd-b+PR37 | M | P1 | M114, M102 |
| 116 | M116 | MANUAL a doppia conferma (OWN-002), principal automation distinto | PR12-b | M | P1 | M115 |
| 117 | M123 | Storico: writer e coordinator LIVE+SIM | PR14 | M | P2 | M031 |
| 118 | M129 | CI: autorizzazioni da fonte fidata + merge markers (#345) | PR49 | S | P2 | #537 |
| 119 | M130 | CI: collection/workflow_run e policy reviewer | PR50 | S | P2 | M129 |
| 120 | M133 | Enforcement esclusione auto-merge (identita' agente, ruleset) | #472 | S | P2 | M129 |
| 121 | M135 | Validatore tracciabilita' exit 0 e denominatore | GateA-a | S | P2 | #537 |
| 122 | M136 | Manifest requisiti core | GateA-b | M | P2 | M135 |
| 123 | M137 | Tracciabilita' Roserpina multi-tavolo (#510) | GateA-b | M | P2 | M136 |
| 124 | M117 | HYBRID_REAL_EXECUTION: identita' e stato distinti, DISABILITATA | HYBRID | M | P3 | M116, M031 |
| 125 | M127 | Ritiro percorsi XTrader legacy (callgraph) | PR48 | M | P3 | M065 |
| 126 | M128 | F015/F020: codice finanziario duplicato o morto | PR48 | M | P3 | M127 |
| 127 | M131 | Tooling pfckpt/pf_trace | #518 | S | P3 | #537 |
| 128 | M132 | Strumenti invarianti G1-G7 (manifest/workflow a merge owner) | FORMAL-VERIF | M | P3 | M131 |
| 129 | M134 | Motore di sblocco autonomo | #515 | M | P3 | M133 |
| 130 | M151 | Parser neutrale deterministico (senza Telegram) | PR06 | M | P3 | #537 |
| 131 | M152 | PF-CAP-08/13/18: nessuno stake o quota inventati | PR06 | S | P2 | M151 |
| 132 | M153 | Rimozione parser regex dopo test di equivalenza | PR10 | M | P3 | M152 |
| 133 | M118 | Catalogo GUI: contratto worker/on_success (#415) | PR07 | S | P2 | #537 |
| 134 | M119 | GUI: scrollbar e DPI comuni | PR05 | M | P2 | M118 |
| 135 | M120 | GUI: controlli safety visibili per tab (57/129) | PR05 | M | P2 | M119 |
| 136 | M121 | Pannello osservabilita' unica | PR13 | M | P2 | M045, M120 |
| 137 | M122 | Safe Mode nella GUI + console readiness (#359) | PR13 | S | P2 | M121 |
| 138 | M125 | Configurazione GUI dei 41 parametri Roserpina | #511 | M | P2 | M079, M120 |
| 139 | M126 | Simulazione combinatoria Roserpina-41 (test) | #511 | M | P2 | M125 |
| 140 | M124 | Storico: pagine GUI | PR14 | M | P3 | M123 |
| 141 | M138 | Backup SQLite cifrato | PR51 | M | P1 | M029 |
| 142 | M139 | Scheduler e retention backup (12 h, 2 copie) | PR51 | S | P1 | M138 |
| 143 | M140 | Restore drill isolato; errori disco/WAL/permessi | PR52 | M | P1 | M139 |
| 144 | M141 | Pacchetto Linux (AppImage) + dipendenze dichiarate (F012) | PR53 | M | P1 | M140 |
| 145 | M142 | Pacchetto Windows (exe) | PR53 | M | P1 | M141 |
| 146 | M143 | Supervisore H24 Linux (systemd) (F008) | PR54 | S | P1 | M141 |
| 147 | M144 | Supervisore Windows (NSSM) + parita' segnali | PR54 | S | P1 | M142, M143 |
| 148 | M145 | VERSION, pipeline Release, manifest verificabile | PR55 | M | P1 | M144 |
| 149 | M149 | Accettazione whole-wiring sul runtime vero | PR59 | M | P0 | M145 |
| 150 | M150 | Sweep #380 e dossier di collaudo | PR60 | M | P1 | M149 |
| 151 | M146 | Version checker, download con hash | PR56 | M | P3 | M145 |
| 152 | M147 | Safe-state e updater separato | PR57 | M | P3 | M146 |
| 153 | M148 | Sostituzione, health, rollback | PR58 | M | P3 | M147 |
# Microstake nativo — Smart Execution sotto il minimo exchange

Decisione owner: **PF-MICROSTAKE-NATIVE-AUTO-01** (#426, commento 6210870965, 16/10/2026). Mandato tecnico:
**PF-MICROSTAKE-NATIVE-INTEGRATION-02** (ripreso dal checkpoint Codex `PF-MULTIAGENT-HANDOFF-SNAPSHOT-01`).
Stato: SIM verificata offline; **adapter LIVE implementato e certificato offline** (PF-MICROSTAKE-OWNER-FINAL-
RECTIFICATION-01, §4-bis), **non verificato su Betfair reale**; LIVE resta NO-GO fino all'autorizzazione owner.

## 1. Regola di prodotto

Rettifica owner **PF-MICROSTAKE-OWNER-RECTIFICATION-20261017** (#426 c.6235508131): il Money Management determina e
richiede lo stake calcolato (esempi owner 0,10 · 0,20 · 0,30 · 0,50 · 1,00 · 1,50 · 2,00 · 5,00 EUR); il Microstake
lo esegue **esattamente** o fallisce con errore esplicito; nessun minimo artificiale, nessun arrotondamento verso
l'alto, nessun cap fisso da 1 o 2 EUR. Grandezze tenute separate: stake richiesto, stake eseguito, importo temporaneo
della procedura, esposizione matched/unmatched, liability LAY, fondi disponibili, controlli di rischio.
`RoserpinaMoneyManagement.calculate` non alza più lo stake al minimo configurato: sotto `min_stake` rifiuta con
`stake_sotto_min_stake` e lo stake richiesto in chiaro (`tests/core/test_money_management_rectification_20261017.py`).
Decisione owner 2 (PF-PR-RECOVERY-DECISIONS-02, 17/10/2026): sopra il tetto del singolo ordine (`max_single_bet`, BACK
stake / LAY liability) lo stake calcolato **non viene più ridotto al cap**: rifiuto esplicito `supera_max_single_bet` con
lo stake richiesto (`requested_stake`) e il cap nei metadati (`tests/core/test_money_management_decision2_20261017.py`).
Conseguenza dichiarata: un passo di recovery Roserpina il cui stake supera il cap viene rifiutato invece di essere
eseguito ridotto (prima: clamp e approvazione); il ciclo si ferma sul rifiuto del Money Management.

Il Microstake non è una flag, un checkbox o un `MIN_STAKE`: è il percorso che il motore sceglie **da solo** quando
l'importo economico richiesto è sotto il minimo del singolo ordine dell'exchange (fonte unica
`core.stake_precision.EXCHANGE_MIN_ORDER`, **riferimento owner 1 EUR**, PF-MICROSTAKE-OWNER-FINAL-RECTIFICATION-01:
non più 2 EUR). L'utente indica
solo l'importo. Se la sequenza sicura non esiste, l'ordine **fallisce esplicitamente**: mai un ordine forzato, mai un
importo alzato per superare il minimo, mai un bypass di RiskGate, cap, idempotenza, persistenza o riconciliazione.

## 1-bis. Precisione dello stake (PF-STAKE-PRECISION-MICROSTAKE-01 e -FINAL-01, owner 24/10/2026)

Lo stake inviato è lo stake economico **per difetto al centesimo** (`core/stake_precision.normalize_stake`,
`ROUND_DOWN`), mai più alto, nessun minimo nuovo, **nessuna tolleranza**: 1,9999999999 è 1,99.

| Stake economico | Inviato |
| --- | --- |
| 0,10 | 0,10 |
| 0,10599 | 0,10 |
| 1,96476 | 1,96 |
| 1,9999999999 | 1,99 |
| 2,00 | 2,00 |
| 3,08595 | 3,08 |
| 10,99999 | 10,99 |

Difetto corretto, emerso dalla sessione integrata della Fase 4.

- Dopo un settlement il money management chiedeva `1.9647599999999998`. Il Microstake lo bloccava
  (`invalid_size_precision`), quindi ogni ingresso Roserpina sotto il minimo falliva.
- Sopra il minimo l'exchange riceveva `3.08595`.

Dove si applica:

- **Money management, alla fonte** (`RoserpinaMoneyManagement.calculate`). Lo stesso punto copre segnali Roserpina,
  stake fisso del segnale (Telegram) e passo successivo / recovery (`evaluate_next_trade_after_settlement`).
  - La formula è la stessa di prima, calcolata in aritmetica razionale esatta (`Fraction`) sugli operandi originali
    letti dalla loro forma decimale (`_base_stake_decimal`): un solo troncamento finale, nessun arrotondamento
    intermedio (una divisione periodica come 1,25 / 11 non perde il centesimo, rilievo Codex P2 su #560). Così
    l'errore binario del float si corregge alla fonte. Esempio: con bankroll 3,333…, in float il 3% dà 0,0999…;
    sugli operandi esatti dà 0,100000000000000005, quindi 0,10.
  - Il float poteva anche arrotondare in su di nascosto: con bankroll 23,333333333333332 dava 0,7, mentre il valore
    esatto è 0,6999…, quindi 0,69.
  - Minimo, cap e liability LAY si valutano sull'importo troncato.
  - Lo stake calcolato resta nei metadati: `requested_stake`, `stake_calcolato`, `stake_normalizzato`,
    `motivo_normalizzazione`.
- **Bet manuale** (`DutchingController.manual_bet`), prima di cap, tavolo e riserva: la riserva è l'ordine inviato.
- **Engine** (`TradingEngine._normalize_request`), come difesa del contratto e prima di risk gate, persistenza e
  scelta diretto/Microstake.
  - È idempotente.
  - Sotto un centesimo l'esito è `INVALID_REQUEST`.
  - Il registro `stake_precision` nel payload dell'ordine riporta `stake_calcolato`, `stake_normalizzato`,
    `motivo_normalizzazione` e `modalita_ordine` (`DIRETTO` / `MICROSTAKE`).
  - Il coordinatore Microstake rifiuta ancora da solo un importo non al centesimo (`invalid_size_precision`).
- **Invariati di proposito:**
  - **Dutching:** gambe già al centesimo (HALF_UP), somma uguale al totale; la normalizzazione è l'identità.
  - **Hedge del cashout:** al centesimo più vicino; non è uno stake d'ingresso e nessun ROUND_DOWN viene applicato.
    L'esposizione residua è entro mezzo centesimo per la quota di chiusura.

Cashout di una posizione Microstake, difetto preesistente scoperto qui. L'ordine economico (nuovo betId del
REPLACE) è nel ledger SIM senza `event_name`, perché la saga piazza via porta SIM. La deduplica per `bet_id` di
`Database.get_bot_active_orders` scartava la riga `orders` con la partita, così un CASHOUT della partita trovava
`positions: 0`. Ora l'`event_name` vuoto si completa con quello dello stesso bet e mercato; l'identità resta il
`bet_id`.

Dutching, comportamento preesistente da decidere a parte (non modificato). `_initial_dutching_stakes` alza una gamba
sotto 0,10 a 0,10, ma l'ultima gamba assorbe la differenza e l'equalizzazione riequilibra:

- nei casi provati la somma resta il totale;
- una gamba può uscire sotto 0,10 (es. 1,00 su quote 1,5/30 → 0,95 + 0,05);
- quella gamba fallisce al minimo del Microstake e il lotto diventa parziale (PF-EXEC-02).

Rifiutare il lotto in anticipo è una decisione owner aperta.

Prove:

- `tests/core/test_stake_precision_microstake_01.py`: contratto, MM, catena reale SIM, engine, saga nativa, bet
  manuale, concorrenza, payload LIVE intercettato, Dutching e hedge invariati, identità del cashout;
- `tests/f026/test_stake_precision_e2e.py`: HeadlessApp reale; dopo un settlement, Roserpina 0,10, crash dopo
  l'invio, parziale, cashout Microstake.

## 2. Dove agisce (choke point unici)

| Percorso | Punto di ingresso | Comportamento sotto il minimo (1 EUR) |
| --- | --- | --- |
| Quick Bet manuale/automatica, Dutching FT/HT (ogni gamba), Roserpina multi-tavolo, Money Management | `TradingEngine._route_native_microstake` (prima del broker diretto, in SIM, in LIVE e nel percorso senza runtime) | esecutore nativo o `MICROSTAKE_EXECUTOR_UNAVAILABLE` |
| Hedge del cashout (totale/parziale) | `core/order_router.OrderRouter.place` (catena `cashout_wiring`, identica per headless e GUI) | esecutore nativo dell'engine o `MICROSTAKE_EXECUTOR_UNAVAILABLE` |

Nessun altro punto del codice piazza ordini (verificato: `order_manager`/`order_router`/`betfair_service.place_order`
sono raggiunti solo dall'engine o dal router). Importi >= 1 EUR restano sul percorso diretto, invariato. Le gambe del
Dutching, gli ingressi Roserpina e Telegram e la Quick Bet arrivano tutti all'engine come `CMD_QUICK_BET`: ogni
ordine sceglie il percorso sul proprio importo, in SIM e in LIVE (`test_mixed_dutching_batch_routes_each_leg_...`).

## 3. Sequenza e invarianti (SIM e LIVE, stesso core)

`core/microstake_safe.py` — planner `SmartExecutionEngine` (DIRECT / MICROSTAKE / BLOCKED) e saga
`MicrostakeCoordinator`:

1. **prova del book**: serve un prezzo di stub **passivo** (che non incrocia la controparte); senza prova => BLOCKED
   (`passive_stub_price_unverified`). Gli stub storici BACK@1.01 / LAY@1000 non sono mai usati;
2. **esposizione temporanea** (PF-MICROSTAKE-OWNER-RECTIFICATION-20261017, #426 c.6235508131): i cap per singolo
   ordine (stake/MAX_EXPOSURE, RiskGate e money management) valgono per lo **stake richiesto**, non per l'importo
   nominale temporaneo della procedura (lo stub, pari al minimo exchange: 1 EUR) — nessun cap fisso 1/2 EUR sul
   Microstake. L'esposizione temporanea dello stub (BACK: 1 EUR; LAY: `1 × (stub − 1)`) resta però un rischio reale
   per pochi istanti e deve stare dentro
   ciò che è davvero disponibile: minimo fra fondi disponibili dell'exchange (servizio Betfair del runtime o, senza
   runtime, il broker SIM) e headroom dell'esposizione totale configurata (`max_total_exposure` sulla cassa
   verificata meno l'esposizione aperta e quella esterna, PFC-008; percentuale mancante o cassa non verificata ⇒
   cap non disponibile per un ingresso). **Eccezione PFC-008 (#548, mandato owner punto 5, PF-EXEC-03):** l'hedge
   del cashout (`protective`, impostato solo dall'`OrderRouter`; l'engine lo toglie dagli ingressi) non è una
   nuova entrata: l'headroom non lo limita mai e resta il solo limite dei fondi disponibili. I fondi disponibili sono una fonte **obbligatoria**: risposta assente, `ok=False` o
   senza `available` ⇒ cap non disponibile, nessuno stub (l'headroom locale da solo non basta; ciclo 4 #512,
   decisione owner 1 del 17/10). Nessuna fonte leggibile => BLOCKED (`transient_cap_unavailable`); uguale al limite
   consentito, oltre bloccato (owner #426 6141450045). Prova: `test_per_order_cap_below_the_stub_does_not_block_...`,
   `test_insufficient_available_funds_block_...`, `test_total_exposure_headroom_caps_...`;
3. **PLACE** dello stub (minimo exchange, 1 EUR) al prezzo passivo, riserva transitoria tenuta (rilasciata all'attivazione: dopo
   la saga i fondi dell'ordine economico li contabilizza l'exchange; resta tenuta solo in AMBIGUOUS);
4. **cancellazione parziale** `sizeReduction = 1 − importo` => residuo esatto = importo economico. L'importo deve
   essere rappresentabile in centesimi: più di due decimali => BLOCKED `invalid_size_precision` **prima** dell'invio
   (l'exchange arrotonderebbe, cioè eseguirebbe un importo diverso da quello richiesto). Un residuo del broker diverso
   dall'importo richiesto non diventa mai ACTIVE: AMBIGUOUS `unexpected_residual_size`, riconciliazione (rilievo
   Codex #512, PF-PR-RECOVERY-DECISIONS-02);
5. **REPLACE** al prezzo target **solo se diverso** dallo stub (Betfair crea un **nuovo betId**: la saga persiste la
   nuova identità e smette di riferirsi alla vecchia). Senza controparte lo stub è già al target: identità invariata;
6. stub abbinato (in tutto o in parte), risposta persa o **qualsiasi eccezione del broker dopo l'invio** (timeout,
   rete: solo i rifiuti pre-invio `INVALID_*` provano che nulla è partito), cancel/replace di esito ignoto =>
   **AMBIGUOUS** con riserva
   tenuta e `order_unknown=True` verso l'engine (riconciliazione, **mai retry**: un ref AMBIGUOUS risponde
   `DUPLICATE_BLOCKED`);
7. replace rifiutato => rollback (cancel del residuo) dall'identità persistita, mai per scansione di prezzo.

Blocco **pre-invio** (`pre_send=True` nel risultato): calcolato **per `customer_ref`** (nessuna `PLACE` di questa
saga nel log operazioni dell'adapter), non dalla lunghezza globale del log: una saga concorrente che invia il proprio
stub nello stesso istante non maschera il blocco (rilievo Grok #512). Solo con `pre_send=True` l'engine rilascia
l'intento dedupe; AMBIGUOUS e rollback dopo PLACE restano `pre_send=False`.

Commissione: 4,5% sul solo profitto netto positivo del mercato (`market_commission`), invariata.

## 4. Adapter SIM (`core/microstake_sim_port.py`)

- `SimulationExchangePort`: la saga sopra il **SimulationBroker di produzione** usando solo operazioni pubbliche:
  `place_bet`, `cancel_orders` con `sizeReduction` (nuovo, semantica Betfair: riduzione >= residuo cancella tutto il
  residuo; non finita/<= 0 => FAILURE senza toccare lo stato), `replace_orders` (nuovo betId), `get_current_orders`
  per la riconciliazione per `customerOrderRef`. Cancellazioni persistite (`save_simulation_bet`).
- `SimulationQuoteProvider`: in SIM l'exchange è il broker; senza controparte sul book nulla può incrociare e lo
  stub si appoggia al target; con controparte: target se passivo, altrimenti primo tick della scala Betfair oltre il
  best (`best_counterparty_price`, lettura pubblica coerente con `_match_order`).
- `DbSagaStore`: saga nel DB dell'app (chiave settings `microstake.sagas`), riavvio-sicura; JSON corrotto =>
  `SagaStoreError` (AMBIGUOUS se l'exchange è già stato toccato, altrimenti BLOCKED). Qualunque altro errore di
  scrittura (es. `sqlite3.OperationalError`) DOPO un'operazione exchange è AMBIGUOUS
  (`saga_store_write_failed_after_exchange_call`): riserva trattenuta, intento dedupe consumato, ordine contato come
  posizione fino alla riconciliazione, mai FAILED; prima dell'invio resta un fallimento (Codex r4311806874). Ordine
  accettato dal broker con lettura successiva fallita (`SimulationExchangePort.place`): `LostResponse`, quindi
  riconciliazione per `customer_ref`, mai riserva rilasciata con lo stub vivo (riesame indipendente ciclo 12). Un
  ordine gia' presente sul broker con lo stesso `customer_ref` (stato SIM ripristinato senza la saga) e' AMBIGUOUS
  `existing_customer_ref` con riserva trattenuta, mai FAILED/pre-invio e mai toccato, nemmeno da `recover()` (saga
  `foreign_ref`: nessuna riduzione/replace/cancel, Codex r4312719704 e riesame indipendente ciclo 14). Stessa scelta di persistenza
  di RSR-12 (`roserpina.recovery_tables_state`): nessuna modifica a `database_schema`.
- Riserva transitoria **in-process** (lock); la riserva durevole cross-process è PR29 (#461), fuori scope.
- Blocchi PRIMA di qualunque operazione exchange (`pre_send: True` nel risultato) diventano nell'engine un errore
  pre-invio tipizzato: l'intento dedupe viene rilasciato e un nuovo tentativo a condizione passata non è un duplicato;
  esiti dopo l'invio restano AMBIGUOUS/FAILED con l'intento consumato. Il broker SIM salva lo snapshot di reload
  (`persist_snapshot`) dopo ogni operazione della saga.
- Costruzione: `TradingEngine.native_microstake_executor()` crea l'esecutore una sola volta dal broker SIM
  (costruzione e rebinding serializzati da `_microstake_executor_lock`: due prime chiamate concorrenti,
  Quick Bet + hedge del cashout, ottengono LO STESSO esecutore, una sola mappa di riserve e un solo store;
  rilievo Codex #512 r4304867227, ciclo 6)
  riconosciuto per identità/classe (`_e_il_broker_di_simulazione`, `isinstance(SimulationBroker)`), mai da un client
  reale; headless e GUI non hanno bisogno di cablaggi aggiuntivi per il choke point, e passano lo stesso esecutore alla
  catena del cashout (`cabla_catena_cashout(..., microstake_executor_provider=...)`).

Esecutore e sessione: l'esecutore nativo in cache è valido solo per il broker SIM e la modalità correnti
(`_bound_native_microstake_executor`): dopo uno stop/start (nuovo `SimulationBroker`) viene ricostruito; in LIVE
l'adapter SIM non è mai riusato (None ⇒ `MICROSTAKE_EXECUTOR_UNAVAILABLE`). Uno store della saga illeggibile o non
scrivibile **prima** dell'invio rilascia la riserva transitoria e conta come blocco pre-invio (ciclo 4 #512).

## 4-bis. Adapter LIVE (`core/microstake_live_port.py`, PF-MICROSTAKE-OWNER-FINAL-RECTIFICATION-01)

Stessa saga, stesso esecutore (`NativeLiveMicrostakeExecutor` estende l'esecutore SIM), solo la porta cambia.
**Esecuzione diretta sotto il minimo: non supportata** dall'exchange (un `placeOrders` sotto il minimo è rifiutato con
`INVALID_BET_SIZE`), quindi la procedura è necessaria; le operazioni usate sono API pubbliche Betfair documentate.

- `LiveExchangePort` sopra il `BetfairClient` reale:
  - `placeOrders` dello stub = minimo exchange (1 EUR) a un prezzo che non si abbina (**BACK 1000, LAY 1.01**), con
    `customerOrderRef` = `customer_ref` della saga (nuovo parametro `BetfairClient.place_bet(customer_order_ref=...)`:
    non conforme ⇒ rifiuto pre-invio `INVALID_CUSTOMER_ORDER_REF`). Diversamente dalla SIM lo stub non si appoggia
    mai al target: in LIVE il book cambia fra lettura e invio e uno stub abbinato sarebbe 1 EUR di esposizione reale;
  - `cancelOrders` con `sizeReduction` (nuovo `BetfairClient.reduce_order`, **single-shot**: mai ripetuto, ridurrebbe
    due volte; argomenti invalidi ⇒ rifiuto pre-invio);
  - `replaceOrders` al target: nuovo betId con lo stesso residuo; `listCurrentOrders` per leggere e riconciliare
    (`sizeRemaining` autorevole; uno stub cancellato con zero abbinato non è mai una posizione).
- Esiti: rifiuto esplicito di Betfair (fondi, importo, mercato) ⇒ FAILED, nessun ordine, riserva rilasciata, intento
  dedupe consumato (nessun nuovo invio automatico); qualunque esito non dimostrato (timeout, `DUPLICATE_TRANSACTION`,
  risposta illeggibile, accettato senza betId o non rileggibile) ⇒ `LostResponse` ⇒ riconciliazione per
  `customerOrderRef` o AMBIGUOUS con riserva tenuta, mai un secondo PLACE. Un `replaceOrders` è un rifiuto solo con
  status `FAILURE` esplicito (nessun nuovo ordine; la cancellazione già fatta da Betfair resta e il rollback non la
  ripete); `TIMEOUT`, status mancante o `SUCCESS` senza nuovo betId sono esito ignoto, mai un rollback.
- Lettura per betId (`LiveExchangePort.order`): ordini correnti e, se l'ordine non c'è più, il suo esito in
  `listClearedOrders`, letti TUTTI (lo stesso betId può essere CANCELLED per la quota cancellata e SETTLED per quella
  abbinata: l'abbinato è la somma dei SETTLED, rilievo GPT-6.1 Sol su #561; solo CANCELLED/LAPSED/VOIDED = nessuna
  esposizione); assente anche lì
  ⇒ identità non dimostrata, fail-closed.
- `LiveQuoteProvider`: `listMarketBook` con `EX_BEST_OFFERS`; mercato `OPEN` e selezione `ACTIVE` obbligatori;
  qualunque errore di lettura ⇒ nessuna prova ⇒ BLOCKED pre-invio (mai un'eccezione letta come esito ignoto). Uno stub
  che incrocerebbe il book (es. qualcuno offre a 1000) ⇒ BLOCKED `stub_price_would_cross_book`.
- Fondi e liability: stesso cap temporaneo della SIM (`TradingEngine._native_transient_cap`: fondi disponibili del
  servizio Betfair e headroom dell'esposizione totale); BACK stub 1 EUR, LAY stub liability `1 × 0,01`.
- Store separato `microstake.sagas.live`; recovery all'avvio dalla modalità corrente (§5).
- Doppia condizione nella saga (`MicrostakeCoordinator._mode_allowed`): policy LIVE (`live_adapter=True`) solo sopra una
  porta che si dichiara LIVE (`is_live_port`), policy SIM mai sopra la porta LIVE; altrimenti BLOCKED
  `adapter_mode_not_allowed` (anche in `recover`, senza toccare l'exchange).
- Engine (`_live_native_microstake_executor`): adapter costruito solo per il `BetfairClient` reale riconosciuto per
  classe (un doppio o il broker SIM non diventano mai l'exchange LIVE), uno per client, tenuto fuori dallo slot
  dell'esecutore SIM (mai riusato in SIMULATION). Raggiunto solo dal ramo LIVE dopo `is_live_allowed()` e con lo
  stesso circuit breaker degli invii diretti; l'hedge del cashout usa lo stesso esecutore via `OrderRouter`.

Certificazione offline: `tests/core/test_microstake_live_adapter.py` (93 test), `BetfairClient` reale con il solo
trasporto JSON-RPC sostituito da un exchange finto con stato: importi 0,10 · 0,20 · 0,30 · 0,50 · 0,90 (Microstake) e
1,00 · 1,50 · 2,00 · 5,00 (diretti), BACK e LAY; lotto Dutching misto; fondi insufficienti (cap e rifiuto
dell'exchange); book sospeso/illeggibile/incrociato; risposte perse su place, cancel e replace; crash del processo dopo
place, cancel e replace con recovery idempotente su riavvii ripetuti e nessun duplicato (casi A-D della decisione
owner, BACK e LAY); rollback con cancellazione non confermata in cinque forme (P1); store corrotto; due processi sullo
stesso DB; multi-tavolo con ordini Microstake e diretti; difese della porta e del coordinatore provate ciascuna da
sola; concorrenza fra ingresso e
hedge con il cap temporaneo; rollback su replace rifiutato; circuit breaker; gate LIVE; doppio del client; policy/porta
non coerenti; payload e rifiuti pre-invio del `BetfairClient`. Prova di logica e contratto, **non di accettazione
Betfair**.

## 5. Recovery all'avvio (`RuntimeController.start`, prima di F018)

`TradingEngine.recover_native_microstake_sagas()` riprende ogni saga non conclusa dalla sua identità persistita
(solo riduzione/replace mancanti, **mai un nuovo PLACE**; una saga salvata **ROLLBACK_PENDING** completa solo la
cancellazione del residuo → ROLLED_BACK; se la cancellazione fallisce il risultato è AMBIGUOUS con riserva tenuta ma la
fase persistita resta ROLLBACK_PENDING, così il recover successivo ritenta solo la cancellazione, mai un replace — cicli
8 e 9, rilievi Codex su 034af59 e Sol). La decisione di rollback è registrata anche come flag durevole
`rollback_intent`, che nessun salvataggio intermedio cancella: un recover che non trova l'ordine lascia la fase
ROLLBACK_PENDING (non AMBIGUOUS generico), e un residuo già cancellato con snapshot SIM non persistito si chiude
ROLLED_BACK al recover successivo senza una seconda cancellazione (riesame indipendente del ciclo 9). Poiché il broker
SIM non elenca più fra gli ordini correnti un ordine interamente cancellato, con rollback deciso la saga cerca l'ordine
anche per bet id persistito e accetta solo una riga terminale senza residuo; un ordine ancora vivo non visto resta
"non trovato" e la saga resta pendente (secondo riesame del ciclo 9). Una riga terminale con qualsiasi abbinato
(SETTLED, LAPSED, VOIDED, CANCELLED parziale, EXECUTION_COMPLETE) resta AMBIGUOUS con riserva tenuta, mai ROLLED_BACK;
lo stesso vale se lo stub si abbina anche in parte appena prima della cancellazione di rollback (`rollback_matched`,
ciclo 10): ROLLED_BACK diventerebbe FAILED nell'engine e libererebbe tavolo e chiave su uno stake reale. Una saga è riconciliata solo se l'esito è ACTIVE o ROLLED_BACK **e** lo store
durevole, riletto dopo il recover, non la elenca più come pendente (ciclo 9, rilievo Codex su 2c6aa09); una saga
pendente nello store ma assente dai risultati dell'esecutore resta pendente (prima del recover le riserve transitorie
delle saghe pendenti che le tengono sono ripristinate sulla porta, ciclo 10); ogni altro
esito (AMBIGUOUS, FAILED, BLOCKED, ERROR) resta pendente. Se resta una saga pendente, lo store è illeggibile o manca
l'esecutore (LIVE), la ricostruzione F018 è marcata non ok (`RECONCILIATION_REQUIRED:MICROSTAKE_SAGA_PENDING:...`)
e il runtime parte in **PAUSED**: nessun nuovo ingresso finché uno stub può essere ancora sull'exchange.
Store per modalità (RECTIFICATION-01): si riprendono le saghe della modalità corrente (`microstake.sagas` in SIM,
`microstake.sagas.live` in LIVE); le saghe pendenti dell'altra modalità non si possono riprendere (porta diversa) e
lasciano `ok` False (`LIVE_SAGAS_PENDING` / `SIM_SAGAS_PENDING`) finché una sessione di quella modalità non le
riconcilia: nessuna saga dimenticata, in particolare nessuno stub LIVE.
Chiusure del recovery (review indipendente della RECTIFICATION-01, comuni a SIM e LIVE):
- stub con betId noto, **mai sostituito** (flag durevole `replace_sent`, scritto prima del replace) e ormai a residuo
  zero senza abbinato (cancellato o lapsato dall'exchange) ⇒ ROLLED_BACK `stub_gone_unmatched`, nessuna esposizione;
  con un replace inviato no: il nuovo ordine potrebbe esistere e la saga resta pendente;
- ordine trovato al prezzo target con importo esatto (abbinato + residuo = stake richiesto), anche già abbinato ⇒
  ACTIVE: è il risultato richiesto, non uno "stub abbinato";
- saga FAILED senza ordine (rifiuto pre-invio o rifiuto esplicito) è terminale: un nuovo tentativo dello stesso
  intento riparte da capo invece di diventare AMBIGUOUS con la riserva tenuta per sempre;
- importo dello stub persistito nella saga (`stub_size`): una saga pendente creata con lo stub da 2 EUR (prima della
  RECTIFICATION-01, campo assente ⇒ 2,00) viene ridotta dallo stub che ha davvero piazzato.

**Recovery LIVE dopo crash o riavvio — decisione owner PF-561-LIVE-MICROSTAKE-P1-HARDENING-01 §4 (ratificata,
implementata in `MicrostakeCoordinator.recover`):** in LIVE il recovery **non completa mai** un vecchio ingresso dalla
sola saga persistita (PF-EXEC-06, PF-OD12: il segnale potrebbe non essere più valido). La differenza con la SIM è
intenzionale: in SIM la saga si completa come approvato con #512.
- **Caso A** (stub intero o ridotto, ingresso non completato): nessun replace al target; cancellazione dell'intero
  residuo, verificata rileggendo l'ordine; ROLLED_BACK solo a residuo zero.
- **Caso B** (ordine richiesto già sull'exchange: prezzo target e importo esatto, trovato per ref o per l'identità
  del replacement persistita): riconciliato ACTIVE, nessun secondo ordine, nessuna cancellazione della posizione.
- **Caso C** (non dimostrabile cosa esiste): AMBIGUOUS con riserva tenuta, nessun PLACE ne' replace, nuovi ingressi
  bloccati dal recovery non ok.
- **Caso D** (quota abbinata): residuo cancellato, abbinato conservato e contabilizzato (AMBIGUOUS `rollback_matched`
  con riserva tenuta), mai un rollback totale.
In più, in ogni modalità, con `is_live_allowed()` falso o uno STOP attivo il recovery non riprezza mai (rilievo Codex
P1 su #561).

**Rollback verificato (P1 del mandato PF-561, riprodotto e corretto):** `BetfairClient.cancel_orders` torna `ok=True`
anche con l'istruzione fallita (`PROCESSED_WITH_ERRORS`, report `FAILURE`, `TIMEOUT`, report assente o di un altro
betId). Prima la saga si chiudeva ROLLED_BACK con lo stub vivo e la riserva rilasciata. Ora:
- `LiveExchangePort.cancel_all_remaining` prende come prova la **rilettura** dell'ordine (correnti, poi
  `listClearedOrders`): residuo positivo ⇒ `CANCEL_NOT_CONFIRMED`; rilettura impossibile ⇒ esito ignoto; residuo già
  zero (anche dopo una risposta persa) ⇒ nessun secondo invio;
- la riduzione (`cancel_reduction`) è confermata solo dal report SUCCESS della propria istruzione; TIMEOUT, report
  assente o contraddittorio ⇒ esito ignoto, mai ripetuta;
- una riga di `listCurrentOrders` senza `sizeRemaining`/`sizeMatched` non è un residuo zero (`CURRENT_ORDER_INCOMPLETE`);
- **invarianti del coordinatore**, indipendenti dalla porta: ROLLED_BACK mai con residuo eseguibile
  (`rollback_residual_open`, ROLLBACK_PENDING con riserva tenuta) né con abbinato (`rollback_matched`); ACTIVE solo
  sull'importo richiesto esatto (`active_size_mismatch`); replacement più piccolo dello stake (stub abbinato durante il
  replace) mai ACTIVE (`replacement_size_mismatch`); identità del replacement persistita prima di ogni lettura.

`resume()` ritenta **entrambe** le riconciliazioni (saghe Microstake e F018) e diventa ACTIVE solo se entrambe
riescono (rilievo Codex #512: prima ritentava solo F018).

Snapshot del broker SIM: dopo ogni operazione della saga l'adapter chiama `SimulationBroker.persist_snapshot()`
(scrittura **sotto il lock** del broker, così due saghe concorrenti non pubblicano uno snapshot più vecchio dopo uno
più nuovo). Un fallimento non è silenzioso: contato in `port.snapshot_failures` e dichiarato nel risultato
dell'esecutore (`snapshot_persisted: false`); la saga persistita resta l'autorità e un riavvio senza stub
ricaricabile finisce AMBIGUOUS, mai un nuovo PLACE. Se a non persistersi è lo snapshot **finale** della saga
(ciclo 8, rilievo Codex su 034af59), la saga NON diventa ACTIVE: viene persistita **AMBIGUOUS** con riserva
tenuta (`reason: snapshot_not_persisted`), il risultato è `AMBIGUOUS`/`order_unknown` (nessun
`instructionReports`) e `recover()` la riconcilia dalla sua identità. `recover_pending()` ritenta prima lo
snapshot del broker: finché non riesce, nessuna saga pendente viene promossa (restano AMBIGUOUS).

## 6. Limiti dichiarati (non PASS)

- **LIVE: IMPLEMENTATO, NON VERIFICATO su Betfair reale** (nessun ordine reale, LIVE NO-GO). Da verificare nel
  collaudo autorizzato dall'owner, ciascuno con esito atteso fail-closed se diverso:
  1. minimo accettato dall'exchange italiano per un piazzamento: 1 EUR (riferimento owner). La pagina Betfair "Betting
     On Italian Exchange" indica ancora 2 EUR e multipli di 0,50; la tabella Currency Parameters più recente indica
     1 EUR. Se 1 EUR venisse rifiutato: rifiuto esplicito pre-accettazione (`INVALID_BET_SIZE`), FAILED, nessuna
     esposizione, mai un importo diverso;
  2. accettazione di `sizeReduction` fino a 0,10 (vincolo dei multipli di 0,50, se in vigore);
  3. `replaceOrders` conserva `customerOrderRef` sul nuovo ordine (se no: dopo una risposta persa del replace la saga
     resta pendente con riserva tenuta, fail-closed, `test_lost_replace_response_stays_pending_when_the_ref_...`);
  4. uno stub cancellato con zero abbinato sparisce da `listCurrentOrders` (gestito in entrambi i casi: dai correnti o
     dal suo esito in `listClearedOrders`); se l'esito tarda a comparire la saga resta pendente fino al riavvio
     successivo, mai chiusa per assenza di dati;
  5. accettazione di un LAY 1 EUR a 1.01 e di un BACK 1 EUR a 1000 sul mercato reale.
- Riserva transitoria in-process anche in LIVE: non è un'autorità finanziaria multi-processo (come in SIM).
- Preesistente (coordinatore, non introdotto qui): se lo stub si abbina in parte prima della riduzione
  (`stub_matched` / `match_cancel_race`) la sua parte non abbinata non viene cancellata in automatico; la saga resta
  AMBIGUOUS con riserva tenuta per la riconciliazione. Improbabile in LIVE (stub a 1000 / 1.01).
- Riserva temporanea in-process: due processi sullo stesso DB non condividono il cap temporaneo (la deduplica durevole
  degli intenti impedisce comunque due invii dello stesso ref; l'autorità esclusiva del runtime, #536, impedisce due
  runtime attivi). Riserva durevole cross-process: PR29 (#461), fuori scope.
- Riserva transitoria in-process (MS-NI-004 Codex): non è un'autorità finanziaria multi-processo.
- `customer_ref` del cashout = `source` del comando (non unico): il router genera un ref proprio per la saga
  (`raw["microstake_ref"]`); il cashout non ritenta mai un esito AMBIGUO. Mitigazione fail-closed (ciclo 4 #512):
  finché esiste una saga pendente sulla stessa posizione (mercato, selezione, lato) il router rifiuta ogni nuovo
  hedge nativo con `MICROSTAKE_PENDING_SAME_POSITION` (`pending_same_position`); store illeggibile ⇒ rifiuto. Un
  esito AMBIGUOUS conserva il `matched` noto nel risultato del router. Identità stabile del ref: decisione di
  design aperta (owner). Finding separato da riconciliare (ref
  non unico anche sul percorso diretto LIVE, de-dup Betfair a 60 s).
- `SimulationBroker.place_bet` accetta uno stake booleano come 1.0 (preesistente, MS-NI-007).
- Control API / MCP: assenti nella baseline, nessuna integrazione possibile qui.

## 6-bis. Audit degli usi di 2 EUR (RECTIFICATION-01, 24/10/2026)

| Dove | Ruolo | Esito |
| --- | --- | --- |
| `core/stake_precision.EXCHANGE_MIN_ORDER` | soglia di instradamento (fonte unica) | 1,00 (riferimento owner) |
| `core/trading_engine._route_native_microstake` (`>= 2.0`) | soglia di instradamento | legge la fonte unica |
| `core/order_router.EXCHANGE_MINIMUM_STAKE` | soglia di instradamento dell'hedge | `float(EXCHANGE_MIN_ORDER)` |
| `MicrostakePolicy.exchange_stub_size` | importo temporaneo della saga | = minimo exchange: il più piccolo piazzamento accettato, riservato come esposizione reale |
| `SmartExecutionEngine` / `NativeMicrostakeExecutor` (`exchange_minimum`) | soglia del planner | fonte unica |
| `core/safety_layer._validate_common_order_rules` (`allow_micro=False`) | validazione | fonte unica |
| `microstake_sim_port._recomputed_stub_exposure` (default `"2.00"`) | ricalcolo esposizione stub | legge la policy |
| `trading_config.MIN_STAKE` (commento "< €2 in live") | documentazione | corretto; il floor software resta 0,10 |
| commenti engine/runtime/saga ("stub da 2 EUR") | documentazione | "stub (minimo exchange)" |
| `dutching_state._tick_step`, `simulation_matching_engine._tick_size`, `price < 2.0` | scala dei prezzi Betfair (quote) | invariati: non è uno stake |
| `dutching.py` `/ Decimal("2")`, timeout/jitter `2.0` | aritmetica e tempi | invariati: non è uno stake |
| test con 2,00/1,99/1,50 come soglia | test | riallineati a 1,00/0,99 (elenco nel PR) |

## 7. Prove (eseguite su `c5e4422` + patch locale, offline)

`tests/test_microstake_native_integration.py`, `tests/test_microstake_safe_recovery.py` (Codex, 53),
`tests/test_microstake_sim_port.py` (adapter, 17, RED prima dell'adapter), `tests/test_microstake_engine_native.py`
(engine, 23), `tests/test_microstake_cashout_and_recovery.py` (cashout + recovery, 14),
`tests/core/test_money_management_rectification_20261017.py` (MM senza arrotondamento, 8),
`tests/integration/test_issue461_pr26a_customer_ref_provenance.py::test_pass_dutching_identity_ignores_order_sensitive_stake_rounding`
(RED nel checkpoint Codex → GREEN: la gamba Dutching da 1,91 EUR nasceva come stub da 2,00 ridotto di 0,09, stessa
identità, 3 ordini; con la soglia a 1 EUR di RECTIFICATION-01 la stessa gamba va diretta e il test resta verde).

Decisione successiva registrata, **non implementata**: PF-BEST-EXECUTION-DYNAMIC-01 (#426 c.6235560755) — miglior
prezzo dinamico sugli unmatched, da costruire sullo stesso motore (saga + port), lotto successivo.

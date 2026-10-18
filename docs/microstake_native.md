# Microstake nativo — Smart Execution sotto il minimo exchange

Decisione owner: **PF-MICROSTAKE-NATIVE-AUTO-01** (#426, commento 6210870965, 16/10/2026). Mandato tecnico:
**PF-MICROSTAKE-NATIVE-INTEGRATION-02** (ripreso dal checkpoint Codex `PF-MULTIAGENT-HANDOFF-SNAPSHOT-01`).
Stato: implementazione SIM verificata offline; **LIVE senza adapter (fail-closed)**; nessuna certificazione.

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
l'importo economico richiesto è sotto il minimo del singolo ordine dell'exchange (Betfair: 2 EUR). L'utente indica
solo l'importo. Se la sequenza sicura non esiste, l'ordine **fallisce esplicitamente**: mai un ordine forzato, mai un
importo alzato per superare il minimo, mai un bypass di RiskGate, cap, idempotenza, persistenza o riconciliazione.

## 2. Dove agisce (choke point unici)

| Percorso | Punto di ingresso | Comportamento sotto 2 EUR |
| --- | --- | --- |
| Quick Bet manuale/automatica, Dutching FT/HT (ogni gamba), Roserpina multi-tavolo, Money Management | `TradingEngine._route_native_microstake` (prima del broker diretto, in SIM, in LIVE e nel percorso senza runtime) | esecutore nativo o `MICROSTAKE_EXECUTOR_UNAVAILABLE` |
| Hedge del cashout (totale/parziale) | `core/order_router.OrderRouter.place` (catena `cashout_wiring`, identica per headless e GUI) | esecutore nativo dell'engine o `MICROSTAKE_EXECUTOR_UNAVAILABLE` |

Nessun altro punto del codice piazza ordini (verificato: `order_manager`/`order_router`/`betfair_service.place_order`
sono raggiunti solo dall'engine o dal router). Importi >= 2 EUR restano sul percorso diretto, invariato.

## 3. Sequenza e invarianti (SIM e futura LIVE, stesso core)

`core/microstake_safe.py` — planner `SmartExecutionEngine` (DIRECT / MICROSTAKE / BLOCKED) e saga
`MicrostakeCoordinator`:

1. **prova del book**: serve un prezzo di stub **passivo** (che non incrocia la controparte); senza prova => BLOCKED
   (`passive_stub_price_unverified`). Gli stub storici BACK@1.01 / LAY@1000 non sono mai usati;
2. **esposizione temporanea** (PF-MICROSTAKE-OWNER-RECTIFICATION-20261017, #426 c.6235508131): i cap per singolo
   ordine (stake/MAX_EXPOSURE, RiskGate e money management) valgono per lo **stake richiesto**, non per l'importo
   nominale temporaneo da 2 EUR della procedura — nessun cap fisso 1/2 EUR sul Microstake. L'esposizione temporanea
   dello stub (BACK: 2 EUR; LAY: `2 × (stub − 1)`) resta però un rischio reale per pochi istanti e deve stare dentro
   ciò che è davvero disponibile: minimo fra fondi disponibili dell'exchange (servizio Betfair del runtime o, senza
   runtime, il broker SIM) e headroom dell'esposizione totale configurata (`max_total_exposure` sul bankroll noto
   meno l'esposizione aperta). I fondi disponibili sono una fonte **obbligatoria**: risposta assente, `ok=False` o
   senza `available` ⇒ cap non disponibile, nessuno stub (l'headroom locale da solo non basta; ciclo 4 #512,
   decisione owner 1 del 17/10). Nessuna fonte leggibile => BLOCKED (`transient_cap_unavailable`); uguale al limite
   consentito, oltre bloccato (owner #426 6141450045). Prova: `test_per_order_cap_of_one_euro_no_longer_blocks_...`,
   `test_insufficient_available_funds_block_...`, `test_total_exposure_headroom_caps_...`;
3. **PLACE** dello stub da 2 EUR al prezzo passivo, riserva transitoria tenuta (rilasciata all'attivazione: dopo
   la saga i fondi dell'ordine economico li contabilizza l'exchange; resta tenuta solo in AMBIGUOUS);
4. **cancellazione parziale** `sizeReduction = 2 − importo` => residuo esatto = importo economico. L'importo deve
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
  `SagaStoreError` (AMBIGUOUS se l'exchange è già stato toccato, altrimenti BLOCKED). Stessa scelta di persistenza
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

## 5. Recovery all'avvio (`RuntimeController.start`, prima di F018)

`TradingEngine.recover_native_microstake_sagas()` riprende ogni saga non conclusa dalla sua identità persistita
(solo riduzione/replace mancanti, **mai un nuovo PLACE**; una saga salvata **ROLLBACK_PENDING** completa solo la
cancellazione del residuo → ROLLED_BACK; se la cancellazione fallisce il risultato è AMBIGUOUS con riserva tenuta ma la
fase persistita resta ROLLBACK_PENDING, così il recover successivo ritenta solo la cancellazione, mai un replace — cicli
8 e 9, rilievi Codex su 034af59 e Sol). Una saga è riconciliata solo se l'esito è ACTIVE o ROLLED_BACK **e** lo store
durevole, riletto dopo il recover, non la elenca più come pendente (ciclo 9, rilievo Codex su 2c6aa09); ogni altro
esito (AMBIGUOUS, FAILED, BLOCKED, ERROR) resta pendente. Se resta una saga pendente, lo store è illeggibile o manca
l'esecutore (LIVE), la ricostruzione F018 è marcata non ok (`RECONCILIATION_REQUIRED:MICROSTAKE_SAGA_PENDING:...`)
e il runtime parte in **PAUSED**: nessun nuovo ingresso finché uno stub da 2 EUR può essere ancora sull'exchange.
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

- **LIVE**: nessun adapter `BetfairClient` per la saga; in LIVE ogni importo sotto 2 EUR fallisce chiuso
  (`MICROSTAKE_EXECUTOR_UNAVAILABLE`), hedge del cashout compreso. L'accettazione Betfair della sequenza
  PLACE → cancel parziale → replace **non è dimostrata** qui: NOT VERIFIED.
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

## 7. Prove (eseguite su `c5e4422` + patch locale, offline)

`tests/test_microstake_native_integration.py`, `tests/test_microstake_safe_recovery.py` (Codex, 53),
`tests/test_microstake_sim_port.py` (adapter, 17, RED prima dell'adapter), `tests/test_microstake_engine_native.py`
(engine, 23), `tests/test_microstake_cashout_and_recovery.py` (cashout + recovery, 14),
`tests/core/test_money_management_rectification_20261017.py` (MM senza arrotondamento, 8),
`tests/integration/test_issue461_pr26a_customer_ref_provenance.py::test_pass_dutching_identity_ignores_order_sensitive_stake_rounding`
(RED nel checkpoint Codex → GREEN: la gamba Dutching da 1,91 EUR nasce come stub da 2,00 ridotto di 0,09, stessa
identità, 3 ordini).

Decisione successiva registrata, **non implementata**: PF-BEST-EXECUTION-DYNAMIC-01 (#426 c.6235560755) — miglior
prezzo dinamico sugli unmatched, da costruire sullo stesso motore (saga + port), lotto successivo.

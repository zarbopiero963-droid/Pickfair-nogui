# Roserpina — grandezze del conto, auto-reset, settlement e ricostruzione al riavvio (F026 v2, F2, F018, F027–F032, RSR-12)

Riferimenti: #453 (PF-ALL-F026, F2, F018), #426/6146717266 («Non confondere il saldo disponibile con l'equity»),
OWNER-SETTLEMENT-02 e OC-04 (nessuna liberazione di capitale ancora impegnato), OD-PF-RECON-02 (un dato mancante
non vale zero), ATOMIC-RESERVE-01 regola 7 (nessun nuovo ingresso prima della riconciliazione completa).

## Grandezze separate (F026 v2)

| Grandezza | Fonte | Uso |
|---|---|---|
| `bankroll_current` (RiskDesk) | saldo **disponibile** dell'exchange | base di stake target, difesa e lockdown |
| `cash_equity_current` (RiskDesk, PFC-008) | `cash_equity` della **stessa** lettura che ha dato `bankroll_current` | base dei cap percentuali di capacità ed esposizione (singolo, totale, evento, headroom Microstake) |
| `available`, `committed` = \|exposure\|, `cash_equity` = disponibile + impegnato | `RuntimeController._account_from_funds` (soli campi exchange) | snapshot del conto (`_account_snapshot`) e picco di cassa (`_cash_equity_peak`) |

- `cash_equity` è il **saldo di cassa regolato**, non un'equity mark-to-market. Non decide né lo stake target
  né l'ammissione degli ordini (che resta sui fondi disponibili e sulle riserve).
- **Cap percentuali sulla cassa (PFC-008, decisione owner #426 c.6369802091).** I cap `max_single_bet_pct`,
  `max_total_exposure_pct` e `max_event_exposure_pct` si calcolano sulla cassa complessiva verificata e
  l'esposizione corrente si sottrae una sola volta. Esempio: cassa 100, esposizione 20, cap 35% ⇒ totale
  ammesso 35, nuova esposizione 15 (prima: 35% di 80 = 28 ⇒ solo 8).
- **Cassa verificata** (`RuntimeController._cash_equity_verification`): disponibile + impegnato solo da uno
  snapshot valido e coerente col contratto del broker. Le riserve unmatched sono una parte dell'esposizione
  (SIM: `reserved_unmatched` ≤ `exposure`; LIVE: l'exposure Betfair include la liability degli unmatched) e la
  liability LAY è già nell'esposizione: niente si somma due volte. In SIM disponibile + impegnato deve
  coincidere con `total` (tolleranza 1 centesimo); in LIVE vale availableToBetBalance + |exposure|.
  `RiskDesk.sync_cash_equity` la registra solo se il disponibile è quello della stessa lettura e solo se la
  lettura non è iniziata prima dell'ultima applicata (`read_at` monotonic: una lettura arrivata tardi non
  riporta in vita una cassa superata); ogni altro cambio del bankroll (`sync_bankroll`, PnL applicato,
  snapshot, ripristino) o una lettura fallita, anche per eccezione, la azzera,
  con il motivo in `cash_equity_reason` (`CASH_NOT_READ`, `CASH_TOTAL_MISMATCH`, `CASH_RESERVE_INCOHERENT`,
  `BALANCE_ERROR:…`, …), esposto anche da `get_status` (`cash_equity_verified`, `cash_equity_reason`).
- **Esposizione oltre i tavoli:** con la cassa si registrano l'impegnato dello snapshot (`cash_committed_exposure`)
  e l'istante di lettura. Al momento della decisione (`validators.desk_cash_basis`) l'esposizione che il cap totale
  vede è la maggiore fra quella dei tavoli del bot e impegnato + riserve del bot non ancora riflesse nella lettura
  (`TableManager.pending_exposure(since=read_at)`, come il gate fondi PR28-e). Così bet manuali, altre
  applicazioni o posizioni non ricostruite si sottraggono, e una riserva pendente non le «assorbe». Esempi:
  cassa 100 con 20 impegnati esterni, cap 35% ⇒ 15; con in più una riserva pendente del bot da 10 ⇒ 5.
  Il tavolo di un lotto Dutching (`legs_total`) risulta a mercato solo quando tutte le gambe distinte
  (`customer_ref`) hanno avuto un esito a mercato: le gambe ancora in volo restano riserve pendenti.
  Il segnale singolo (BACK/LAY) ricontrolla **tutti e tre i cap percentuali** (singolo ordine, totale, evento)
  anche sulla **lettura fresca d'ingresso** del gate fondi (`RuntimeController._entry_total_cap_reason` →
  `RoserpinaMoneyManagement.fresh_caps_reason`): stesse formule sulla cassa e sull'impegnato appena letti
  (cassa registrata 100, fresca 50 + 0, cap 18%: un BACK da 15 viene rifiutato con `supera_max_single_bet`).
  Se mentre la lettura d'ingresso è in volo il RiskDesk registra una lettura più recente (per esempio da
  `get_status`), i cap si ricontrollano anche su quella; se quella non è verificata l'ingresso è rifiutato.
  Un impegnato esterno cresciuto o una cassa scesa dopo l'ultima lettura registrata riducono quindi il limite
  prima di `CMD_QUICK_BET` (cassa 100 registrata con 20 impegnati, lettura fresca 60 + 40, cap 35%: un BACK
  da 15 viene rifiutato con `supera_max_total_exposure`); lettura fresca non verificata ⇒
  `cassa_non_verificata:<motivo>`.
  Perché impegnato fresco e riserve pendenti non si sovrappongano, il TradingEngine pubblica l'ACK sincrono
  `QUICK_BET_SUBMIT_ACK` quando il broker accetta l'ordine (stato SUBMITTED, il ciclo «ACCEPTED»): il runtime
  marca la riserva a mercato (`placed_at`), senza rilasciarla né avviare il TTL. Prima, sul percorso SIM del
  TradingEngine, la marca arrivava solo con un esito terminale e ogni lettura successiva contava l'ordine due
  volte. Un ordine AMBIGUOUS non ha ACK: resta contato sia nell'impegnato sia fra le pendenti (fail-closed).
- **Cassa non verificata ⇒ BLOCKED** delle nuove entrate (MM `cassa_non_verificata:<motivo>`, Dutching,
  cap temporaneo Microstake), nessun ripiego neppure sul disponibile. Le uscite protettive (hedge del cashout,
  `protective` dall'`OrderRouter`, che l'engine toglie dagli ingressi) non vengono bloccate né dall'headroom del
  cap totale: il loro cap temporaneo resta limitato ai fondi disponibili (#548, mandato owner PFC-008 punto 5).
- **Gate fondi sugli ingressi del DutchingController (#546 D2):** lotto Dutching e bet manuale rileggono il conto
  fuori dal lock d'ammissione (`RuntimeController.read_funds_for_controller_entry`) e, sotto il lock, dopo aver
  preso il tavolo e prima di costruire gli ordini, confrontano il disponibile fresco con impegnato + tutte le
  riserve non ancora riflesse nella lettura, compresa quella dell'ingresso
  (`controller_entry_funds_reason`, stesse regole del gate PR28-e del segnale). Fondi insufficienti ⇒
  `fondi_insufficienti:…`; lettura con `error`, campi mancanti, eccezione o runtime senza il metodo ⇒
  `fondi_non_verificabili`. Rifiuto ⇒ nessun `CMD_QUICK_BET`, tavolo e chiave rilasciati. Prima l'unico freno con
  una cassa vecchia era il rifiuto a valle dell'exchange. I tre cap percentuali restano calcolati sulla cassa
  registrata (a differenza del segnale, che li ricontrolla anche sulla lettura fresca).
- **Cap evento di lotto Dutching e bet manuale, misura unica (#549, #554):** `precheck` del lotto e `manual_bet`
  applicano `max_event_exposure_pct` (cassa verificata·%) con la STESSA misura dei segnali,
  `RuntimeController.controller_event_exposure(entry, event_key=…)`. Conta i tavoli attivi:
  - dello stesso evento reale (event_id, poi nome);
  - dello stesso `market_id`, anche se il nome evento è scritto diversamente;
  - con la stessa chiave dell'ingresso.

  Ogni tavolo conta una volta (unione, nessun doppio conteggio). Dentro ci sono tavoli da segnale, Dutching e
  manuali, abbinato e non abbinato, riserve non ancora a mercato ed esiti incerti trattenuti: sono tutti tavoli
  attivi. Per i LAY conta la liability. L'impegnato esterno non attribuibile a un evento resta nel cap globale.

  L'id evento ha un'unica forma canonica (`DutchingController._canonical_event_id`): 123.0, 123 e «123» sono lo
  stesso evento; float non interi, NaN/inf e altri tipi non valgono come id. Il tavolo del lotto conserva
  `event_id`, come quello della bet manuale. Anche la corsia bus (`RiskMiddleware`) lo conserva, e
  non trasforma più un nome assente nel nome «None». Così un ingresso successivo dello stesso evento, identificato dal solo id, lo riconosce anche su
  un altro mercato.

  Rifiuti:
  - senza nome né id evento il cap non è verificabile ⇒ `Esposizione evento non verificabile` (il controller non
    inventa l'evento). Valgono le regole della misura: un id vuoto, di soli spazi, 0 o bool non è un'identità;
  - runtime senza la misura, eccezione, valore non finito o negativo ⇒ `Esposizione evento oltre limite`.

  Prima del #554 il lotto misurava solo il tavolo con la propria chiave, e un'eccezione valeva 0 (fail-open). La
  bet manuale, con un runtime senza misura, ripiegava sullo stesso tavolo.
- **Zero verificato dopo il settlement (#546 D1):** un disponibile pari a 0 senza `error` è un saldo letto, non un
  ripiego (dal PFC-007 ogni ripiego del servizio porta `error`): motivo `BALANCE_ZERO_VERIFIED` (prima
  `BALANCE_ZERO_AMBIGUOUS_OR_FALLBACK`), snapshot di conto `source=settlement` registrato (la cassa del RiskDesk non si risincronizza: con bankroll
  invariato andrebbe in `CASH_READING_MISMATCH` e bloccherebbe le entrate anche al ritorno dei fondi; il freno sul
  conto vuoto è il gate fondi freschi d'ingresso), stato
  `SYNC_FAILED_BALANCE_UNAVAILABLE` (auto-next resta saltato). Il bankroll **non** viene allineato a 0 fuori
  dall'avvio, per non portare in LOCKDOWN un conto vuoto solo per riserve impegnate.
- **Letture fuori ordine:** bankroll e cassa della stessa lettura si applicano insieme
  (`RiskDesk.sync_funds_reading`), con il controllo di `read_at` prima di toccare il bankroll: una lettura iniziata
  prima dell'ultima applicata non riscrive il disponibile né invalida la cassa più recente.
  Quando il conto cambia fuori da una lettura (P&L realizzato al settlement, snapshot, ripristino dello stato) la
  cassa viene invalidata e la soglia di `read_at` passa ad adesso: una lettura iniziata prima non la rimette in
  vigore (audit PF-FINANCIAL-IMPACT-GATE-01).
- Un campo mancante, non numerico, non finito o negativo rende lo snapshot non valido. Un'esposizione mancante
  **non** vale zero.
- La base dello stake target non cambia rispetto a prima di F026. È provato da `stake_base_unchanged` nei test
  d'integrazione: ogni sincronizzazione dall'exchange usa il disponibile di quell'istante.

## Auto-reset

`RuntimeController._maybe_auto_reset`, chiamata a ogni settlement contabilizzato. `ROSERPINA_AUTO_RESET`
ribasa il picco e libera risorse. Per questo si decide sul drawdown dell'equity di cassa,
cioè del **regolato**, e mai sul solo disponibile: le puntate aperte non sono perdite.

- Snapshot del conto non valutabile ⇒ nessun auto-reset (fail-closed).
- Il reset trattiene i tavoli e le chiavi anti-duplicato delle posizioni **non regolate** e delle riserve incerte
  (F024). Libera solo i mercati con settlement confermato (`_mark_market_settled`).
- Il reset ribasa anche il picco di cassa, come il picco del RiskDesk (OC-04).
- Il payload dell'evento porta `drawdown_basis: cash_equity` e, per confronto, `available_drawdown_pct`.

## Limiti noti, decisioni owner aperte

- **Lockdown e difesa** restano sul drawdown del disponibile, cioè sulla base «caso peggiore». Con un'esposizione
  aperta oltre la soglia di lockdown (default 20%), un settlement porta in LOCKDOWN anche senza perdite equivalenti.
  La base del drawdown è una decisione owner aperta (registro D04, punto 6.O2): non viene cambiata qui.
- Il trattamento dei prelievi e dei depositi nel drawdown non è deciso. Oggi muovono l'equity di cassa come
  qualsiasi variazione del conto.

## F2 — rilascio al settlement

`RuntimeController._release_settled_tables` (dalla #461 PR29-c il tavolo si individua per **identità durevole**,
vedi la sezione PR29-c sotto):
- con `table_id` nel payload (chiusura del `PnLEngine`) si rilascia solo se il payload porta `customer_ref` ed
  `event_key` della posizione e il tavolo porta ancora quella riserva (`customer_ref`, `reservation_id`); un tavolo
  riusato da un'altra riserva non si libera e la perdita della posizione entra una volta nel suo recovery;
- senza `table_id`: settlement cleared per bet => bet → riga `orders` (unica) → `customer_ref` → tavolo;
  settlement SIM per mercato => solo con un tavolo e un solo ordine del bot sul mercato, e quell'ordine è
  l'intento che il tavolo porta. Identità non dimostrabile o settlement senza chiave => nulla si libera;
- una riserva incerta resta trattenuta (F024/F004);
- con più tavoli sullo stesso mercato non si rilascia nulla automaticamente (log);
- un hedge di cashout passa da OrderRouter senza riga in `orders`, quindi il settlement di una gamba non è il netto
  del tavolo: ogni esito di cashout (`CASHOUT_SUCCESS` / `CASHOUT_FAILED` con mercato; `CMD_EXECUTE_CASHOUT` resta
  a consumatore unico) marca il mercato nel registro durevole `roserpina.hedged_markets` (conservazione 365 giorni,
  scrittura serializzata da un lock) e su quel mercato un settlement **per bet** (`cleared:<mercato>:<bet>` o chiave
  assente) non rilascia per mercato; registro illeggibile o non persistito => nessun rilascio per mercato
  (fail-closed). Il settlement SIM e' **per mercato** (`sim:<mercato>:<id>`, netto di tutte le gambe, hedge
  compreso) e continua a rilasciare. Scrittura del registro fallita: oltre al flag in memoria si scrive una
  sentinella durevole su un canale diverso dal DB, `<db_path>.hedged_markets.unreliable` (una riga JSON per mercato,
  nessun segreto). Se esiste all'avvio, il registro e' inaffidabile: nessun rilascio per mercato con settlement per
  bet — su **tutti** i mercati, non solo quelli elencati — finche' l'operatore, dopo aver riconciliato i mercati con
  cashout elencati nella sentinella, non la rimuove a mano **e riavvia** (la sentinella si legge solo all'avvio); i
  tavoli gia' trattenuti si liberano poi con `reset_cycle()` o con l'auto-reset (ciclo 15 della #512, bloccante Sol
  su 180ffa3). Lo stato e' visibile in `get_status()["hedged_markets_unreliable"]` e nel log critico; l'evento
  `HEDGE_REGISTRY_UNRELIABLE` viene pubblicato ma oggi non ha consumatori cablati (notifica Telegram/health: #521).
- **Riconciliazione sull'exchange** (cicli 16-17, bloccanti Sol su 23bdbfe e 9f0d6ec): prima di un rilascio per
  mercato da OGNI settlement che non sia SIM per mercato (`cleared:<mercato>:<bet>`, chiusura di posizione, chiave
  assente) il runtime legge `listClearedOrders` per il **mercato**;
  una bet regolata che non e' del bot (hedge, ordine manuale), una lettura fallita o una risposta vuota/malformata
  => nessun rilascio per mercato. Non dipende da scritture locali, quindi copre anche registro e sentinella non
  scrivibili (filesystem in sola lettura); registro e sentinella restano difese aggiuntive. Limiti residui dichiarati (#521): un crash fra il piazzamento dell'hedge e la
  consegna dell'esito sul bus; una sentinella non scrivibile (stesso filesystem guasto del DB: resta il solo flag in
  memoria e il log critico). Corretto nel ciclo 14 della #512 (verifica indipendente,
  r4303297587): l'affermazione precedente «invariato rispetto a main» era falsa per l'effetto sulla perdita di
  recovery, perché su main il rilascio per mercato senza `table_id` non esisteva.

## PR29-c — un settlement, un solo effetto finanziario (PF-PR29-C-FINANCIAL-IDEMPOTENCY-01)

Contratto PF-C29-01 .11/.13/.18 (#461). Phase 0 su main 409bcf9: quattro consegne concorrenti dello stesso
settlement portavano il debito di recovery da 10 a 40 (S7); con due bet sullo stesso mercato nessun tavolo si
liberava (S2); il settlement della propria bet non chiudeva l'incertezza AMBIGUOUS (S3); la chiusura con `table_id`
liberava la riserva di un altro evento e raddoppiava la perdita (S5/S6); dopo il riavvio la chiave appuntata da F018
restava per sempre (S9).

- **Catena d'identità durevole** `bet_id → customer_ref → reservation_id → table_id`: `TableManager.activate`
  assegna un `reservation_id` alla riserva; il comando `CMD_QUICK_BET` lo porta e la riga `orders` lo persiste;
  F018 lo ripristina nel meta del tavolo ricostruito insieme al `customer_ref`. `Database.find_orders_by_bet_id`
  lega la bet all'intento (righe zero o più di una => nessuna attribuzione).
- **Registro durevole `settlement_effects`** (tabella additiva, `CREATE TABLE IF NOT EXISTS`, nessuna colonna
  esistente toccata; un registro creato da una versione precedente della tabella riceve la colonna `payload_json`
  con `ALTER TABLE`): chiave = chiave del settlement (`batch_id|event_key|table_id|bet_id|order_id`), esito,
  identità, P&L, istanza, `realized_applied`. `record_settlement_effect` inserisce la riga **e** lo stato durevole
  dei tavoli di recovery nella stessa transazione `BEGIN IMMEDIATE`: lo stesso settlement, consegnato più volte,
  in concorrenza, da due processi sullo stesso DB o dopo un riavvio, produce il suo effetto una volta sola.
- **Ordine dei lock**: lock del consumer (`_settlement_lock`, un settlement alla volta nel processo) → lock dei
  tavoli → transazione DB. Effetto sul tavolo applicato in memoria, poi registrato; registrazione persa (duplicato)
  o fallita => tavolo ripristinato dallo snapshot, nessun effetto (ritentabile).
- **P&L realizzato** una volta: set in memoria + flag `realized_applied`, marcato dopo il monitor della perdita
  giornaliera. Quando la registrazione non è di questa consegna (riga già presente ma non marcata: crash prima
  della marcatura o altro processo in volo; registro illeggibile; riconciliazione di un `held`) si applica solo
  una **perdita**: contarla due volte alza la perdita misurata (fail-closed); una vincita contata due volte la
  abbasserebbe e terrebbe aperti gli ingressi oltre il limite giornaliero/di sessione (fail-open), quindi non si
  riapplica. Chi perde la corsa di registrazione contro un altro processo non applica il realizzato.
- **Il P&L in memoria non è capitale**: `bankroll_current` (stake) e il gate fondi PR28-e leggono il conto
  dell'exchange; `_apply_realized_pnl_without_mutating_bankroll` non tocca il bankroll. Il realizzato in memoria
  alimenta solo i limiti di perdita giornaliera e di sessione, per cui vale la regola qui sopra.
- **Settlement trattenuti (`held`) riconciliabili**: il registro conserva payload, P&L e mercato. Una nuova
  consegna dello stesso settlement, la riconciliazione esplicita `reconcile_held_settlements()` o l'avvio (dopo
  una ricostruzione F018 riuscita) lo rivalutano: identità ora dimostrabile => effetto sui tavoli una volta
  (`UPDATE ... WHERE outcome = 'held'` nella stessa transazione dello stato tavoli), nessun realizzato ripetuto;
  ancora non attribuibile => resta `held`. Una lettura fallita (ordini, exchange) non trattiene più per sempre.
  All'avvio e alla ripresa un `held` ancora in attesa di una lettura fallita (o il registro illeggibile) tiene
  il runtime in **PAUSED** (`HELD_SETTLEMENTS_PENDING:<chiavi>` / `HELD_SETTLEMENTS_UNREADABLE`): il suo effetto
  (riserva, debito di recovery) non è ancora applicato. Gli `held` non attribuibili in modo definitivo (hedge,
  più righe `orders`, più tavoli SIM, bet non del bot) restano trattenuti senza fermare il runtime.
- **Stato dei tavoli non scrivibile** (RSR-12 illeggibile, serializzazione fallita): nessuna riga registrata ed
  effetto annullato, così la riconsegna resta applicabile e il debito non si perde dopo un crash.
- **Chiave della chiusura di posizione** (`table_id`): `_build_bankroll_sync_key` include l'identità canonica
  della riserva, `customer_ref` (un intento = una riserva) oppure, solo in sua assenza, `reservation_id`, in OGNI
  fase (registro degli effetti, sync del bankroll, checkpoint del ciclo, auto-next): lo stesso tavolo riusato per
  lo stesso evento da un'altra riserva produce un'altra chiusura, non un replay; una riconsegna della stessa
  chiusura che omette `reservation_id` resta un duplicato (nessun debito ripetuto).
- **Tavolo della riserva assente** (es. riavvio con meno tavoli): il settlement resta `held` in attesa
  (avvio in PAUSED) finché il tavolo non esiste; mai registrato come effetto senza debito.
- **Auto-next**: la request persistita porta `reservation_table_id` e `reservation_id` (il campo `table_id`
  resta assente come prima), così il settlement della bet ritrova la riserva dopo un riavvio.
- **Scritture dello stato tavoli**: snapshot, scrittura ed esito (`_recovery_persisted`) avvengono sotto il lock
  dei tavoli anche fuori dal settlement, quindi uno snapshot vecchio non sovrascrive una perdita appena registrata.
  Ogni scrittura della mappa (registrazione di un settlement, scrittura ordinaria, ritentativo, reset) è un
  insieme di **variazioni** rispetto a ciò che questo processo ha ripristinato all'avvio o scritto per ultimo,
  sommate alla mappa letta dentro la stessa transazione `BEGIN IMMEDIATE`; una voce si rimuove solo se il valore
  risultante è ≤ 0. Con più processi sullo stesso DB, anche sullo stesso tavolo, nessuno cancella o sovrascrive
  le perdite registrate dagli altri (marker di reset e baseline viaggiano a parte). Un settlement senza effetto
  non riscrive lo stato salvo per ritentare una scrittura fallita.
- **Un solo runtime attivo per database**: `start()` prende un lock esclusivo del sistema operativo su
  `<db_path>.runtime.lock` (percorso canonico: link simbolici, relativi e maiuscole su Windows portano allo stesso
  lock; fcntl su Linux, msvcrt sul byte 0 su Windows), tenuto finché il runtime non è fermo: lo rilasciano
  `stop()` (dopo lo smontaggio dei servizi) e un avvio non riuscito, mai un runtime ancora attivo (nemmeno un suo
  secondo `start()` fallito); un avvio fallito dopo la connessione smonta prima i servizi già avviati (poller, feed, Telegram,
  Betfair) e, se lo smontaggio fallisce, trattiene il lock finché uno smontaggio successivo non riesce (ritentato a
  un nuovo avvio rifiutato, o da uno `stop()` completo); anche `stop()` lo rilascia solo dopo uno smontaggio riuscito; `start()` e `stop()` sono serializzati da un lock di ciclo di vita, così uno STOP non
  chiude il lock mentre un avvio lo sta usando; il sistema lo libera anche in caso di crash. Un secondo runtime
  sullo stesso DB viene rifiutato all'avvio (`RUNTIME_INSTANCE_ACTIVE`, evento `RUNTIME_START_REFUSED`): i tavoli
  vivono in memoria per processo e due copie attive divergerebbero su operazioni globali come il reset di ciclo.
  **#536**: il lock OS da solo non bastava (file di lock cancellato o sostituito ⇒ secondo `flock` su un nuovo
  file). Ora l'autorità è a tre strati (`docs/runtime_authority.md`): identità del file di lock (inode) verificata
  a ogni operazione, **lease persistente** nella tabella `runtime_lease` con **fencing token monotono** (TTL 30 s,
  heartbeat 10 s; tenuta da un altro runtime vivo ⇒ `RUNTIME_LEASE_HELD`), fence su ogni scrittura del `Database`
  dentro la stessa transazione, verifica prima di segnale, auto-next, cashout, reset di ciclo, sweep F028 e di
  ogni invio del TradingEngine, ricontrollo dopo l'invio. Autorità persa ⇒ LOCKDOWN senza cancel-all e nessuna
  scrittura. Ledger exactly-once e scritture a variazioni restano come seconda difesa (crash, riavvio, processi
  di servizio).
- **Effetto non registrato** (registro non scrivibile): nessun checkpoint durevole, nessun sync durevole del
  bankroll, nessun auto-next; la perdita entra comunque nel realizzato (limite giornaliero). Il settlement resta
  in coda nel processo e si ritenta a ogni giro del poller (`_retry_unrecorded_settlements`, anche in ACTIVE),
  oltre che in `reconcile_held_settlements()`; dopo un riavvio lo riconsegna il poller. Un ritento riuscito marca
  il realizzato già applicato alla consegna (`realized_applied`), così la riconsegna dopo un riavvio non lo riconta.
  Anche un settlement `held` per una lettura fallita (ordini, exchange) entra nella stessa coda e si riconcilia nel
  processo appena la lettura torna disponibile.
- **F018** ricostruisce la riserva viva dell'auto-next sul suo `reservation_table_id`.
- **Esiti del lifecycle**: un tavolo che porta un intento si libera (tavolo e chiave) solo con l'esito di quello
  stesso intento; un esito senza `customer_ref` non libera nulla e non chiude un'incertezza AMBIGUOUS registrata
  per un intento (nemmeno un esito senza tavolo). Un AMBIGUOUS registrato senza tavolo
  (auto-next) si chiude con il settlement della stessa riserva.
- **Replay** di un settlement già registrato: nessun effetto economico (tavolo, recovery, realizzato, ordini);
  restano solo letture informative (sync del saldo, checkpoint del ciclo).
- **Incertezza per riserva**: l'AMBIGUOUS registra il `customer_ref`; l'esito certo di un altro intento non la
  chiude. Il settlement della propria bet chiude l'incertezza della sola sua riserva e libera la chiave
  anti-duplicato (anche appuntata da F018) solo se nessun altro tavolo porta l'evento. L'esito terminale del
  lifecycle (`force_unlock_if_event`) non libera un tavolo la cui riserva porta un altro `customer_ref`.
- **Semantica cambiata, dichiarata**: un settlement senza chiave o senza identità dimostrabile non libera più nulla
  (prima liberava il tavolo univoco del mercato); `RUNTIME_CLOSE_POSITION` non rilascia più in modo incondizionato
  la chiave anti-duplicato del payload.

Limiti residui dichiarati (fail-closed): le viste in memoria (risk desk, tavoli) restano per processo fino a
29-g1/g2; il settlement SIM per mercato con più tavoli resta trattenuto (netto non attribuibile); finestra di
sovra-conteggio del realizzato su crash (direzione sicura); con `auto_close_enabled` (default `False`, dormiente)
la chiusura di una gamba Dutching non libera il tavolo del lotto e la sua perdita entra nel recovery del tavolo
(rilascio a livello di lotto: PR30).

## F027 — settlement fuori da ACTIVE (decisione owner, CLOSURE-03)

LOCKDOWN, PAUSE ed emergency stop bloccano i **nuovi ingressi**, non la riconciliazione delle posizioni aperte
(`RuntimeController._settlement_reconciliation_allowed`: ACTIVE, PAUSED, LOCKDOWN; non STOPPED, dove il servizio
è disconnesso e la riconciliazione riparte da `start()`).
- I settlement confermati si contabilizzano: P&L, commissione una volta, conto, tavoli. Il modo resta quello che era.
- La riconciliazione non produce ordini: l'auto-next del ciclo è riservato ad ACTIVE (`_runtime_not_active_reason`).
- Emergency stop scattato in LIVE: il servizio passa in SIM e il registro SIM non descrive le posizioni reali.
  Nessuna emissione, esposizione mantenuta, evento `SETTLEMENT_RECONCILIATION_UNCERTAIN` (una volta). Le posizioni
  LIVE si riconciliano al successivo avvio autorizzato (F018 + sweep completo). Verifica LIVE: MANUAL_ONLY.
- La provenienza (`_settlement_source_before_emergency`) si fissa PRIMA del salvataggio dell'emergenza e si persiste
  nello stesso `save_settings` del flag (`emergency_settlement_source`, anche dal marker durevole del daily-loss); al
  riavvio si ripristina. Assente (stato legacy), non valida o settings illeggibili => `UNKNOWN`, trattata come LIVE:
  il registro SIM non si usa mai per posizioni di origine ignota. `reset_emergency()` la azzera (Codex r4311806886).
- Nessuna doppia contabilizzazione dopo un crash: dedupe del consumer (checkpoint durevole) invariato.

## F028 — ordine mai abbinato (decisione owner, CLOSURE-03)

`RuntimeController._release_final_unmatched_orders`, a ogni giro del poller dopo i settlement:
- solo righe `SUBMITTED` con betId e riserva non incerta; AMBIGUOUS/INFLIGHT non sono mai «annullati per timeout»;
- SIM: stato del registro del broker `CANCELLED/LAPSED/VOIDED/EXPIRED` con abbinato zero;
- LIVE: betId assente dagli ordini correnti, presente fra i cleared LAPSED/CANCELLED/VOIDED, assente dai cleared
  SETTLED; un errore di lettura non libera nulla (verifica contro Betfair reale: MANUAL_ONLY);
- il tavolo liberato e' quello dell'ORDINE (`table_id` persistito, verificato contro l'evento), mai un altro tavolo con
  lo stesso evento (anti-duplicazione disattivata: due ordini dello stesso evento su tavoli diversi); `table_id`
  assente e piu' tavoli sull'evento => nessun rilascio (ciclo 11 #512, Codex r4309039372);
- rilascio idempotente (`force_unlock_if_event`), perdita di recovery del tavolo conservata, chiave anti-duplicato
  trattenuta (un segnale riconsegnato non si rigioca); evento `UNMATCHED_ORDER_RELEASED`;
- `listCurrentOrders` con `currentOrders` assente o null e' `CURRENT_ORDERS_MALFORMED`, mai "nessun ordine": solo una
  lista, anche vuota, e' autorevole per la ricostruzione F018 (Codex r4312360620).
- crash fra conferma della cancellazione e rilascio: al riavvio F018 trova l'ordine assente e non ricostruisce il tavolo.

## F031 — parziale abbinato con residuo annullato (parità SIM/LIVE)

Su Betfair un ordine con parte abbinata e residuo annullato resta fra gli ordini correnti (EXECUTION_COMPLETE) fino
al settlement. Il broker SIM lo marcava CANCELLED e lo escludeva: al riavvio F018 non ricostruiva una posizione
abbinata viva. `SimulationBroker.get_current_orders` ora lo riporta come EXECUTION_COMPLETE.

## F032 — espansione sulla baseline del ciclo (decisione owner, CONTINUOUS-DELIVERY-04)

La soglia `expansion_profit_pct` si confronta con la crescita dell'**equità di cassa regolata** rispetto a una baseline
persistente del ciclo (`roserpina.expansion_baseline_state`), fissata solo da transizioni esplicite: primo avvio
senza baseline, auto-reset, `reset_cycle`. Sopravvive al riavvio, con una regola di freschezza (PR #512 ciclo 7,
rilievo Codex r4306544231): ogni reset di ciclo scrive il marker durevole `roserpina.cycle_reset_state`
(`{"at_ts": …}`) nella stessa transazione dello stato tavoli RSR-12, con lo **stesso istante** `set_at_ts` della
baseline fissata da quel reset; al riavvio una baseline con `set_at_ts` anteriore al marker (la sua scrittura era
fallita), un marker presente ma illeggibile, impostazioni illeggibili o un `set_at_ts` non finito (NaN/inf/testo)
⇒ baseline scartata ⇒ nessuna espansione finché una transizione esplicita non la fissa di nuovo (baseline legacy
senza `set_at_ts` = 0.0: valida solo finché non esiste alcun marker). In SIM `reset_cycle()` non azzera il broker simulato finché un tavolo resta
trattenuto (esito `sim_reset_skipped` con gli id dei tavoli; r4306544236). È distinta dal saldo, dal disponibile e dal picco
del drawdown (invariato). Profitti netti di commissione (fondi dell'exchange), nessun profitto virtuale: i fondi
impegnati in posizioni aperte contano come capitale, non come guadagno. Snapshot non valido, riserva incerta o
baseline assente ⇒ crescita ignota ⇒ nessuna espansione (NORMAL prima di qualunque confronto con la soglia,
anche con `expansion_profit_pct` ≤ 0 — ciclo 8, rilievo Codex su 034af59). Lo stake espanso resta soggetto a cap, RiskGate, STOP e
LOCKDOWN (il money management applica i cap dopo il moltiplicatore).

## RSR-12 — tavoli di recovery persistenti (decisione owner)

`max_recovery_tables` conta i tavoli con recovery attiva o pendente anche senza ordini aperti. Lo stato (perdita da
recuperare per tavolo) è salvato a ogni settlement e reset (`roserpina.recovery_tables_state`) e ripristinato al
riavvio prima della ricostruzione F018. Un tavolo esce dal conteggio solo a perdita recuperata o con un reset
autorizzato del ciclo; uno stato illeggibile (JSON rotto, perdita non finita o **negativa**: lo zero e' ammesso,
Codex r4312360615) blocca la capacità di recovery **e l'avvio**: `start()` tratta
`_restore_recovery_tables()` fallita come ricostruzione incompleta (`RECONCILIATION_REQUIRED:RECOVERY_STATE_UNREADABLE`,
runtime in PAUSED, stesso percorso di `EXCHANGE_ORDERS_UNAVAILABLE`), `resume()` non lo scavalca e
`_persist_recovery_tables` rifiuta di sovrascrivere lo stato illeggibile (serve un riavvio con stato leggibile).
Scrittura fallita (settlement, `reset_cycle`, auto-reset): capacità di recovery considerata esaurita finché una
scrittura riesce, evento `RECOVERY_STATE_PERSIST_FAILED` pubblicato (`source` per i reset) e `persisted: false` nel
risultato di `reset_cycle` — mai un reset "riuscito" in memoria che un crash farebbe tornare indietro (ciclo 8,
rilievo Codex su 034af59).

## F029 — tavoli in recovery

`TableManager.allocate` assegna un tavolo in recovery solo se **libero** (nessuna posizione aperta) ed entro
`table_count`, come i tavoli liberi. Senza questo vincolo, dopo una perdita, un secondo ingresso riceveva il
tavolo in recovery già occupato e `activate` ne sovrascriveva la posizione: esposizione persa dai cap, due
posizioni sullo stesso tavolo, tavolo non più rilasciabile al settlement della prima.

## F018 — ricostruzione al riavvio

`RuntimeController._rebuild_open_positions`, chiamata da `start()` dopo la recovery e prima di ACTIVE:

| Riga ordine (DB) | Exchange (`list_current_orders`) | Esito |
|---|---|---|
| `SUBMITTED` con betId | betId presente | posizione viva: tavolo (preferito `table_id`), esposizione (LAY = liability), chiave anti-duplicato |
| `SUBMITTED` con betId | assente | chiusa (regolata o annullata): nessun tavolo; il P&L arriva dal settlement |
| `SUBMITTED` senza betId | mercato con ordini | riserva **incerta** |
| `AMBIGUOUS` / `INFLIGHT` | qualsiasi | riserva **incerta** |
| qualsiasi | **illeggibile** | riserva incerta; avvio in **PAUSED** (`RECONCILIATION_REQUIRED`) |

- Posizioni senza tavolo libero ⇒ PAUSED.
- `resume()` ritenta la riconciliazione e rifiuta finché non è completa.
- Le righe senza `market_id` o `event_key` sono riportate in `unattributed`, mai indovinate.
- Il risultato è pubblicato come `STARTUP_POSITIONS_REBUILT`.

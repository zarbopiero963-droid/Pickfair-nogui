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
- **Freschezza della conferma dei fondi (F-16, decisione owner PF-LIVE-FUNDS-THRESHOLD-01, #426 c.6162806642):** il
  gate fondi d'ingresso (`RuntimeController._entry_funds_reason`, usato da segnale, DutchingController e auto-next)
  ammette una lettura solo se ha al massimo `LIVE_FUNDS_MAX_AGE_S` = 30 s (soglia effettiva esposta da un solo
  accessore, `RuntimeController.funds_max_age_s()`, per la Control API), misurati con orologio monotono
  dall'inizio della lettura (29,999 s ammessa, 30,001 s rifiutata). Lettura più vecchia, `read_at` assente, non
  numerico, non finito o nel futuro (anche di 1 ms) ⇒ `fondi_non_verificabili:FUNDS_STALE`, nessun ingresso, tavolo
  e chiave rilasciati. Per un segnale (o un lotto Dutching) il rifiuto è ritentabile con un nuovo segnale e una nuova
  lettura. L'auto-next usa lo snapshot del settlement: se il settlement è stato elaborato oltre 30 s dopo la lettura,
  l'ingresso è rifiutato, mai eseguito su fondi scaduti, e il rifiuto è **definitivo per quel settlement** (la sua
  chiave entra in `_processed_auto_trade_keys`: lo stesso settlement non riprova). L'età si misura **al gate**
  d'ingresso, non all'invio dell'ordine. Prima il limite
  era il TTL tecnico delle tracce delle riserve (120 s) e `read_at` assente saltava il controllo. Vale anche in SIM
  (parità decisionale PF-OD12-12). L'invalidazione anticipata su eventi (PF-LIVE-FUNDS-INVALIDATE-01) non fa parte di
  questa correzione.
- **Cap evento di lotto Dutching e bet manuale, misura unica (#549, #554):** `precheck` del lotto e `manual_bet`
  applicano `max_event_exposure_pct` (cassa verificata·%) con la STESSA misura dei segnali,
  `RuntimeController.controller_event_exposure(entry, event_key=…)`. Conta i tavoli attivi:
  - dello stesso evento reale (event_id, poi nome);
  - dello stesso `market_id`, anche se il nome evento è scritto diversamente;
  - con la stessa chiave dell'ingresso.

  Ogni tavolo conta una volta (unione, nessun doppio conteggio). Dentro ci sono tavoli da segnale, Dutching e
  manuali, abbinato e non abbinato, riserve non ancora a mercato ed esiti incerti trattenuti: sono tutti tavoli
  attivi. Per i LAY conta la liability. L'impegnato esterno non attribuibile a un evento resta nel cap globale.

  L'id evento ha un'unica forma canonica (`validators.canonical_event_id`, usata dal controller e dal matcher
  del runtime per ingressi e tavoli salvati): 123.0, 123 e «123» sono lo
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
- **Lettura del conto: un solo contratto per avvio, `get_status`, settlement e ripresa (PF-POST-556 Phase 2,
  decisione owner).** `RuntimeController._apply_funds_reading` classifica ogni lettura e la applica allo stesso
  modo ovunque. Distingue disponibile, impegnato, cassa (disponibile + impegnato), bankroll registrato e stato
  incerto:

  | Stato (`account_state`) | Lettura | Bankroll | Cassa (PFC-008) | Nuove entrate |
  |---|---|---|---|---|
  | `FUNDS_VERIFIED` | disponibile > 0 | = disponibile | della stessa lettura | ammesse ai gate |
  | `BALANCE_ZERO_COMMITTED` (A) | disponibile 0, impegnato > 0 | **invariato** | non verificata, motivo = stato | bloccate |
  | `ACCOUNT_EMPTY_VERIFIED` (B) | disponibile 0, impegnato 0 | **invariato** | non verificata, motivo = stato | bloccate |
  | `FUNDS_UNVERIFIABLE` (C) | `error`, campo mancante, valore non valido | invariato | non verificata | bloccate |

  - €0 disponibili con €100 impegnati **non** sono capitale perso. Prima l'avvio allineava il bankroll a 0
    (falso drawdown, possibile LOCKDOWN) e il settlement lo azzerava se lo zero era marcato
    `ok`/`authoritative`/`balance_confirmed`. Ora nessuna lettura azzera il bankroll: tavoli, posizioni ed
    esposizione restano intatti. Lo zero non apre la baseline del ciclo (F032).
  - Il settlement usa le stesse regole anche per un campo mancante (`exposure`): `BALANCE_FIELD_MISSING`,
    nessuna sincronizzazione (prima allineava il bankroll al solo disponibile).
  - Caso D, fondi tornati: la prima lettura d'ingresso valida con disponibile > 0 riallinea bankroll e cassa con lo
    stesso contratto (`_recover_funds_at_entry`). Vale dopo uno zero, una lettura non valida o in errore (anche
    un'eccezione in `get_status` o al settlement) e dopo una cassa invalidata fuori da una lettura (P&L applicato,
    snapshot, ripristino). Il segnale rilegge il conto prima dell'ammissione, solo a runtime ACTIVE; Dutching e bet
    manuale lo fanno nella lettura dei fondi d'ingresso. La lettura del segnale avviene fuori dal lock, con il fence
    d'ammissione catturato prima e rivalidato dopo: un cashout completato nel frattempo invalida il segnale. Se
    l'avvio a zero non ha aperto la baseline del ciclo (F032), si apre alla prima lettura verificata, da qualunque
    percorso (stato, settlement, ingresso). L'apertura avviene sotto il lock con cui un reset di ciclo scrive
    baseline e marker: non sovrascrive mai un reset concorrente e si rinvia se un reset è in sospeso. Un'apertura
    fallita si ritenta alla lettura successiva. Così anche headless,
    senza `get_status`, le entrate riprendono, senza doppi conteggi: il bankroll è il disponibile letto.
  - Il settlement con zero mantiene motivo `BALANCE_ZERO_VERIFIED` e stato `SYNC_FAILED_BALANCE_UNAVAILABLE`
    (auto-next saltato) e riporta `account_state`. `get_status` espone `account_state` e `account_state_source`.
  - Ordine delle letture: una lettura iniziata prima dell'ultima applicata non cambia stato, bankroll né cassa
    (controllo di `read_at` del RiskDesk su ogni ramo, sotto `_account_state_lock`).
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


## Hard-stop di drawdown, picco persistente e DEFENSE/LOCKDOWN (D-1/D-2/D-3, RO-DD-01..07)

Contratti owner del 09/11/2026: PF-ROSERPINA-D1-D2-B2-B3-FINANCIAL-SAFETY, PF-ROSERPINA-DRAWDOWN-AND-RECOVERY,
PF-ROSERPINA-FINANCIAL-CONTRACT-FINAL, PF-ROSERPINA-D2-LOCKDOWN-UNLOCK-FINAL (#586).

- **Formula.** `drawdown_abs = max(0, picco_cassa_verificata - cassa_verificata_corrente)`, in Decimal, confronto esatto.
  Anche la cassa (disponibile + impegnato) e' sommata in Decimal: 80.01 + 0.01 = 80.02, cosi' una perdita esattamente
  pari al limite scatta (in float sarebbe 9.99999999999999; Codex su b9cbe0d7).
  La cassa e' disponibile + impegnato della stessa lettura (PFC-008): riserve e puntate aperte non sono perdite.
- **Picco** (`core/drawdown_hard_stop.py`): massimo della cassa verificata, persistito in settings
  (`roserpina.drawdown_hard_stop_state`, con `seq`) prima di valere. Lo alza ogni lettura verificata
  (`_sync_cash_equity_from_funds`); auto-reset, reset di ciclo, settlement e riavvio non lo abbassano. Una lettura non
  verificata non lo alza mai. Prima esecuzione dopo l'aggiornamento: il picco parte dalla prima cassa verificata
  (non e' una ricostruzione dello storico, che resta il metodo owner del 13/10).
- **Hard-stop.** `max_drawdown_abs` (euro) e `max_drawdown_hard_stop_pct` (% del picco), opzionali: senza valore non
  bloccano nulla. Valutati a ogni lettura verificata del conto (superamento => LOCKDOWN subito) e prima di ogni
  ingresso (segnale, auto-next, manuale/Dutching). Stessa regola in SIM e LIVE (prima il % valeva solo in LIVE).
  Anche la lettura fresca del conto fatta per un ingresso entra nell'hard-stop, pure con la cassa del desk gia'
  verificata (Codex su 58e635e6): il superamento si memorizza (LOCKDOWN) e il gate fondi d'ingresso lo rifiuta. Come
  ogni lettura verificata alza il picco (Sol su 53c99ceb: 130 poi 115 con picco 100 e limite 10 blocca), ma non
  cambia la cassa del desk (PFC-008: una lettura concorrente del desk resta da ricontrollare): il gate dell'hard-stop
  usa l'ultima cassa valutata (del desk o d'ingresso), aggiornata insieme al picco nella stessa sezione critica, cosi'
  un picco nuovo non si confronta mai con una cassa piu' vecchia e il desk non viene letto a pezzi (Sol su 96b03d7e);
  cassa del desk non verificata resta un rifiuto. La stessa cassa piu' recente vale
  per la misura di DEFENSE/LOCKDOWN (`_persistent_drawdown_pct`). Una lettura iniziata prima dell'ultima gia' valutata
  (ingresso o contratto) e arrivata tardi non diventa la cassa corrente e non misura una perdita (Sol su 4a171465:
  100 arrivato dopo 130 non e' una perdita di 30), ma resta un massimo osservato davvero: alza il picco persistente e,
  se lo alza, il superamento si rivaluta contro l'ultima cassa valutata (130 arrivato dopo 115 con limite 10 blocca;
  D-631-08, Sol/Claude D su c715c59e). Vale anche sul percorso reale del conto: la lettura che il RiskDesk scarta
  perche' arrivata tardi (bankroll e cassa del desk invariati) arriva comunque all'hard-stop, che decide col proprio
  istante (Sol su 9209f0d8). Una lettura iniziata prima dell'ultimo ribasamento owner
  (`DrawdownHardStop.rebased_at`) appartiene all'epoca precedente del picco: tardiva o no, non lo rialza e non
  diventa la cassa corrente. Cronologia delle letture (D-631-10/11, Codex/Claude D su 3e6a3e0a e b76466a3): ogni
  lettura verificata resta in memoria con il suo istante (`DrawdownHardStop.record_reading`). Una lettura tardiva
  (`evaluate_late`) si valuta (A) contro il massimo letto STRETTAMENTE PRIMA del suo istante e (B), se lo supera,
  contro il minimo letto STRETTAMENTE DOPO (per una lettura precedente alla storia conservata, di cui si conosce solo
  un limite superiore del massimo, (B) si verifica sempre: D-631-12, Sol/Claude D su f0f521dc): 130 a t1, 130 a t3 e poi 115 arrivata da t2 memorizzano la perdita di 15
  come in ordine, e un massimo tardivo rende reale un calo successivo. Mai contro un massimo successivo; letture dello
  stesso istante non si confrontano fra loro. Memoria limitata per l'intera struttura (`READING_HISTORY_LIMIT` = 512):
  al massimo 256 letture recenti (`RECENT_READINGS_LIMIT`); le piu' vecchie passano in un riepilogo esatto di al
  massimo 256 punti (`SUMMARY_LIMIT`): la scala dei massimi crescenti (massimo letto prima di t, ricerca binaria) e la
  scala dei minimi successivi (minimo letto dopo t). Oltre il riepilogo la finestra avanza: cio' che precede non e'
  piu' esatto e il picco noto all'apertura vale solo come limite superiore. Una lettura che cade prima della finestra
  ha tre esiti possibili: superamento dimostrato con le letture conservate => hard-stop; superamento possibile ma non
  dimostrabile (contro il limite superiore, oppure perche' puo' essere un massimo che rende reale un calo non piu' in
  memoria) => storia insufficiente; nessun superamento possibile nemmeno nel caso peggiore => nulla. Lo stato e'
  **DRAWDOWN_HISTORY_INSUFFICIENT** (motivo `drawdown_storia_insufficiente`): persistito con motivo, istante, cassa ed
  epoca; NON e' un superamento (nessuna perdita dichiarata, nessun LOCKDOWN) e non e' un PASS: nega i nuovi ingressi
  su ogni percorso (segnali, auto-next, ripresa via `_loss_block_reason`; manuale e Dutching via
  `absolute_cap_reason_for_orders`), mai le uscite, le cancellazioni e la riconciliazione. Una nuova lettura non lo
  scioglie (non ricostruisce la storia mancante), ne' STOP/START, crash, subentro o ribasamento: solo la procedura
  owner `owner_clear_drawdown_history_insufficient` (stesse regole e verifiche dello sblocco dell'hard-stop, nella
  sezione critica). Nessuno sblocco automatico: la storia mancante non si puo' ricostruire da letture successive. La
  finestra si apre allo START (picco riletto come base), a ogni ribasamento owner e a ogni sblocco owner (superamento o
  storia insufficiente): le letture iniziate prima di una decisione owner sono ignorate, cosi' una lettura fuori ordine
  non fa rinascere un superamento gia' sbloccato (Sol/Codex su b76466a3); il picco resta. Senza alcun picco noto la
  finestra e' esatta da subito. Sola memoria: dopo un riavvio valgono picco, superamento e storia insufficiente
  persistiti. Allo START lo stato si rilegge PRIMA che inizi la lettura dei fondi (P3, Sol su 3e6a3e0a): un
  ribasamento di un altro processo adottato in quel momento non rende «precedente al ribasamento» la prima lettura;
  controllo
  dell'istante e valutazione sono un'unica sezione critica fra letture d'ingresso e del desk (Sol su 6d4ac43f), e
  nella stessa sezione il gate sceglie la cassa e valuta (Sol su b9cbe0d7), come la misura di DEFENSE/LOCKDOWN
  (Codex su 2a847087). Anche il controllo di proprieta' avviene dentro la sezione critica, e lo STOP toglie la
  proprieta' prendendo lo stesso lock: una scrittura gia' ammessa finisce prima che lo STOP rilasci il lock d'istanza.
  Sblocco owner e conferma della baseline sono ammessi solo al controller proprietario, dopo una rilettura dello stato
  durevole sotto lock (Sol su 2a847087); lo STOP di un'istanza che ha perso l'autorita' non scrive la chiusura della
  sessione (controllo esplicito, oltre al fence del database: Sol su 96b03d7e); la rilettura non scambia la propria sessione attiva per una chiusura non
  pulita, e le verifiche su tavoli e ordini si leggono prima della sezione critica (nessuna inversione dei lock).
  Allo START la proprieta' dello stato comincia solo a rilettura finita; un avvio fallito con servizi smontati chiude
  la sessione prima di rilasciare il lock (nessuna conferma owner ingiustificata al riavvio); un controller mai
  avviato uscito da STOPPED non scrive se nel database un altro runtime tiene una lease attiva (lease illeggibile =>
  nessuna scrittura). Lo stato dell'hard-stop lo aggiorna solo il controller
  proprietario del runtime (avviato, o nello START dopo la rilettura sotto lock, con l'autorita' dimostrata): un
  controller mai avviato, fermo o che ha perso l'autorita' non scrive nulla, nemmeno leggendo il conto per lo stato
  (Codex su 53c99ceb). Lo stesso vale per i gate d'ingresso (segnale, manuale/Dutching): un controller non
  proprietario nega l'ingresso (`drawdown_controller_non_proprietario`, o il motivo dell'autorita' persa) senza
  valutare, perche' la valutazione salverebbe il picco (Grok su 6d4ac43f).
- **Disponibile 0: l'hard-stop valuta comunque la cassa (Sol e Codex su dc2f5f25).** Con una lettura valida a
  disponibile 0 l'hard-stop riceve la cassa verificata della stessa lettura: con impegnato
  (`BALANCE_ZERO_COMMITTED`) e' l'impegnato (85 impegnati dopo un picco 100 = perdita 15), senza
  (`ACCOUNT_EMPTY_VERIFIED`) e' 0, cioe' perdita totale e non dato mancante. Il superamento si memorizza, anche con
  soglia percentuale 100 %, e resta dopo una ricarica (regola piu' severa). Bankroll e cassa del desk non cambiano
  (PF-POST-556) e gli ingressi restano negati; una cassa 0 non diventa mai picco. Cassa negativa, non finita o
  assente resta `max_drawdown_abs_cassa_non_verificata`.
- **Blocco persistente.** Il superamento si memorizza (kind, perdita, limite, picco, cassa) e resta dopo riavvio,
  settlement in utile, reset strategico, rimozione del limite dalla configurazione e nuovo segnale. Blocca solo i nuovi
  ingressi: riconciliazione, settlement e riduzione dell'esposizione continuano.
- **Sblocco owner** (`RuntimeController.owner_unlock_drawdown_hard_stop(authorization, reason, operation_id,
  confirm)`): tutti i campi obbligatori, `operation_id` monouso, verifica di cassa verificata, esposizione leggibile e
  assenza di ordini AMBIGUOUS, registrazione durevole prima che valga (salvataggio fallito => resta bloccato). Le
  verifiche si calcolano dentro la sezione critica dell'hard-stop, subito prima della transizione, e la registrazione
  di un ordine AMBIGUOUS (`_mark_uncertain`) prende lo stesso lock: un ordine che diventa incerto durante la procedura
  la nega, mai in mezzo fra verifiche e salvataggio; vale anche per la conferma della baseline (D-631-09). Non
  ribasa il picco: se la perdita e' ancora oltre il limite, la lettura successiva blocca di nuovo. La nuova baseline e'
  un'operazione distinta (`DrawdownHardStop.owner_rebase_peak`). Nessun endpoint Control API/MCP; il comando owner che
  le invoca non e' ancora previsto (limite dichiarato).
- **DEFENSE/LOCKDOWN.** La componente di drawdown e' la perdita cumulativa di cassa dal picco persistente
  (`RuntimeController._persistent_drawdown_pct`). Caso della transizione: cassa 100 con 30 impegnati e nessuna perdita,
  prima LOCKDOWN (30 % sul disponibile), ora NORMAL; il capitale impegnato resta limitato dai cap di esposizione gia'
  configurati (`max_total_exposure_pct`, evento, singolo, `max_open_exposure`, liability LAY, riserve). Senza alcuna
  cassa verificata resta la misura storica (gli ingressi sono comunque negati dai controlli della cassa).
- **Fail-closed.** Stato persistito illeggibile => desk in LOCKDOWN e hard-stop `*_stato_illeggibile`, mai
  sovrascritto; picco non salvato => nessun nuovo ingresso su ogni percorso, con o senza hard-stop configurati
  (`drawdown_picco_non_persistito`, in coda ai motivi dei gate a monte; RO-DD-08: altrimenti un riavvio ripartirebbe
  dal picco salvato e renderebbe aggirabile il LOCKDOWN cumulativo), con nuovo tentativo di salvataggio a ogni
  ammissione; con hard-stop configurato anche `max_drawdown_abs_picco_non_persistito`; cassa non verificata =>
  `max_drawdown_abs_cassa_non_verificata`. Picco non persistito e baseline non confermata negano anche bet manuale e
  Dutching (`absolute_cap_reason_for_orders`), con o senza cap configurati (Codex/Grok/Claude D su dc2f5f25: prima
  quel percorso usciva subito senza cap e ignorava la baseline); stato illeggibile => `drawdown_stato_illeggibile`
  anche li' senza hard-stop configurati (Sol su 92155c24). Sessione non salvata all'avvio => log critico e
  ingressi negati come picco non persistito, finche' un salvataggio la rende durevole.
- **Doppio guasto (RO-DD-09).** Se il DB non salva il picco, lo stesso record va in un marker su file separato
  (`<db>.drawdown_peak_pending.json`, scrittura atomica, stesso schema del marker della perdita giornaliera). Al
  riavvio vale il picco piu' alto fra DB e marker; il marker si riporta nel DB e si rimuove solo dopo il salvataggio.
  Sessione, baseline e superamento li decide il record piu' recente (`seq`): un marker rimasto da un guasto rientrato
  (rimozione fallita, file bloccato su Windows) non annulla uno sblocco o una conferma owner salvati dopo nel DB; a pari
  `seq` vale l'unione. Il marker e' il ripiego dei soli stati che stringono (picco che sale, superamento, sessione
  aperta): sblocco owner, conferma della baseline, ribasamento del picco e chiusura della sessione richiedono il DB, e
  se il DB non salva non scrivono neanche il marker (Sol su 92155c24: altrimenti l'operazione fallita varrebbe al
  riavvio come record piu' recente). Marker illeggibile => stato illeggibile (fail-closed). Un marker superato (`seq`
  inferiore al DB) che porta un picco piu' alto della stessa epoca viene prima salvato nel DB e solo dopo rimosso: un
  crash fra le due cose non perde il picco; se non porta nulla in piu' si rimuove soltanto. Una scrittura rifiutata
  dal fencing del runtime (`RuntimeFenceViolation`: l'istanza non e' piu' il proprietario) **non** usa il marker:
  un'istanza superata non pubblica nulla che il nuovo proprietario leggerebbe come stato dell'hard-stop, e resta
  negata (Sol su e98e84c7).
- **Baseline dopo un riavvio non pulito (RO-DD-09 C, decisione owner PF-ROSERPINA-DD09-FAIL-CLOSED-AND-BB3-20261109).**
  Lo START del trading registra su disco una sessione aperta PRIMA di qualunque lettura del conto, e si interrompe
  (`DRAWDOWN_SESSION_NOT_DURABLE`) se non riesce a salvarla: un picco osservato e poi perso per un doppio guasto di
  storage seguito da un crash lascia la sessione aperta, quindi baseline da confermare (D-631-15); solo uno STOP pulito con il picco durevole la chiude,
  e la chiude PRIMA di rilasciare il lock di istanza (Sol su dc2f5f25): dopo il rilascio un nuovo processo puo' aver
  gia' aperto la propria sessione, che la chiusura del vecchio sovrascriverebbe. Smontaggio fallito => sessione aperta.
  Simmetricamente lo START rilegge lo stato dell'hard-stop con il lock di istanza tenuto, prima della prima lettura del
  conto e della sessione (Codex su 92155c24): un controller costruito mentre un altro processo era proprietario non
  sovrascrive il picco o il superamento salvati da quello nel frattempo. La lettura alla costruzione e' di sola
  lettura (`restore(reconcile=False)`): migrazione del marker nel DB e rimozione di un marker superato avvengono solo
  allo START, sotto il lock (Sol su 58e635e6). La verifica della baseline segue il record durevole piu' recente anche
  verso «verificata»: letta mentre un altro processo aveva la sessione aperta, dopo il suo STOP pulito non resta
  bloccata; lo STOP chiude la sessione solo se e' stato questo controller ad aprirla allo START, e una volta sola
  (un controller mai avviato o un secondo STOP non scrivono; Codex su 58e635e6). Allo stesso modo uno sblocco owner
  salvato da un altro processo dopo la costruzione vale alla rilettura dello START, con la sua storia; un superamento
  piu' recente rimasto solo in memoria (salvataggio fallito) resta (Codex su 53c99ceb). La storia degli sblocchi
  (operation_id gia' usati) si riprende dal disco anche senza superamento attivo: un riavvio non rende di nuovo
  valido un operation_id consumato (Sol su 4a171465). Il picco riletto non scende mai sotto quello gia' in memoria,
  salvo un ribasamento owner esplicito (`owner_rebase_peak`), che apre un'epoca nuova del picco (`peak_epoch`, nel
  record): conta solo l'epoca piu' recente fra memoria, DB e marker, e dentro quella il massimo di tutti i record.
  Cosi' un ribasamento a 85 seguito da un picco a 90 vale anche per un controller costruito prima (Codex su
  4a171465, Sol su 6d4ac43f); un ribasamento non salvato non avanza l'epoca. Anche l'operation_id del ribasamento e'
  monouso, persistito nel record (`rebase_ops`) e valido dopo il riavvio (Grok su b9cbe0d7).
  Se al riavvio la sessione risulta aperta (crash, kill, STOP con picco non salvato, DB e marker entrambi non
  scrivibili) il picco salvato potrebbe non essere il massimo: la baseline e' **non verificata** e nessun nuovo
  ingresso passa (`drawdown_baseline_non_verificata`), anche dopo piu' riavvii e dopo un reset strategico.
  Riconciliazione, settlement e riduzione del rischio restano disponibili. Sblocco solo con
  `RuntimeController.owner_confirm_drawdown_baseline(authorization, reason, operation_id, confirm, confirm_again,
  declared_peak=None)`: doppia conferma, `operation_id` monouso, verifica di cassa verificata, esposizione leggibile e
  assenza di ordini AMBIGUOUS, registrazione durevole. La baseline diventa il massimo fra picco salvato, picco
  dichiarato dall'owner e cassa corrente, ed e' registrata come conferma owner, **non** come prova del massimo storico.
  Nessun endpoint Control API/MCP. Un primo avvio senza alcuno stato e' un nuovo perimetro contabile **solo** per
  un'installazione nuova. Soluzione definitiva prevista in BB-3: ricostruzione del picco da un registro finanziario
  indipendente.
- **Stato assente su un'installazione che ha gia' operato (PR-03b-1 P1, F-11 / AUD-GA-008, mandato owner
  PF-OWNER-PR03B1-FOUR-FINDINGS-GO-20261116).** Senza record persistito, `DrawdownHardStop.restore` verifica in sola
  lettura se l'installazione ha uno storico: stato Roserpina persistito (`roserpina.expansion_baseline_state`,
  `roserpina.recovery_tables_state`, `roserpina.cycle_reset_state`) oppure righe in `orders`, `order_saga`,
  `settlement_effects`, `dutching_batches`, `simulation_bets`. Con storico, o con storico non leggibile (anche un DB
  che non permette di interrogarlo), la prima cassa letta **non** e' il massimo storico: baseline non verificata
  (`drawdown_baseline_non_verificata`, `snapshot()["baseline_missing"]` = `stato_assente_con_storico` /
  `stato_assente_storico_illeggibile`), nessun nuovo ingresso su ogni percorso, in SIM e LIVE, anche dopo crash e
  riavvii (il primo record salvato porta `baseline_verified: false`). Esempio owner: picco storico 100 EUR, cassa 80,
  limite 10 %: su main 5daa6a51 il picco diventava 80 e l'ingresso partiva; ora nessun ingresso, e dopo la conferma
  owner con `declared_peak=100` la perdita del 20 % e' un hard-stop. Sblocco solo con
  `owner_confirm_drawdown_baseline`, invariata. Il rilevamento non scrive nulla e non ricostruisce alcun picco.
  Installazione nuova (nessuno storico): invariato, picco dalla prima cassa verificata. Test:
  `tests/d1/test_pr03b1_four_findings.py`; mutazioni `pr03b1_*`.
- **Limiti dichiarati.** Un `seq` regressivo al riavvio non e' rilevabile. Il marker sta sullo stesso filesystem del DB
  (stesso dominio di guasto): la sua perdita insieme al DB e' coperta dal blocco della baseline non verificata, non
  da una ricostruzione. Grandezze
  esposte in `get_status`: `loss_drawdown_pct`, `capital_at_risk_pct` (solo informativo), `drawdown_hard_stop`.

- **Ingressi gia' in coda e avvio fallito (D-631-13/14, ciclo 16).** Il punto finale d'invio delle nuove entrate del
  `TradingEngine` ricontrolla il blocco del drawdown (`RuntimeController.drawdown_entry_block_reason`: superamento,
  storia insufficiente, picco non persistito, baseline non verificata, stato illeggibile), come gia' l'emergenza
  giornaliera: un ingresso approvato e messo in coda prima del blocco non parte dopo; il rifiuto e' certo, prima
  dell'invio (mai AMBIGUOUS). Le uscite protettive passano dall'OrderRouter del cashout e restano libere. Uno START
  partito da fermo e non riuscito torna a STOPPED anche dal LOCKDOWN nato durante l'avvio, cosi' lo smontaggio rilascia
  lock, autorita' e sessione; superamento e storia insufficiente restano su disco e il prossimo START li rilegge.

- **Ingressi di una sessione precedente (M65, ciclo 20, PF-OWNER-PR631-CYCLE20-M65-GO-20261112).** Un ingresso
  ammesso (gate fondi, esposizione, limiti) in una sessione e ancora in coda allo STOP non parte nella sessione
  successiva: la sua ammissione valeva per i fondi e l'esposizione di allora. Ogni START, preso il lock di istanza,
  apre una generazione di sessione nuova sotto il lock d'ammissione; la registrazione dell'ingresso (fence
  d'ammissione: segnali, Dutching, ordini manuali) conserva la propria e il controllo finale del motore
  (`admission_fence_verdict`) la confronta nella stessa sezione critica: generazione diversa =>
  `ingresso_di_sessione_precedente`, negato prima dell'invio e mai ripristinato. Il confronto avviene dentro
  l'operazione finanziaria in volo (`authority_operation`): lo STOP la attende e un nuovo START non acquisisce la lease
  finche' non finisce (`runtime_authority_draining`), quindi nessun ingresso della sessione precedente viene inviato
  sotto la nuova. Il controllo del fence annota la sessione sulla richiesta e il punto finale d'invio del motore la
  ricontrolla (`drawdown_entry_block_reason(request)`): vale anche per un invio eseguito in ritardo dall'esecutore.
  Gli ordini gia' inviati con esito incerto restano alla riconciliazione.

- **Letture con lo stesso istante (ciclo 21, PF-OWNER-PR631-C21-EQUAL-TIMESTAMP-GO-20261112).** Due letture del conto
  iniziate nello stesso istante (`read_at` uguale: possibile con la granularita' del clock, es. Windows) hanno ordine
  reale ignoto. La seconda arrivata si valuta come tardiva (`evaluate_late`): le prove di calo (letture strettamente
  prima o dopo) restano invariate e una perdita dimostrata e' un hard-stop; poi, se in uno qualsiasi dei due ordini il
  calo fra le due (anche con una gemella gia' passata al riepilogo dei massimi e dei minimi) raggiunge un limite, il
  superamento e' solo possibile => storia insufficiente
  (`letture_con_lo_stesso_istante`), ingressi negati. Mai un hard-stop inventato, mai un PASS (picco 100, limite 10,
  letture 130 e 115 dello stesso istante => storia insufficiente in entrambi gli ordini di arrivo). Lo sblocco owner della storia
  insufficiente rilegge il conto (prova fresca, valutata come lettura ordinata) prima di sbloccare: con letture ambigue
  anche la cassa del desk e quella del gate sono ambigue (l'ultima arrivata). Perdita reale => hard-stop dimostrato, che
  lo sblocco non cancella; nessuna perdita => il gate riparte dalla cassa fresca, mai da una cassa ambigua confrontata
  col picco (perdita fittizia), in entrambi gli ordini di arrivo e dopo un riavvio. Rilettura non verificabile =>
  nessuno sblocco (`cassa_fresca_non_verificata`).

- **Ciclo 23 (PF-OWNER-PR631-C23-P1-P2-GO-20261113).** (1) La sessione d'ammissione sta sul comando fin dalla
  registrazione del fence: un ingresso della sessione precedente consegnato piu' volte dopo STOP -> START e' negato al
  punto d'invio a ogni consegna (prima la seconda consegna, trovata la registrazione gia' consumata, arrivava al broker).
  (2) Con storia insufficiente attiva il gate d'ammissione resta sulla storia insufficiente e non confronta la cassa
  ambigua col picco: nessun hard-stop memorizzato su un calo non dimostrato. Le perdite dimostrate restano hard-stop:
  nascono dalla valutazione delle letture, anche con la storia insufficiente attiva.

- **Ciclo 24 (PF-OWNER-PR631-C24, Sol P1 su cddc14c9): proprieta' del fence.** Alla registrazione il comando riceve
  anche un token d'ammissione (`admission_token`). Il verdetto consuma il fence in testa solo se la richiesta ne e' la
  proprietaria: stessa sessione d'ammissione E stesso token. Altrimenti nega (`ingresso_di_sessione_precedente`) senza
  consumare il fence e senza toccare la sessione annotata sul comando. Caso chiuso: dopo STOP -> START lo stesso segnale
  riammesso nella sessione B (stesso customer_ref) creava il fence di P_B; una riconsegna di P_A (sessione A) lo
  consumava, prendeva la sessione B e piazzava l'ordine calcolato in A. Ora P_A e' negato a ogni consegna, anche in
  concorrenza con P_B, e P_B parte: un solo ordine.
  Prova end-to-end Pickfair SIM (direttiva owner): app reale (runtime, money management, fence, motore,
  `SimulationBroker`), sessioni A/B con STOP -> START, riconsegne ripetute e ingresso legittimo; una spia registra ogni
  tentativo d'invio al broker e lo confronta con un oracolo indipendente (arriva solo P_B). Differenze SIM/LIVE: fence e
  ricontrollo al punto d'invio stanno prima della scelta del trasporto, quindi sono gli stessi in LIVE; cambia solo
  il trasporto finale (`SimulationBroker` invece del client Betfair), non provato contro i server Betfair.

- **Ciclo 25 (PF-OWNER-PR631-C25-GROK-SOL-FAILOPEN-20261116): due fail-open riprodotti end-to-end in SIM.** Entrambi
  colpivano gli ingressi gia' ammessi e in coda, che al punto d'invio non rileggono il conto e si fidano dello stato
  del drawdown.
  (A, Grok) Lo sblocco owner della storia insufficiente accetta la rilettura del conto solo se e' una lettura
  ORDINATA: iniziata dopo l'ultima lettura valutata (istante preso prima della rilettura) e diventata la cassa del
  gate. Una rilettura nello stesso tick del clock (granularita' Windows), o superata da una lettura concorrente dello
  stesso tick, non prova nulla: sblocco rifiutato (`cassa_fresca_non_ordinata`), storia insufficiente intatta,
  l'owner riprova. Prima lo sblocco riusciva, il gate restava sulla cassa ambigua (130 con conto reale 115, limite
  10) e l'ingresso in coda partiva.
  (B, Sol) La valutazione delle letture dello stesso istante considera anche la soglia LOCKDOWN del desk
  (`lockdown_drawdown_pct`), con o senza hard-stop configurato: se una lettura piu' bassa della gemella (gia' cassa
  del gate) puo' essere la cassa corrente e la sua perdita dal picco raggiunge la soglia, la perdita e' possibile ma
  non dimostrata => storia insufficiente, ingressi negati, mai un hard-stop. Perdita dimostrata (letture ordinate) o
  gemella gia' bassa => LOCKDOWN del desk come prima; nessuna perdita oltre soglia possibile (130 e 120) => nessun
  blocco. Prima 130 poi 100 dello stesso istante lasciavano la cassa del gate a 130 (0%) e l'ingresso in coda partiva
  con un calo possibile del 23%. Uscite protettive (cashout, cancellazioni, riduzioni) invariate.

- **Ciclo 26 (PF-OWNER-PR631-C26-ZERO-FAILCLOSED-20261116 e PF-OWNER-PR631-C26-DESK-OUT-OF-ORDER-20261116).** Altri due
  fail-open degli ingressi gia' in coda, riprodotti end-to-end in SIM.
  (A) Il ricontrollo al punto d'invio applica anche lo stato dell'ultima lettura del conto (contratto Phase 2):
  lettura non valida (`FUNDS_UNVERIFIABLE`), disponibile 0 verificato senza impegnato (`ACCOUNT_EMPTY_VERIFIED`) o con
  impegnato (`BALANCE_ZERO_COMMITTED`) => `cassa_non_verificata:<stato>`, ingresso negato. Prima la misura protettiva
  ripiegava sul bankroll precedente (0%) e l'ingresso partiva con cap alto, senza cap e, a lettura fallita, anche col
  cap 10. Mai una perdita zero, mai la vecchia lettura positiva; una nuova lettura verificata positiva riapre. Il
  contratto Phase 2 resta: il saldo zero non forza il LOCKDOWN (puo' essere un prelievo), nega gli ingressi; con un
  hard-stop configurato la cassa 0 e' perdita totale (hard-stop e LOCKDOWN). I blocchi del drawdown piu' specifici
  restano prioritari nel motivo.
  (B) L'istante d'inizio della lettura non dimostra l'ordine degli snapshot: una lettura iniziata prima e arrivata
  dopo quella che e' la cassa del gate puo' essere la cassa corrente. Se e' piu' bassa e la sua perdita dal picco
  raggiunge la soglia LOCKDOWN del desk => storia insufficiente (`lettura_fuori_ordine_soglia_lockdown`), anche senza
  hard-stop; nessun hard-stop. Cassa del gate gia' bassa => LOCKDOWN del desk; calo possibile sotto soglia => nessun
  blocco. Prima la 100 iniziata prima della 130 e arrivata dopo lasciava il gate a 130 e l'ingresso in coda partiva.

- **Ciclo 27 (PF-OWNER-PR631-C27-M89-THREE-FAILOPEN-20261116, #628 M89).** Tre varianti dei fail-open dei cicli 25-26,
  riprodotte end-to-end in SIM (1 ordine al broker ciascuna).
  (A, Codex) Lettura con disponibile positivo ma incoerente (riserva unmatched oltre l'impegnato o negativa): conto
  `FUNDS_VERIFIED` ma cassa del desk invalidata da quella lettura (`CASH_*`) => `cassa_non_verificata:<motivo>`,
  ingresso in coda negato. La barriera `CASH_NOT_READ` (P&L/settlement applicato senza lettura) resta com'era.
  (B, Sol 1) Sblocco owner della storia insufficiente: oltre alla rilettura non deve esistere un'altra lettura iniziata
  nello stesso istante o dopo con un valore diverso (`DrawdownHardStop.readings_since`); una lettura concorrente dello
  stesso tick arrivata dopo la rilettura (nuova ambiguita' assorbita dallo stato gia' attivo) => sblocco rifiutato
  (`cassa_fresca_non_ordinata`), anche con lo stesso valore (precisazione owner: 130/130 senza prova temporale non
  sblocca); l'owner riprova con una rilettura successiva.
  (C, Sol 2) La lettura fuori ordine piu' bassa della cassa del gate si valuta SEPARATAMENTE contro la soglia LOCKDOWN
  del desk e contro i cap dell'hard-stop (assoluto e %): perdita dal picco oltre il cap => storia insufficiente
  (`lettura_fuori_ordine_limite_hard_stop`), mai un hard-stop; calo dimostrato => hard-stop come prima.

- **Ciclo 28 (PF-OWNER-PR631-C28-SETTLEMENT-FUNDS-FAILOPEN-20261116, Codex su b4b03636).** Validita' dell'ultima lettura
  del conto e necessita' di rileggere i fondi sono due informazioni distinte. `_apply_funds_reading` conserva l'esito
  della verifica della cassa della lettura applicata (`_funds_reading_block`: vuoto solo se `FUNDS_VERIFIED` con cassa
  del desk verificata, altrimenti il motivo). Il punto d'invio lo controlla: un aggiornamento contabile senza lettura
  (settlement `apply_closed_pnl`, snapshot dell'esposizione: `CASH_NOT_READ`) non lo cancella e non riapre un ingresso
  bloccato da una lettura incoerente o non verificabile. Solo una nuova lettura coerente lo toglie (poi decidono tutti
  gli altri gate); una nuova lettura fallita lo mantiene. Dopo un riavvio la lettura d'avvio lo ricalcola.
  Barriera di freschezza (Sol 1, PF-OWNER-PR631-C28-SETTLEMENT-FRESHNESS-20261116): `CASH_NOT_READ` non e'
  un'esenzione. Dopo una lettura del conto, un aggiornamento contabile senza lettura (settlement con P&L chiuso,
  snapshot dell'esposizione, ripristino) invalida la cassa e sposta la soglia di `read_at` del RiskDesk: fondi da
  rileggere (`fondi_da_rileggere:CASH_NOT_READ`), ingressi negati anche gia' in coda. Una lettura iniziata prima della
  barriera e completata dopo e' scartata dal RiskDesk e non la toglie; la toglie solo una lettura iniziata dopo, che
  rifissa la cassa (poi decidono tutti gli altri gate). Prima un ingresso in coda partiva nella finestra fra il P&L del
  settlement e la rilettura (P&L -15 con cap 10: 1 ordine, hard-stop solo dopo). Nessun LOCKDOWN persistente, nessun
  cap; settlement, cashout, cancellazioni e riconciliazione non passano da questo controllo.

- **Picco e superamento in un'unica scrittura (ciclo 22, PF-OWNER-PR631-C22-CRASH-PEAK-ATOMICITY-20261112).** Il
  picco che dimostra un superamento (un massimo osservato o confermato dall'owner, anche da una lettura tardiva) sale nello
  STESSO record persistito del superamento: un crash non puo' piu' lasciare su disco picco 115 con superamento da 130.
  Recovery dei record scritti prima: alla rilettura il picco e' almeno quello di un superamento attivo, non ribasato e
  della stessa epoca (un ribasamento owner esplicito resta valido). Picco del superamento attivo corrotto => stato
  illeggibile (fail-closed). Cosi' nessun crash, riavvio, sblocco o conferma owner riporta implicitamente il picco sotto
  il massimo dimostrato: lo sblocco non ribasa e la lettura successiva riblocca se la perdita supera ancora il limite.

- **Stato del bot al punto d'invio (M68, PF-OWNER-PR631-C20-FINAL-SAFETY-GATES-20261112).** Nessun nuovo ingresso
  parte se il bot non e' ACTIVE: PAUSA (`runtime_non_attivo:PAUSED`), fermo (`runtime_non_attivo:STOPPED`), LOCKDOWN
  del runtime (`runtime_lockdown:...`) o del desk (`desk_lockdown`: drawdown oltre la soglia di lockdown del money
  management, anche senza hard-stop configurati; stato del desk illeggibile => `desk_non_verificabile`), anche con
  l'autorizzazione tecnica ancora valida. Il controllo sta nel controllo finale
  d'ingresso del motore, nella stessa sezione critica del drawdown; una PAUSA arrivata dopo la risposta e' successiva
  all'ammissione (l'ordine eventualmente inviato si riconcilia). Cancellazioni, cashout e uscite protettive
  (OrderRouter) e la riconciliazione non passano di qui e restano consentiti. Gestione completa delle code, politiche
  di ripresa, scadenza e rivalutazione degli ingressi restano alla PR-05. F2: cambio del flag di modalita' e
  invalidazione dell'ambito sono una sola sezione critica con la dichiarazione dell'ambito; un test deterministico
  dimostra che una lettura SIM concorrente al passaggio a LIVE non lascia dichiarato l'ambito SIM.

- **Controllo finale, scrittori stantii e picco confermato (D-631-13/16/17/18, ciclo 17).** Il controllo finale
  d'ingresso (`drawdown_entry_block_reason`) rifiuta anche qualsiasi LOCKDOWN del runtime, qualunque ne sia l'origine
  (amministrativo, rischio, desk), per segnali, ordini manuali (`REQ_QUICK_BET`), Dutching e auto-next, che arrivano
  tutti allo stesso handler del motore; Emergency Stop e STOP BOT (autorita' rilasciata o in svuotamento) restano i
  controlli dedicati subito prima e subito dopo. L'intera decisione e' una sola sezione critica sotto lo stesso lock
  in cui ogni lettura del conto registra un superamento: un superamento registrato prima della risposta non viene mai
  ignorato; uno registrato dopo e' successivo all'ammissione e l'ordine eventualmente inviato si riconcilia. Un
  controller mai avviato non scrive lo stato del drawdown su un database governato da un runtime (lease presente,
  anche scaduta o rilasciata): dopo lo STOP del proprietario la sua pausa non riscrive piu' il picco ne' cancella il
  superamento. In piu' ogni salvataggio usa la concorrenza ottimistica sul `seq`: uno stato in memoria piu' vecchio
  del record su disco non lo sovrascrive mai (nessun marker), l'istanza nega gli ingressi finche' non rilegge lo stato
  (START o procedura owner). La conferma owner della baseline registra istante ed epoca (numero progressivo delle
  conferme, persistito nella storia della baseline; adottata anche da un altro processo alla rilettura) e il picco
  confermato (massimo fra salvato, dichiarato e cassa, mai ribassato), senza cancellare la cronologia delle letture.
  Una lettura iniziata dalla conferma in poi, anche arrivata fuori ordine, misura il calo da quel picco. Una lettura
  iniziata prima della conferma si valuta sempre come lettura tardiva, anche se arriva in ordine (ciclo 18, G17-1):
  il picco dichiarato non ha un istante noto ed e' per lei solo un limite superiore. Calo dimostrato dalle letture
  conservate => hard-stop; calo possibile solo rispetto al picco dichiarato => storia insufficiente (ingressi negati,
  mai un hard-stop non dimostrato); nessun calo possibile oltre il limite => nessun blocco dal drawdown.

- **Precedenza delle prove, ripristino integrale e ambito dello stato (ciclo 19: S19-A, S19-B, S18-1).** Per una
  lettura tardiva si cercano prima tutte le prove di un calo fra letture vere conservate con il loro istante (anche
  quelle con istante precedente all'apertura della finestra): dal massimo delle letture vere lette prima di lei, dal
  riferimento esatto, e dal minimo letto dopo di lei. Solo senza prova un limite superiore (picco confermato di istante
  ignoto, base della finestra) porta alla storia insufficiente: un calo dimostrato resta sempre un hard-stop (S19-A,
  110 -> 90 con limite 15). Un ribasamento owner con salvataggio fallito ripristina tutto lo stato precedente, picco
  confermato e istante della conferma inclusi (S19-B). Lo stato persistito porta la **provenienza**: ambiente (`SIM`,
  `LIVE`, oppure una sorgente non marcata) e identita' del conto (in LIVE l'hash dello username di login Betfair, mai il
  nome in chiaro), ricavati da ogni lettura del conto. Protezione fail-closed fino alla separazione completa per
  ambiente e conto della PR-04: con uno stato di un altro ambito (`drawdown_stato_di_altro_ambito`), senza provenienza
  (`drawdown_stato_senza_provenienza`, record scritto prima della protezione: quarantena) o con un conto LIVE non
  identificabile (`drawdown_conto_non_identificabile`) nessuna valutazione, nessuna scrittura, nessuna procedura owner
  (ribasamento, sblocchi, conferma) e nessun nuovo ingresso su ogni percorso, controllo finale prima dell'invio compreso.
  Lo stato salvato resta intatto: nessun azzeramento del picco. Tornando all'ambito del record il drawdown riprende con
  il suo picco. Un cambio di modalita' SIM/LIVE (`set_simulation_mode`) invalida l'ambito gia' dichiarato: fino alla
  prima lettura del conto nella nuova modalita' gli ingressi sono negati (`drawdown_conto_non_identificabile`), anche quelli
  gia' in coda. Una lettura della modalita' precedente consegnata dopo il cambio non si valuta e non riapre gli ingressi;
  il ramo del disponibile zero usa l'ambito della propria lettura come gli altri. Verifica della modalita', dichiarazione dell'ambito e
  cambio modalita' (con l'invalidazione) sono una sola sezione critica: un passaggio a LIVE concorrente non lascia
  dichiarato l'ambito SIM (Sol su 559b39ca). Uno stato con dati finanziari (picco,
  superamento, storia insufficiente) e provenienza assente o nulla resta in quarantena anche dopo le scritture di
  sessione, che non lo attribuiscono mai d'ufficio, nemmeno con l'ambito gia' dichiarato da una lettura (Grok su 559b39ca). L'unica uscita e' la procedura owner `owner_assign_drawdown_scope` (tracciata, stesse verifiche degli
  sblocchi, nessun ordine AMBIGUOUS): attribuisce lo stato all'ambito corrente senza reset (picco, superamento e storia
  insufficiente restano) e rimette la baseline da confermare. Emergency Stop, STOP BOT e LOCKDOWN restano globali.

## PR-04a — perdita giornaliera e risk-stop per identita' finanziaria (PF-OWNER-PR04A-SCOPE-MM029-GO-20261116)

Decisione owner `PF-OWNER-PR04A-LIVE-GLOBAL-SIM-LOCAL-20261116` (opzione 1, LIVE globale / SIM locale). Codice:
`core/financial_scope.py`, `core/daily_loss_store.py` (formato v2), `core/runtime_controller.py`; test
`tests/d1/test_pr04a_financial_scope.py`.

- **Identita' finanziaria.** `provider_id|environment_id|account_id|capital_scope`: `betfair|SIM|sim|EUR` per la SIM,
  `betfair|LIVE|betfair:<hash dello username>|EUR` per il LIVE (mai lo username in chiaro). EUR e USDC non condividono
  mai un ambito; nessun adapter SX Bet. Ambito LIVE senza conto identificato, ambiente sconosciuto o campo mancante =
  **non verificabile**.
- **P&L per ambito.** Il P&L di un settlement va all'ambito della scommessa, registrato nella riserva del tavolo
  (`meta.financial_scope`, modalita' del runtime all'invio) per ogni percorso d'ingresso: segnale, auto-next, Dutching e
  puntata manuale (`DutchingController._allocate_table`). L'ambito e' persistito con la request in `orders` e
  ricostruito da F018 dopo un riavvio; il settlement lo ritrova dalla riserva (tavolo+evento, evento o lotto) letta
  PRIMA del rilascio (solo la riserva della stessa scommessa: stesso evento e stesso `customer_ref`; l'ambito portato dal
  rilascio prevale sempre, anche vuoto) e, per un tavolo gia' riassegnato a un'altra riserva, dalla request persistita della scommessa
  regolata (mai dall'occupante attuale del tavolo; identita' non recuperabile => quarantena). Riserva trovata ma senza ambito (ordine precedente a PR-04a) => perdita non attribuibile
  sull'ambito `betfair|UNVERIFIED||EUR` (origine non verificabile: escalation globale; conta anche per ogni ambito
  esistente, LIVE compreso), profitto non accreditato: mai l'ambito corrente per supposizione. Anche il ramo recovery
  fail-closed proietta la perdita non attribuibile su ogni ambito. Totale del desk e
  attribuzione si aggiornano insieme sotto il lock dello stato giornaliero (nessun monitor concorrente li vede a meta'). Senza identita' dimostrata: una PERDITA va all'ambito
  corrente se verificato, altrimenti e' **non attribuibile** (quarantena: conta per ogni ambito fino al cambio di
  giorno; ogni stato d'ambito gia' esistente la registra e la persiste subito, PRIMA della marcatura del settlement,
  cosi' al riavvio la quarantena si combina col minimo senza perdere perdite; anche la perdita di sessione si rivaluta in
  ogni ambito interessato); un PROFITTO non si accredita a nessun ambito. Nessuna compensazione SIM/LIVE ne'
  fra conti LIVE. Un superamento SIM proiettato arma una barriera d'ingresso LOCALE prima della sincronizzazione del
  bankroll e lo registra in modo durevole (stato breached dell'ambito salvato prima dell'I/O; nessun pending-stop ne'
  marker globale); per una perdita non attribuibile ambito attribuito, altri ambiti e quarantena si salvano con UNA
  sola scrittura del libro; una perdita non attribuibile si proietta su ogni ambito esistente
  e arma le barriere (globale o locale) di ciascuno.
- **Superamento del limite giornaliero.** SIM verificata: bloccati solo i nuovi ingressi di quell'ambito
  (`daily_loss_breached:SIM`); nessun Emergency Stop, nessuna cancellazione di ordini LIVE, LIVE non disattivato,
  nessun LOCKDOWN globale, nessun pending-stop globale. LIVE o ambito non verificabile: Emergency Stop globale come
  prima (SIM e LIVE bloccati; passare a SIM non lo scioglie), armato e persistito anche quando il superamento arriva da
  un settlement a runtime fermo (STOPPED/LOCKDOWN; marker d'emergenza durevole scritto PRIMA dello stato, tolto solo a
  settlement completato), cosi' un avvio successivo in SIM o con un altro conto non riapre gli ingressi. Emergency Stop manuale: sempre globale, invariato.
- **Perdita di sessione e RISK_STOP.** La perdita di sessione si misura nell'ambito corrente. Un RISK_STOP nato da una
  SIM verificata resta locale (motivo con suffisso `@ambito=<ambito SIM>`): blocca solo quell'ambito, il CASHOUT_ALL
  parte solo mentre il runtime e' in quell'ambito (mai instradato al LIVE), il reset si fa da quell'ambito
  (`risk_stop_di_altro_ambito` altrimenti). LIVE, ambito non verificabile o stop senza ambito (esposizione): globale.
  La perdita di sessione si valuta nell'ambito del settlement (un `cleared:` LIVE regolato mentre il runtime e' in SIM
  arma lo stop globale). Il latch dell'allarme di sessione e' per ambito; uno stop globale successivo SOSTITUISCE uno stop SIM locale
  (escalation, persistita, con CASHOUT_ALL), mentre uno stop locale non sostituisce mai un globale.
- **Controllo finale d'invio.** Il superamento giornaliero dell'ambito corrente (in attesa o registrato oggi, o stato
  non salvato) blocca anche un ingresso gia' approvato e in coda (`drawdown_entry_block_reason`, punto d'invio del
  TradingEngine), con lo stesso motivo dell'ammissione (`daily_loss_breached:SIM`); per la SIM resta locale, nessuna
  emergenza globale (Codex su c5dd0bab). Vale per ogni produttore di ingressi: segnali, puntata manuale e ogni gamba
  Dutching (M111, `PF-OWNER-PR638-M111-SIM-ENTRY-BYPASS-GO-20261116`); il DutchingController rifiuta inoltre in modo
  esplicito all'ammissione (`Limite giornaliero dell'ambito superato: daily_loss_breached:SIM`; lettura fallita =>
  rifiuto). CASHOUT ALL e le uscite protettive non passano da questi controlli e restano disponibili.
- **Persistenza (v2).** `daily_loss_state = {"version": 2, "scopes": {ambito: {day_utc, intraday_realized_pnl,
  breached, breached_at}}, "legacy": {...}|null, "settled": {"day_utc", "keys"}|null, "seq": n}`. `settled` porta le
  chiavi dei settlement gia' realizzati oggi, nella STESSA scrittura dello stato: al riavvio entrano fra i settlement
  realizzati, quindi una riconsegna (anche dopo un crash fra lo stato e la marcatura) non conta due volte il P&L; una
  consegna gia' realizzata non registra mai un superamento proiettato. La chiave di un settlement appena applicato resta
  in attesa e diventa salvabile solo nella stessa sezione critica (lock dello stato giornaliero) che ricalcola e salva lo
  stato dell'ambito interessato (per una perdita non attribuibile: dopo il ricalcolo di TUTTI gli ambiti); un salvataggio
  concorrente (es. `get_status`) non puo' rendere durevole la chiave con lo stato vecchio, quindi dopo un crash la
  riconsegna riapplica l'effetto invece di saltarlo (Sol/Codex su c5dd0bab). Una perdita non attribuibile tocca la
  quarantena e TUTTI gli ambiti: mentre e' in corso, ogni salvataggio intermedio del libro (es. `get_status` durante il
  sync) e' rinviato, e il libro si salva una sola volta con tutti gli ambiti, la quarantena e la chiave insieme (mai
  quarantena nuova accanto ad ambiti vecchi = sottostima, Sol su 385c7598; mai ambito gia' ricalcolato senza la chiave
  = doppio conteggio dopo la riconsegna, Grok su 3389ace9). Se il settlement si interrompe con un'eccezione dopo
  l'applicazione, il libro si completa comunque, cosi' i salvataggi non restano rinviati; se in quel libro un ambito
  LIVE (o non verificabile) risulta superato, il marker durevole dell'Emergency Stop e il pending-stop restano: al
  riavvio l'emergenza globale si ripristina anche se la chiave gia' salvata fa saltare la riconsegna (D su 4e1ff71b).
  Se fallisce anche il completamento, la perdita esce dall'attesa (i salvataggi successivi non restano rinviati in
  silenzio), il gate segnala `daily_loss_state_non_persistito` finche' un salvataggio riesce e il marker globale resta
  (Sol su 4e1ff71b). Ogni ambito si scrive, anche a zero (uno zero omesso
  farebbe risorgere un profitto piu' vecchio di un marker rimasto).
  Record v1 (senza ambito) = quarantena `legacy`, applicata a ogni ambito solo come perdita (mai come profitto) e col
  suo breach, mai attribuita in automatico a un conto. Db e marker si riconciliano per ambito (seq piu' alta,
  altrimenti perdita maggiore; breach se uno dei due); la `seq` riparte sopra quella ripristinata. Stato illeggibile
  => breach per ogni ambito fino al nuovo giorno. Un crash durante la prima scrittura v2 lascia il marker v2, che al
  riavvio si riconcilia con il record v1 senza perdere ne' la quarantena ne' le perdite d'ambito.
- **Ciclo straordinario 11 (owner, opzione «A first, then B»; C1, S1, C3, G1, C2, S2).**
  - *Emergenza da esposizione LIVE (C1).* L'Emergency Stop nato da un superamento LIVE o non verificabile
    (`emergency_stop(..., live_exposure=True)`) agisce sulla sede LIVE qualunque sia la modalita' corrente del runtime
    (prima, col runtime in SIM, leggeva e cancellava solo il broker simulato). Blocco dei nuovi ordini, cancellazione e
    verifica restano distinti; conto LIVE irraggiungibile o ordini estranei aperti => `manual_action_required`, mai una
    cancellazione dichiarata riuscita.
  - *Marker dopo un'eccezione (S1).* Superamento globale proiettato ed effetto gia' applicato, ma settlement interrotto
    prima dell'enforce: il marker durevole e il pending-stop restano, quindi al riavvio l'Emergency Stop si ripristina
    anche se la chiave salvata fa saltare la riconsegna.
  - *Perdita di sessione LIVE durante il sync (C3).* Dopo l'applicazione e prima del sync, una perdita di sessione di un
    ambito che sale al globale arma una barriera dedicata (`_session_loss_pending_global`) letta sia dall'ammissione
    (`_limits_block_reason`, qualunque sia l'ambito corrente, anche un altro conto LIVE) sia dal controllo finale
    d'invio (comandi gia' in coda): nessun nuovo ingresso nella finestra, ma il CASHOUT_ALL dello stop globale non viene
    bloccato. Tolta quando
    lo stop e' registrato, da `reset_risk_stop` o `reset_emergency`.
  - *Prenotazione legacy (G1).* Settlement `cleared:` con prenotazione nota ma senza ambito: quarantena, mai il conto
    LIVE connesso al posto dell'identita' della prenotazione (`sim:` resta SIM per costruzione).
  - *Puntata manuale e Dutching (C2).* L'ambito viaggia nella request di ogni gamba, quindi nel payload in `orders`:
    dopo un riavvio F018 lo ricostruisce e una perdita SIM tardiva resta SIM (mai quarantena ne' ES globale).
  - *Doppio guasto (S2).* Se fallisce anche il completamento del libro, la chiave resta in attesa e il salvataggio
    successivo completa il libro (tutti gli ambiti + quarantena + chiave insieme); fino ad allora salvataggi rinviati e
    gate `daily_loss_state_non_persistito`.
- **Ciclo straordinario 12 (owner, sicurezza degli ordini LIVE).**
  - *Riavvio in emergenza.* Un Emergency Stop di provenienza LIVE (o ignota) ripristinato al riavvio riconcilia gli
    ordini LIVE del bot: cancellazione verificata sul conto LIVE, ordini estranei mai cancellati e sempre elencati. Si
    tenta alla costruzione e si ritenta al primo settlement o segnale finche' la verifica riesce; esito non verificato,
    conto irraggiungibile o esposizione residua => emergenza mantenuta, evento `EMERGENCY_STOP_MANUAL_ACTION_REQUIRED`
    ed esposizione irrisolta salvata (`emergency_unresolved_exposure`). Evento d'esito: `EMERGENCY_STOP_RECONCILED`.
  - *Marker LIVE col runtime in SIM.* Il marker durevole di un superamento LIVE (o non verificabile) registra la sede
    LIVE anche col runtime in SIM: prima registrava SIM e al riavvio la riconciliazione sarebbe andata sulla sede
    sbagliata.
  - *Stop di sessione LIVE col runtime in SIM.* Oltre al CASHOUT_ALL della sede corrente, sweep verificato sulla sede
    LIVE (`RISK_STOP_LIVE_PROTECTION`): cancellati solo gli ordini del bot non abbinati; posizioni abbinate, ordini
    estranei o verifica fallita => esposizione irrisolta segnalata, mai dichiarata chiusa. L'hedge di posizioni LIVE
    abbinate da un runtime in SIM non e' automatico (limite dichiarato: intervento manuale).
  - *Perdita non attribuibile e sessione.* La barriera globale di sessione si valuta su ogni ambito interessato
    (anche cumulativamente), non solo su quello attribuito.
- **Ciclo 12, estensione (owner: S-a, S-b, R1, R2).**
  - *Atomicita' della barriera di sessione (S-a).* La perdita di sessione si proietta PRIMA di applicare il P&L, su
    ogni ambito interessato; se un ambito che sale al globale supera il limite, la barriera
    `_session_loss_pending_global` si arma sotto `_daily_loss_stop_lock` prima che il settlement sia visibile (letta
    da ammissione e controllo finale d'invio), solo per un settlement che verra' davvero realizzato.
  - *Persistenza dello stop di sessione (S-b).* Prima dell'applicazione si salva il marker durevole
    `session_loss_stop_pending`; si toglie solo dopo che lo stop globale e' durevole e pubblicato. Settlement interrotto
    (anche con il solo `max_session_loss`): RISK_STOP globale ripristinato subito dal marker; crash: ripristinato al
    riavvio. In entrambi i casi la riconciliazione LIVE resta in attesa (cancellazione verificata o esposizione
    segnalata). Stesse garanzie della perdita giornaliera, anche quando e' configurato un solo limite.
  - *Esposizione LIVE esplicita (R1).* `get_status()["live_exposure"]`: `state` (`NESSUNA`, `VERIFICATA`,
    `NON_RICONCILIATA`, `SCONOSCIUTA`), `amount` (None = sconosciuto, mai 0), ordini del bot aperti, ordini estranei,
    posizioni abbinate, verifica, riconciliazione in attesa, intervento manuale; persistito in
    `emergency_unresolved_exposure` (anche dopo il riavvio e con il runtime in SIM). `total_exposure` resta
    l'esposizione dei tavoli locali (significato invariato). Mostrato nella mini GUI (`Esposizione LIVE:`), in headless
    all'avvio (CRITICAL) e nel preflight LIVE, che non e' mai PRONTO con esposizione irrisolta o sconosciuta.
  - *Idempotenza del motivo (R2).* Il pending-stop globale e il suo marker (che scrive il motivo dell'emergenza) nascono
    solo per un settlement che verra' realizzato: una riconsegna gia' contabilizzata non riscrive il motivo (prima 35
    diventava 50) ne' arma barriere.
- **Ciclo 12, M115 (owner GO): recovery LIVE senza riavvio.** Pickfair non dipende dal riavvio per gestire gli ordini
  LIVE residui.
  - *Settlement interrotto, processo vivo.* Errore (anche `SystemExit`) dopo l'applicazione del P&L con superamento
    globale proiettato e marker durevole gia' scritto: nel `finally` si applica SUBITO lo stop che il riavvio
    ripristinerebbe dal marker, cioe' l'Emergency Stop sulla sede LIVE (`_recover_interrupted_global_stop`). Per la
    perdita di sessione, dopo il ripristino del RISK_STOP dal marker parte subito la riconciliazione LIVE.
  - *Recovery dedicato* (`_ensure_live_recovery`). Finche' `_emergency_reconcile_pending` resta vero, un thread daemon
    (`pickfair-live-recovery`, uno per runtime, riferimento debole) ritenta la riconciliazione verificata ogni
    `_LIVE_RECOVERY_INTERVAL_S` secondi (30), senza attendere segnali o settlement. Si avvia quando uno sweep LIVE
    (Emergency Stop, protezione LIVE del RISK_STOP, riconciliazione al riavvio) resta irrisolto e si ferma da solo
    quando la verifica riesce o non c'e' piu' uno stop globale. Lo stato da riconciliare e' durevole (marker
    d'emergenza di provenienza LIVE, marker di sessione, `emergency_unresolved_exposure`): al riavvio, anche durante una
    riconciliazione interrotta, il recovery riparte.
  - *Mai in parallelo.* Gli sweep LIVE passano da un lock dedicato: una riconciliazione concorrente a uno sweep in corso
    non legge e non cancella (nessuna cancellazione duplicata).
  - *Esiti.* Si cancellano solo gli ordini del bot, con verifica sul conto LIVE; estranei, posizioni abbinate,
    cancellazioni fallite o conto irraggiungibile restano `NON_RICONCILIATA`/`SCONOSCIUTA` con intervento manuale,
    mai dichiarati chiusi senza conferma dell'exchange. La verifica riuscita non riapre gli ingressi (solo
    `reset_emergency()`).
  - *Stato.* `get_status()` espone anche `live_exposure_unresolved` e `live_exposure_manual_action_required` (piatti,
    derivati da `live_exposure`; `SCONOSCIUTA` e' irrisolta).
- **Ciclo 12, R1 liability LIVE (owner GO) e rilievi Codex su `afbd5c89`.**
  - *Liability, non stake* (`live_liability`, `Decimal`): BACK = stake; LAY = stake x (quota - 1). La parte non abbinata
    si calcola su `size_remaining` alla quota limite dell'ordine; la parte abbinata su `size_matched` alla quota media
    di esecuzione. Un ordine parzialmente abbinato conta residuo e abbinato una volta sola ciascuno; gli ordini
    estranei portano residuo e abbinato nella stessa voce. Valore = **stima conservativa lorda** (`amount_basis =
    STIMA_CONSERVATIVA_LORDA`), senza compensazioni fra esiti alternativi dello stesso mercato; arrotondamento per
    eccesso al centesimo. Lato, quota o stake mancanti o non validi (quota LAY <= 1, NaN/inf, stake negativo) =>
    importo `SCONOSCIUTO` (`amount` None), mai zero e mai lo stake. `live_exposure["liability"]` riporta `unmatched`,
    `matched`, `total` e le voci non calcolabili.
  - *Posizioni abbinate.* La cancellazione non chiude una posizione abbinata: con `matched_positions` lo sweep richiede
    intervento manuale, l'esposizione e' `NON_RICONCILIATA` (mai `VERIFICATA` ne' zero) e il recovery resta attivo.
  - *Riavvio.* Un record durevole dell'esposizione irrisolto (o illeggibile, o legacy risolto ma con posizioni
    abbinate) riarma la riconciliazione e il recovery dedicato all'avvio, anche quando lo stop globale e' un RISK_STOP
    di sessione gia' completato.
- **M130 (owner): invariante durevole dello stop, N4 e N5.**
  - *Invariante.* Nessuno stato recuperabile dopo crash con un settlement registrato e lo stop obbligatorio assente.
    Il marker dello stop di sessione si toglie SOLO se una rilettura dal disco (`_durable_global_stop_confirmed`:
    `restore_risk_stop` fail-closed o Emergency Stop salvato) conferma uno stop globale durevole; una scrittura che
    restituisce `False`, un'eccezione o una lettura fallita non lo confermano mai. Vale alla fine del settlement e nel
    ripristino al riavvio (che non si fida di uno stop solo in memoria). Marker conservato => stop in memoria,
    riconciliazione LIVE e recovery dedicato nel processo; al riavvio con disco scrivibile lo stop diventa durevole e
    il marker si toglie (nessun blocco permanente).
  - *Libro giornaliero verificato.* `save_settings` non ha un contratto di ritorno: il libro vale salvato solo se
    RILETTO identico (`daily_loss_store._save_verified`); altrimenti marker di riserva e stato «non salvato» (cancello
    d'ingresso). Una scrittura persa senza eccezione non fa piu' sparire la perdita al riavvio.
  - *Blocco globale dal libro.* Al riavvio, un ambito LIVE (o non verificabile, compresa la quarantena) superato OGGI
    nel libro salvato ricostruisce l'Emergency Stop anche se la sua scrittura era fallita, senza attendere la
    riconsegna (`_restore_breached_live_scope_stop`); un superamento solo SIM resta locale.
  - *Ex limite residuo F1.* Chiuso da M134 (sessione di rischio persistente, sotto): la riconsegna dopo un crash prima
    di qualunque scrittura conta nella sessione di rischio ripristinata.
  - *N4 (regressione rispetto a main).* Lo stato giornaliero dell'ambito del settlement nasce PRIMA del P&L, anche
    senza `max_daily_loss`: un limite attivato a meta' giornata vede la perdita gia' regolata (35, non 15).
- **M134/M136 (owner): F1 sessione di rischio persistente, N6 conto dello stop, N3 posizioni estranee, N7 verifica.**
  - *F1 — sessione operativa e sessione di rischio.* La sessione OPERATIVA e' il ciclo `start()`/`stop()`; la sessione
    di RISCHIO e' quella su cui si misura `max_session_loss` (sostituisce P37 «perdita dall'ultimo start()»). Ha un id,
    l'istante di apertura e il P&L di sessione per ambito, salvati nel libro giornaliero verificato
    (`risk_session`, nella STESSA scrittura delle chiavi dei settlement: ogni P&L contato una volta sola) e
    sopravvive al cambio di giornata. L'apertura e' salvata anche in una chiave propria (`risk_session_open`) PRIMA
    di ogni effetto del settlement, cosi' un crash prima del libro non la perde. Al riavvio la sessione riprende con la
    sua perdita (base = «adesso meno il P&L di sessione salvato»), e lo stop resta: un riavvio, uno `stop()`/`start()`,
    un cambio SIM/LIVE o di conto non azzerano la perdita ne' tolgono lo stop.
  - *Quando si apre e quando si chiude.* Si apre al primo `start()` senza sessione aperta (o alla prima base di
    sessione). Si chiude SOLO (1) con un `reset_risk_stop()` esplicito riuscito — mai all'avvio — che chiude la
    sessione superata se il ricontrollo completo (limite giornaliero, Emergency Stop, esposizione, altri limiti) e'
    pulito e l'esposizione LIVE e' `NESSUNA` o `VERIFICATA`; (2) con `close_risk_session()` esplicito, ammesso solo
    senza stop, senza superamento e senza esposizione LIVE irrisolta. La sessione nuova (perdita zero) si salva PRIMA
    di togliere lo stop; se il libro non e' verificato il reset e' rifiutato (`sessione_di_rischio_non_salvata`) e tutto
    resta com'era.
  - *G1 — reset confermato e tracciato (owner M138/M139).* Il reset che CHIUDE una sessione superata richiede la
    doppia conferma dell'operatore: `request_risk_session_reset(operator, authorization, reason)` (prima conferma:
    sfida monouso legata a operatore, autorizzazione e sessione, scadenza `_RESET_CHALLENGE_TTL_S` = 120 s, con
    riepilogo di sessione, perdita, conti LIVE ed esposizione) e poi `reset_risk_stop(operator=…, authorization=…,
    challenge_id=…, confirm=True, confirm_again=True)`. Rifiuti espliciti: `conferma_operatore_richiesta`,
    `doppia_conferma_richiesta`, `conferma_non_richiesta_o_diversa`, `operatore_o_autorizzazione_diversi`,
    `conferma_scaduta`, `conferma_gia_usata` (monouso), `sessione_cambiata`, `registro_non_verificabile`,
    `audit_non_scrivibile`. Protocollo atomico verificabile nel registro append-only (`core/risk_session_ledger.py`,
    tabella `audit_events`, catena di hash): `RESET_STARTED` (operatore, autorizzazione, motivo, sessione, nuova
    sessione, perdita, limite, conti, esposizione, motivo dello stop, istante) riletto PRIMA di ogni cambio di stato;
    sfida consumata; transizione della sessione; `RESET_COMPLETED` riletto = **punto di commit** (M144); poi lo stop si
    toglie e infine la sessione chiusa entra nell'archivio (`SESSION_CLOSED`). La transizione (fotografia della
    sessione che si chiude, passaggio alla nuova, libro e chiave di apertura) avviene tutta sotto il lock che applica
    ogni P&L (`_switch_risk_session`): un settlement concorrente cade per intero nella sessione chiusa o nella nuova,
    mai in nessuna (CX-F2). Senza commit il reset si ANNULLA (`_rollback_reset_switch`): la sessione d'origine torna
    corrente con l'eventuale P&L entrato nel frattempo, l'id nuovo resta registrato come ritirato (mai riusato),
    `RESET_FAILED`, stop presente. Un crash si risolve all'avvio PRIMA di archiviare: reset senza commit => annullato
    (`RESET_FAILED` recuperato); reset committato con lo stop ancora attivo => completato dal registro (stop tolto solo
    se e' ancora quello confermato, se la sessione corrente e' quella aperta da quel reset e non e' a sua volta oltre il
    limite registrato; evento `RISK_STOP_RESET` con l'operatore). Lo stato durevole e' sempre uno dei due: stop + sessione
    d'origine corrente + nessun `RESET_COMPLETED`, oppure nessuno stop + sessione chiusa + `RESET_COMPLETED` (G1e/N1).
    Un errore del registro e' un esito del reset (`audit_non_scrivibile`), mai un'eccezione propagata (G-F). Stesso
    contratto per GUI (`RESET SESSIONE DI RISCHIO`, due conferme), headless (`--risk-session-reset --operator=…
    --authorization=… --reason=… --confirm --confirm-again`, rc 0 solo a reset riuscito, 4 altrimenti; non avvia il
    trading) e API (i due metodi del runtime, unico ingresso). Il reset non tocca gli altri limiti. Headless (CX-F4):
    con uno stop globale e l'esposizione LIVE non ancora verificata, la CLI si connette al conto LIVE configurato ed
    esegue la stessa riconciliazione dell'avvio (solo ordini del bot, sola verifica se lo stop e' nato col runtime
    LIVE, N6: un conto diverso da quello dello stop non certifica nulla) prima della richiesta; nessun ingresso, nessun
    avvio del trading; conto irraggiungibile => reset rifiutato (rc 4). Il latch del superamento di sessione e' per
    sessione di rischio: dopo il reset il superamento della sessione nuova arma di nuovo RISK_STOP, evento e
    CASHOUT_ALL (CX-E3). La modalita' di sola verifica dello sweep LIVE (N7) e' salvata
    (`emergency_live_verify_only`) PRIMA che il CASHOUT_ALL possa piazzare una copertura: dopo un riavvio, anche se il
    processo muore dentro il cashout, la riconciliazione non annulla le coperture; valore illeggibile => sola verifica
    (CX-E4, Codex su f8feabac).
  - *Conto dello stop durevole (CX-G1, owner M145).* In `risk_stop()` il conto LIVE che genera uno stop globale (N6) e
    l'invalidazione dell'esito LIVE precedente si salvano PRIMA dello stop durevole; il vincolo vale solo se riletto. Il
    marker dello stop di sessione, al ripristino, lega il conto del suo ambito e si toglie solo con stop E vincolo
    durevoli (altrimenti resta e il riavvio ricrea il vincolo). Nessun crash lascia uno stop su disco senza il suo conto:
    il conto B non certifica mai l'esposizione del conto A; quando A torna raggiungibile la riconciliazione su A procede
    e il reset diventa possibile.
  - *M146 (owner, ciclo integrato H1–H4).* Invarianti:
    - **I1 (CX-H2).** Ogni conto LIVE che supera il limite entra nel vincolo dello stop globale, anche se uno stop
      esiste gia': il conto B si aggiunge (PENDING), l'esito verificato del conto A non vale piu', riconciliazione e
      protezione ripartono; il reset resta rifiutato finche' B non e' riconciliato.
    - **I2 (CX-H1, opzione A).** La modalita' dello sweep LIVE si salva PRIMA dello stop ("1" sola verifica col runtime
      LIVE, "0" cancellazione dei soli ordini del bot se lo stop nasce in SIM) ed e' anche nel marker dello stop di
      sessione (`runtime_live`). Al riavvio, con uno stop globale attivo e la modalita' mancante o illeggibile, la
      riconciliazione riparte in SOLA LETTURA: lo stop resta, nessun ordine nuovo, nessuna cancellazione o cashout alla
      cieca, nessuna copertura dichiarata chiusa senza riscontro dell'exchange; esposizione irrisolta e intervento
      manuale. Il marker si toglie solo con stop, vincolo e modalita' durevoli.
    - **I3 (CX-H3).** L'archivio ricostruito dal registro non dipende dall'ordine delle scritture: un
      `LATE_SETTLEMENT` scritto prima del `SESSION_CLOSED` della sua sessione si applica quando la chiusura compare,
      una volta per chiave; la fotografia della chiusa e' una copia coerente presa sotto il lock dei tardivi.
    - **I4 (CX-H4).** La sessione corrente contiene solo P&L di origine verificata: il PROFITTO di una sessione
      registrata ma non verificabile resta nel desk e nel giornaliero, fuori dalla sessione corrente; la PERDITA non
      verificabile resta conservativa (stop `sessione_di_rischio_non_verificabile`).
  - *M147 (owner): registro unico dei superamenti LIVE.* La chiave `emergency_live_accounts` (formato v2:
    `{"version": 2, "accounts": {ambito: {"state": PENDING|VERIFIED, "breaches": [...]}}}`; il v1 `{ambito: stato}` si
    legge ancora) registra per ogni conto LIVE i suoi superamenti (chiave idempotente, tipo, limite, perdita, sessione di
    rischio, fonte, istante). Ogni superamento LIVE giornaliero entra nel registro dal settlement e dal libro all'avvio
    QUALUNQUE sia lo stato degli stop (CX-I1: un Emergency Stop gia' attivo per il conto A non nasconde il superamento
    del conto B); un superamento nuovo rimette il conto PENDING, invalida l'esito LIVE e fa ripartire la riconciliazione.
    Lo stesso superamento gia' registrato su un conto VERIFICATO non lo riapre; un registro illeggibile non viene mai
    sovrascritto (nessun conto certifica). Ogni conto si riconcilia separatamente; l'esposizione e' VERIFICATA solo con
    tutti i conti verificati. `get_status()["live_exposure"]["accounts"]` mostra lo stato per conto.
    **Cambio di contratto (R6):** `reset_emergency()` e' rifiutato (`emergency_reset: False`,
    `reason: esposizione_live_irrisolta`, `accounts_pending`) finche' un conto del registro e' PENDING o il registro e'
    illeggibile; prima toglieva l'Emergency Stop senza verifiche (solo API, nessuna GUI/CLI lo chiama). Un Emergency Stop
    senza conti registrati (manuale, SIM) si toglie come prima. Impostazioni non leggibili (es. lock SQLite, anche
    l'emergenza `EMERGENCY_STATE_UNREADABLE` all'avvio) => registro non leggibile => reset rifiutato; riapre quando le
    impostazioni tornano leggibili e nessun conto e' PENDING (`tests/safety/test_emergency_stop.py`).
    **M148:** un registro illeggibile non si toglie mai dall'app; l'uscita e' la procedura operatore
    [`ops/live_registry_recovery.md`](../ops/live_registry_recovery.md) (ricostruzione da fonti verificabili, tutti i
    conti PENDING, riconciliazione reale di ogni conto, doppia conferma). Svuotare la chiave o scrivere VERIFIED a mano
    e' un aggiramento vietato.
    **M149 (owner, A + C):** un obbligo LIVE (stop LIVE, `live_exposure`, superamento LIVE o non verificabile) con
    conto NON identificabile registra la sentinella `betfair|LIVE|<conto_non_identificato>|EUR` PENDING: un ambito non
    verificabile non e' mai attribuito al conto collegato; nessuno sweep certifica la sentinella (la sessione senza
    identita' cancella ancora per betId i soli ordini del bot; un conto identificato estraneo non legge ne' cancella,
    N6). Uscita solo con la procedura M148 (passo 4.4: conti identificati PENDING e data di attribuzione
    `emergency_live_unidentified_attributed_through`, valida solo per i superamenti giornalieri fino a quel giorno).
    C: Emergency Stop LIVE (`emergency_settlement_source = "LIVE"`, salvata col flag) o esito LIVE irrisolto su disco
    con registro vuoto = incoerenza: nessun conto certifica, `reset_emergency()` rifiutato con
    `registro_incoerente_obbligo_live`. Un Emergency Stop solo SIM resta resettabile. Cambio di contratto: un ES LIVE
    con sessione senza conto di login non si toglie piu' con il solo reset (test esistenti adeguati: riconciliazione
    reale prima del reset, conto di login nei finti LIVE).
    **M147-D1:** il reset di uno stop non cancella verifiche ed esito LIVE se resta attivo l'altro stop globale (ES o
    RISK_STOP): ES + stop di sessione si sciolgono senza stallo quando tutti i conti sono verificati.
    **M151 (owner, F1 + F2 integrati, disegno M150 rev. 3):**
    - *Coperture protettive (decisione 1, K3):* con la sola verifica LIVE attiva (in memoria o salvata in
      `emergency_live_verify_only`) nessuno sweep dell'Emergency Stop cancella una copertura protettiva del cashout
      (riga `HEDGE_PLACED`, ruolo `HEDGE` o origine `CASHOUT_HEDGE`, e il betId che ne porta il ref), di qualunque
      conto, neanche nel ramo «exchange illeggibile»; gli ingressi del bot si cancellano come prima. La protezione
      della sede LIVE (conto aggiunto allo stop) rispetta la sola verifica (CX-J2). Le coperture restano esposizione
      aperta da riconciliare: nessun conto e' dichiarato verificato con una copertura aperta.
    - *Durabilita' (invariante 8):* lo stato dell'Emergency Stop e' dichiarato persistito solo se riletto identico
      dal db. Se il db non lo rende durevole, un ES ATTIVO va nel giornale di sicurezza `<db>.safety_pending.json`
      (scrittura atomica): all'avvio il giornale prevale sul db, viene riportato sul db e poi rimosso; giornale
      illeggibile => si riparte in emergenza. `reset_emergency()` e la dichiarazione dell'ES sono serializzati
      (`_safety_state_lock`): il reset avviene solo se lo stato azzerato e' riletto dal db, altrimenti esito
      `reset_non_durevole` e l'ES resta in memoria e su disco (prima: memoria azzerata con `persist_error`, ES
      ricomparso al riavvio, H-d). Lo stop di rischio (`persist_risk_stop`) e' salvato sul db solo se riletto,
      altrimenti marker (T-E6).
    - *Registro dei conti (CX-J1, operatore || runtime):* ogni lettura-modifica-scrittura del registro riparte da una
      rilettura dal disco (non dalla cache), unita alla memoria (PENDING prevale, storia unita per chiave); due letture
      fallite => nessuna scrittura (mai un registro ricostruito dal nulla che cancelli un conto sul disco).
    - *Tardivi e reset annullato (CX-J3):* i tardivi entrati nella sessione d'origine mentre il reset G1 era in corso
      restano «compresi» (`late_keys` della sessione corrente, salvati nel libro) quando il reset si annulla: alla
      chiusura il record li dichiara e l'archivio non li somma una seconda volta.
    - *Letture transitorie all'avvio (K2, H-a):* una lettura fallita della chiave di apertura della sessione di
      rischio si ritenta prima di dichiararla illeggibile: l'identita' della sessione aperta non si perde.
    - *M151-bis (owner, CX-K1 + CX-K2, oracolo D `tests/d1/test_d_pr638_m151_k.py` 8f0eb6ba):* la protezione delle
      coperture e' uno stato proprio (`_hedge_guard`, chiave `emergency_hedge_guard`), armato insieme alla sola
      verifica, MAI azzerato dall'Emergency Stop (N7 azzera solo la sola verifica in memoria) e salvato sia con la sola
      verifica sia con lo stato dell'ES (durevole o giornale di sicurezza): una sola verifica non salvata seguita da un
      ES manuale nello stesso processo non rende cancellabile una copertura (CX-K1). Si toglie solo quando si spegne la
      sola verifica. Un binding di conto LIVE con il registro non rileggibile non perde l'obbligo: resta in sospeso in
      memoria e nel giornale (`emergency_live_pending_bindings`), entra PENDING alla prima rilettura riuscita, e
      finche' e' in sospeso `reset_emergency()` lo elenca fra i conti irrisolti; valore illeggibile all'avvio =>
      vincolo illeggibile (CX-K2).
    - *M152 (owner, M151-ter: CX-L1 + CX-L2, oracoli D `cx_l1` b5a804de, `cx_l2` e89f1e5c, `cx_l2_protezioni`
      93adc4cd):* un giornale di sicurezza presente ma illeggibile non viene MAI riscritto (una lettura fallita si
      ritenta una volta; poi la scrittura e' rifiutata, il file resta e all'avvio vale emergenza e vincolo LIVE
      illeggibile): un Emergency Stop solo nel giornale non si perde (I8, CX-L1). Ogni reset di uno stop GLOBALE
      (completamento da audit di un reset G1 gia' committato e reset normale, superato o no) passa da un'unica
      condizione `_live_reset_blocker`: registro illeggibile, un conto non VERIFIED (sentinella compresa), un obbligo
      in sospeso o un'esposizione LIVE diversa da NESSUNA/VERIFICATA => `esposizione_live_irrisolta`, e lo stop, la
      sola verifica e la protezione delle coperture restano (CX-L2). Uno stop SIM locale non ne dipende; dopo la
      riconciliazione reale (VERIFIED, esposizione verificata) il reset torna possibile: nessun blocco permanente.
    Cambio di contratto dichiarato: un reset non durevole non e' piu' un reset (test esistenti di
    `tests/safety/test_emergency_stop.py` adeguati: db con impostazioni reali; uscita da impostazioni illeggibili
    all'avvio tramite la procedura M148 quando il db, tornato leggibile, conserva l'esito LIVE).
  - *G2 — archivio delle sessioni chiuse (owner M139).* Le ultime 20 chiuse restano nel libro come finestra operativa;
    l'archivio durevole è il registro append-only (`SESSION_CLOSED`: id, apertura e chiusura, P&L per ambito, conti,
    limite), senza eliminazione automatica, verificato dalla catena di hash a ogni lettura. Una sessione esce dalla
    finestra solo se già archiviata; un archivio non scrivibile la trattiene e all'avvio si ripara. I P&L tardivi di
    una sessione chiusa, anche fuori dalla finestra, vanno all'archivio una volta per chiave di settlement
    (`LATE_SETTLEMENT`): prima in attesa nel libro (`archive_pending`, nella stessa scrittura della chiave), poi nel
    registro; all'avvio le attese si scrivono senza doppioni. Registro manomesso o illeggibile => i tardivi fuori dalla
    finestra non sono attribuibili e producono un blocco conservativo. M144: ogni tardivo di una sessione gia'
    archiviata aggiorna anche l'indice dell'archivio in memoria, pure mentre e' ancora nella finestra (CX-F1); il record
    della chiusa elenca i tardivi gia' compresi (`late_keys`), cosi' un `LATE_SETTLEMENT` con la stessa chiave non si
    somma due volte. Le scritture del registro sono serializzate (`risk_session_ledger.LOCK`, lock foglia): due scrittori
    concorrenti non producono mai due record con lo stesso `prev` (CX-F3); fra processi la serializzazione la da'
    l'autorita' del runtime (#536).
  - *Libro e marker di riserva (CX-E1/G-D).* Fra il libro nel db e il marker di riserva la revisione conta solo fra
    copie della STESSA sessione; sessioni diverse si ordinano con la `seq` del libro o con la catena delle chiusure;
    ordine non determinabile => libro illeggibile (fail-closed). Sessioni chiuse e tardivi in attesa delle due copie si
    uniscono (mai persi). Libro illeggibile ma chiave di apertura leggibile => la sessione corrente resta QUELLA
    (identita' nota, perdita non verificabile, stop presente, chiusura solo col reset G1 confermato).
  - *Settlement tardivi.* L'ingresso registra la sessione di rischio (`risk_session_id`, con la request e nella
    riserva): segnali, puntate manuali e Dutching (CX-E2) e ingressi automatici del ciclo. Un settlement di una sessione gia' chiusa va a QUELLA sessione (fra le ultime 20 chiuse, nel libro), una
    sola volta, e la sessione corrente non lo vede. Se porta la sessione d'origine oltre il limite: protezione
    PERSISTENTE sul conto interessato (`session_loss_breached:sessione_precedente=<id>`; una perdita tardiva non
    attribuibile si valuta su OGNI ambito della sessione d'origine, anche dopo un reset confermato — R5; LIVE: stop globale legato al
    conto e barriera durevole prima del P&L; SIM: stop locale). Sessione registrata ma non verificabile (ne' corrente
    ne' fra le chiuse) e perdita => blocco conservativo (`sessione_di_rischio_non_verificabile`). Ordini precedenti a
    F1 (senza sessione registrata) => sessione corrente. Libro illeggibile => stop di sessione globale persistente
    (`sessione_di_rischio_illeggibile`) fino al reset esplicito.
  - *N6 — conto dello stop.* Ogni stop globale che coinvolge il LIVE (Emergency Stop LIVE, marker durevole, RISK_STOP
    globale, ricostruzione dal libro) e' legato al conto che lo ha generato (`emergency_live_accounts`: ambito LIVE
    del superamento, altrimenti il conto LIVE configurato), salvato prima del flag. Lo sweep verifica SOLO su un conto
    vincolato: con un altro conto connesso nessuna lettura ne' cancellazione (`ACCOUNT_MISMATCH`), l'esposizione nota
    del conto originario resta visibile, intervento manuale e riconciliazione in attesa; il recovery riprende appena il
    conto originario torna connesso. Con piu' conti vincolati l'esposizione e' chiusa solo quando tutti sono
    verificati (`accounts_pending`). Vincolo illeggibile => nessun conto certifica (intervento manuale). Mai
    cancellazioni di ordini estranei.
  - *N3 — posizioni estranee abbinate.* Una posizione estranea al bot gia' interamente abbinata (non piu' eseguibile)
    resta esposizione LIVE: elencata fra le posizioni abbinate con `origin: "ESTRANEA"` (quelle del bot senza
    origine), contata nella liability (LAY 10 @ 3.00 = 20.00), mai `VERIFICATA`, mai cancellata o coperta dal bot,
    mai nel P&L o nel bankroll del bot (solo visibilita'). Un estraneo ancora eseguibile e' fra gli ordini estranei
    aperti (residuo e abbinato nella stessa voce: nessun doppio conteggio); abbinato o prezzo illeggibile =>
    `SCONOSCIUTO`.
  - *N7 — verifica programmata col runtime LIVE.* Uno stop globale scattato col runtime gia' LIVE arma la
    riconciliazione e il recovery dedicato (ritenta a intervalli, conto irraggiungibile compreso) in SOLA LETTURA: il
    CASHOUT_ALL lavora sul LIVE e uno sweep che cancellasse ne annullerebbe le coperture. Un Emergency Stop successivo
    torna a cancellare. Il reset resta rifiutato finche' il superamento e' attivo.
- **M128 (owner): N1 e N2 su `440351a1`.**
  - *N1 (regressione di `440351a1`).* Il ripiego del marker di sessione non salvato passa dallo stesso `risk_stop()` del
    percorso normale: lo stop e' durevole E ne partono le azioni di protezione (CASHOUT_ALL, protezione verificata della
    sede LIVE col runtime in SIM, recovery dedicato se irrisolta). Prima lo stop si salvava ma l'ordine LIVE del bot
    restava aperto e non letto.
  - *N2.* Marker dello stop di sessione presente ma illeggibile o non valido (JSON malformato, tipo errato, motivo non
    di sessione) o impostazioni non leggibili al ripristino => RISK_STOP globale fail-closed
    (`session_loss_breached:marker_illeggibile`), mai la sola riconciliazione in attesa.
- **Ciclo 12, tre P1 Codex su `b902eca3`.**
  - *Marker di sessione non salvato.* Se `session_loss_stop_pending` non si scrive, la decisione di stop diventa
    durevole subito come RISK_STOP globale persistito (`_persist_session_stop_now`); se nemmeno questo e'
    persistibile il settlement NON si applica (errore prima del P&L e della chiave: la riconsegna lo rielabora) e la
    barriera d'ingresso globale resta armata. Mai un settlement registrato senza lo stop obbligatorio (M121).
  - *Riconsegna dopo il rilascio.* Rilascio della riserva gia' registrato ma P&L non applicato (crash fra i due): la
    riconsegna `duplicate` recupera l'ambito dall'identita' registrata nell'effetto (`customer_ref` => request
    persistita in `orders`); identita' non recuperabile => quarantena, mai l'ambito corrente.
  - *Verifica LIVE di uno stop precedente.* All'inizio di un nuovo stop globale che coinvolge il LIVE (Emergency Stop
    di provenienza LIVE, RISK_STOP globale col runtime in LIVE o nato da un ambito LIVE) un esito VERIFICATO si
    invalida (`IN_ATTESA_DI_VERIFICA`, stato `SCONOSCIUTA` con intervento manuale) finche' uno sweep di quello stop non
    lo conferma; un esito irrisolto resta. Al reset dello stop un esito verificato si toglie (vista `NESSUNA`).
- **EXPANSION.** La baseline del ciclo porta l'ambito del conto su cui e' stata letta; la crescita si calcola solo se
  lo snapshot del conto e' dello STESSO ambito verificato. Baseline legacy senza ambito, di un altro ambito o di un
  conto LIVE non identificato => nessuna crescita (nessun aumento di stake).
- **Fuori da PR-04a (dichiarati).** Picco del drawdown per ambito (resta la protezione fail-closed S18-1:
  `drawdown_stato_di_altro_ambito`); stop di esposizione complessiva (globale, somma dei tavoli SIM e LIVE); N3
  (DEFENSE/AUTO-RESET/EXPANSION, PR-03b-3); PR-04b. La prova LIVE e' a livello di decisione del runtime (LIVE emulato),
  mai un'esecuzione LIVE end-to-end.

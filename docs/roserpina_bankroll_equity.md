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
  Nessun endpoint Control API/MCP. Un primo avvio senza alcuno stato e' un nuovo perimetro contabile (per le
  installazioni esistenti vale la decisione owner del 13/10, opzione A). Soluzione definitiva prevista in BB-3:
  ricostruzione del picco da un registro finanziario indipendente.
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

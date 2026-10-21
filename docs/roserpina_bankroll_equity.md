# Roserpina — grandezze del conto, auto-reset, settlement e ricostruzione al riavvio (F026 v2, F2, F018, F027–F032, RSR-12)

Riferimenti: #453 (PF-ALL-F026, F2, F018), #426/6146717266 («Non confondere il saldo disponibile con l'equity»),
OWNER-SETTLEMENT-02 e OC-04 (nessuna liberazione di capitale ancora impegnato), OD-PF-RECON-02 (un dato mancante
non vale zero), ATOMIC-RESERVE-01 regola 7 (nessun nuovo ingresso prima della riconciliazione completa).

## Grandezze separate (F026 v2)

| Grandezza | Fonte | Uso |
|---|---|---|
| `bankroll_current` (RiskDesk) | saldo **disponibile** dell'exchange | base invariata di stake, limiti %, difesa e lockdown |
| `available`, `committed` = \|exposure\|, `cash_equity` = disponibile + impegnato | `RuntimeController._account_from_funds` (soli campi exchange) | snapshot del conto (`_account_snapshot`) e picco di cassa (`_cash_equity_peak`) |

- `cash_equity` è il **saldo di cassa regolato**, non un'equity mark-to-market. Non decide né stake, né limiti,
  né ammissione degli ordini.
- Un campo mancante, non numerico, non finito o negativo rende lo snapshot non valido. Un'esposizione mancante
  **non** vale zero.
- La base di stake e limiti non cambia rispetto a prima di F026. È provato da `stake_base_unchanged` nei test
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

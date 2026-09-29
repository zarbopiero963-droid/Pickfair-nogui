# Settlement poller — il ciclo di chiusura reale (PR `runtime_settlement_wiring`)

Prima di questa PR il settlement reale non aveva **nessuna via d'ingresso**: il
`core.pnl_engine.PnLEngine` (unico publisher di `RUNTIME_CLOSE_POSITION`) non
era mai istanziato, il PnL realizzato non arrivava mai e il monitor daily-loss
confrontava il limite con un valore fermo a zero. Questa pagina documenta il
filo nuovo e le sue regole operative.

## Il filo

```
listClearedOrders (SETTLED, bet_ids del bot, righe per-bet)
  → BetfairService.list_cleared_orders          (facade fail-closed LIVE)
  → RuntimeController._poll_cleared_settlements (identita' per-bet + dedupe)
  → PnLEngine.apply_cleared_market_settlement   (per (mercato, betId);
                                                 commissione market-net policy)
  → RUNTIME_CLOSE_POSITION                      (payload canonico "accepted")
  → RuntimeController._on_close_position        (contratto → realized PnL →
                                                 daily-loss → bankroll sync →
                                                 hard-stop se il limite sfonda)
```

- Il `PnLEngine` e' costruito in `RuntimeController.__init__` (entrambi gli
  entrypoint, headless e GUI) con la **commissione di policy**
  (`BETFAIR_ITALY_COMMISSION_PCT`): e' l'unica forma che il contratto
  settlement accetta. Il `profit` del report Betfair e' **GROSS**
  (pre-commissione); la commissione riportata da Betfair viaggia nel payload
  solo come osservabilita' (`betfair_reported_commission`). Il saldo VERO
  resta il bankroll sync post-settlement (`get_account_funds`).
- **Auto-close mark-to-market: DISARMATO di default**
  (`auto_close_enabled=False`). La chiusura a soglia non piazza ordini reali:
  realizzerebbe PnL contabile fantasma su una posizione ancora viva. Il
  guardrail di cablaggio asserisce che sull'app reale resti disarmato.

## Il filo SIM (PR03 #461)

Fino alla #484 in SIMULATION il settlement non esisteva: nessuno rilevava la
chiusura di un mercato, il poller non partiva e `record_realized_settlement`
lo chiamavano solo i test (e dopo dei fill avrebbe contato due volte stake e
liability). Ora:

```
book CHIUSO (status CLOSED, runner WINNER/LOSER/REMOVED)
  → MarketTracker.on_market_book                (ingresso reale dei book SIM)
  → BetfairService.update_simulation_market_book
  → SimulationBroker.update_market_book         (regola il mercato UNA volta,
                                                 registro persistito nello
                                                 stato SIM)
  → RuntimeController._poll_simulated_settlements_locked
                                                (stesso thread/chiave del LIVE)
  → PnLEngine.apply_simulated_market_settlement (un evento per mercato)
  → RUNTIME_CLOSE_POSITION → _on_close_position (contratto → realizzato →
                                                 bankroll sync dal saldo SIM →
                                                 daily-loss → ciclo/drawdown)
```

Regole del settlement nel broker:

- **Lordo per bet stile exchange** (fonte unica
  `core.pnl_engine.exchange_settled_gross_pnl`), su quota e importo ABBINATI:
  BACK vince `size x (price - 1)`, perde `-size`; LAY col runner vincente
  `-size x (price - 1)`, col runner perdente `+size`; runner rimosso 0.
- **Selezioni aggregate PRIMA della commissione**: la commissione di policy
  (4,5%) si applica una volta sul netto dell'intero mercato, e il mercato si
  chiude con UN evento. Nel LIVE le righe cleared arrivano per bet, qui no:
  nessuna gamba vincente puo' arrivare «dopo» una perdente.
- **Accredito = netto − effetto di cassa gia' applicato dai fill.** Stake
  BACK e liability LAY escono dal saldo al fill, e il ledger accredita
  subito il realizzato di una posizione chiusa (BACK poi LAY). L'effetto dei
  fill si ricostruisce ripercorrendo gli ordini abbinati, non dal ledger
  vivo che dopo un riavvio perde il realizzato: alla fine il mercato pesa
  sul saldo esattamente il suo netto. Esempio: BACK 10 @ 3.0 vincente e
  LAY 5 @ 2.0 sul perdente ⇒ lordo +25, commissione 1,125, netto 23,875; ai
  fill il saldo scende di 15 (985), al settlement sale di 38,875 (1023,875).
- I **PositionLedger** del mercato si chiudono (esposizione rilasciata); gli
  ordini abbinati diventano `SETTLED` (esclusi dagli ordini correnti, come
  su Betfair), i residui non abbinati `LAPSED`. Un ordine piazzato dopo sul
  mercato gia' regolato decade senza abbinarsi: il mercato e' chiuso.
- **Fail-closed, senza toccare nulla** (posizioni aperte, nessun effetto sul
  saldo): runner con puntate senza stato terminale noto (`ACTIVE`, `HIDDEN`,
  `PLACED` each-way, assente); runner rimosso con puntate su ALTRI runner
  (fattori di riduzione non modellati); piu' di un `WINNER` senza un
  `numberOfWinners` dichiarato (al livello alto nel book REST, dentro
  `marketDefinition` nel book dello stream) o oltre quel numero, cioe' un
  possibile dead heat; ordini incoerenti coi ledger; valori non validi;
  commissione fuori policy.

Idempotenza sulla chiave `sim:<market_id>:<settlement_id>`, a tre livelli
piu' il registro: il broker non regola due volte lo stesso mercato (book
CHIUSO ripetuto o riavvio); il poller deduplica in memoria e sul checkpoint
durevole del consumer (stato illeggibile ⇒ emissione sospesa senza marcare);
il motore rifiuta `SIM_SETTLEMENT_DUPLICATE`. Nel motore applicazione
contabile e consegna sono separate: se il publish fallisce il settlement
resta applicato ma non consegnato, e il giro dopo ripubblica lo stesso
payload senza ricalcolare la commissione. La consegna in corso e' prenotata
sotto lock: una chiamata concorrente o rientrante sulla stessa chiave riceve
`SIM_SETTLEMENT_IN_FLIGHT` e non pubblica. L'ultima difesa resta il
consumer: una seconda consegna dello stesso payload non cambia saldo,
realizzato, bankroll e checkpoint (provato sull'app vera). Il giro emette in
ordine cronologico di chiusura.

**Riavvio fra persistenza e aggiornamento del ciclo**: il service salva lo
stato SIM subito dopo il book CHIUSO, quindi saldo e registro sopravvivono
al riavvio; il poller, al primo giro del nuovo processo, consegna al ciclo i
settlement senza checkpoint, una volta sola. Un settlement gia' consegnato
non si riconsegna.

Limiti dichiarati:

- **In produzione il SIM oggi non riceve book**: il feed di mercato parte
  solo in LIVE e in SIM non c'e' sessione Betfair. Il filo e' cablato
  dall'ingresso reale (`MarketTracker`), ma trasporta dati solo quando
  arriva il feed Italy in SIM (PR11 della #461).
- Stessa chiave `settlement.poll_enabled` del LIVE, default OFF invariato: il
  broker regola comunque il saldo simulato, ma il ciclo del runtime lo vede
  solo col poller acceso.
- Con il poller acceso una perdita SIM puo' far scattare lo stop daily-loss:
  il marker d'emergenza e' unico per SIM e LIVE (la separazione e' la PR15).
- Il settlement SIM non arrotonda al centesimo come Betfair.
- Il registro dei settlement non si pota, come gli ordini SIM: cresce con i
  mercati regolati e viaggia nel JSON dello stato SIM.
- Il realizzato del RiskDesk e lo stato daily-loss restano in memoria: dopo
  un riavvio ripartono da zero, in LIVE come in SIM (preesistente, seguito
  in #453).

## Chiavi di configurazione (settings, pattern market-data)

| chiave | default | note |
|---|---|---|
| `settlement.poll_enabled` | `False` | **dormiente di default** (decisione owner, stesso precedente del TTL DIRECT B6.3.2b). Finche' e' OFF il filo esiste ma non trasporta dati. Vale anche per il giro SIM (PR03 #461). |
| `settlement.poll_sec` | `60` | intervallo del giro, clamp minimo 5s. |
| `settlement.lookback_hours` | `24` | finestra `settled_after`, clamp 1..168h. |

Qualsiasi errore di lettura/parse della config ⇒ poller **disabilitato**
(fail-closed). Le chiavi non sono ancora enumerate in `config_registry.py`
(superficie console/GUI): entreranno con la PR di GUI parity.

## Regole fail-closed (non negoziabili)

- **Identita' del bot PER-BET (I1)** — hardening dal giro review di #440: il
  poll interroga SOLO i `betId` registrati dal bot
  (`db.get_bot_active_orders`, ledger a DENYLIST SIM+LIVE: le bet settlate
  RESTANO nel ledger — si escludono solo `CANCELLED` in SIM e `INFLIGHT` in
  LIVE — la stessa allowlist d'identita' del cashout routing), passati come
  `bet_ids` alla API (a blocchi da 1000) e ri-verificati client-side riga
  per riga (betId nell'allowlist E mercato coerente col ledger). Le
  scommesse MANUALI dell'account — anche sullo STESSO mercato del bot — non
  entrano MAI nel daily-loss: un profitto esterno maschererebbe le perdite
  del bot (fail-open sul kill-switch). Gli id del ledger SIM (`SIMBET-*`,
  non numerici) sono esclusi dal poll LIVE. Identita' illeggibile o vuota ⇒
  nessuna chiamata.
- **Facade cleared solo LIVE**: in SIMULATION il giro non chiama mai
  `list_cleared_orders`, che solleva
  `CLEARED_ORDERS_UNAVAILABLE_IN_SIMULATION`; legge invece il registro del
  broker simulato (sezione «Il filo SIM» sotto, PR03 #461).
- **Runtime non ACTIVE** ⇒ giro no-op (vale anche dopo emergency/lockdown).
- **Primo giro di ogni generazione = sweep completo** (senza
  `settled_after`, orizzonte Betfair ~90 giorni): recupera i settlement
  maturati durante un downtime piu' lungo del lookback; il dedupe filtra il
  gia' consegnato. Lo sweep si dichiara fatto SOLO se fetch ED emissioni
  sono riusciti (una riga transitoriamente fallita si ritenta ancora a
  orizzonte completo) e vale per la SOLA generazione del thread che l'ha
  eseguito (un thread vecchio oltre il join non puo' bruciare lo sweep di
  quella nuova). Poi finestra mobile.
- **Fetch fallito ⇒ round abortito**: mai una lista vuota spacciata per
  «nessun settlement»; si ritenta al giro successivo.
- **Riga malformata scartata** (marketId/profit assenti o non numerici): il
  poller non inventa MAI un profit.
- **Idempotenza a TRE livelli** sulla chiave deterministica
  `cleared:<market_id>:<bet_id>` (la stessa `settlement_key` del consumer;
  piu' bet dello stesso mercato si applicano in sequenza e l'aggregatore
  ricalcola il market-net a ogni passo):
  1. **nel motore**: `PnLEngine` rifiuta di ri-realizzare lo stesso
     settlement (mercato, ref) — `CLEARED_SETTLEMENT_DUPLICATE` — qualunque
     sia il chiamante;
  2. in-memory nel poller per il processo vivo;
  3. **durevole al riavvio**: prima di emettere si interroga il cycle
     recovery state; se il checkpoint del consumer esiste, il settlement e'
     gia' stato consegnato e NON si ri-applica. Stato db illeggibile ⇒
     emissione sospesa senza marcare (ritentata quando il db risponde).
- **L'unita' atomica e' il CLUSTER TEMPORALE di mercato**: gambe dello
  stesso mercato settlate entro 2s **dall'ANCORA** (la prima gamba del
  cluster — la finestra NON e' transitiva: una catena t=0/1.9/3.8s non si
  fonde in un cluster illimitato, hardening R7) sono lo stesso evento di
  settlement (il jitter dei timestamp non e' un ordine reale) e dentro il
  cluster valgono i profit DECRESCENTI — il minimo dei prefissi coincide
  col netto del cluster, quindi le gambe perdenti di un dutching non
  possono far scattare l'emergency stop su un settlement che netta
  positivo. Gambe dello stesso mercato OLTRE la finestra (settlement
  parziali) sono eventi DISTINTI: cluster separati, rigiocati in ordine
  cronologico anche intrecciati con altri mercati — un dip storico reale
  scatta il breach quando sarebbe scattato, mai attenuato da una vincita
  successiva. Le date sono confrontate come datetime REALI (Z/offset/
  frazioni via `fromisoformat`: il confronto lessicografico tra formati
  ISO misti non e' cronologico; il floor supportato — Python 3.11 su CI,
  EXE Windows e venv — parsa nativamente anche offset senza `:`, frazioni
  lunghe e virgola decimale). Gambe non databili: **worst-case PER GAMBA,
  senza netting** (hardening R7: una coppia +100/-80 senza date non e'
  provabilmente un evento unico, e piazzata in coda col netto avrebbe
  potuto occultare un breach storico) — perdite non databili in TESTA,
  profitti non databili in coda. Costo accettato e documentato: una gamba
  di dutching perdente SENZA data accanto alla vincente datata puo' far
  scattare uno stop spurio su un mercato che netta positivo (fail-closed,
  mai fail-open); col caso reale Betfair (date presenti) vale il cluster
  winners-first.
- **Nota (degradazione conservativa)**: l'aggregatore market-net e' in
  memoria. Se un RIAVVIO cade tra due bet dello stesso mercato, il rimborso
  di commissione tra le due non viene riconosciuto: la perdita risulta al
  massimo SOVRA-stimata e il profitto SOTTO-stimato (es. +100 poi restart
  poi -30: net -30 invece di -28.65). Mai nella direzione fail-open.
- **Thread-safety**: lo stato del `PnLEngine` (posizioni, ledger market-net)
  e' serializzato da un lock unico tra worker del bus (fill/market update) e
  thread del poller; i GIRI di poll sono serializzati da un round-lock (un
  giro in corso esclude il successivo, anche di un thread vecchio); il loop
  del thread usa il SUO stop-event passato come argomento (un restart che
  rimpiazza l'attributo non gli fa mai adottare l'event nuovo).
- Lifecycle: thread daemon avviato da `start()` (solo se abilitato), fermato
  da `stop()` con join; l'emergency stop lo rende inerte via gate ACTIVE.

## Come si abilita (owner)

1. Impostare `settlement.poll_enabled = true` nelle settings (DB).
2. Riavviare il runtime (la config e' letta allo start del poller).
3. Verifica: log `Settlement poller avviato (poll_sec=… lookback_hours=…
   simulation=…)` e, al primo mercato settlato, `[PnL] Cleared settlement
   <market> …` in LIVE o `[PnL] SIM settlement <market> …` in SIM.

## Test

- `tests/unit/test_pnl_engine_cleared_settlement.py` — motore: payload
  canonico accettato dal contratto VERO, market-net con esempio numerico,
  fail-closed senza stato parziale, flag auto-close in entrambe le direzioni.
- `tests/core/test_settlement_poller.py` — poller: emissione e parametri
  fetch, filo intero fino a breach+emergency stop, SIM/non-ACTIVE, fetch
  fallito, righe malformate, dedupe in-memory e durevole, db illeggibile,
  config fail-closed, lifecycle thread (anche in SIM), facade fail-closed;
  giro SIM dal registro del broker (PR03).
- `tests/integration/test_simulation_broker_commission_semantics.py` —
  broker SIM: settlement del book CHIUSO con i fill veri (piu' selezioni,
  perdita, match parziale, BACK poi LAY, runner rimosso), duplicato, stati
  non regolabili, fattori di riduzione, dead heat, ledger incoerente,
  riavvio prima e dopo il settlement.
- `tests/acceptance/test_issue437_pr03.py` — app headless VERA (Database,
  EventBus, broker reali): catena dal book CHIUSO al ciclo, replay e fuori
  ordine, riavvio fra persistenza e aggiornamento del ciclo, parita' del
  netto SIM/LIVE sullo stesso portafoglio, kill-switch daily-loss e reset
  Roserpina da drawdown.
- `tests/guardrails/test_cablaggio_runtime.py` — voce promossa da GAP a
  CABLATO: `MARKET_BOOK_UPDATE` sottoscritto, PnLEngine reale sul bus
  dell'app per entrambi gli entrypoint, auto-close disarmato.

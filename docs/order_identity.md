# Identità degli ordini: `customerRef`, `customerOrderRef`, attribuzione (F-13)

Riferimenti: roadmap #586, scheda F-13 (ID-01, D-582-01, D1); decisione owner #426 c.6125270694 (12/10,
customerOrderRef approvato).

## 1. Due ref, due scopi

| Ref Betfair | Dove sta | A cosa serve |
|---|---|---|
| `customerRef` | top-level della richiesta `placeOrders` | de-dup della **richiesta** per 60 s: la stessa richiesta ripetuta (retry dopo un timeout) non piazza una seconda bet, Betfair risponde `DUPLICATE_TRANSACTION` |
| `customerOrderRef` | nell'istruzione, per ordine | identità **dell'ordine**, restituita da `listCurrentOrders`: ritrova l'ordine dopo una risposta persa, un crash o un replace |

Vincoli comuni: al massimo 32 caratteri del charset `[A-Za-z0-9-._+*:;~]`
(`core/order_ref.py`, stesso pattern di `betfair_client._CUSTOMER_REF_RE`).

## 2. Place diretti (F-13a)

I quattro adattatori del contratto wire (`guardrails/contracts/core.trading_engine.json`) inviano
`customer_order_ref = core.order_ref.customer_order_ref(customer_ref)`:

- `core.order_router.OrderRouter.place` (cashout, hedge);
- `order_manager.OrderManager._raw_place_bet`;
- `services.betfair_service.BetfairService.place_order`;
- `core.trading_engine.TradingEngine._kwargs_per_place_bet` (segnali). A firma non ispezionabile resta il contratto
  minimo, senza `customer_order_ref`: un client che non lo accetta solleverebbe `TypeError`.

Il ref del percorso ordini reale (`uuid4().hex`, 32 hex; hedge `hedge:` + 26 hex) è conforme e diventa anche
`customerOrderRef`. Un ref non conforme o assente viene **omesso**, mai troncato o ripulito e mai motivo di rifiuto:
`BetfairClient.place_bet` rifiuterebbe un `customer_order_ref` non conforme (`INVALID_CUSTOMER_ORDER_REF`), e qui non
ne arriva mai uno. Il Microstake LIVE lo inviava già (`core/microstake_live_port.py`).

## 3. Parità SIM

`SimulationBroker`:

- accetta `customer_order_ref`; lo espone come `customerOrderRef` (se assente, come prima, il `customer_ref`);
- **de-dup della richiesta come Betfair.** Un `customerRef` conforme già accettato negli ultimi 60 s
  (`CUSTOMER_REF_DEDUP_SEC`) dà `FAILURE` / `errorCode=DUPLICATE_TRANSACTION`, e nulla viene piazzato. Un batch
  (`place_orders`) è **una** richiesta: le sue gambe condividono il ref e il batch ripetuto è rifiutato per intero. Il
  ref si registra solo quando un ordine viene memorizzato (un rifiuto per fondi non lo consuma). Un ref vuoto o non
  conforme non si deduplica, perché LIVE non lo invierebbe;
- il de-dup **sopravvive al riavvio**. La mappa ref → istante (orologio di parete) sta in
  `SimulationState.customer_ref_seen`, nello snapshot `simulation_state` scritto con l'ordine (anche dalle gambe di un
  batch), e viene ripristinata da `load_from_dict`. Le voci malformate vengono scartate e quelle scadute non bloccano
  (Codex su `3774a68`); alla registrazione di un nuovo ref le voci scadute si tolgono, così la mappa resta limitata
  alla finestra;
- il replace conserva il `customerOrderRef` dell'ordine sostituito.
- i current orders portano `averagePriceMatched`, come il `CurrentOrderSummary` di Betfair: serve alla riconciliazione di un
  replace (§4).
  Un ordine con parte abbinata e residuo annullato compare `EXECUTION_COMPLETE` con `sizeRemaining` 0 e il residuo in
  `sizeCancelled`, come su Betfair.

Il duplicato è un **esito ignoto**, come in LIVE: la prima richiesta è viva. La risposta porta
`order_unknown: True`, il marcatore che ogni consumer riconosce già:
- `TradingEngine` (percorso dei segnali, che chiama `place_bet` direttamente) → AMBIGUOUS;
- `OrderRouter._normalize_sim` lo riporta come `order_unknown=True`, `status=AMBIGUOUS`;
- `OrderManager` registra la saga `AMBIGUOUS`.

In nessuno dei due casi è un FAILED definitivo né un retry.

## 4. Attribuzione in riconciliazione

`ReconciliationEngine._lookup_remote_order` attribuisce una gamba a un ordine dell'exchange solo per identità
dimostrata, in quest'ordine:
1. `bet_id`;
2. `customer_ref` (= `customerOrderRef`).

**Mai per `market_id::selection_id`.** Quell'indice è last-wins e attribuiva alla gamba l'ordine di qualcun altro
sullo stesso runner (D-582-01).

Se nessuna identità corrisponde ma sul runner della gamba c'è un ordine (`_unattributed_on_selection`):
- l'ordine non viene adottato, perché può essere altrui;
- la gamba non si chiude FAILED per timeout, perché può essere la sua, piazzata senza ref prima di F-13;
- resta com'è, con la decisione `UNATTRIBUTED_ORDER_ON_SELECTION` registrata, finché l'identità non si dimostra.

Se invece l'identità porta a **più** ordini distinti (un replace: originale con la parte abbinata e sostituto col
residuo, stesso ref), decide `_leg_identity_view`:
- **replace riuscito.** La gamba punta col suo `bet_id` all'ordine vivo. Tutti gli altri ordini dell'identità sono
  `EXECUTION_COMPLETE` e stanno sullo stesso mercato, runner e lato. La gamba si riconcilia su una vista aggregata:
  - l'ordine della gamba, con `sizeRemaining` pari al residuo dei soli ordini vivi (`EXECUTABLE`): un ordine completo
    non ne ha (Codex su `9200468`);
  - `sizeMatched` pari alla somma degli abbinati di tutti gli ordini dell'identità;
  - `averagePriceMatched` pari alla loro media ponderata, mai la sola quota del sostituto (Grok su `d5e7f54..b6e82d2`).

  Esempi:
  - originale con 2.00 abbinati e sostituto con 3.00 vivi danno PARTIAL;
  - sostituto abbinato per intero dà MATCHED (Grok su `95f6752..3774a68`);
  - 2.00 a 3.0 più 1.00 a 2.5 danno un prezzo medio di 2.833.

  Il `raw_response` della gamba porta `identityBetIds`;
- **in ogni altro caso** la gamba non si chiude su uno solo: resta com'è, con la decisione
  `MULTIPLE_EXCHANGE_ORDERS_FOR_LEG`. Sono i casi in cui un altro ordine è ancora vivo (replace con risposta persa: la
  gamba punta ancora al vecchio `bet_id`, Sol su `95f6752`), la gamba non ha `bet_id`, lo stesso ref compare su un altro
  mercato, runner o lato, oppure uno stato, una size o un prezzo abbinato sono illeggibili.

Senza nessun ordine sul runner, il timeout si applica come prima.

Il rilevamento degli ordini fantasma (`_detect_ghost_orders`) usa ancora il runner, ma solo per **non** segnalare
come fantasma un ordine sostituito. Non attribuisce nulla.

## 5. Il ref come identità del bot (F-13b)

Il `customerOrderRef` sopravvive a un replace (nuovo `betId`) e a una risposta persa (riga `orders` senza `betId`).
Fino a F-13b il bot riconosceva i propri ordini solo dal `betId` registrato. Il ref è ora la **seconda chiave**:
- **`Database.get_bot_order_refs`.** Restituisce i ref conformi di ogni riga `orders`, in qualunque stato, anche
  `INFLIGHT`, con mercato ed evento. Un ref non conforme non è mai stato inviato, quindi non identifica niente.
  `get_bot_active_orders` (solo `betId`) è invariato.
- **Cashout (identità I1, `cashout_router`).** Un ordine corrente è del bot se il suo `betId` è nei record del bot
  oppure se il suo `customerOrderRef` è un ref del bot. Prima, un ordine sostituito o con risposta persa restava
  fuori: hedge sottodimensionato, resting non cancellato, oppure `no_bot_orders` con la posizione aperta. Ora entra
  nella posizione e nella cancellazione. Il `CASHOUT` singolo trova l'evento anche dalle righe senza `betId`. Se i
  ref non sono leggibili il cashout non parte (`CASHOUT_FAILED`). Un ordine con un ref diverso, o senza ref, non
  viene mai adottato.
- **Ricostruzione al riavvio (F018, `RuntimeController._rebuild_open_positions`).** Una riga `SUBMITTED` il cui
  ref è vivo sull'exchange viene ricostruita come certa: tavolo, esposizione e chiave anti-duplicato. Vale anche con
  il `betId` vecchio (replace) o senza `betId` (risposta persa). Prima la prima veniva chiusa e il segnale poteva
  ripetersi; la seconda restava incerta. Un ordine con un altro ref sullo stesso mercato lascia la riga incerta, come
  prima.
- **Hedge del cashout (`CashoutExecutor._previous_hedge_block`).** Un hedge precedente sostituito, con lo stesso ref
  `hedge:…` e un nuovo `betId`, ancora vivo blocca un secondo hedge (`previous_hedge_still_live`). Prima risultava
  sparito e il secondo hedge sovracopriva la posizione. Un hedge senza `betId` resta
  `hedge_liveness_unverifiable`, come prima.

Fuori da F-13b, perché fail-closed già oggi: il rilascio per settlement (`_market_has_foreign_cleared_legs`)
considera estranea una bet regolata con un `betId` sconosciuto, quindi tiene il tavolo, non lo rilascia mai per
errore. Il polling dei settlement per `betId` non vede il `betId` di un sostituto. Entrambi restano limiti noti:
nessun rilascio sbagliato, al più un rilascio ritardato.

Prova LIVE del ref restituito da Betfair: BLOCKED (serve un GO D specifico, scheda F-13 §14).

Test: `tests/f13/`, `tests/f13b/`; mutazioni `f13_*`, `f13b_*` in `guardrails/mutation_catalog.json`.

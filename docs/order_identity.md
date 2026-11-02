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
- il replace conserva il `customerOrderRef` dell'ordine sostituito.

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

Se invece l'identità porta a **più** ordini distinti (`_leg_identity_spans_orders`), la gamba non si chiude su uno
solo: anche qui resta com'è, con la decisione `MULTIPLE_EXCHANGE_ORDERS_FOR_LEG`. È il caso di un replace con risposta
persa, dove l'originale (con la parte abbinata) e il sostituto (col residuo) hanno lo stesso ref e la gamba punta
ancora al vecchio `bet_id`.

Senza nessun ordine sul runner, il timeout si applica come prima.

Il rilevamento degli ordini fantasma (`_detect_ghost_orders`) usa ancora il runner, ma solo per **non** segnalare
come fantasma un ordine sostituito. Non attribuisce nulla.

## 5. Seguito (F-13b)

F018 (`RuntimeController._rebuild_open_positions`) e `cashout_router` (identità I1) usano il solo `bet_id`. Il
`customerOrderRef` come chiave secondaria richiede di estendere `Database.get_bot_active_orders` (oggi solo righe con
`bet_id`) ed è la PR successiva della scheda.

Prova LIVE del ref restituito da Betfair: BLOCKED (serve un GO D specifico, scheda F-13 §14).

Test: `tests/f13/`; mutazioni `f13_*` in `guardrails/mutation_catalog.json`.

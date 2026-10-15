# Roserpina — grandezze del conto, auto-reset, settlement e ricostruzione al riavvio (F026 v2, F2, F018, F027–F031)

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

`RuntimeController._release_settled_tables`:
- con `table_id` nel payload il comportamento è invariato;
- senza `table_id` (settlement dal poller SIM o cleared) il tavolo si individua dal `market_id` e si libera,
  con il P&L del tavolo, **solo se è univoco e certo**;
- una riserva incerta resta trattenuta (F024/F004);
- con più tavoli sullo stesso mercato non si rilascia nulla automaticamente (log).

## F027 — settlement fuori da ACTIVE (decisione owner, CLOSURE-03)

LOCKDOWN, PAUSE ed emergency stop bloccano i **nuovi ingressi**, non la riconciliazione delle posizioni aperte
(`RuntimeController._settlement_reconciliation_allowed`: ACTIVE, PAUSED, LOCKDOWN; non STOPPED, dove il servizio
è disconnesso e la riconciliazione riparte da `start()`).
- I settlement confermati si contabilizzano: P&L, commissione una volta, conto, tavoli. Il modo resta quello che era.
- La riconciliazione non produce ordini: l'auto-next del ciclo è riservato ad ACTIVE (`_runtime_not_active_reason`).
- Emergency stop scattato in LIVE: il servizio passa in SIM e il registro SIM non descrive le posizioni reali.
  Nessuna emissione, esposizione mantenuta, evento `SETTLEMENT_RECONCILIATION_UNCERTAIN` (una volta). Le posizioni
  LIVE si riconciliano al successivo avvio autorizzato (F018 + sweep completo). Verifica LIVE: MANUAL_ONLY.
- Nessuna doppia contabilizzazione dopo un crash: dedupe del consumer (checkpoint durevole) invariato.

## F028 — ordine mai abbinato (decisione owner, CLOSURE-03)

`RuntimeController._release_final_unmatched_orders`, a ogni giro del poller dopo i settlement:
- solo righe `SUBMITTED` con betId e riserva non incerta; AMBIGUOUS/INFLIGHT non sono mai «annullati per timeout»;
- SIM: stato del registro del broker `CANCELLED/LAPSED/VOIDED/EXPIRED` con abbinato zero;
- LIVE: betId assente dagli ordini correnti, presente fra i cleared LAPSED/CANCELLED/VOIDED, assente dai cleared
  SETTLED; un errore di lettura non libera nulla (verifica contro Betfair reale: MANUAL_ONLY);
- rilascio idempotente (`force_unlock_if_event`), perdita di recovery del tavolo conservata, chiave anti-duplicato
  trattenuta (un segnale riconsegnato non si rigioca); evento `UNMATCHED_ORDER_RELEASED`;
- crash fra conferma della cancellazione e rilascio: al riavvio F018 trova l'ordine assente e non ricostruisce il tavolo.

## F031 — parziale abbinato con residuo annullato (parità SIM/LIVE)

Su Betfair un ordine con parte abbinata e residuo annullato resta fra gli ordini correnti (EXECUTION_COMPLETE) fino
al settlement. Il broker SIM lo marcava CANCELLED e lo escludeva: al riavvio F018 non ricostruiva una posizione
abbinata viva. `SimulationBroker.get_current_orders` ora lo riporta come EXECUTION_COMPLETE.

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

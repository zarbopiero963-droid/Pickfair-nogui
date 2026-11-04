# Simulazione Roserpina 100 € (fittizi) — report end-to-end

Harness: `tests/f026/child_f026.py` (HeadlessApp reale in sottoprocesso, SQLite reale, Money Management reale; sostituito
solo il confine dell'exchange con il broker SIM). Report: `tools/sim_report/roserpina_report.build_report`, in Decimal,
dai dump di una o più esecuzioni consecutive dello stesso HOME (i riavvii sono contati).

## Provenienza dei dati (obbligatoria)

| Valore | Significato |
|---|---|
| `SINTETICO` | scenario e quote scritti a mano nel test, dichiarati tali |
| `PDF_MONEYFLOW` | dati del PDF MoneyFlow dell'owner (non disponibile al 04/11/2026) |
| `REGISTRATO` | storico reale registrato |

Una provenienza diversa o assente viene rifiutata. Le funzioni che il motore su `main` non ha sono elencate in
`not_covered`, mai simulate: oggi SL/TP (F-02 non è su main; F-04 pronto su branch) e trailing stop (MISSING, K-7).

## Contenuto del report

`initial_capital`, `tables` (per mercato: ingressi, uscite = hedge di cashout, stati, P&L lordo e netto del
settlement dell'exchange, commissione = lordo − netto), `entries`, `exits`, `realized_net_pnl`, `commission_total`,
`exposure_max` (massimo di `funds.exposure`), `cash_drawdown_max_pct` (equità di cassa rispetto al suo massimo
progressivo), `desk_drawdown_max_pct` (drawdown di Roserpina), `final_balance_total`, `final_available`,
`unreconciled_orders` (righe `orders` INFLIGHT/AMBIGUOUS e ordini dell'exchange ancora vivi su mercati regolati),
`restarts`.

## Scenario `roserpina-100-multitavolo-v1` (SINTETICO)

Fase 1: tre tavoli (A 1.801 @2.0, B 1.802 @1.5, C 1.803 @3.0 non abbinato); quota di A scesa a 1.6 (gol); cashout
parziale di A; B sospeso e riaperto; A regolato vinto; crash del processo. Fase 2 (riavvio sullo stesso HOME): B
regolato perso; C regolato (non abbinato, decade); nuovo ingresso D 1.804 @2.2 con stake del MM, regolato vinto.

Oracolo indipendente (nel test): per ogni mercato, dagli ordini abbinati sull'exchange prima della chiusura e
dall'esito dichiarato, netto lordo; commissione 4,5 % del netto lordo del mercato se positivo. Il report deve
coincidere al centesimo per tavolo, nel totale e nel saldo finale (100 + somma dei netti), senza ordini non
riconciliati.

Esito osservato il 04/11/2026 (base `ada4076`): capitale iniziale 100,00; P&L netto −2,67 (A +0,67, B −6,00, C nessun
settlement, D +2,66); commissioni 0,16; esposizione massima 10,50; drawdown di cassa massimo 5,96 %; saldo finale
97,33; nessun ordine non riconciliato; 1 riavvio. Valori di uno scenario sintetico: non sono una previsione.

Esecuzione: `python3 -m pytest -q tests/f026/test_roserpina_100_report.py -s` (stampa il report).

Operazioni aggiunte all'harness per lo scenario: `price` (quota del book mossa, mercato OPEN), `suspend` / `reopen`.

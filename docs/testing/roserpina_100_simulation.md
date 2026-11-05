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

## Campagna 2 `roserpina-100-protezioni-v1` (SINTETICO): SL, TP, trailing

`tests/f026/test_roserpina_100_campagna2.py`. Gira solo dove esistono F-02 + F-04 + F-03 (`core.trailing_stop`,
branch `claude/d-f03-trailing-stop`); altrove è **saltata** con il motivo (le protezioni non sono su `main`).
Quattro mercati, BACK @2.0 con stake del MM: 1.901 SL 10 % (quota a 3.0), 1.902 TP 10 % (quota a 1.6), 1.903
trailing 20 % (1.4 → 1.3 → 1.6), 1.904 senza protezioni (quota a 3.0); il poller di F-02 (2 s) valuta i book; crash;
al riavvio un giro del poller (gli scatti si ripetono) e i settlement. Il report ha la sezione `protections`
(scatti F-02/F-03 ed esiti F-04 per esecuzione).

Esito osservato il 05/11/2026 sull'albero `claude/d-f03-trailing-stop` + questo branch: SL, TP e trailing scattano
una volta ciascuno e sono instradati (`SL_TP_CLOSE_ROUTED`), un hedge per tavolo; il tavolo senza protezioni non è
toccato; dopo il riavvio solo `SL_TP_CLOSE_SKIPPED`, nessun hedge duplicato. Capitale iniziale 100,00; P&L netto
+3,20 (1.901 −1,01, 1.902 +0,67, 1.903 +0,67, 1.904 +2,87); commissioni 0,20; esposizione massima 12,00; drawdown di
cassa massimo 1,01 %; saldo finale 103,20; nessun ordine non riconciliato; 1 riavvio. Coerente al centesimo con
l'oracolo indipendente (arrotondamento HALF_UP come il report). Valori di uno scenario sintetico.

## Campagna 4 (SINTETICO): LAY con SL e trailing, SL a mercato sospeso, protezioni su LAY non abbinato

`tests/f026/test_roserpina_100_campagna4.py`, stessi requisiti della campagna 2 (F-02 + F-04 + F-03), altrimenti
saltata. Scenari G7-S22..S26. L'op `price` accetta `status` (es. `SUSPENDED`) per spostare quota e stato insieme.

- B1 (G7-S22) LAY @3.0 con SL 10 %, quota a 2.0: un solo hedge BACK, `SL_TP_CLOSE_ROUTED`, coerente con l'oracolo.
- B2 (G7-S23) LAY @2.0 abbinato a 1.92, trailing 20 %, quota 3.0 → 3.4 → 2.4: `TRAILING_TRIGGERED`, un solo hedge BACK.
- B3 (G7-S24) **XFAIL, difetto D-C4-01**: SL superato a mercato sospeso. Il monitor valuta lo SL anche su book non
  negoziabile, imposta il latch `sl_tp_triggered`, F-04 esita `SL_TP_CLOSE_NOT_EXECUTED` (ritentabile) ma il latch
  impedisce un nuovo scatto: alla riapertura, con la perdita ancora oltre lo SL, la posizione resta aperta.
- B4 (G7-S25) **XFAIL, difetto D-C4-02**: trailing su un LAY mai abbinato. `sl_tp_pnl.position_stake` usa lo stake
  richiesto (`meta['stake']`) e non quello abbinato: il trailing scatta su un profitto inesistente e la chiusura F-04
  cancella l'ingresso in attesa.

- B5 (G7-S26) ingresso BACK abbinato in parte (book con 1 EUR), SL a quota 3.0: l'hedge della sola parte abbinata
  (~0,66) e' sotto il minimo exchange e passa dal Microstake nativo cablato nell'HeadlessApp; un solo hedge per
  l'importo esatto, resting cancellato, `SL_TP_CLOSE_ROUTED`. Nell'harness F-04 di accettazione (senza Microstake
  executor) lo stesso caso verifica solo il fail-closed (`MICROSTAKE_EXECUTOR_UNAVAILABLE`, nessun ordine): li' l'E2E e'
  NOT_TESTED, la copertura E2E e' questa.

Esito osservato il 05/11/2026 sull'albero d3b2d41 (main 911562a + F-14 + F-16 + F-R06 + F-03 + simulazione) piu'
questa campagna: 3 PASS (B1, B2, B5), 2 XFAIL strict (B3, B4). B3 e' xfail solo senza la correzione D-C4-01
(`sl_tp_close.latch_releasable`, branch `claude/d-f03-trailing-stop`): con la correzione deve passare.

## Registro G7 e riesecuzione

Gli scenari della campagna finanziaria G7 hanno ID stabili `G7-Snn` in `tests/g7/g7_scenarios.json`: per ognuno il
test, i seed, i requisiti `G7-Rnn` coperti (elenco di Claude B: `pf-gov-persistent-roadmap-01:docs/roadmap/g7_financial_campaign.md`)
e il codice che deve essere su main perche' l'esito sia certificativo (`requires`). Gli ID non si riusano ne' si
rinumerano.

`python3 tools/sim_report/run_g7.py --out art/g7/<sha>.json [--published-sha <sha>] [--only G7-S12 ...]` riesegue
gli scenari sull'albero corrente e scrive l'evidenza (`pf-g7-evidence/2`): SHA dell'albero, albero pulito o no, hash
del registro, esito per scenario (`PASS`, `FAIL`, `XFAIL` = difetto noto, `NOT_TESTED` = test assente o saltato,
`UNSUPPORTED` = dichiarato nel registro, es. G7-S21 auto-next), durata, e lo stato per requisito: **PASS solo se tutti i
suoi scenari sono PASS**, altrimenti il peggiore (FAIL > XFAIL > UNSUPPORTED > NOT_TESTED). `mode` e' `development`
salvo albero pulito con SHA uguale a `--published-sha` (`certification_candidate`); la certificazione la decide comunque
il verificatore, solo su SHA pubblicati (head di PR o main). Nessuna rete, nessun ordine.

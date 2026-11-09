# Audit indipendente del Money Management Roserpina (PF-ROSERPINA-FINANCIAL-ORACLE-CLAUDE-D-20261109)

Claude D, 09/11/2026. Base: `main` `6519794b` (F-14, F-16, F-R06). Ingresso: campagna Grok PF-ROSERPINA-5T-OFFLINE-20261109
(#586 c.6747305390). Nessuna modifica al motore; solo test e documentazione. SIM, LIVE OFF.

Test: `tests/roserpina_oracle/` (questo branch: oracolo Decimal, drawdown sul `RuntimeController` reale) e
`tests/f026/test_roserpina_100_campagna5_5tavoli.py` (branch `claude/d-roserpina-100-sim-report`, HeadlessApp reale,
5 tavoli, 100 EUR). I difetti sono `xfail(strict=True)`: falliscono oggi per il motivo dichiarato e diventano FAIL
(XPASS) appena il comportamento e' corretto, cosi' la correzione deve togliere il marcatore.

## 1. Formula finanziaria verificata (dal codice di `main`)

| Grandezza | Codice | Definizione oggi |
|---|---|---|
| bankroll (`RiskDesk.bankroll_current`) | `_apply_funds_reading` -> `sync_funds_reading(bankroll=available)` | **disponibile** = cassa - impegnato |
| cassa verificata | `_cash_equity_verification` | disponibile + impegnato della stessa lettura (PFC-008) |
| equity | `RiskDesk.equity_current` | bankroll + P&L non realizzato |
| picco RiskDesk (`equity_peak`) | `sync_bankroll`, `reset_recovery_cycle` | massimo del **disponibile**; il reset lo porta al disponibile corrente |
| picco di cassa (`_cash_equity_peak`) | `_record_account_snapshot` | massimo della cassa; il reset lo porta alla cassa corrente |
| drawdown % (desk mode, lockdown) | `RiskDesk.drawdown_pct`, `determine_desk_mode` | (picco RiskDesk - equity) / picco RiskDesk |
| drawdown assoluto (`max_drawdown_abs`) | `absolute_cap_reason_for_orders`, ammissione | picco RiskDesk - bankroll |
| hard stop % (`max_drawdown_hard_stop_pct`) | ammissione, solo `execution_mode == LIVE` | (picco RiskDesk - bankroll) / picco |
| auto-reset | `_maybe_auto_reset` | drawdown di **cassa** >= `auto_reset_drawdown_pct` (default 15) |
| effetto del reset | `_reset_recovery_cycle_keeping_losses` | P&L di ciclo a 0, picchi ribasati; baseline giornaliera e di sessione spostate (quelle perdite restano) |
| stake | `_base_stake_decimal` + `normalize_stake` | (bankroll x target% + chase) / (quota - 1) x moltiplicatori, razionale esatto, PER DIFETTO al centesimo |
| rischio | `validators.order_exposure_or_inf` | BACK = stake; LAY = stake x (quota - 1) (float) |
| commissione | `PnLEngine._commission_amount` | 4,5 % del lordo di mercato se positivo (float, nessun arrotondamento) |
| cap | `validators.exceeds_cap` | supera se `amount > cap` **e non** `isclose(rel 1e-9, abs 1e-9)` |
| soglia di arresto | `validators.reaches_limit` | scatta se `amount >= limit` **o** `isclose(...)` (anticipa, mai permissiva) |

Oracolo (`tests/roserpina_oracle/oracle.py`): stesse grandezze in Decimal esatto, con cassa = disponibile + impegnato,
picco = massimo storico della cassa, le puntate aperte non sono perdite (F026), il reset non cancella le perdite
maturate, cap ammesso solo se importo <= limite, arresto se importo >= limite.

## 2. Esiti e classificazione

| ID | Esito | Classe | Gravita' | Riproduzione |
|---|---|---|---|---|
| RO-DD-01 (Grok D-1) | **RED** | bug implementativo (regola owner «il reset non cancella le perdite» applicata a giornaliera/sessione, non al drawdown) | **P1** | `test_ro_dd_01*` (runtime) + `test_ro_dd_01_e2e_*` (HeadlessApp): 16 EUR realizzati, limite 10, dopo il reset nessun blocco; con limite 20 nessun blocco nemmeno a 38,60 EUR di perdita cumulativa |
| RO-DD-02 (Grok D-1b) | **RED** | bug implementativo | **P1** | `test_ro_dd_02_*`: numeri di Grok, picco 87,11 (disponibile) invece di 91,07 (cassa), a 77,50 misura 9,61 invece di 13,57 |
| RO-DD-03 (Grok D-2) | **RED** | bug implementativo (contraddice F026 «le puntate aperte non sono perdite») | P2 (fail-safe: blocca di piu') | `test_ro_dd_03_*`: 30 EUR impegnati e zero perdite => `max_drawdown_abs_active` e LOCKDOWN |
| RO-DD-04 | **RED** | conflitto di requisiti (default auto-reset 15 % < lockdown 20 %) | P2 / decisione owner | `test_ro_dd_04_*`: perdita cumulativa 30 % mai in LOCKDOWN |
| RO-DD-05 | **confermato (statico)** | parita' SIM/LIVE violata (PF-OD12) | P2 | `test_ro_dd_05_*`: hard stop % dentro `if LIVE`; riproduzione runtime NOT_RUN |
| RO-DEC-01 (Grok D-3) | **RED** | bug implementativo vs regola owner «nessuna tolleranza» | P2 (eccesso < 1e-9 relativo: sotto il centesimo fino a ~10 milioni) | `test_ro_dec_01*`: 60.0000000001, 60.00000005, 100000.00009 ammessi; anche `absolute_cap_reason` e `fresh_caps_reason` |
| RO-DEC-02 | **RED** | rappresentazione float | P3 (nessun effetto al centesimo dimostrato) | `test_ro_dec_02_*`: LAY 2,27 @1,44 = 0.9987999999999999 |
| RO-REC-01 (Grok B-1) | **RED** | protezione/riconciliazione incompleta (fail-closed preservato) | P2 (capacita'; nessun rischio finanziario) | campagna 5: dopo crash con risposta in volo, ordine EXECUTION_COMPLETE sull'exchange, riga INFLIGHT dopo due riavvii |
| RO-CAP-01 (Grok B-2) | **RED** | conflitto di requisiti (recovery OFF vs tavoli in RECOVERY) | P2 / decisione owner | campagna 5: 5 perdite da 1 EUR (5 % del capitale) => nessun tavolo, nuovo segnale respinto |
| Stake, rischio, esposizione aggregata, riserve, parziale, perdita simultanea, cashout, settlement, crash/riavvio, assenza di doppia contabilizzazione, liberazione del capitale | **PASS** | — | — | `test_decimal_oracle.py`, campagna 5 |
| Soglie di arresto (`reaches_limit`) | **PASS** (piu' severe dell'esatto, mai permissive) | — | — | `test_soglia_di_arresto_*` |
| SL/TP/trailing nel percorso Roserpina (Grok S04/S05) | **protezione non implementata su main** | F-02/F-04/F-03 + D-C4-01/02 su `claude/d-f03-trailing-stop`, consegnati a Claude B | P1 per il LIVE | campagne 2-4, G7-S11..S26 (sul branch) |
| Prezzo stale all'ingresso (Grok B-3) | **protezione non implementata** | soglia d'eta' non decisa dall'owner (nessuna soglia arbitraria) | P2 / decisione owner | rischio coperto dalla riserva worst-case |

Configurazione specifica della simulazione: `lockdown_drawdown_pct` 60 (Grok) maschera RO-DD-04; `max_drawdown_abs` 10 e'
un valore della campagna, **non** un limite fisso del prodotto (opzionale, None = non attivo, decisione owner 18/10).

## 3. Condizioni minime per FIXED

- RO-DD-01: i limiti di arresto per drawdown (`max_drawdown_abs`, `max_drawdown_hard_stop_pct`, lockdown) misurano la
  perdita rispetto a un picco che il reset di ciclo **non** ribasa (o con baseline spostata come giornaliera/sessione);
  `test_ro_dd_01`, `test_ro_dd_01b` e l'e2e passano senza xfail; nessun blocco oltre il limite anche attraverso N reset.
- RO-DD-02: picco e misura sulla **cassa** (disponibile + impegnato della stessa lettura), anche dopo un reset con
  esposizione aperta; `test_ro_dd_02` passa con i numeri Grok.
- RO-DD-03: le puntate aperte non generano drawdown; `test_ro_dd_03` passa; i cap di esposizione restano l'unico
  limite sull'esposizione.
- RO-DD-04: decisione owner esplicita (lockdown cumulativo o di ciclo); il test si adegua alla decisione.
- RO-DD-05: hard stop % applicato anche in SIM (parita').
- RO-DEC-01: confronto dei cap in Decimal esatto (o centesimi interi), nessuna tolleranza sul lato permissivo;
  `test_ro_dec_01*` passano. RO-DEC-02: importi monetari in Decimal nei confronti e nei registri.
- RO-REC-01: un ordine trovato sull'exchange per `customerOrderRef` (stesso mercato, lettura completa) riconcilia la
  riga, che non resta INFLIGHT/AMBIGUOUS; riserva = esposizione reale.
- RO-CAP-01: decisione owner; se un tavolo in perdita senza recovery deve tornare libero, il test passa.
- Per ogni correzione: test RED -> PASS senza xfail, mutanti che lo uccidono, suite completa senza regressioni,
  campagna 5 ripetuta sullo SHA pubblicato di `main`.

## 4. Criteri di riapertura della campagna a 10+ tavoli

1. RO-DD-01 e RO-DD-02 FIXED su `main` (P1): nessuna campagna oltre 5 tavoli con limiti di drawdown aggirabili.
2. Decisioni owner registrate per RO-DD-04 e RO-CAP-01 (altrimenti la capacita' e il lockdown della campagna non sono
   interpretabili).
3. RO-REC-01 FIXED o accettato dall'owner come limite noto (con 10+ tavoli i crash moltiplicano i tavoli bloccati).
4. Campagna 5 e oracoli di questo branch tutti PASS (nessun xfail residuo tra quelli sopra) sullo SHA pubblicato.
5. Per SL/TP/trailing: catena F-02/F-04/F-03 con D-C4-01/02 integrata, oppure campagna dichiarata senza protezioni.

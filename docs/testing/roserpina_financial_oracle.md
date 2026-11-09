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
| RO-DD-01 (Grok D-1) | **RED** | bug implementativo (regola owner «il reset non cancella le perdite» applicata a giornaliera/sessione, non al drawdown) | **P0** (blocco LIVE, owner) | `test_ro_dd_01*` (runtime) + `test_ro_dd_01_e2e_*` (HeadlessApp): 16 EUR realizzati, limite 10, dopo il reset nessun blocco; con limite 20 nessun blocco nemmeno a 38,60 EUR di perdita cumulativa |
| RO-DD-02 (Grok D-1b) | **RED** | bug implementativo | **P0** (blocco LIVE, owner) | `test_ro_dd_02_*`: numeri di Grok, picco 87,11 (disponibile) invece di 91,07 (cassa), a 77,50 misura 9,61 invece di 13,57 |
| RO-DD-03 (Grok D-2) | **RED** | bug implementativo (contraddice F026 «le puntate aperte non sono perdite») | P2 (fail-safe: blocca di piu') | `test_ro_dd_03_*`: 30 EUR impegnati e zero perdite => `max_drawdown_abs_active` e LOCKDOWN |
| RO-DD-04 | **RED** | violazione del contratto owner (lockdown sul drawdown cumulativo dal picco persistente) | P0 (famiglia RO-DD-01) | `test_ro_dd_04_*`: perdita cumulativa 30 % mai in LOCKDOWN |
| RO-DD-05 | **RED** | parita' SIM/LIVE violata (PF-OD12) | P2 | `test_ro_dd_05_hard_stop_percentuale_anche_in_sim`: in SIM picco 100, cassa 89, limite 10 % => ingresso ammesso (controllo senza perdita: ammesso) |
| RO-DEC-01 (Grok D-3) | **RED** | bug implementativo vs regola owner «nessuna tolleranza» | P2 (eccesso < 1e-9 relativo: sotto il centesimo fino a ~10 milioni) | `test_ro_dec_01*`: 60.0000000001, 60.00000005, 100000.00009 ammessi; anche `absolute_cap_reason` e `fresh_caps_reason` |
| RO-DEC-02 | **RED** | rappresentazione float | P3 (nessun effetto al centesimo dimostrato) | `test_ro_dec_02_*`: LAY 2,27 @1,44 = 0.9987999999999999 |
| RO-REC-01 (Grok B-1) | **RED** | protezione/riconciliazione incompleta (fail-closed preservato) | P2 (capacita'; nessun rischio finanziario) | campagna 5: dopo crash con risposta in volo, ordine EXECUTION_COMPLETE sull'exchange, riga INFLIGHT dopo due riavvii |
| RO-CAP-01 (Grok B-2) | **RED** | violazione del contratto owner (tavolo regolato, esposizione 0, nessun incerto => libero) | P2 (capacita') | campagna 5: 5 perdite da 1 EUR (5 % del capitale) => nessun tavolo, nuovo segnale respinto |
| Stake, rischio, esposizione aggregata, riserve, parziale, perdita simultanea, cashout, settlement, crash/riavvio, assenza di doppia contabilizzazione, liberazione del capitale | **PASS** | — | — | `test_decimal_oracle.py`, campagna 5 |
| Soglie di arresto (`reaches_limit`) | **PASS** (piu' severe dell'esatto, mai permissive) | — | — | `test_soglia_di_arresto_*` |
| SL/TP/trailing nel percorso Roserpina (Grok S04/S05) | **protezione non implementata su main** | F-02/F-04/F-03 + D-C4-01/02 su `claude/d-f03-trailing-stop`, consegnati a Claude B | P1 per il LIVE | campagne 2-4, G7-S11..S26 (sul branch) |
| Prezzo stale all'ingresso (Grok B-3) | **protezione non implementata** | soglia d'eta' non decisa dall'owner (nessuna soglia arbitraria) | P2 / decisione owner | rischio coperto dalla riserva worst-case |

Configurazione specifica della simulazione: `lockdown_drawdown_pct` 60 (Grok) maschera RO-DD-04; `max_drawdown_abs` 10 e'
un valore della campagna, **non** un limite fisso del prodotto (opzionale, None = non attivo, decisione owner 18/10).

| RO-DD-06 | **RED** | violazione del contratto (regola 1: picco persistente) | **P0** | campagna 5 `test_ro_dd_06_*`: dopo crash e riavvio picco 88 (era 100), ingresso ammesso con 12 EUR di perdita e limite 10 |
| RO-DD-07 | **RED** | violazione del contratto (blocco alla variazione finanziaria) | **P0** | `test_ro_dd_07_*` (runtime ed e2e): dopo il settlement con 12 EUR di perdita il runtime resta ACTIVE fino al segnale successivo |

## 2-bis. Contratto owner (PF-ROSERPINA-DRAWDOWN-AND-RECOVERY-20261109, PF-ROSERPINA-FINANCIAL-CONTRACT-FINAL-20261109)

`drawdown_abs = max(0, verified_cash_peak - verified_cash_current)`; picco persistente (crash/restart), mai ribasato
dall'auto-reset; riserve ed esposizione non sono perdite (controllo d'esposizione separato e fail-closed); cassa non
verificabile o riconciliazione incerta => nessun nuovo ingresso; Decimal esatto; limite opzionale; reset della baseline
solo con procedura owner tracciata; blocco alla variazione finanziaria e mantenuto fino a uno sblocco autorizzato.
LOCKDOWN (RO-DD-04) sullo stesso picco persistente. Recovery OFF (RO-CAP-01): tavolo con settlement confermato,
esposizione residua zero e nessun ordine incerto => libero; altrimenti bloccato; nessun ingresso di recupero.
Test del contratto: `test_contratto_*` (PASS dove il codice gia' rispetta, xfail strict dove no).

## 3. Condizioni minime per FIXED

- RO-DD-01: i limiti di arresto per drawdown (`max_drawdown_abs`, `max_drawdown_hard_stop_pct`, lockdown) misurano la
  perdita rispetto a un picco che il reset di ciclo **non** ribasa (o con baseline spostata come giornaliera/sessione);
  `test_ro_dd_01`, `test_ro_dd_01b` e l'e2e passano senza xfail; nessun blocco oltre il limite anche attraverso N reset.
- RO-DD-02: picco e misura sulla **cassa** (disponibile + impegnato della stessa lettura), anche dopo un reset con
  esposizione aperta; `test_ro_dd_02` passa con i numeri Grok.
- RO-DD-03: le puntate aperte non generano drawdown; `test_ro_dd_03` passa; i cap di esposizione restano l'unico
  limite sull'esposizione.
- RO-DD-04: lockdown sul drawdown dal picco persistente (decisione owner): `test_ro_dd_04` passa.
- RO-DD-06: picco persistito e ricaricato al riavvio PRIMA di ammettere ingressi; il blocco sopravvive al riavvio
  (`test_ro_dd_06_e2e`). Cautele owner: il picco non si inizializza ne' si alza da una cassa non verificata
  (`test_contratto_lettura_non_verificata_non_alza_mai_il_picco`, PASS oggi: deve restare PASS); un errore di lettura o
  scrittura del picco persistito => nessun nuovo ingresso (fail-closed), da provare con un test sulla patch.
- RO-DD-07: il superamento rilevato a una lettura del conto/settlement mette il runtime in blocco subito (`test_ro_dd_07*`).
- RO-DD-05: hard stop % applicato anche in SIM (parita').
- RO-DEC-01: confronto dei cap in Decimal esatto (o centesimi interi), nessuna tolleranza sul lato permissivo;
  `test_ro_dec_01*` passano. RO-DEC-02: importi monetari in Decimal nei confronti e nei registri.
- RO-REC-01: un ordine trovato sull'exchange per `customerOrderRef` (stesso mercato, lettura completa) riconcilia la
  riga, che non resta INFLIGHT/AMBIGUOUS; riserva = esposizione reale.
- RO-CAP-01: tavolo regolato, esposizione 0, nessun incerto => libero; nuovo ingresso con stake senza inseguimento
  della perdita (`test_ro_cap_01`); un tavolo incerto resta bloccato (`test_ro_rec_01`, riserva mantenuta).
- Per ogni correzione: test RED -> PASS senza xfail, mutanti che lo uccidono, suite completa senza regressioni,
  campagna 5 ripetuta sullo SHA pubblicato di `main`.

## 4. Criteri di riapertura della campagna a 10+ tavoli

1. RO-DD-01, RO-DD-02, RO-DD-04, RO-DD-06, RO-DD-07 FIXED su `main` (P0): nessuna campagna con limiti aggirabili.
2. RO-CAP-01 FIXED secondo la decisione owner; RO-DD-03 (D-2) FIXED senza indebolire il controllo d'esposizione.
3. RO-REC-01 FIXED o accettato dall'owner come limite noto (con 10+ tavoli i crash moltiplicano i tavoli bloccati).
4. Campagna 5 e oracoli di questo branch tutti PASS (nessun xfail residuo tra quelli sopra) sullo SHA pubblicato.
5. Per SL/TP/trailing: catena F-02/F-04/F-03 con D-C4-01/02 integrata, oppure campagna dichiarata senza protezioni.

## 5. Verifica indipendente della correzione D-1/D-2/D-3 (09/11/2026)

SHA `15960bac13883113c0445da5fb15328e99586c0f` (Claude B, branch di conservazione `keep/roserpina-d1-drawdown-20261109`,
base main `6519794b`, nessuna PR), con la copia di Claude D degli oracoli e `--runxfail`.

| Voce | Esito |
|---|---|
| RO-DD-01, 01b (reset), RO-DD-02 (picco sulla cassa), RO-DD-03 x2 (esposizione/riserva non sono perdita), RO-DD-04 (lockdown cumulativo), RO-DD-05 (hard stop % in SIM), RO-DD-07 (blocco alla lettura) | PASS |
| RO-DEC-01 x4 (nessuna tolleranza), RO-DEC-02 (LAY in Decimal) | PASS |
| e2e campagna 5: RO-DD-01, RO-DD-06 (blocco dopo riavvio), RO-DD-07 (blocco al settlement); 7 verifiche gia' PASS | PASS (nessuna regressione) |
| `test_hard_stop_patch.py`: picco persistito, blocco dopo riavvio / rientro / limite tolto, auto-reset, 6 stati corrotti, lettura e scrittura che falliscono, sblocco (requisiti, ordini incerti, picco non ribasato, riblocco, operation_id monouso, salvataggio fallito), nessun chiamante MCP/Control API, cap di esposizione invariati | PASS (22) |
| **RO-DD-08** (nuovo, patch): picco NON salvato senza hard stop configurato => ingressi ammessi e, dopo un riavvio, il LOCKDOWN cumulativo (RO-DD-04) riparte dal picco salvato piu' basso (100 invece di 120: 16,7 % reale, nessun LOCKDOWN al 15 %) | **FAIL** (xfail strict) |
| RO-REC-01, RO-CAP-01 | aperti (PR successive, fuori dalla patch) |
| Windows; `seq` regressivo fra processi (record unico, limite dichiarato da Claude B); comando owner locale di sblocco (non ancora scritto) | NOT_RUN |

Gli xfail dei difetti chiusi sono condizionati alla correzione (`D1_FIX`, presenza di `core.drawdown_hard_stop`): restano
xfail su un albero senza correzione e devono passare con la correzione. Al merge il condizionale si toglie.

### 5-bis. Riverifica su `3f59fa6798f29c62fe9590df87b6bd458f07472c` (correzione RO-DD-08)

- RO-DD-08: XPASS strict (picco non salvato => nessun nuovo ingresso, con o senza hard stop; ritentativo a ogni ammissione).
- Non regressione: oracoli 74/74 con `--runxfail`; campagna 5: 10 PASS, 2 xfail attesi (RO-REC-01, RO-CAP-01).
- **RO-DD-09 (rischio residuo, doppio guasto)**, xfail strict: rialzo del picco non salvato (100 -> 120), perdita delle
  posizioni gia' aperte durante il guasto (cassa 100: LOCKDOWN corretto nel processo), riavvio col disco tornato =>
  picco ricaricato 100, desk NORMAL, ingresso ammesso con drawdown reale 16,7 % >= lockdown 15 %. Con un solo supporto
  di persistenza non e' eliminabile del tutto; e' fail-open sul denaro reale, quindi NON si accetta come limite noto
  senza decisione owner esplicita. Mitigazioni possibili: ricostruzione prudente del picco all'avvio da un registro
  durevole indipendente (scatola nera BB-1 su file separato, letture verificate del conto) o dal ledger dei settlement;
  oppure blocco degli ingressi al riavvio finche' una procedura owner non conferma il picco quando l'ultimo stato
  conosciuto e' "salvataggio in sospeso".

### 5-ter. Riverifica su `c2a7a49944c74bcaa4f407bb568ddcc59fb3e726` (correzione RO-DD-09)

Marker `<db>.drawdown_peak_pending.json` scritto in modo atomico quando il DB non salva il picco; al riavvio vale il
picco piu' alto fra DB e marker, un superamento in uno dei due vale, il marker si cancella solo dopo il salvataggio
riuscito nel DB, marker illeggibile => fail-closed.

- RO-DD-09: PASS (picco 120 ritrovato dopo il riavvio, LOCKDOWN al 15 %).
- Verifiche indipendenti del marker: consumato solo dopo il salvataggio riuscito (guasto attivo prima del riavvio,
  marker conservato); marker corrotto => nessun ingresso; superamento registrato solo nel marker => blocco al riavvio.
  Tutte PASS.
- Non regressione: oracoli e meccanismo 78/78 con `--runxfail`; campagna 5: 10 PASS, 2 xfail attesi (RO-REC-01,
  RO-CAP-01).
- Limite residuo dichiarato da Claude B come KNOWN_LIMITATION: DB e marker entrambi non scrivibili, poi riavvio con
  lo storage ripristinato => picco vecchio. Durante il guasto gli ingressi restano negati (RO-DD-08). E' un percorso
  fail-open su denaro reale: per CLAUDE.md una KNOWN_LIMITATION di questo tipo richiede l'accettazione esplicita e
  tracciata dell'owner, non la classificazione di un agente.

### 5-quater. Riverifica su `2a9cedad1858f800e68fac818ac1a16265b34c4b` (RO-DD-09, opzione C owner)

Decisione owner PF-ROSERPINA-DD09-FAIL-CLOSED-AND-BB3-20261109: opzione C ora (fail-closed al riavvio), B con BB-3,
A (KNOWN_LIMITATION) respinta. Ogni riavvio dopo una chiusura non pulita (sessione aperta nello stato) blocca gli
ingressi (`drawdown_baseline_non_verificata`) finche' l'owner non conferma la baseline con
`owner_confirm_drawdown_baseline` (autorizzazione, motivo, operation_id monouso, doppia conferma, nessun ordine
incerto); uno STOP pulito non richiede conferma.

Prove indipendenti (`tests/roserpina_oracle/test_hard_stop_patch.py`, attive solo con l'API C presente):
- crash => riavvio bloccato; conferma owner => ingresso ammesso; stop pulito => ammesso senza conferma;
- doppio guasto DB + marker, poi riavvio con lo storage ripristinato: picco su disco 100 (vecchio) ma ingressi
  bloccati fino alla conferma (il residuo di 5-ter e' chiuso in modo fail-closed);
- conferma incompleta (6 varianti: autorizzazione, motivo, operation_id, `confirm`, `confirm_again` mancanti o
  falsi) => rifiutata, nessun ingresso;
- ordini incerti => conferma rifiutata; dopo la risoluzione accettata; operation_id gia' usato con successo =>
  rifiutato dopo un nuovo crash (un tentativo rifiutato non consuma l'id: accettabile, nessun effetto);
- `declared_peak=120` => picco 120 e LOCKDOWN al 15 %; salvataggio fallito della conferma => baseline non verificata,
  ingresso negato (il primo motivo della catena puo' essere `drawdown_picco_non_persistito`);
- il reset automatico del ciclo non sblocca.

Esito: 91/91 (con e senza `--runxfail`). Campagna 5 e2e con l'operazione `confirm_baseline` del child f026:
12 PASS, 2 xfail attesi (RO-REC-01, RO-CAP-01); crash senza perdite => bloccato, conferma => ammesso; stop pulito =>
ammesso. Su `main` (senza la correzione) le prove C si saltano e la suite resta 38 PASS, 1 skip, 13 xfail.
Verdetto Claude D: RO-DD-09 VERIFICATO sull'albero di sviluppo; va riconfermato sullo SHA della PR e poi su `main`.

# CHECKPOINT Gate A — close02 (13/10/2026, ~16:00 ora di Roma)

**Solo continuità del lavoro.** Questo branch NON è integrazione, NON è revisione e NON è certificazione indipendente. Nessuna PR, nessun merge, nessun deploy, nessun LIVE, nessuna modifica alla #505. **Gate A: NON PASS.** Spesa AI aggiuntiva: 0 €. Branch costruito da `028cee9` (main).

## Ordine di applicazione (su 028cee9)
1. `artifacts/patches/close02_findings_proposal_v13.patch`: **VERIFICATO** (patch isolata cumulativa v1–v13).
2. `artifacts/patches/close02_v13_to_v14_delta.patch`: v14, delta sopra v13. Suite completa verde; **non verificato in modo indipendente, nessun picco ratificato dall'owner.**
3. `artifacts/patches/close02_v14_to_v15_delta.patch`: v15, solo test (`tests/close02/test_close02_v15.py`), mirati a M8 e al mutante M27 originale.

4. `artifacts/patches/close02_v15_to_v16_delta.patch`: v16 (Claude), solo test (`tests/close02/test_close02_v16.py`), uccide M51. Cumulativo: `close02_findings_proposal_v16.patch`.
5. `artifacts/patches/close02_v16_to_v17_delta.patch`: v17 (Claude), correzione CL16-MINSTAKE-001 + test. Cumulativo: `close02_findings_proposal_v17.patch`.
6. `artifacts/patches/close02_v17_to_v18_delta.patch`: v18 (Claude), correzione CL17-RST-GHOST-001 + test. Cumulativo: `close02_findings_proposal_v18.patch`. Verificato: v17 cumulativa + delta = v18 cumulativa (sha256 7226963402c3…).
7. `artifacts/patches/close02_v18_to_v19_delta.patch`: v19 (Claude), correzioni CL18-CASH-LEDGER-001 e CL19-CASH-REF-001 + test. Cumulativo: `close02_findings_proposal_v19.patch`. Verificato: v18 + delta = v19 (sha256 8f9004ea0234…).
8. `artifacts/patches/close02_v19_to_v20_delta.patch`: v20 (Claude, CLOSE03), correzione CL20-PAR-MANUAL-001 + test. Cumulativo: `close02_findings_proposal_v20.patch`. Verificato: v19 ricomposta + delta = v20 (sha256 20c7dadc7887…).
9. `artifacts/patches/close02_v20_to_v21_delta.patch`: v21 (Claude, CLOSE03), correzione CL21-RACE-ADMIT-001 + test. Cumulativo: `close02_findings_proposal_v21.patch` (**ultimo**). Verificato: v20 + delta = v21 (sha256 a3c0ed140bb7…).

`git apply --check` in sequenza su 028cee9 senza conflitti: verificato. In alternativa `close02_findings_proposal_v14.patch` = v1–v14 in un solo patch.

## Requisiti
- Python **3.11** (verificato con 3.11.17), pytest 9.1.1, pytest-xdist 3.8.0.
- `pip install -r requirements-test.txt` dalla radice del repository.
- Nessuna rete, nessuna credenziale, nessuna chiamata a Betfair: tutto gira su mock, DB temporanei e fixture sintetiche.

## Comandi
```
git clone --branch checkpoint/gate-a-close02 https://github.com/zarbopiero963-droid/Pickfair-nogui.git && cd Pickfair-nogui
python3.11 -m venv .venv && . .venv/bin/activate && pip install -r requirements-test.txt
PY=python bash checkpoint/close02/reproduce.sh v13      # oppure v14 (WIP)
# suite completa (dentro il worktree stampato come WORKDIR/bf2/close02/wt):
python -m pytest -q -p no:cacheprovider -o addopts="" -n 4
# mutazione (dalla radice del worktree con patch applicata):
python <WORKDIR>/bf2/close02/mut_all.py            # tutti i mutanti, oppure: ... mut_all.py M8 M27
```
`reproduce.sh` crea due worktree (base 028cee9 e patch applicata), riscrive in una copia temporanea i percorsi assoluti degli script (`/workspace/pickfair/bf2/…`) ed esegue i controlli.

## Risultati attesi
| Controllo | v13 | v14 (WIP) |
|---|---|---|
| `tests/close02` | 248 passed, 1 xfailed | 271 passed, 1 xfailed |
| Suite completa | 7502 passed, 15 skipped, 2 xfailed, 0 failed | 7525 passed, 15 skipped, 2 xfailed, 0 failed |
| preflight L7b (record base) | 21 / 21 PASS | = |
| preflight L9 (record base) | 38 / 38 OK | = |
| indip L7b (patched) | 21 / 21 completi, t80u 7 / 7 | = |
| indip L9 (patched) | 38 / 38 completi | = |
| test_preflight_L7b/L9 + test_t80u | 39 passed | = |
| L7b eval | v13: 315 CONFORMI / 0 NON / 11 N.A. | v14: 315 / 0 / 11 (`record_specific_L7b_v14.json`) |
| L9 RESTART | v13: 357 / 357 CONFORMI | v14: 357 / 357 (`record_specific_L9_v14.json`) |
| RED→GREEN | v12: 16 RED → 17 GREEN; v13: 24 RED → 25 GREEN | v14: 23 RED → 23 GREEN |
| Mutazione | v12 9/10 (M51 equivalente), v13 13/13 | M1–M80 completa: 77/80 uccisi. Sopravvissuti anche sulla suite completa: M8, M27, M51 (vedi nota) |

**v15 (solo test).**
- M8 ora è ucciso: il gate di segnale regge anche con il modo forzato ad ACTIVE.
- M81, il mutante M27 originale (etichetta P/L nello storico Risk Desk), è ucciso.
- Equivalenti giustificati:
  - M27 ricostruito: `MarketNetRealizedSettlementAggregator` emette sempre `commission_labels(False)`, e VERIFICATO arriva solo dopo, via `payload.update`.
  - M51.
- Log: `mutation_v15_targeted.txt`.

**Nota storica sui mutanti sopravvissuti (v14, suite completa).**
- **M8:** toglie solo il gate di recovery a livello di segnale. A valle restano i gate di modo PAUSED e di pre-submit, quindi è ridondante. Il mutante originale v9 ("gate rimosso **e** modo attivo") era stato ucciso.
- **M27:** il pattern ricostruito è equivalente, perché `realized` porta sempre STIMATO.
- **M51:** equivalente (giustificato in v18).
- Nessuno dei tre è "chiuso con prova": restano da formalizzare.

**Stato v14:** suite completa verde e mutazione completa eseguita. **NON VERIFICATO in modo indipendente** e non ratificato dall'owner.

## v16 — passaggio Grok → Claude (13/10/2026)
Solo continuità. Patch isolata, nessuna integrazione. Gate A **NON PASS**. #505 non toccata; nessuna PR, nessun merge, nessun LIVE. Spesa AI extra: 0 €.

**Verifica del checkpoint (Claude, copia pulita).**
- Ramo `checkpoint/gate-a-close02` alla testa `79e2978` (v15); `681353d` (v14) ne è l'antenato.
- `SHA256SUMS_checkpoint`: **328/328 OK** dalla radice del repo.
- Ambiente: Python **3.11.15** (venv), pytest 9.1.1, xdist 3.8.0, 4 CPU.

| Controllo | Dichiarato (Grok) | Riprodotto (Claude) | Esito |
|---|---|---|---|
| `reproduce.sh v14`: tests/close02 | 271 passed, 1 xfailed | 270 passed, **1 skipped**, 1 xfailed | differenza solo ENV (vedi sotto) |
| preflight L7b / L9 (base) | 21/21, 38/38 | 21/21, 38/38 | uguale |
| indip L7b / L9, t80u | 21/21 + 7/7, 38/38 | uguale | uguale |
| test preflight/indep | 39 passed | 39 passed | uguale |
| Suite completa v14 (-n 4) | 7525 passed, 15 skipped | 7524 passed, **16 skipped**, 2 xfailed, 0 failed | differenza solo ENV |
| Sonda LOAD v3 su v14 (`claude_v16/probe_load_v3_v14_claude.json`) | `probe_load_v3_v14.json` | identica a parte `pid_sonda` (21 campi, volatili) | uguale |
| Valutazione L7b su v14 dalla sonda di Claude | 315 / 0 / 11 | **315 / 0 / 11** | uguale |
| Sonda RESTART v2 su v14 (`claude_v16/probe_restart_v2_v14_claude.json`) | `probe_restart_v2_v14.json` | **0 differenze** su 38 chiavi | uguale |
| L9 ricostruito dalla sonda di Claude (`claude_v16/record_specific_L9_v14_claude.json`) | 38 record, 357/357 | **38 record, 357/357 CONFORMI** | uguale |
| Mutanti M8, M81 (v15) | uccisi | uccisi | uguale |

**Unica differenza.** `tests/close02/test_close02_tk_fallback.py` viene saltato: "ENV: richiede DISPLAY e customtkinter assente". Il container Claude non ha tkinter (CL02-ENV-001). Non è una regressione.

**Mutazioni (`claude_v16/mutation_v16_claude.txt`).**
- **M27 (ricostruito): EQUIVALENTE DIMOSTRATO.**
  - Il sito mutato è `core/pnl_engine.py:556`, `str(realized.get("pnl_status","STIMATO"))`.
  - `realized` viene solo da `_apply_realized_market_net_commission` (`:370-371`), che restituisce `MarketNetRealizedSettlementAggregator.apply()` (`:43-75`).
  - Quel dizionario contiene sempre `**commission_labels(False)`, cioè `pnl_status="STIMATO"` (`:21-26`, perché `False is True` è falso).
  - Fra `:526` e `:556` `realized` non viene modificato.
  - Nessuna sottoclasse né riassegnazione di `_apply_realized_market_net_commission` o di `_market_net_realized_aggregator` nel codice di produzione (grep, test esclusi; le riassegnazioni in `simulation_broker.py` riguardano un altro oggetto).
  - Quindi l'espressione vale `"STIMATO"` per ogni input, come il mutante. VERIFICATO arriva solo dopo, via `payload.update(commission_labels(True))` (`:576`).
- **M51: NON equivalente, ora UCCISO** da `tests/close02/test_close02_v16.py` (solo test).
  - In `_apply_realized_pnl_without_mutating_bankroll` l'hook `on_new_peak` scatta nella finestra solo se `equity_peak <= 0`. Il ripristino `sync_bankroll(bankroll_before)` (`core/risk_desk.py:101-103`) notifica infatti un "nuovo massimo".
  - Col mutante, `_persist_equity_peak` scrive su DB un picco non proveniente da un saldo verificato (contro la regola v12, commento `runtime_controller.py:4096-4098`) e azzera il blocco `equity_peak_non_persistito`.
  - Esiti: v15 + v16 **2 passed**; con il mutante **1 failed**.
- **M8 e M81: uccisi** (riprodotti).
- Mutazione completa M1–M81 su v16: **non ancora rieseguita** da Claude (prossima attività).

**Suite completa v16** (v15 + test v16, -n 4): **7528 passed, 16 skipped, 2 xfailed, 0 failed**.

**Patch.**
- `close02_v15_to_v16_delta.patch`: solo il nuovo file di test.
- `close02_findings_proposal_v16.patch`: cumulativo v1–v16. Verificato che il cumulativo v15 ricostruito dall'albero coincide con `close02_findings_proposal_v15.patch` (a parte le righe `index`).
- `git apply --check` su 028cee9: cumulativo v16 OK; delta v15→v16 sopra v15 OK.

**Prossime attività (in ordine, regola anti-stallo owner).**
1. Mutazione completa M1–M81 su v16 (FULL), per confermare il conteggio 77/80 di v14 più i 3 di v15–v16.
2. Lotti L4, L6 e CASH (record NOT_READY) con le regole L7b/L8b/L9.

**Bloccato (decisione o autorizzazione owner).**
- Revisione indipendente L1/L2 (Sol, a pagamento).
- Ratifica del picco equity ricostruito.
- Semantica `betIds`+`groupBy=MARKET`.

## v17 — Claude, PF-AUTO-MASTER-01 (13/10/2026)
Solo continuità. Patch isolata, nessuna integrazione. Gate A **NON PASS**. #505 non toccata. Nessuna PR, nessun merge, nessun LIVE. Spesa extra: 0 €.

**Ordine di applicazione.** Applicare `close02_v16_to_v17_delta.patch` dopo la v16, oppure in alternativa il cumulativo `close02_findings_proposal_v17.patch`. Verificato su 028cee9 da copie pulite:
- v16 + delta = cumulativo v17, alberi identici;
- l'albero ottenuto coincide con quello testato.

### 1. Mutazione M1–M81 su v16: completa
- Esito: **80 uccisi su 81**. L'unico sopravvissuto è **M27, equivalente dimostrato** (vedi sezione v16).
- Harness: `artifacts/mut_all.py` con soli percorsi locali (`claude_v17/mut_all_local_claude.py`), sottoinsieme di test predefinito, `-n 4`.
- Log: `claude_v17/mutation_all_v16_claude.txt` e riepilogo `claude_v17/mutation_v16_final_summary.txt`.
- **Nota di integrità (errore dell'operatore, corretto).** Per alcuni minuti le modifiche v17 sono state scritte per errore nello stesso albero in cui girava la campagna.
  - Mutanti coinvolti: circa M36–M52.
  - L'albero è stato ripristinato a v16 e gli stessi mutanti sono stati rieseguiti sull'albero pulito: **tutti uccisi** (`claude_v17/mutation_rerun_M36_M52.txt`).
  - Il riepilogo usa i risultati della riesecuzione.

### 2. Lotto L6-APPLY, sottoinsieme monetario: sonda dinamica sulla catena reale
**Strumento.** `claude_v17/probe_apply_money.py`, eseguito sulla catena reale: `RuntimeController` + money management vero + `SimulationBroker`, harness `_catena`.
- Oracoli ricavati dalle regole owner: D1/D2 (#426/6141902809), Q3/Q4 (#506/6141450045), t80u(1) (#426/6143637861), Small Bet (#506/6114505989).
- **Casi "loader reale":** le chiavi assenti o corrotte passano dal vero `SettingsService`, con `absent_keys`/`corrupt_keys`.
- **Controllo positivo** su tutti gli alberi (`CTRL-A1`, `CTRL-L`): configurazione valida ⇒ ordine ammesso. Serve a escludere i falsi CONFORME dovuti a blocchi generici.

**Errori dell'autore corretti prima della pubblicazione.**
1. La fixture non conteneva RSR-17 (`max_open_exposure`), che v16 rende obbligatoria. I blocchi osservati erano quindi dovuti a RSR-17. Corretto aggiungendo RSR-17 alla fixture.
2. L'oracolo di RSR-41 assente era errato. D1 fissa un tetto di 10 €, non un blocco totale. Rifatto con perdita 0 / 9,99 / 10,00.

**Risultati** (`claude_v17/apply_money_{main028,v16,v17,pr505}.json`, riepilogo `apply_money_summary.json`):

| Regola | main 028cee9 | v16 | v17 | #505 (618884e) |
|---|---|---|---|---|
| RSR-38/39/40: cap ordine, mercato, evento (= cap ammesso, oltre bloccato, LAY = liability) | NON CONF | NON CONF (cap non presenti nel patch) | NON CONF | **CONFORME** |
| RSR-38/39/40 assenti via loader ⇒ blocco (D2) | NON CONF | NON CONF | NON CONF | **NON CONF**: il loader inserisce i default 1/2/2 e l'ordine passa |
| RSR-38/39/40 corrotti ⇒ blocco | NON CONF | CONF | CONF | CONF |
| RSR-02/15 assenti o corrotti via loader ⇒ blocco (t80u 1) | NON CONF (default 18%/10000) | **CONF** | **CONF** | **NON CONF** (default 18%/10000) |
| RSR-41 drawdown 10 € (perdita ≥ 10,00 ⇒ stop, 9,99 ⇒ ammesso; anche con chiave assente) | **NON CONF** (CL02-DD-001) | CONF | CONF | CONF |
| RSR-17 max_open_exposure (= cap, oltre bloccato, assente ⇒ blocco D2) | assente ⇒ NON CONF | CONF | CONF | assente ⇒ NON CONF |
| RSR-18 perdita di sessione (5,00 ⇒ stop, 4,99 ⇒ ammesso) | CONF | CONF | CONF | CONF |
| RSR-14 min_stake: nessun aumento automatico | **NON CONF** | **NON CONF** | **CONF** | **NON CONF** |

### 3. Nuovo difetto CL16-MINSTAKE-001 (P1, corretto in v17)
**Il difetto.**
- Il money management portava lo stake sotto il minimo fino al minimo (`core/money_management.py`, `_clamp(recommended, min_stake, …)`). Esempio: segnale da 0,30 € inviato come 0,50 €.
- Questo viola la regola owner «non aumentare automaticamente stake/liability per raggiungere un minimo» (#506/6114505989, #426/6068305820).
- Presente su main, v16 e #505.

**Correzione v17.** Stake sotto il minimo ⇒ rifiuto `stake_sotto_minimo`. Il confronto è esatto (`validators.exceeds_cap`, valori illeggibili ⇒ rifiuto). La riduzione al tetto singolo resta invariata.

**Prove.**
- RED su v16: 2 su 5 falliscono (`tests/close02/test_close02_v17_min_stake.py`).
- GREEN su v17: 5 su 5.
- Mutazione mirata della correzione: rimozione del controllo ⇒ ucciso; tolleranza di 0,01 ⇒ ucciso.

**Due test esistenti aggiornati** (codificavano l'aumento vietato; lo scopo dichiarato è conservato):
- `tests/core/test_min_stake_single_source.py::test_money_management_fallback_follows_trading_config`.
  - Asseriva che 1,0 venisse portato a 2,0.
  - Ora asserisce il rifiuto con `metadata["min_stake"] == 2.0`, che continua a dimostrare la fonte unica `trading_config.MIN_STAKE`. Con il vecchio fallback di 0,10 fallirebbe.
- `tests/core/test_money_management_caps_and_malformed.py::test_tiny_bankroll_rejects_with_suggested_cap_stake`.
  - Lo scenario passa a quota 1,05, dove lo stake è ≥ minimo e supera il cap. Il contratto `supera_max_single_bet` con stake suggerito 0,075 resta testato.
  - Aggiunto `test_tiny_bankroll_stake_below_minimum_is_rejected_not_raised` per il vecchio scenario.

**Suite completa v17:** **7534 passati, 16 saltati, 2 xfailed, 0 falliti**.

### 4. Difetto di specifica nel lotto L6, così come preparato
I casi APPLY generati da template non sono eseguibili così come sono:
- **oracoli contraddittori:** K3/K6/K9/K12 dicono insieme «10,00 = limite ⇒ ammesso» e «cap assente/corrotto ⇒ nessun ordine»;
- **casi non discriminanti:** K2 usa un ordine da 0,01 € che resta sotto sia il cap 25 sia il cap 15, quindi non distingue il valore vecchio dal nuovo;
- **duplicati:** K1/K4/K7/K10 identici, cambia solo `covers`.

Sostituiti, per le chiavi monetarie, dai casi discriminanti della sonda. **I testi normativi non sono stati modificati.** Il resto di L6 (FAILSAFE, PARITY, RACE, AUDIT, OBSERVABLE, S1) resta da strutturare con lo stesso metodo.

### 5. Finding per il Gate A #505 (fase 3, correzioni #505: NON applicate qui)
- **F-505-D2 (P0):** con RSR-38/39/40 assenti, il loader della #505 inserisce i default 1/2/2 e l'ordine passa. Questo viola D2 («nessun default implicito; assente ⇒ blocco»). Anche con attributo `None`, la #505 non applica alcun limite (casi `RSR-38-A5`, `-39-A3`, `-40-A3`).
- **F-505-T80U (P0):** la #505 non contiene il gate t80u della v16. RSR-02/15 assenti ⇒ si usano i default 18%/10000.
- **F-505-RSR17 (P0):** la #505 non contiene il gate D2 di RSR-17 della v16.
- **F-505-MINSTAKE (P1):** CL16-MINSTAKE-001 presente anche nella #505.
- **Conseguenza:** le correzioni della #505 devono incorporare il patch close02 (t80u, D2, drawdown, min_stake) e aggiungere D2 per RSR-38/39/40.

### Prossime attività
1. Strutturare il resto di L6 (FAILSAFE, PARITY, RACE…) con sonde discriminanti.
2. L4 (461 record: BF, AUTH/SCHEMA/ERROR/STATE/PROOF, OBSERVABLE, S1…) e CASH (5 record).
3. Preparare il pacchetto di revisione indipendente con le nuove evidenze (senza chiamate a pagamento).

### 6. Lotto CASH (aggiunta v17, nessuna modifica al codice)
Il lotto CASH ha 5 record (PF-CASH-04/19/21/23/25), con 3 casi ciascuno, dalla fonte #461/6085516609 (sha 56fc7836…, paragrafo «Dopo richiesta cashout» V1–V5). Le sonde sono dinamiche, girano su `#505` (618884e) e su `v17`, senza rete né credenziali.

Script e output stanno in `artifacts/claude_v17/cash/` (`probe_cash04.py`, `probe_cash25.py`, `probe_cash_rest.py`, `cash*_{pr505,v17}.json`, `cash_summary.json`).

Riproduzione: `PROBE_ROOT=<albero> PROBE_OUT=<out.json> python3.11 probe_cash*.py`.

| Caso | #505 | v17 | Nota |
|---|---|---|---|
| CASH-04-R1 | CONFORME | CONFORME | Cancel del residuo B2 confermato prima dell'hedge; hedge LAY 2.86@7.0 solo sul matched |
| CASH-04-R1 cancel KO | CONFORME | CONFORME | Nessun hedge |
| CASH-04-R2 | CONFORME | CONFORME | listCurrentOrders KO ⇒ CASHOUT_FAILED |
| CASH-04-R3 | NON_CONFORME | NON_CONFORME | Ordine ambiguo senza betId ignorato, hedge pubblicato (identità solo per betId, cfr. L06-F1) |
| CASH-25-CTRL | CONFORME | CONFORME | |
| CASH-25-R1 | CONFORME | NON_CONFORME | Il fencing esiste solo nella #505 |
| CASH-25-R1b | NON_CONFORME | NON_CONFORME | Barriera per selezione: l'altra selezione dello stesso mercato passa. Interpretazione dello scope, da confermare in revisione |
| CASH-25-R2 | CONFORME | NON_CONFORME | `cashout_invalidation_stale` |
| CASH-25-R3 | NON_CONFORME | NON_CONFORME | Il fencing finisce con l'hedge parziale anche con il residuo aperto. Oracolo preparato, non verbatim |
| CASH-21-R1 (×2 esiti) | CONFORME | CONFORME | Lordo dai profit per-bet = −50/7 su entrambi gli esiti |
| CASH-21-R2 | CONFORME | CONFORME | Eseguito 1,50: lordo +1,00, netto 0,955. Il residuo esposto ricade su CASH-25-R3 |
| CASH-21-R3 | CONFORME | CONFORME | Retry ⇒ `CLEARED_SETTLEMENT_DUPLICATE` per ogni bet, un solo evento per bet |
| CASH-19-R1/R2 | NON_CONFORME | NON_CONFORME | Il poller di chiusura legge solo i cleared, senza confronto con i current (L07-F5, MISSING) |
| CASH-19-R3 | test | test | `test_betfair_client_cleared_orders.py` (moreAvailable incompleto ⇒ raise): PASS su entrambi |
| CASH-23-R1 | NON_CONFORME | NON_CONFORME | **Nuovo CL17-RST-GHOST-001**, vedi sotto |
| CASH-23-R2 | NON_CONFORME | CONFORME | recover KO ⇒ v17 blocca (CL02-RST-001). La #505 ignora l'esito della recovery (`runtime_controller.py:2456-2464`) |
| CASH-23-R2b | NON_CONFORME | NON_CONFORME | Hedge ambiguo pre-crash non visibile al riavvio: nessuna traccia locale, perché l'hedge passa da `OrderRouter.place` senza saga. Debole: Betfair rende visibile l'ordine se esiste |
| CASH-23-R3 | test | test | `test_poll_dedupe_durevole_dopo_riavvio_nessuna_riemissione`, `test_sim_giro_checkpoint_durevole_presente_non_riemette`: PASS su entrambi (24/24 con i test cleared) |

**CL17-RST-GHOST-001 (P0, aperto, non corretto).** Al riavvio, un hedge LAY 2.86@7.00 ancora EXECUTABLE sull'exchange viene recuperato come ghost order (`LIVE_WITH_RECOVERED_GHOSTS`), ma:
- `STARTUP_GHOST_ORDER_RECOVERED` non ha alcun subscriber (main, #505, v17);
- `table_manager.total_exposure()` resta 0;
- un nuovo ingresso sullo stesso mercato viene ammesso, con una liability reale di ≈17,14 € invisibile al tetto `max_open_exposure` di 10 €.

Questo viola OWNER-EXPOSURE-01 («matched e unmatched», «non sottostimare», «esito non modellato ⇒ fallisce in sicurezza») e V3.

**Perché non è corretto nel patch.** La ricostruzione dell'esposizione dai ghost order richiede il collegamento ordine ↔ tavolo ↔ posizione e le condizioni di rilascio. OWNER-SETTLEMENT-02 li ordina ai passi 1 e 5 e vieta tavoli bloccati in permanenza. Due opzioni per la correzione, da scegliere in revisione o in #505 fase 3:
- (a) occupare un tavolo `RECOVERY` con il caso peggiore dell'ordine, rilasciato dal settlement o dalla scomparsa dai current;
- (b) bloccare il nuovo rischio finché esistono ordini remoti non modellati, ricontrollando a ogni giro.

La sonda `probe_cash_rest.py` è la riproduzione RED.

**Bilancio CASH.** Casi valutati: 20 (15 dal record più R1b, R1-cancel-ko, R2b e lo sdoppiamento per esito di CASH-21-R1).

| Albero | Conformi | Non conformi | Coperti da test esistenti |
|---|---|---|---|
| #505 | 10 | 8 | 2 |
| v17 | 9 | 9 | 2 |

Il lotto resta **NOT_READY** per il Gate A: CASH-19 è MISSING, CL17-RST-GHOST-001 è aperto, CASH-04-R3 e CASH-25-R3 sono aperti.

## v18 — Claude, PF-AUTO-MASTER-01 (13/10/2026, mandato owner di correzione autonoma)
Nessuna modifica a #505 o a `main`. **Gate A: NON PASS.** Le prove stanno in `artifacts/claude_v18/`.

### 1. Correzione CL17-RST-GHOST-001 (P0), RED → GREEN
**Dipendenze, nell'ordine di OWNER-SETTLEMENT-02.**
- Passo 1 (collegamento): l'identità è il `betId` dell'exchange, unica e già usata dal poller dei settlement (`cleared:<market>:<betId>`).
- Passi 2-4 (riconciliazione, settlement ritardato/UNKNOWN, idempotenza): restano al poller esistente; questo patch non li tocca.
- Passo 5 (condizioni di rilascio): un ordine esce dal registro solo quando una lettura COMPLETA e RIUSCITA dei correnti del suo mercato non lo contiene più. Su Betfair un ordine resta fra i correnti (anche EXECUTION_COMPLETE) finché il mercato non è regolato, quindi la sua assenza significa che non c'è più capitale impegnato. Lettura fallita ⇒ nessun rilascio.
- Passo 6: non applicabile, il patch non rimuove alcun rilascio esistente.
- Nessun tavolo viene occupato, quindi nessun tavolo resta bloccato in permanenza.

**Calcolo (OWNER-EXPOSURE-01).** Il nuovo modulo `core/remote_exposure.py` calcola il caso peggiore netto per mercato sugli esiti mutuamente esclusivi:
- ogni selezione presente vince, oppure nessuna;
- parte abbinata certa; il residuo a riposo al peggiore fra «non si abbina» e «si abbina per intero»;
- liability LAY al prezzo più alto fra medio e limite; vincita BACK al più basso;
- arrotondamento per eccesso al centesimo;
- riga non modellabile (lato ignoto, prezzo ≤ 1, importi non finiti o negativi, handicap ≠ 0) ⇒ esposizione infinita del mercato, quindi blocco di ogni nuovo rischio finché una rilettura pulita non la scioglie.

**Collegamento nel runtime.**
- `StateRecovery.recover()` restituisce l'istantanea completa `remote_active_orders`, compresi gli ordini già registrati in un riavvio precedente.
- `start()` carica il registro prima di passare ad ACTIVE.
- `RuntimeController._open_exposure_total()` (tavoli + registro) sostituisce `table_manager.total_exposure()` in ingresso segnale, auto-trade, stop di esposizione e snapshot.
- La rilettura (`_refresh_remote_exposure`) avviene all'ammissione, solo se il registro non è vuoto.
- Gli ordini piazzati da questo processo non entrano nel registro: li contano i tavoli.

**Prove.**
- **RED su v17 pulito** (snapshot `refs/claude/v17snap`, modulo presente ma non collegato): 7 test runtime falliscono, perché esce `CMD_QUICK_BET` con 10 € di esposizione remota. File `red_v17_clean.txt`.
- **GREEN:** `test_close02_v18_remote_exposure.py` 21/21; suite completa py3.11 finale 7555 passed, 0 failed.
- **Suite completa py3.11:** `suite_v18_py311.summary.txt`, rilanciata sull'albero finale.
- **Mutazione mirata:** 16 mutanti, 15 uccisi. G10 è equivalente: `validators.money_sum` (`core/validators.py:242-263`) restituisce già Infinity per un addendo non finito. File `mut_v18.txt`.
- **Sonda CASH-23 aggiornata** per seguire `start()`: su v18 R1 è CONFORME (esposizione dopo la recovery 17,16 €, ingresso rifiutato `max_open_exposure_exceeded`); su #505 resta NON_CONFORME.

**Limiti dichiarati.**
- Un ordine ambiguo pre-crash e assente dai correnti al riavvio non lascia traccia locale (CASH-23-R2b). Se assente, non c'è capitale impegnato sull'exchange; il caso resta registrato come osservazione.
- Il P/L realizzato di questi ordini resta al poller: vedi il punto 3.

### 2. CASH-25 R3 e R1b: verifica della fonte (gli oracoli preparati non sono requisiti)
- **Fonte verbatim:**
  - PF-CASH-25: «Fencing contro ingressi in volo per tutto lo scope cashout»;
  - PF-CASH-01: «Richiesta identificata e scope esplicito»;
  - V2: «parziale→contabilizzare solo eseguito e mantenere residuo».
- **R3:** nessuna fonte estende il fencing oltre la richiesta di cashout. V2 riguarda quantità e liability residue, cioè l'esposizione, non il fencing. Riclassificato `NA_ORACOLO_SENZA_FONTE`.
- **R1b:** nessuna fonte estende lo scope di un cashout per selezione alle altre selezioni. Riclassificato `NA_ORACOLO_SENZA_FONTE`.
- **Caso aggiunto con fonte, CASH-25-R-EVENT** (fonti #351/6087232395 PF-HARD-F06-FENCE «scope event e market entrambi coperti» e #453/6085517147 F06 «begin/end CASHOUT event_name-only o altro mercato stesso evento: valid=True, atteso False»):
  - #505: NON_CONFORME. La barriera di un cashout con solo `event_name` ha scope vuoto e l'ingresso su un altro mercato dell'evento passa. Coincide con F06 già registrato su #453, non è un finding nuovo.
  - v18: NON_CONFORME, perché il fencing manca del tutto.

### 3. Nuovo finding CL18-CASH-LEDGER-001 (P0 LIVE, aperto)
- **Difetto:** l'hedge del cashout passa da `CashoutExecutor → core.OrderRouter.place` e non viene mai scritto nel ledger ordini del bot (`orders`, scritto solo da `TradingEngine.insert_order`). Il poller dei settlement LIVE filtra le righe cleared per l'allowlist del ledger e scarta quella dell'hedge.
- **Sonda `probe_cash_ledger.py`** (BACK 10@2 vince, hedge LAY 20/7@7 perde): P/L realizzato applicato **+10,00 invece di −7,14**, su v17/v18 e su #505.
- **Effetto:** daily-loss e drawdown sottostimati di 17,14 €.
- **SIM:** non colpita, perché regola dal registro del broker, che contiene tutti i bet.
- **Fonti:** V1 «fill completo→riconciliare payoff netto», PF-CASH-21, #506/6124429007 «P&L realizzato (solo da listClearedOrders)».
- **Prossima correzione isolata (v19):** registrare l'hedge nell'identità del bot con il riferimento della posizione che riduce (#426/6067971776 «cashout/hedge orders inheriting the ref of the position they reduce»).

### Bilancio CASH aggiornato (22 casi)
| Albero | Conformi | Non conformi | NA (oracolo senza fonte) | Coperti da test |
|---|---|---|---|---|
| #505 | 10 | 8 | 2 | 2 |
| v18 | 10 | 8 | 2 | 2 |

Note sui file:
- Il ricalcolo di CASH-25 è girato sull'albero v18 (`cash25_v18tree.json`): il codice di fencing è identico a v17.
- `cashrest_v17.json` (in `claude_v17/cash/`) resta l'esito pre-correzione.

### Prossime attività
1. v19: CL18-CASH-LEDGER-001 (RED→GREEN, una sola modifica attiva).
2. L6 resto: FAILSAFE con chiave corrotta via loader reale per le 41 chiavi su main/#505/v18; poi PARITY, RACE, AUDIT, OBSERVABLE, S1.
3. L4.

## v19 — Claude, PF-AUTO-MASTER-01 (13/10/2026)
Nessuna modifica a #505 o a `main`. **Gate A: NON PASS.** Le prove stanno in `artifacts/claude_v19/`.

**Ordine di applicazione, passo 7:** `artifacts/patches/close02_v18_to_v19_delta.patch`. Cumulativo `close02_findings_proposal_v19.patch` (**ultimo**). Verificato: v18 cumulativa + delta = v19 cumulativa (sha256 8f9004ea0234…).

### 1. CL18-CASH-LEDGER-001 (P0 LIVE) e CL19-CASH-REF-001 (P0) corretti nel patch isolato
**CL18 — l'hedge mancava dal ledger del bot.**
- `CashoutExecutor` riceve il ledger del bot (`Database`).
- Scrive la riga `INFLIGHT` PRIMA dell'invio; se la scrittura fallisce, nessun ordine (`CASHOUT_FAILED` REJECTED `ledger_write_failed`).
- Dopo l'invio aggiorna la riga con l'esito normalizzato dell'OrderRouter e il `betId`. Stati: `MATCHED`, `PLACED` (a riposo), `FAILED`, `AMBIGUOUS` (`order_unknown` o eccezione del place). Mai `INFLIGHT` dopo l'invio.
- Un aggiornamento fallito dopo un ordine possibile è registrato come allarme critico: l'ordine può esistere e richiede riconciliazione.
- L'hedge entra così in `get_bot_active_orders()`, e il poller LIVE dei settlement realizza anche il suo P/L.
- Cablaggio: `cabla_catena_cashout(..., ledger=...)`; headless e GUI passano `self.db`. Nessun cambiamento visivo, quindi design handoff N/A.

**CL19 — riferimento `customerRef` condiviso fra tutti i cashout.**
- **Difetto:** il `customerRef` dell'hedge era il campo `source` (`"TELEGRAM"`/`"UI"`), valido per Betfair e quindi inviato sul wire (`betfair_client.py` `_normalize_customer_ref`). Ogni cashout aveva lo stesso ref: con la de-dup Betfair di 60 s (#506 BF2-D001), un CASHOUT_ALL su più mercati poteva perdere gli hedge successivi al primo.
- **Correzione:** il ref è derivato per intento (`derive_customer_ref("co", mercato, selezione, lato, prezzo, stake, event_key)`), conforme a Betfair. Lo stesso intento ripetuto dà lo stesso ref; mercati diversi danno ref diversi.

**Prove.**
- **RED su v18 pulito:** 11/11 falliscono (`red_v19_clean_v18.txt`). Il RED comportamentale con l'API esistente mostra `'TELEGRAM' == 'TELEGRAM'` su due mercati diversi. Il RED di CL18 è anche la sonda `claude_v18/probe_cash_ledger.py` (+10,00 invece di −7,14).
- **GREEN:** `test_close02_v19_cashout_ledger.py` 11/11; il test end-to-end ledger → poller dà un P/L lordo di −7,16 (hedge 2,86).
- **Suite completa py3.11 sull'albero finale:** 7566 passed, 0 failed (`suite_v19_py311.summary.txt`).
- **Mutazione mirata:** 11/11 uccisi (`mut_v19.txt`).

### 2. L6 FAILSAFE: 41 chiavi × 3 valori corrotti via loader reale
- **Sonda:** `l6/probe_l6_failsafe.py`. Per ogni chiave: valore `abc`, `nan` o `inf` salvato su DB temporaneo, caricato da `SettingsService` reale, poi segnale valido sulla catena reale. Nessuna rete.
- **Oracolo uniforme dalla fonte** (phase_row_verbatim L6 FAILSAFE, #461/6085510337): «Dati corrotti … non devono consentire nuovi effetti monetari non autorizzati». Controllo con config valida ⇒ ammesso.

| Albero | Casi | Non conformi | Note |
|---|---|---|---|
| main 028cee9 | 123 | 114 | Conformi solo RSR-17/18/19 (cap già validati singolarmente) |
| #505 618884e | 123 | 102 | In più RSR-38/39/40 |
| v18 | 123 | **0** | `config_corrotta` 108, gate t80u 15; ogni chiave marcata corrotta dal loader |

- **Conseguenza per #505 (fase 3):** serve il gate `config_corrotta` del patch close02 (FAILSAFE in 34 controlli).

### Prossime attività
1. L6 resto: PARITY (BACK/LAY, GUI/headless), RACE, AUDIT, OBSERVABLE, S1, con sonde discriminanti.
2. L4.
3. Pacchetto di revisione indipendente (senza spesa).

## Tabella SHA-256 (file chiave; elenco completo in `SHA256SUMS_checkpoint`)
| File | sha256 |
|---|---|
| artifacts/patches/close02_findings_proposal_v13.patch | 6fef237dc74a37ec2ee9e447e6ac791e058b64ca5a317222c3302f8ebed4a1eb |
| artifacts/patches/close02_v13_to_v14_delta.patch | 645da8f195233aa885a0096f1d96f6f335348f30b5e569b08fe74b27292fde64 |
| artifacts/patches/close02_findings_proposal_v14.patch | 13c1bf8d2988a49c9ec48e6e9f7c49346d89a9cb2e98d1fd955420805cd47d22 |
| artifacts/record_specific_L7b.json | 4246a4f198fdd81122ca7c3ba8caf8d1572407a53c487419229ecc697ad2d537 |
| artifacts/record_specific_L7b_patched.json | 4291887f768332d81b0cee9efb2d6a7d153db558a6bad51784c2ea290ea2cf9e |
| artifacts/record_specific_L9.json | d192c8b3cf45ea1624007452816a3dc7a00ebbcfaa240d26b0349a64a70205a5 |
| artifacts/record_specific_L9_patched.json | 731c0933c79da95d13b4142b2dd8d828ef65475b0d97a6081e555011a2ef338c |
| artifacts/findings_register.json | 2ef0c09fc32e30d2c8a0ff13ccd854bf473fff6aa7e269915de7e1bce28bed21 |
| artifacts/mut_all.py | 390260a975cf08c8859538506b2cbce9f97971d5f3d89e9276c18fd02c0b1df5 |
| reproduce.sh | 6aba84823816656aa2c0a0fdcd2e33e64b8d9712b7d171cf86abd7055af17ee2 |
| artifacts/patches/close02_v14_to_v15_delta.patch | 3edf28b9e864ebdf32c46b7d335c87293cce64ca3f8da38250b34710ade85bf1 |

## Contenuto
- **`artifacts/`**: copia della cartella di lavoro close02.
  - Script: probe_*, preflight_*, indep_check_*, eval_pred*, build_*, mut_*, test_preflight*, test_t80u.
  - Record P0 L1–L9 (base e patched, incluse le viste v13), findings_register, lotti (lot_*, sp_*, review_digest_*).
  - Output delle sonde (probe_*.json), chiavi di sonda (*_keys*.json), decisioni owner (owner/), patch e delta v8–v14 (patches/).
  - Log RED/GREEN/mutazione, post e read-back, SHA256SUMS_*.
- **`bf2/`**: dipendenze degli script.
  - `s3_461.jsonl`: commenti pubblici della #461.
  - `repair01/semantic_preflight.py` e `values_registry.json`.
  - Fonti `opt01/sources/608*.md`, `d03v2/data/ALIGN-D02_ATOMIC_WORK_REGISTER.json`, `repair02/src_6089077286.md`.
- **Esclusi:**
  - `sol_L7` (richieste e risposte AI);
  - i log completi di regressione che contengono segreti FITTIZI dei test di redazione: restano solo le ultime righe, in `*.summary.txt`;
  - i worktree.

## Controlli prima del push
- **Scansione credenziali:** pattern ghp_/github_pat_/sk-/xai-/AKIA/chiavi private/token Telegram/IBAN/ssoid/app_key/session_token ed email. Nessuna credenziale reale.
  - Due hit "AKIA" in `s3_461.jsonl` sono dentro un blob base64 di un allegato.
  - I segreti fittizi dei test (es. `SEGRETISSIMO`) sono stati rimossi dai log.
- **Dati Betfair:** nessun estratto conto reale; le fixture v14 sono sintetiche (movimenti 1000/250/−11,25/−300/−100/40).

## Finding
- **Chiusi nel patch** (proposta, non certificati):
  - t80u regole 1–3;
  - RSR-38..40;
  - M10/M19/M27 (v9);
  - picco persistito a ogni nuovo massimo;
  - registro sparito ⇒ fail-closed;
  - etichette STIMATA/VERIFICATA;
  - netto conservativo 4,5% + 0,01 € per daily-loss e drawdown;
  - picco gonfiato dal P/L transitorio;
  - M39 ucciso con prova;
  - migrazione pre-v9 opzione A (v13).
- **Aperti:**
  - semantica `betIds`+`groupBy=MARKET` non confermata (P/L STIMATO);
  - picco delle installazioni base (v14 WIP);
  - picco confrontato con `availableToBetBalance` invece dell'equity (scelta conservativa, da ratificare);
  - lotti L4 (379 + 20), L6 (190), CASH (5) NOT_READY;
  - L1/L2 in attesa di revisione indipendente (serve autorizzazione owner).

## Decisioni owner (#426)
- t80u: https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/426
- Migrazione pre-v9, opzione A: https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/426#issuecomment-6145958246
- Ricostruzione del picco, opzione A: https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/426#issuecomment-6146717266

## Prossime attività / come riprendere
1. Lotti Gate A L4, L6 e CASH con le regole L7b/L8b/L9.
2. Lotti Gate A L4, L6 e CASH (sonde dinamiche) con le stesse regole.
3. Revisione indipendente L1/L2 solo con autorizzazione owner.

## CLOSE03 — v20, Claude (mandato owner «PICKFAIR GATE A CLOSE03», 13/10/2026)
Nessuna modifica a #505 o a `main`. **Gate A: NON PASS. LIVE: NO-GO.** Le prove stanno in `artifacts/claude_v20/`.

### 0. Verifica della baseline v19 dichiarata
| Punto | Esito |
|---|---|
| Branch remoto | `8ef215b` = commit dichiarato |
| `main` | `028cee9` (invariato) |
| `#505` | `618884e` (invariata) |
| SHA256SUMS_checkpoint | 385/385 OK |
| Catena delle patch | v13 + delta v14…v19 applicati in sequenza su 028cee9 = cumulativa v19 (sha256 8f9004ea…); tree ricomposto `ccb4a751` = tree testato |
| Riproducibilità | suite completa sull'albero v19 ricomposto da zero: **7566 passed, 0 failed** (`suite_v19_baseline_ricomposta.summary.txt`) |
| Scansione segreti dell'intero checkpoint | unico match: la riga di policy in questo file; nessuna credenziale |
| #506 | commenti 6151964578 (v18) e 6152140837 (v19) restituiti dall'API alla pubblicazione |

**Differenze rispetto alla baseline:** nessuna.

### 1. L6 PARITY — sonda fra corsie d'ingresso (`probe_l6_parity.py`)
**Perché una sonda nuova.** I 771 casi del template PARITY ripetono gli stessi tre oracoli (K1…K12 identici), quindi non discriminano fra alberi. Sostituiti, come per APPLY, da casi discriminanti sull'invariante della fase: «stessa regola per BACK/LAY e GUI/headless/MCP; non confondere stake e liability».

**Sonda:** corsia segnale (`_on_signal_received`) contro corsia GUI (`DutchingController.manual_bet`), stessa catena reale.

| Caso | main | #505 | v19 | v20 |
|---|---|---|---|---|
| controllo | C | C | C | C |
| LAY liability oltre il cap | NC (manuale ammette) | NC | C | C |
| BACK uguale al cap | C | C | C | C |
| perdita ≥ 10 € (D1) | NC | NC | NC | C |
| config corrotta | NC | NC | NC | C |
| risk stop (P38) | NC | NC | NC | C |
| picco non persistito | NC | NC | NC | C |
| esposizione remota (CL17) | n/a | n/a | NC | C |
| MCP | NOT VERIFIED (corsia inesistente) | | | |

**SIM/LIVE.** La regola è lo stesso codice del runtime per entrambe le modalità. Nessuna prova sull'exchange reale: **NOT VERIFIED** per LIVE.

### 2. CL20-PAR-MANUAL-001 (P0) corretto in v20, RED → GREEN
**Difetto.** Le corsie GUI (bet manuale e dutching `precheck`) pubblicavano `CMD_QUICK_BET` senza i blocchi di nuovo rischio della corsia segnale:
- drawdown 10 €;
- config corrotta;
- risk stop;
- picco non persistito;
- recovery di avvio bloccata;
- daily-loss pending;
- esposizione remota;
- inoltre il `precheck` dutching non applicava il cap assoluto `max_open_exposure` (RSR-17, D2).

`RiskGate` del TradingEngine controlla solo limiti per-ordine (stake minimo, quota, max_win, book, liquidità). Fonti: L6 PARITY; D1/D2; t80u; #506/6144980907 (blocco anche «nel dutching manuale»).

**Correzione.**
- `RuntimeController.entry_risk_block_reason()`: stessa regola della corsia segnale, senza effetti collaterali.
- `manual_bet` e `precheck` la applicano; un gate che solleva ⇒ blocco.
- L'esposizione delle corsie GUI è `_open_exposure_total(refresh=True)` (tavoli + registro remoto).
- Il `precheck` applica `max_open_exposure`.

**Prove.**
- **RED su v19 pulito:** 14/15 falliti (`red_v20.txt`).
- **GREEN:** 19/19.
- **Mutazione mirata:** 12/12 uccisi (`mut_v20.txt`). Il primo giro (8/12) ha mostrato 3 test mancanti, aggiunti: equity NaN, daily-loss pending, gate che solleva.
- **Suite completa py3.11:** **7585 passed, 0 failed**.
- **Test esistenti adeguati:** 9 file di test dutching usavano config finti senza `max_open_exposure`; per D2 un cap assente blocca ogni nuovo rischio, quindi le fixture ora dichiarano il cap (`max_open_exposure = 1_000_000.0`). Nessuna asserzione indebolita.
- **Caso di blocco esplicito aggiunto:** cap assente ⇒ `precheck` bloccato.

### 3. L6 RACE — nuovo CL21-RACE-ADMIT-001 (P0, aperto, prossima correzione v21)
**Sonda `probe_l6_race.py`.** Il bus reale consegna con 4 worker. Due segnali concorrenti da 0,60 su mercati diversi, con `max_open_exposure` 1,00; interleaving forzato con una barriera nel sizing.

| Albero | Ordini ammessi | Esposizione registrata | Verdetto |
|---|---|---|---|
| v20 | 2 | 0,60 | NON_CONFORME |
| main | 2 | 0,60 | NON_CONFORME |
| #505 | 1 | 0,60 | CONFORME (lock di ammissione) |

Due effetti:
1. il cap viene superato (1,20 > 1,00);
2. entrambi i segnali prendono lo stesso tavolo FREE e il secondo `activate` sovrascrive il primo: esposizione registrata 0,60 invece di 1,20, quindi una prenotazione persa.

Con cap 1,20 (uguale) #505 registra correttamente 1,20.

### 4. CL21-RACE-ADMIT-001 (P0) corretto in v21, RED → GREEN
**Correzione.** `RuntimeController._risk_admission_lock` (`RLock`, con fallback di classe come in #505) rende atomici lettura dell'esposizione e prenotazione del tavolo su tutte le corsie che creano nuovo rischio:
- `_on_signal_received`;
- `_evaluate_and_maybe_submit_auto_next_trade`;
- `reload_config`;
- `DutchingController.manual_bet` ed `execute`.

Ordine dei lock verificato: sempre ammissione → `_daily_loss_stop_lock`. L'auto-trade viene chiamato fuori dal lock daily-loss (`runtime_controller.py`, `_on_close_position`), quindi nessuna inversione e nessun deadlock.

**Prove.**
- **RED su v20:** 5/5 falliti (`red_v21.txt`): 2 segnali oltre il cap; tavolo condiviso (0,60 registrati invece di 1,20); 4 segnali → 4 ordini con cap 1,80; manuale + segnale → 2 ordini con cap 1,00.
- **GREEN:** 8/8 (con i test di attesa del lock per reload, auto-trade ed execute).
- **Mutazione mirata:** 7/7 uccisi.
- **Suite completa py3.11:** **7593 passed, 0 failed**.
- **Sonda `probe_l6_race.py` su v21:** CONFORME (1 ordine con cap 1,00; 2 ordini ed esposizione 1,20 con cap 1,20).

**Residuo dichiarato.** v21 serializza ma non porta il fencing per epoca della #505. Un ingresso già accodato prima di un cashout ed eseguito dopo resta ammesso: CASH-25-R2 NON_CONFORME su v21, CONFORME su #505.

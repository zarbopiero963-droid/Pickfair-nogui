# CHECKPOINT Gate A — close02 (13/10/2026, ~16:00 ora di Roma)

**Solo continuità del lavoro.** Questo branch NON è integrazione, NON è revisione e NON è certificazione indipendente. Nessuna PR, nessun merge, nessun deploy, nessun LIVE, nessuna modifica alla #505. **Gate A: NON PASS.** Spesa AI aggiuntiva: 0 €. Branch costruito da `028cee9` (main).

## Ordine di applicazione (su 028cee9)
1. `artifacts/patches/close02_findings_proposal_v13.patch`: **VERIFICATO** (patch isolata cumulativa v1–v13).
2. `artifacts/patches/close02_v13_to_v14_delta.patch`: v14, delta sopra v13. Suite completa verde; **non verificato in modo indipendente, nessun picco ratificato dall'owner.**
3. `artifacts/patches/close02_v14_to_v15_delta.patch`: v15, solo test (`tests/close02/test_close02_v15.py`), mirati a M8 e al mutante M27 originale.

4. `artifacts/patches/close02_v15_to_v16_delta.patch`: v16 (Claude), solo test (`tests/close02/test_close02_v16.py`), uccide M51. Cumulativo: `close02_findings_proposal_v16.patch`.

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

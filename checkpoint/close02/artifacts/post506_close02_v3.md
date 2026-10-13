## PF-OWNER-CLOSE-ALL-02 — v3: decisioni owner #426/6141902809 applicate (costo AI 0 €)

**Gate A: NON PASS.** F1–F6 e i nuovi D1/D2 sono solo PROPOSTE in un worktree isolato da 028cee9. Nessun push, merge, #505, deploy o LIVE.

### Decisioni owner (testo letto verbatim su #426/6141902809)
- **D1 — drawdown:** la soglia è configurabile con la nuova chiave `roserpina.max_drawdown_abs`. La soglia effettiva è min(valore configurato, 10,00 €), che è il tetto della campagna #351, in SIM e LIVE. Se la chiave è assente vale il tetto. Una soglia presente ma non valida porta al rifiuto `drawdown_threshold_non_valido`. La soglia resta distinta dal cap di esposizione.
- **D2 — cap mancante:** se `max_open_exposure` è assente, il nuovo rischio viene bloccato (`max_open_exposure_assente`) nel runtime A2, nell'auto-next e in `manual_bet`. Non c'è alcun default da 10 €.
- **Requisiti e oracoli:** values_registry aggiornato (OWNER_D1_DRAWDOWN_CEILING, OWNER_D2_MISSING_CAP). I ruoli delle fixture distinguono il tetto drawdown di campagna dal cap di esposizione configurato. Preflight 570/570 e 17/17, test_preflight 31/31.

### Test (risultati reali)
- **RED v4 su 028cee9:** 16 fail / 6 pass.
- **GREEN:** 22/22.
- **Suite completa:** 1 fail / **7275 pass**, cioè 7269 + 6 test nuovi, senza regressioni. Una prima esecuzione aveva dato 68 fail; erano tutte fixture senza cap esplicito, fallite per D2. Ho aggiunto a 9 file di test un cap esplicito da 1000 €, etichettato come fixture.
- **pr04_quater [True]:** fallisce per causa d'ambiente. customtkinter manca nel venv e DISPLAY=:7 è presente, quindi si attivano i widget fallback e `_FallbackComboBox` passa `command=` a ttk.Combobox (mini_gui.py:182-184 e :919-925). Senza DISPLAY: 1 passed, 1 skipped. È un difetto latente nel fallback, non una regressione.

### Upgrade record-specific P0
- **L1 v2 (22 BF):** oracoli per operazione, fixture concrete, comportamento osservato da sonde con stub di `_post_jsonrpc`. 10 gap candidati distinti; esempi:
  - un result di tipo inatteso diventa `[]`, `None` o saldo 0,00, indistinguibile da un esito valido;
  - cancel → ok=True con 0 cancellazioni;
  - replace restituisce il risultato grezzo;
  - SIM place_bet su un mercato sconosciuto restituisce SUCCESS.
- **L2 (97 RSR SAVE/LOAD/VALID/RESTART):** sonde su DB sqlite temporaneo reale. 56 gap candidati distinti, tra cui:
  - 'abc' diventa il default in silenzio;
  - NaN viene propagato;
  - '-1' viene accettato;
  - per i booleani, un valore corrotto diventa False.
- Preflight 22/22 e 97/97.
- **P0 con oracoli record-specific: 119 di 322.** Restano 203 P0, poi P1/P2.
- **Accettati/chiusi: 0.**

### Revisione indipendente
Richiesta preparata e NON eseguita (`review_request_P0_lots.md`):
- R-L1: 22 record, circa 0,15 €, tetto 0,25 €;
- R-L2a: 20 record, circa 0,30 €, tetto 0,40 €.

Criterio di arresto: primo FAIL, tetto raggiunto o 2 errori di trasporto.

### Blocker
- Revisione indipendente non autorizzata.
- 20 + 18 record PENDING_OWNER (Q1/Q2 RSR).
- D1: "inferiore solo se validata" ha solo una validazione tecnica, nessun flusso di convalida.
- D2: le operazioni di riduzione del rischio non sono ancora verificate da un test dedicato.
- Controlli 38-41 assenti a 028cee9 (RSR-41 aggiunto solo nella proposta).
- PF-QA-02 dopo il merge autorizzato di #505.

### SHA256 (prefisso 16)
```
71e484bac3513fc0  patches/close02_findings_proposal_v3.patch
b4d4129d36b34897  patches/ANALISI_F1_F6.md
daac7c68194d6be0  red_run_v4.txt
d55b9edecbfab5da  green_run_v4.txt
c40b60cb5bdaea9b  patched_full_v5.txt
590b186f912fe4bc  tk_nodisplay.txt
6eba3bde0c419ca5  record_specific_L1.json
b548f3b2c4474f0a  record_specific_L2.json
4c2e200b971e4f3c  sp_L1.json
c5c74b29a570c257  sp_L2.json
662999ba6012b352  L1_gap_candidates.json
aa0e4fcaefe66986  L2_gap_candidates.json
e45fe1b7909ea874  probe_all.json
d6dc4fab3dbe1454  probe_rsr.json
4b2c4e3d04e1d49a  review_request_P0_lots.md
7f90b13c8ae5b738  review_digest_L1.json
3f9724d2d57ded46  review_digest_L2.json
9a25e57367232a20  record_specific_status.json
7188be765523265f  ../repair01/master1035.json
3e11d67f1cba9bef  ../repair01/values_registry.json
3420b0b28eb3a4fb  ../repair01/gen_all.py
87edcbc7cdc5d6e0  ../repair01/rebuild17.py
e0d64295511b3aad  ../repair01/rebuilt570.json
37297350705e4be7  ../repair01/sp570.json
```

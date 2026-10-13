## PF-OWNER-CLOSE-ALL-02 — avanzamento (nessun merge/deploy/LIVE, costo AI 0 €)

**Gate A: NON PASS.** Nessun finding dichiarato risolto; le patch sono SOLO proposte.

### 0. Riconciliazione baseline vs #506
Il master contava 651 PASS perché i 60 parent di split erano marcati PASS. Ora i parent sono `N/A_PARENT (figli x/y PASS)` → **591 PASS**, coerente con #506. Preflight 570/570, 17/17, Q6 145/145.

### 1. Classificazione owner
Registrata su #426 (commento 6141450045), read-back identico, sha 2174b32f…bffb. CAP_OPERATOR e LAY_CAP_BASE ora OWNER_RULE.

### 2. Q1/Q2/Q8/T4 (close02/q1q2q8t4.json)
- Q1 (13 campi %): PENDING_OWNER RSR-01..08 + limite sup. RSR-22; TECNICA_DELEGATA RSR-13/21/27; RSR-20 FONTE 0–100 (zero PENDING).
- Q2 (26 controlli): FONTE 16–19, 24, 26 (vincita max 10.000 € incl. stake), 30, 31–37; PENDING 09, 12, 14 (T4), 15; TECNICA 10, 11, 23, 28; BLOCKED 38–41 (assenti a 028cee9).
- Nuovi rilievi di sicurezza: `_b()` booleano corrotto → False (fail-open per 32/33); `_f()` default silenzioso; GUI float() accetta NaN/Inf/negativi (mini_gui.py:1989-2006); clamp table_count (table_manager.py:48,88).
- Q8 DETERMINATO: soglia = SL/TP utente in € o quota (#461).
- T4: doc pubblica 2687808 v39 (31/07/2026) conferma min 2,00 € BACK, passi 0,50 €, LAY ≥0,50 € back corrispondente, max 50 istruzioni, vincita max 10.000 €. Applicabilità all'account NON verificata; T6 in attesa.

### 3. Oracoli Q3/Q4
Preflight: nuovo C13 (DD6 = stop a 10,00; casi limite `=10.00 = limite` ⇒ "ammesso") + FORBID `operatore/lay_cap_base … pending`. test_preflight 31/31 PASS (mutazioni DD6→PENDING 36/36, uguaglianza rifiutata 41/41).
master1035: 532 AWAITING_INDEPENDENT_REVIEW, 38 PENDING_OWNER, 21 ATOMIC_NO_SPLIT, 50 SPLIT_CORRECTED, 10 SPLIT_VERIFIED, 46 RECHECK_BLOCKED, 338 CODE_FIRST_OWNER_TRACK.

### 4. Pack record-specific
Upgrade record-specific in questo giro: 0. Revisione indipendente gratuita/consentita: nessuna → AWAITING. **Accettati/chiusi: 0.** Richiesta del lotto a pagamento preparata, NON eseguita (vedi sotto).

Bozza lotto a pagamento (NON chiamato, serve autorizzazione owner): 17 record P0 costruiti a mano, ~143k token, tetto 0,60 € sul residuo 16,7883 €, stop al primo FAIL di revisione o al raggiungimento del tetto.

### 5. Finding codice F1–F6 (worktree isolato da 028cee9, nessun push)
- RED v2 (codice 028cee9): 10 failed / 4 passed, tutti per il motivo atteso. Errata RED v1: i test F3 LIVE fallivano per deploy gate NO-GO; harness corretto.
- GREEN con patch proposta: 14/14.
- Regressione suite completa: baseline 1 fail (pr04_quater tkinter, preesistente) / 7253 pass; con patch 20 fail / 7248 pass. 19 nuovi fail: 13 doppi DB senza `transaction()` (F6), 3 test pr28 tolleranza 1e-9 (#393 H-14 in conflitto con Q3), 3 max_win_guard con stake >10 € (bloccati dal limite owner).
- Analisi concorrenza/recovery/fail-closed: patches/ANALISI_F1_F6.md. Residui: check-then-act non atomico in manual_bet, F6 lost update multi-processo (BEGIN deferred), errori float dei chiamanti a cap esatto (decisione: quantizzare al centesimo?).

Stato di F1–F6: **APERTI** fino alla sequenza autorizzata #505. PF-QA-02 (10 cicli) dopo il merge autorizzato #505, non anticipato.

### SHA256
```
c52be35b3217b65716332389130947ad370c4aad91a0647ed4a2aa5ccbbc9dfd  gen_all.py
dfb4feefd9895350472bb648099df0f3ab41f7ed645c8ccedca5b360984ab15c  rebuild17.py
bd5eb15d452ee068ff562783c1b52f38664a611284482a30bb57dea509068b38  values_registry.json
633d6280cf13001bf5463afd8fe7969024ca399cdaa244d08759b9f5187ea2fe  semantic_preflight.py
af1085b0024b1ae45ab3cff00b3e96593f3429636a4dfd544cb18ce8d0ddf535  test_preflight.py
43d5ac955c3a64a38c2e738147aaf9debbb82627f4d04aaab4ae976d365f8a29  rebuilt570.json
37297350705e4be73f29c79184439249aa75d25556a12013df00a2fffa4f4d12  sp570.json
4ff486a56aed0547ffc2e70585b26b4a708cdd05377d031af966f32e7b542945  rebuilt17v2.json
64790bf110850c4863c78ddd8597eb74dc9093b2a48f883a6503d67578904511  sp17v2.json
30064423dae22a644f92a42ae97e3f15c8f0e69f81b4c89eca216418bdadcce9  master1035.json
f9999f66683441064d0b0ffe33f0cbd768565ba632c65bf0e5a5704ef8035893  ../close02/update_master.py
828203718caab767ffd51b73e8e3f3c33638f91a4886987d54ab68401a6e90db  ../close02/q1q2.py
6fda2d299257a7699f5082b2092cb3be742550bb34f66814cbfb6dfbfdf109fb  ../close02/q1q2q8t4.json
a3b64437879ee68ad9552feb91fb9484a3af70535985498766a2fc7db6209fed  ../close02/rsr_controls.json
8312fcd74dac40ca585ae77ac1f568b0b4fa95b19100a2298d782e0dc83bc857  ../close02/italy_live.json
2174b32f2c07ae69aaf0e1d36621c0ae4ef2507a2521278edac7e7dc3a2abffb  ../close02/od426_close02.md
54110e40ce58ab7e3aff3181725bda103fdc399806e8d88b7499da7cad982549  patches/ANALISI_F1_F6.md
a13737a65e2ebd600e7535d987a50f888e2934a2c3102ebcfdd2568a9f12b9eb  patches/close02_findings_proposal.patch
0011da3bbc7dd1c20574d706693a3f9ede81eb0c50ceb458a72270a39ce9e670  red_run.txt
4c50255626907c651bbcaf84da8f0efce939e62c711104e650c742a815ae1dbd  red_run_v2.txt
afd29dde277673ae5fdf4065212d4ff9dfcfb69674649964c2810313752808d3  green_run.txt
dcd786b59a5ed9427ec101da6c008ec81642fd5a2d701c28b5eddbd18d83f167  baseline_full.txt
2c46b2b5a0ddab39cc416847eb835597ad885a0f7e257e916d1e9950ece18e20  patched_full.txt
4b2895c951e0cd8bc52e3a69fde835a361d6769869368f5057e4f5e458c506d0  wt/tests/close02/test_close02_findings.py
```

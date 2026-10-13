## PF-OWNER-CLOSE-ALL-02 — correzione e avanzamento v2 (costo AI 0 €, nessun push/merge/#505/LIVE)

**Gate A: NON PASS.** F1–F6 restano APERTI; patch solo proposta.

### Correzione al commento 6141732126 (read-back: identico, 5306 caratteri)
Il commento precedente attribuiva a Q3 un cap di esposizione di 10 € e classificava come "attesi" 3 fallimenti di `test_dutching_max_win_guard`. **Era errato.** Q3 (#426/6141450045) definisce solo l'operatore. Il 10 € di esposizione esiste come limite della **campagna #351** (#426/5656005416 P02/P06 "Esposizione complessiva massima €10"), non come costante di prodotto. Patch v2: rimosso l'hardcode; `manual_bet` usa il cap configurato `max_open_exposure` (stessa regola A2 del runtime). I 3 test max_win_guard ora passano senza modifiche alle loro asserzioni.

### Float al cap (Q3/T7)
Somme e prodotti in Decimal sugli importi canonici (`money_sum`, `order_exposure` in Decimal, `total_exposure` dei tavoli in Decimal): 0,1+0,2 = 0,3 esatto, nessuna tolleranza, 10,000000001 > 10 resta superamento. Non è una questione owner.

### Tolleranza 1e-9 vs Q3
Letto #393: la sola decisione owner è **LAY = liability** (4974645091, 4974754190). "H-14" è l'id di un finding (core/risk_gate.py:26), non una regola owner. La tolleranza è una scelta tecnica di PR28. **Nessuna contraddizione owner**: i 3 test pr28 sono aggiornati nella proposta.

### Test
- 13 doppi DB in 10 file: aggiunto `transaction()`.
- RED v3 su 028cee9: 11 fail / 5 pass (uno è strutturale: `money_sum` assente).
- GREEN v3: 16/16.
- Suite completa con patch v2: **1 fail / 7269 pass**, identico al baseline (pr04_quater tkinter, preesistente).

### Upgrade record-specific (P0)
Lotto L1: 22 record P0 BF (AUTH/PROOF/ERROR) con riferimenti di codice. Ancore 618884e risolte alla funzione e ritrovate a 028cee9 (22/22, 0 mancanti); 66 casi della clausola P1 con input specifico dell'operazione; expected verbatim invariato. Preflight 22/22 PASS. Stato: `RECORD_SPECIFIC_L1_PREFLIGHT_PASS_AWAITING_INDEPENDENT_REVIEW`. Restano 300 P0 generati. **Accettati/chiusi: 0.**

### Residui owner
- Il drawdown 10 € resta costante nel codice (fonte #426/6131980558 §2 + Q5): da confermare se deve essere configurabile.
- Cap di esposizione assente in config ⇒ nessun cap (regola A2 esistente): da confermare se l'assenza deve bloccare.

### SHA256
```
4b4bb37540641495f48b075b4b63ad42a5d33a24e28b9bcbe35c991d7c3df6be  patches/close02_findings_proposal_v2.patch
f472adaa632cef0cd38f5bcfcc78ac53f9c85eb6ff9e4e1c823af066d41b5574  patches/ANALISI_F1_F6.md
377eb93bffada06d08b01b7b8bc63bb5aa05803312384d10303b155e6964b753  red_run_v3.txt
fd4b93811bf776e6eb026321c2ed111649d14492302f5cf92999156928e0b4ca  green_run_v3.txt
e411a621da296132735d2d4dada7484c5d781bd5251d900a2c25a2328e975b0d  patched_full_v3.txt
d55de0a8917941d78fc6718f6aafa4232ad975ab7fd6444109d9a76e287bd556  record_specific_L1.json
4c2e200b971e4f3c073fca114eebfc67cc804411e5ebc866e0a6e89b3d93565b  sp_L1.json
502487a83fd517afe1ef272daffa5cf769ca93d7de704ecfa41f53f093d816f9  record_specific_status.json
7dd9d00efc4696ea2a6fb85cf9df2c1bc4dcd6d5d29142d1d5f660bead4bf656  upgrade_batch1.py
a5b0f6642480a5ee8136ed611b2c18c8832c053b9fce0bc9a97aea92a22b4e43  ../repair01/master1035.json
83f73d3843578b183bc4690f0eb1ccb3ad144912ca33c81951cea87e301db177  wt/tests/close02/test_close02_findings.py
```

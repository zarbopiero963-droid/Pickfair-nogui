## PF-OWNER-CLOSE-ALL-02 — aggiornamento v6 (owner 11:37): controllo C15, P0 per chiave, ri-etichettatura dei lotti

Spesa AI: **0 €**. Nessuna chiamata a pagamento: **R-L1 non chiamato**. Nessuna modifica a #505, nessun push, merge, deploy o LIVE. **Gate A resta NON PASS.**

### 1. Nuovo controllo di preflight C15 (`semantic_preflight.py`)
Le regole owner Q3/D2 (cap) richiedono un soggetto cap o esposizione. Q4 richiede un soggetto LAY, liability o stake. D1/Q5 richiedono drawdown o perdita. Le fixture in € sono ammesse solo su record con soggetto monetario o di rischio. Il soggetto è dato dalle clausole del record, dalla chiave di controllo e dall'id del requisito.

Di conseguenza C2 e il gruppo € di C6 non si applicano più ai record non monetari.

**Fallimenti C15 al primo passaggio:**

| Insieme | Record falliti |
|---|---|
| L1 | 0 / 22 |
| L2 | 0 / 97 |
| **L3** | **28 / 135** |
| **L4** | **108 / 461** (P0 18, P1 33, P2 57) |
| rebuilt570 (base) | 12 / 570 |

I casi tipici:
- cashout (CASH-*) con "cap assente ⇒ blocco nuovo rischio", mentre D2 esclude le operazioni che riducono il rischio;
- BF-11 listMarketProfitAndLoss e C15-03 con regole Q3/D2;
- chiavi non monetarie (table_count, max_recovery_tables, allow_recovery, anti_duplication) con lo scenario "totale 10.01 €".

**Test di mutazione** (test_preflight 40/40 PASS, `test_preflight_v6.txt`):
- Q3 iniettato su record senza soggetto cap: rifiutato 479/479;
- fixture in € su record non monetario: rifiutata 136/136;
- controllo: Q3 su record con soggetto cap resta PASS (0/400 falsi positivi);
- le mutazioni C14 ora girano solo sui record PASS: 879/879 rifiutate per ciascuna delle 5.

### 2. P0 resi specifici (L5, `upgrade_L5.py`)
Lavorati **192 record P0 RSR**: tutti i 135 di L3 più 57 P0 RSR di L4, partendo da quelli con fixture monetarie inappropriate.

- **Fixture per chiave:** valori di TEST per unità, non owner né default.
- **Rimozioni:** 66 fixture monetarie, 204 calcoli di cap su chiavi senza soggetto cap, la riga "totale capitale a rischio" dove la chiave non è cap.
- **Oracolo:** REQUIREMENT con clausola verbatim su ogni caso. Regole owner solo con il soggetto.
- **impl_attuale:** dalla sonda `probe_rsr` (028cee9) dove applicabile.
- **Preflight C1–C15:** 192/192 PASS.

L4, ripulito con `sanitize_C15.py`: 2224 frammenti owner senza soggetto rimossi, 66 fixture rimosse. Preflight 461/461 PASS, ma il livello resta "derivato da clausola".

### 3. Ri-etichettatura dei lotti

| Lotto | Stato |
|---|---|
| **R-L1** (22 BF) | **READY**: livello L1, 22/22, C1–C15 PASS; ~0,21 €, tetto 0,30 €. Non chiamato. |
| R-L4-P0-1..4 | **NOT_READY**: ritirati. |
| R-L3-1..3 | **NOT_READY**: sostituiti da L5. |
| L5 (192) | **NOT_READY** |

Perché L5 non è pronto:
- `lot_check_L5`: 33/192 soddisfano i criteri meccanici di L1 più la specificità;
- la revisione manuale di un campione (RSR-03-GUI-OBSERVABLE) mostra un oracolo ancora generico per tipo di caso e un `pre` con "valore A" incoerente;
- APPLY, RACE, AUDIT e PARITY non hanno una sonda per impl_attuale.

Oggi è proponibile **solo R-L1**.

### master1035 (sha `7597a2f8225e…`)

| Stato | N. |
|---|---|
| L1v3 | 22 |
| L2v3 NOT_PROPOSED | 67 |
| L2v3 PENDING_OWNER | 14 |
| L2v3 BLOCKED | 16 |
| L4 C15_SANITIZED_NOT_READY | 281 |
| L4 C15_SANITIZED_NOT_READY PENDING_OWNER | 20 |
| L5 NOT_READY | 150 |
| ATOMIC | 21 |
| SPLIT | 60 |
| RECHECK_BLOCKED | 46 |
| CODE_FIRST | 338 |

Gli altri 42 record L5 sono figli Q6, fuori dal conteggio di master.

### SHA (prefisso 12)

| File | SHA |
|---|---|
| semantic_preflight.py | 59da4a3be9c6 |
| test_preflight.py | d60628ec53a4 |
| record_specific_L4 | 1c275e571805 |
| record_specific_L5 | f91f2ab53343 |
| sp_L5 | 8f672e50d4ce |
| lot_check_L5.json | 418a160ed91a |
| review_request | b45314efc684 |

Elenco completo: `SHA256SUMS_v6.txt`.

**Accettati/chiusi: 0.**

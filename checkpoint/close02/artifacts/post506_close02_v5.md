## PF-OWNER-CLOSE-ALL-02 — aggiornamento v5: ricostruzione con provenienza dei P0 rimanenti, P1 e P2

Spesa AI: **0 €** (residuo 16,7883 €). R-L1 **non chiamato**. Nessuna modifica a #505, nessun push, merge, deploy o LIVE. **Gate A resta NON PASS.**

**Read-back di #506/6142632292.** Riletto separatamente: corpo identico al file inviato (sha256 `5c0f2609b6cad8e8…`, updated_at 11:34 Rome).

### Ricostruzione L4 (`upgrade_L4.py`)
461 pacchetti, divisi così:

| Priorità | Record (rebuilt570) | Figli Q6 |
|---|---|---|
| P0 | 85 | 78 |
| P1 | 94 | 41 |
| P2 | 137 | 26 |

- **Copertura:** zero record con pack e senza oracolo tra i rimanenti di P0 (126), P1 (115) e P2 (156), parent split coperti dai figli.
- **Frammenti di oracolo** (8240 in totale):
  - 4154 REQUIREMENT, con la clausola del record citata verbatim;
  - 4086 OWNER_DECISION (Q3/Q4, D1/D2, Q5);
  - **0 OBSERVED_ONLY**.
- **I 182 casi DD** (che coprono `O1`) ora hanno l'oracolo OWNER_DECISION D1/Q5.
- **Testo corretto:** "limite owner" è diventato "cap configurato (campagna #351, D2)" in 497 punti.
- **Limite dichiarato:** L4 è al livello **ORACOLO_DERIVATO_DA_CLAUSOLA**. Le fixture sono generiche e non c'è nessuna sonda: non è record-specific come L1. Le fixture monetarie dei template restano anche su record non monetari, e quindi vi sono agganciati i frammenti Q3/D2. Il revisore lo deve valutare.

### Verifiche
- Preflight C1–C14 L4: **461/461 PASS** (`sp_L4.json`).
- test_preflight: 37/37 PASS. Le mutazioni C14 ora girano su **715 record** (L1+L2+L3+L4) e sono tutte rifiutate, 715/715 per ciascuna delle 5.
- Codice: invariato rispetto alla v4 (suite 7318 passed / 0 failed).

### master1035 (sha `0f51c0ed9956…`)

| Stato | N. |
|---|---|
| L1v3 | 22 |
| L2v3 | 67 |
| L2v3 PENDING_OWNER | 14 |
| L2v3 BLOCKED | 16 |
| L3 | 135 |
| L4 in attesa di revisione | 296 |
| L4 PENDING_OWNER | 20 |
| ATOMIC (invariato) | 21 |
| SPLIT parent | 60 |
| RECHECK_BLOCKED | 46 |
| CODE_FIRST | 338 |

Preflight PASS: 591.

### Lotti di revisione preparati (NON chiamati, serve l'autorizzazione owner)

| Lotto | Record | Costo stimato | Tetto |
|---|---|---|---|
| R-L1 | 22 | ~0,21 € | 0,30 € |
| R-L4-P0-1 | 41 | ~0,50 € | 0,70 € |
| R-L4-P0-2 | 41 | ~0,23 € | 0,32 € |
| R-L4-P0-3 | 41 | ~0,50 € | 0,71 € |
| R-L4-P0-4 | 40 | ~0,23 € | 0,33 € |
| R-L3-1 | 45 | ~0,41 € | 0,58 € |
| R-L3-2 | 45 | ~0,40 € | 0,57 € |
| R-L3-3 | 45 | ~0,40 € | 0,56 € |

Totale P0 ≈ 2,88 €, tetti ≈ 4,07 €.

Criterio di arresto: primo FAIL su provenienza o osservato, tetto, o secondo errore di trasporto. Dettagli in `review_request_P0_lots.md`.

### SHA (prefisso 12)

| File | SHA |
|---|---|
| record_specific_L4 | 088e07f3ec22 |
| sp_L4 | 79d3e686e46e |
| upgrade_L4.py | 4dbce1674e77 |
| record_specific_status | 308569e5e8cc |
| review_request | 021ca57b124c |
| test_preflight | eee8acebdf04 |

Elenco completo: `SHA256SUMS_v5.txt`.

**Accettati/chiusi: 0.** Preflight PASS non è accettazione.

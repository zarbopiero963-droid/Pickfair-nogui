# Richiesta revisione indipendente — primi lotti P0 (v2, dopo la verifica di provenienza; NON eseguita)

Serve l'autorizzazione owner. Residuo AI: 16,7883 €. Tariffa di riferimento: circa 4,2 €/M token (stima precedente: 17 record ≈ 143k token ≈ 0,60 €).

| Lotto | Record | Contenuto | Token stimati (in+out) | Costo stimato | Tetto |
|---|---|---|---|---|---|
| R-L1 | 22 BF AUTH/PROOF/ERROR (v3, con fonti complete e fixture) | review_digest_L1.json | ~36k + ~13k | ~0,21 € | 0,30 € |
| R-L2a | 20 RSR SAVE (v3) | sottoinsieme SAVE di review_digest_L2.json | ~76k + ~12k | ~0,37 € | 0,50 € |

Nota: se un lotto supera il tetto si riduce il lotto, non si alza il tetto.

**Motivazione.** Dopo la verifica di provenienza (owner, Opzione B), ogni oracolo ha provenienza REQUIREMENT con clausola verbatim, OWNER_DECISION con regola registrata o TECNICA_DELEGATA. Il comportamento osservato a 028cee9 sta solo in impl_attuale.

**Evidenza di preflight (C1–C14).**
- L1 22/22 (sp_L1.json), L2 97/97 (sp_L2.json), L3 135/135 (sp_L3.json).
- test_preflight 37/37, comprese 5 mutazioni C14 rifiutate 119/119.
- La versione precedente di L1 (v2, rigenerata) fallisce C14 22/22.

**Domande al revisore, per ogni record.**
- (a) Ogni oracolo deriva dalla fonte citata?
- (b) L'oracolo contiene comportamento osservato? Deve essere NO.
- (c) Le fixture sono concrete e non sono valori owner inventati?
- (d) Le divergenze tra impl_attuale e oracolo sono registrate come finding (findings_register.json)?

**Criterio di arresto.**
- Lo stop scatta al primo FAIL su (a) o (b), al raggiungimento del tetto del lotto o al secondo errore di trasporto; segue un rapporto.
- Nessun lotto successivo senza una nuova autorizzazione.

**Esito massimo:** "independently reviewed". Non "accepted" né "closed".

## Stato di prontezza R-L1 (13/10/2026)
Il controllo semantico del lotto (lot_check_L1.py → lot_check_L1.json) dà **22/22 OK** su sei criteri:
- fonti complete: citazione verbatim e sha;
- oracoli con provenienza REQUIREMENT;
- fixture concrete (payload stub);
- controlli negativi;
- preflight C1–C14;
- comportamento osservato solo in impl_attuale.

Preventivo finale: ~49k token, ~0,21 €, tetto 0,30 €. **NON chiamato.**

## Lotti successivi preparati (13/10/2026 11:5x) — NON chiamati
Oracoli L3 e L4 con provenienza REQUIREMENT o OWNER_DECISION (C14 PASS). L4 è al livello **ORACOLO_DERIVATO_DA_CLAUSOLA**: le fixture sono generiche e non c'è nessuna sonda. Il revisore deve quindi verificare anche se la clausola basta come oracolo o se il record va reso specifico.

| Lotto | Record | Digest | Token (in+out) | Costo | Tetto |
|---|---|---|---|---|---|
| R-L4-P0-1 | 41 | review_digest_R-L4-P0-1.json | ~94k + ~24k | ~0.50 € | 0.70 € |
| R-L4-P0-2 | 41 | review_digest_R-L4-P0-2.json | ~29k + ~24k | ~0.23 € | 0.32 € |
| R-L4-P0-3 | 41 | review_digest_R-L4-P0-3.json | ~94k + ~24k | ~0.50 € | 0.71 € |
| R-L4-P0-4 | 40 | review_digest_R-L4-P0-4.json | ~31k + ~24k | ~0.23 € | 0.33 € |
| R-L3-1 | 45 | review_digest_R-L3-1.json | ~70k + ~27k | ~0.41 € | 0.58 € |
| R-L3-2 | 45 | review_digest_R-L3-2.json | ~68k + ~27k | ~0.40 € | 0.57 € |
| R-L3-3 | 45 | review_digest_R-L3-3.json | ~67k + ~27k | ~0.40 € | 0.56 € |

Criterio di arresto: lo stesso di R-L1 (primo FAIL su (a)/(b), tetto o secondo errore di trasporto). Ordine proposto: R-L1, poi R-L4-P0-*, poi R-L3-*. Nessun lotto senza autorizzazione owner.

## Ri-etichettatura dei lotti (owner 13/10 11:37), aggiornata 13/10

| Lotto | Stato |
|---|---|
| **R-L1 (22 BF)** | **READY**: livello L1, controllo 22/22, preflight C1–C15 PASS; ~0,21 €, tetto 0,30 €. NON chiamato. |
| R-L4-P0-1..4 | **NOT_READY**: oracoli derivati da clausola, fixture generiche, nessuna sonda. Ritirati dalla proposta. |
| R-L3-1..3 | **NOT_READY**: sostituiti da L5. Ritirati dalla proposta. |
| L5 (192 RSR P0, per chiave) | **NOT_READY**: vedi sotto. Nessun lotto proposto. |

**Perché L5 non è pronto.**
- Fixture per chiave e regole monetarie ripulite (C15 PASS 192/192), ma il testo degli oracoli è ancora generico per tipo di caso ("la clausola X è soddisfatta …"). Non è un enunciato specifico come in L1.
- Le fasi APPLY, RACE, AUDIT e PARITY non hanno una sonda per impl_attuale.
- I 6 criteri meccanici di L1, più la specificità, sono soddisfatti da 33/192 record. Neanche questi sono pronti: la revisione manuale di un campione (RSR-03-GUI-OBSERVABLE) mostra un oracolo generico e un `pre` con "valore A" incoerente.

Totale proponibile oggi: **solo R-L1**.

## Ri-etichettatura v7 (13/10, owner 11:43): controllo di prontezza più severo (`lot_check_RS.py`)

Criteri, tutti obbligatori:
- (1) fonti con sha;
- (2) oracolo su ogni caso;
- (3) fixture concrete;
- (4) casi negativi;
- (5) preflight C1–C15;
- (6) sonda **dinamica** a 028cee9 in impl_attuale (una sonda statica non basta);
- (7) testo non riducibile a template di fase;
- (8) decisione della chiave da clausola, non dal solo titolo;
- (9) la sonda esercita lo scenario di **ogni** caso.

| Lotto | Stato | Stima | Tetto |
|---|---|---|---|
| R-L1 (22 BF) | **RETROCESSO: READY solo per l'ambito P1** (66 casi su 513). I casi P2–P5 e D1–D4 (447) non hanno oracolo, fixture né sonda; con `lot_check_RS` il risultato è 0/22 a livello di record intero | ~0,21 € (solo P1) | 0,30 € |
| **R-L6-LOAD** (2: RSR-02-LOAD, RSR-03-LOAD) | **READY** | ~0.05 € (~11k + ~1k token) | 0.08 € |
| L6 restanti (190) | NOT_READY | — | — |
| CASH hand-built (5) | NOT_READY | — | — |
| L4 / L3 / L5 | NOT_READY (superati) | — | — |

Perché i 190 L6 restano NOT_READY:
- 104 hanno solo una sonda statica;
- 36 hanno un testo di template di fase;
- 48 hanno la decisione tratta dal solo titolo;
- 190 hanno una sonda che non esercita lo scenario di ogni caso.

Perché i 5 CASH restano NOT_READY: nessuna sonda dinamica, e le clausole P1 sono solo titoli di riga.

Nessuna chiamata eseguita.

**Correzione.** Il "22/22 OK" di `lot_check_L1` controllava solo i casi P1. Il lotto R-L1 si può chiamare solo come revisione limitata a P1, dichiarando che i record restano incompleti. Lotto a livello L1 su record intero: solo **R-L6-LOAD**.

## R-RS21 RITIRATO (t75u: 0 record interi verificati; NON proporre) — 13/10 11:5x
Record interi READY L1 (lot_check_RS 10 criteri, incl. c9 sonda per scenario e c10 clausole UI/log/audit non sondate => NOT_READY): 21
IDs: DIFF-PF-RSR-04-LOAD, DIFF-PF-RSR-12-LOAD, DIFF-PF-RSR-13-LOAD, DIFF-PF-RSR-14-LOAD, DIFF-PF-RSR-15-LOAD, DIFF-PF-RSR-16-LOAD, DIFF-PF-RSR-17-LOAD, DIFF-PF-RSR-18-LOAD, DIFF-PF-RSR-19-LOAD, DIFF-PF-RSR-23-LOAD, DIFF-PF-RSR-27-LOAD, DIFF-PF-RSR-29-LOAD, DIFF-PF-RSR-31-LOAD, DIFF-PF-RSR-36-LOAD, DIFF-PF-RSR-38-LOAD, DIFF-PF-RSR-39-LOAD, DIFF-PF-RSR-40-LOAD, DIFF-PF-RSR-41-LOAD, DIFF-PF-RSR-01-RESTART, DIFF-PF-RSR-02-LOAD, DIFF-PF-RSR-02-RESTART
Stima: ~0.025 €/record => ~0.53 €, tetto 0.80 €. Residuo 16.7883 €. Richiede approvazione owner esplicita. Nessuna chiamata eseguita.

## R-L7-LOAD20 (PROPOSTA, NON CHIAMATA) — 13/10
20 record interi (fase LOAD: RSR-02,03,04,12-19,23,27,29,31,36,38-41), 280 casi. Esclusi: RSR-20-LOAD (clausola owner D1 drawdown non osservabile in LOAD).
Criteri t75u per record: fonti con sha ricalcolato su #461/6085510337 (riga fase) + sezione controllo (verbatim trovato); oracolo per chiave/fase/clausola; fixture complete (DB sqlite reale nuovo per scenario); sonda dinamica probe_load_v2 a 028cee9; verdetto osservato-vs-oracolo per ogni caso (180 NON_CONFORME, 90 CONFORME, 10 NON_APPLICABILE D3 su chiavi non monetarie); verifica indipendente verify_L7.py (risonda riproducibile, sha, copertura P/N/B per clausola, no €/cap su chiavi non-EUR, ricalcolo verdetti) 20/20.
File: lot_R_L7_LOAD20.json (sha 67abe530…). Stima ~65k token input + ~30k output ≈ 0.40 €, **tetto massimo 0.60 €**. Residuo 16.7883 €. Richiede autorizzazione owner esplicita. Nessuna chiamata eseguita.

## PF-OWNER-CLOSE-ALL-02 — aggiornamento v7 (owner 11:43): oracoli L5 per chiave e fase, sonde APPLY/RACE, CASH, prontezza severa

Spesa AI **0 €**. Nessuna chiamata a pagamento. Nessuna modifica a #505, nessun push, merge, deploy o LIVE. **Gate A resta NON PASS.**

### 0. Integrità dei file
- La riscrittura delle 11:40 coincide con il riavvio del box (nota owner).
- `SHA256SUMS_v6` e `SHA256SUMS_v7` verificati, nessuna differenza.
- **Lo store persistente `/cursor/stores/store-163c…` non è montato** né visibile da shell o file tool. Copia di riserva in `/home/box/pickfair_backup/` (tgz di close02 e repair01, patch e file non tracciati del worktree).

### 1. L6: 192 P0 RSR con oracoli specifici per chiave e fase (`upgrade_L6.py`, `rsr_decisions.py`)
- **Decisione per chiave** (41 voci):
  - 128 record con decisione citata come REQUIREMENT, tramite la clausola S0 = riga verbatim "Semantica/rischio" della sezione di controllo #461;
  - 64 record con decisione TECNICA_DELEGATA ricavata dal titolo, perché la riga di fonte è un'osservazione (FAIL, parser, loader).
- **Enunciati:** 2049 casi riscritti con enunciati per fase e tipo che citano nome, chiave, valore di test e decisione. Nei casi negativi la decisione usa il nuovo valore.
- **'valore A'** corretto in 150 pre (0 residui).
- **Frammento Q4** tolto dai casi drawdown O1.
- **Preflight C1–C15:** 192/192 PASS.

### 2. Sonde a 028cee9, solo per il campo impl_attuale (`probe_apply_race.py`)
- **APPLY:** sonda statica dei consumatori della chiave. Per esempio RSR-12 è consumata solo da `hard_verify_issue_320.py`; RSR-38..41 sono assenti dalla config.
- **RACE:** sonda dinamica con due salvataggi interleaved. **32/41 chiavi perdono la modifica concorrente** (salvataggio dell'intera config da copia stantia) e `config_version` non è persistito.
- **Nuovo finding CL02-RSR-RACE-001 (OPEN_CANDIDATE):** da riprodurre con test RED. La proposta F6 non lo risolve.

### 3. CASH (`build_cash.py`)
- **5 P0 resi specifici:** CASH-04, 19, 21, 23, 25.
  - Fixture dello scenario della scheda: BACK 10@2, LAY@7, hedge 20/7, payoff −50/7.
  - Clausole derivate V1..V5 verbatim dal paragrafo "Dopo richiesta cashout" di #461/6085516609.
  - Oracoli REQUIREMENT.
  - Preflight 5/5 PASS.
- **CASH-08** (7 clausole di scenario) non ancora fatto.
- **Limiti:**
  - le sonde CASH sono solo statiche;
  - le clausole P1 sono titoli di riga, quindi gli oracoli P1 restano un'interpretazione.

### 4. Prontezza severa (`lot_check_RS.py`, 9 criteri)
Rispetto a prima si aggiungono:
- **sonda dinamica obbligatoria** (non statica);
- testo non riducibile a un template;
- decisione da clausola e non da titolo;
- **sonda che esercita lo scenario di ogni caso.**

**Esiti:**

| Insieme | READY |
|---|---|
| L6 | **2/192** (RSR-02-LOAD, RSR-03-LOAD) |
| CASH | 0/5 |

**Correzione su R-L1.** Il precedente "22/22 OK" controllava solo i casi P1 (66 su 513). I casi P2–P5 e D1–D4 (447) non hanno oracolo, fixture né sonda; a record intero il risultato è **0/22**. R-L1 è **retrocesso a "READY solo per l'ambito P1"**.

**Lotti:**
- **READY:** solo **R-L6-LOAD** (2 record, ~0,05 €, tetto 0,08 €).
- R-L1: chiamabile solo come revisione limitata a P1, dichiarandolo.
- Tutto il resto: NOT_READY.
- Nessuna chiamata eseguita.

### 5. Test
- test_preflight 40/40 PASS. Le mutazioni C14 e C15 girano ora su 1104 record:
  - Q3 su record senza soggetto cap: rifiutato 511/511;
  - fixture € su record non monetario: rifiutata 146/146;
  - controllo: 0/565 falsi positivi.
- Codice invariato rispetto alla v4.

### SHA (prefisso 12)

| File | SHA |
|---|---|
| record_specific_L6 | e90fe2de4998 |
| record_specific_CASH | 1d6d31e11023 |
| probe_apply_race.json | c56ebc41b20a |
| lot_check_RS.py | 22671e7fec8b |
| lot_check_L6 | 3d45a282c151 |
| findings_register | 2ef0c09fc32e |
| review_request | 5b0e60df0fa4 |
| master1035 | e1f5cfee77c3 |

Elenco completo: `SHA256SUMS_v7.txt`.

**Accettati/chiusi: 0.**

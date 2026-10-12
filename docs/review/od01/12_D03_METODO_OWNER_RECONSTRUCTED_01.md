> Revisione indipendente BF2/D03 (OD-01, #426). Istruzioni: `00_ISTRUZIONI_REVISORE.md`. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO + motivo.

# D03-v2-OWNER-RECONSTRUCTED — metodo normativo PROPOSTO (OD-02, owner 2026-10-12 16:58)

**Stato: PROPOSTA, non vincolante.** Diventa vincolante solo dopo che il revisore indipendente lo ha confrontato con il metodo pubblicato in #506/6124224080 e le incompatibilità sono state risolte. **Non è una trascrizione di R1–R7** (fonte §3: SOURCE_MISSING).

| # | Principio (owner) | Regola operativa proposta | Controllo nel validatore |
|---|---|---|---|
| P1 | Identità atomica stabile per ogni differenza | `difference_id` e `requirement_id` uguali al registro D03 (dataset `f7f8ea10…`), unici nel lotto; i record SPLIT hanno un child-set con ID propri | esistenza, unicità, corrispondenza del requirement_id, child-set per SPLIT |
| P2 | Fonti A/B identificabili e versionate | Fonte A: riferimento più sha256 del body o dell'estratto. Fonte B: riferimento più versione o data o sha256, più citazione | presenza di `source_A.ref` e hash, `R1_source.ref` con versione o hash, citazione non vuota |
| P3 | Oracle indipendente e motivato | Input, atteso e negativo concreti; `basis` dichiara da quale fonte deriva l'atteso (non dal codice sotto esame) | campi non generici, non identici tra record, `basis` presente e diverso da "codice" |
| P4 | Chiusura semantica distinta da implementazione e test | Lo stato semantico non contiene mai IMPLEMENTED, TESTED, HARD; R5 e R6 descrivono il codice e i test separatamente | enum degli stati; R6 solo NOT_RUN o EXISTING_VERIFIED |
| P5 | Tracciabilità individuale anche nei gruppi | Un `group_id` è ammesso, ma ogni membro conserva requisito, oracle, ref e test propri | per ogni membro di un gruppo: R3, R4, R5 e R6 individuali |
| P6 | Revisione indipendente e contestazioni | SEMANTIC_RESOLVED solo con `review.status=APPROVED_BY_DISTINCT_REVIEWER` e `review.reviewer ≠ producer`. Una contestazione aperta (`DISPUTED`) impedisce la chiusura finché non è risolta (Grok come secondo revisore, OD-01) | controllo dello stato di revisione, del produttore e delle contestazioni |
| P7 | Nessun PASS senza denominatore verificato ed evidenze | Il denominatore globale è il numero di record del registro D03 verificato con hash. Il Gate PASS richiede 100% dei record in stato finale con evidenza | in modalità globale: hash del dataset, conteggio, copertura; PASS solo se 0 aperti |

**Incompatibilità note da sottoporre al revisore:**
- (a) P3 introduce `basis`, assente dal metodo pubblicato.
- (b) P5 ammette gruppi; il metodo pubblicato non li menziona.
- (c) P6 introduce lo stato di contestazione.
- (d) Il metodo pubblicato non fissa la fonte del denominatore; P7 lo lega all'hash del dataset.
- Nessuna incompatibilità sugli stati finali ammessi.


## Metodo pubblicato da confrontare (#506/6124224080, riassunto dell'agente; §3 owner SOURCE_MISSING)
Stati finali ammessi: SEMANTIC_RESOLVED; …_DUPLICATE (con duplicate_of); …_SUSPENDED_SCOPE; …_NON_REQUIREMENT; BLOCKED_SPEC, OWNER_DECISION_REQUIRED, SOURCE_MISSING, UNRESOLVED (con blocker).
R1 fonte verificata con citazione · R2 precedenza owner verificata · R3 requisito atomico, child-set per SPLIT · R4 oracle concreto (input, expected, negativo) · R5 codice verificato LINKED/PARTIAL/MISSING (con termini cercati)/LATENT · R6 specifica di test NOT_RUN o EXISTING · R7 PR responsabile e finding. Chiusura documentale con fonte, contratto, oracle e revisione indipendente; SEMANTIC_RESOLVED ≠ IMPLEMENTED ≠ TESTED ≠ HARD.

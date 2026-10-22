# PF-TEST-CI-ACCELERATION-01 — Dove Pickfair perde tempo fra test, CI, mutation e review

Mandato owner del 22/10/2026. È un audit in sola lettura: non ho modificato codice, workflow, branch o PR, né fatto commit, push o merge.

Tutti i benchmark sono girati sullo SHA `152613b` (main), con Python 3.11 come in CI, in un venv isolato nello scratchpad, su una macchina a 4 core e 15 GB di RAM.

**Legenda:**
- **[M]** = misurato
- **[S]** = stima ricavata da una misura
- **[NV]** = non verificato

## 1. Misure

### 1.1 GitHub Actions — PR #537 (3 push completi) e #535 (7 push) [M]
- **Run per push:** 38 workflow run.
  - 12 workflow partono due volte, una su `push` e una su `pull_request`: Core Guardrails, Core Invariants, E2E, Integration, Recovery, Repo Guardrails, Smoke, failure, network, smoke-core-fast, unit.
- **Suite completa per push:** gira **17 volte**, in serie in ogni job (~8 880 test, 15,7–17,9 minuti l'una).
  - 10 volte come «Run full test suite (backstop)» nei job `_module-ultra-check` di `ci-master-gate`;
  - 3 volte dentro `ci-dynamic-intelligent`, la copia annidata dentro `ci-master-gate` più quella autonoma;
  - 3 volte con `merge-simulation-hard`, una diretta e due annidate;
  - 1 volta con `pr-check`.
- **Mutation guardrails:** gira 3 volte per push (7,3–13,0 minuti l'una).
- **Minuti-job per push:** 342–391.
  - Le suite complete valgono circa 300, cioè l'80%;
  - la mutation 23–31;
  - Merge Readiness 25–29, quasi tutti di attesa;
  - le review AI 2–15.
- **Totale per PR:** #537 ha consumato 1 173 minuti-job (parziale), #535 2 527.
- **Installazione delle dipendenze:** 0,05–0,30 minuti per job, 7,5 minuti su 50 job. Checkout, setup-python e upload degli artifact costano ciascuno ≤0,11 minuti. **Una cache dei pacchetti pip non porterebbe risparmio.**
- **Attesa in coda:** mediana 0,33 minuti, 90° percentile 0,88, massimo 9,8 (#537). Per #535: 0,58, 3,47 e 7,6.
- **Dal push all'ultimo check concluso:** 25,2–29,7 minuti (#537), 21–34 minuti (#535).
  - Le attese più lunghe sono le review: Merge Readiness attende tutti i check (mediana 28,8 minuti);
  - `ci-master-gate` impiega 25,3 minuti, Grok 9,2 minuti di wall, Sol 2,8 minuti.
- **Fallimenti:**
  - 3 sono **reali ma instabili** (asserzioni che dipendono dai tempi):
    - `test_F032_01` su 1 solo dei ~17 job dello stesso SHA;
    - `test_F026_05`;
    - `test_nonpertinent_market_or_selection_cashout_does_not_invalidate_entry`, con `Event.wait(timeout=2)`.
  - Gli altri sono di **policy**: Merge Readiness con thread aperti.
  - Nessun fallimento ambientale.
  - Con 17 esecuzioni per push, un test instabile ha 17 occasioni di colorare di rosso un push sano.
- **Run superati non cancellati:** solo Merge Readiness, `ci-quarantine-guard` e `build-windows-exe` usano `cancel-in-progress`. Su #535 sono andati sprecati **162 minuti-job** in suite complete su SHA già superati da un push successivo.
- **xdist:** `pytest-xdist` è installato in 2 workflow, ma **nessun comando pytest dei workflow usa `-n`**.
- **Xvfb:** nessun job CI ha uno step Xvfb o display. Il tempo di avvio di Xvfb in CI non si può misurare [NV].

### 1.2 Pytest locale, stesso SHA e stesso ambiente [M]

| Esecuzione | Tempo totale | Esito |
|---|---|---|
| Seriale (`pytest -q`, come in CI) | **1 182,8 s** (19 minuti 42 secondi) | 8 831 passati, 14 skipped, 5 xfail, 1 failed |
| xdist `-n 4` (distribuzione `load`) | 542,0 s | identico |
| xdist `-n 8` (`load`) | 554,2 s | identico |
| xdist `-n 4 --dist worksteal` | 320,6 s | identico |
| **xdist `-n 8 --dist worksteal`** | **241,8 / 245,2 / 245,0 s** (3 ripetizioni) | identico in tutte e 3 |
| `-n 8 --dist worksteal` senza il test da 181 s | 182,9 s | identico (8 830 passati) |

Note sulle misure:
- **L'unico fallimento** è lo stesso in tutte le 7 esecuzioni: `test_gui_save_reopen_file_pickers_and_live_boundary[True]`, perché tkinter manca in questo ambiente. È un limite ambientale, non del test.
- **Equivalenza:** nessun test diverso fra seriale e parallelo, e nessun fallimento comparso solo in parallelo in 6 esecuzioni.
- **Il carico è I/O, non CPU:** durante l'esecuzione seriale il carico medio era 0,4; con `-n 4 worksteal` 1,72 di mediana su 4 core. La suite passa gran parte del tempo in `sleep`, attese e sottoprocessi.
- **Raccolta dei test:** 19,2 secondi per ogni invocazione di pytest, quindi anche per ognuno dei 17 job.
- **Distribuzione dei tempi** (somma dei test 1 173 s):
  - un solo test vale il 15,4%: `tests/reconciliation/test_reconciliation_hardening.py::test_retry_does_not_stack_overflow`, 181 s. Il motivo è un backoff reale: `max_transient_retries=10` con `time.sleep` in `core/reconciliation_engine.py:695`;
  - 10 test valgono il 33,6%, 50 test il 61,6%;
  - `tests/f026` pesa 460 s (sottoprocessi con `sleep` fissi, #528), `tests/guardrails` 230 s;
  - 8 104 test sotto 0,1 s valgono in tutto 47 s.
- **Storico locale delle sessioni agente** (xdist con 4 worker in `load`): regressione completa fra 8 e 16 minuti.

### 1.3 Mutation, catalogo di main (148 mutazioni) [M]

| Esecuzione | Tempo totale | Esito |
|---|---|---|
| Runner ufficiale seriale | **431,1 s** | 147 KILLED, 1 EQUIVALENT, gate PASS |
| 4 shard per modulo, in parallelo | **237,4 s** | **identico mutazione per mutazione** |

Note sulle misure:
- Lo shard di `core.runtime_controller` da solo vale 197 s: è lui a limitare il parallelismo per modulo.
- Costruire la sandbox e la baseline costa circa 95 s.
- Uno sharding **per mutazione** invece che per modulo scenderebbe a circa 2–2,5 minuti [S].
- **Selezione incrementale «per hunk»** sui 6 push di #537 (catalogo del suo head: 194 mutazioni) [M]:
  - selezione per file: 129, 123, 113, 123, 112, 89 mutazioni. Riduce poco, perché `runtime_controller.py` ne ha 71;
  - selezione per hunk (mutazioni a ±30 righe dalle righe cambiate): **35, 24, 21, 17, 10, 3**, cioè l'82–98% in meno.

### 1.4 Review AI — 17 PR mergiate e #537 [M]
- **Costo:** $0,070 per review, circa $0,14 per round (Sol più Grok), $0,99 per PR fino a 25 000 caratteri e $1,82 per PR grandi.
- **Durata:** Grok 9,2 minuti di wall (mediana; esecuzione 5,9 minuti), Sol 2,8 minuti. Girano **in parallelo** alla CI.
- **Deduplica:** il marker per range esiste già e impedisce doppie review dello stesso push.
- **Fonte dei thread inline:** tutti i 181 thread di #490–#535 vengono da Codex, che è andato 24 volte in limite d'uso.

## 2. Analisi (consegna A–F)

### A. I cinque colli di bottiglia, ordinati per tempo perso
1. **La suite completa gira in serie in CI: 17,6–17,9 minuti** su ogni push, ed è il cammino critico. Con `-n 8 --dist worksteal` la stessa suite gira in 4,0 minuti in locale (×4,9) [M].
2. **Merge Readiness attende il check più lento**: 25–29 minuti per push. Si accorcia da solo se si accorciano le suite [M].
3. **Lavoro duplicato: 17 suite complete e 3 mutation per push**, circa 300 minuti-job sprecati per push, cioè circa 2 000 per PR di 6,7 round. Pesa poco sul wall perché i job girano in parallelo, ma moltiplica di 17 le occasioni di un falso rosso da test instabile e satura la coda dei runner [M].
4. **Mutation seriale**: 7–13 minuti in CI e 7,2 minuti in locale per ogni ciclo di fix, contro 4,0 minuti con 4 shard e meno di 1,5 minuti con la selezione incrementale [M]/[S].
5. **Test lenti per attese reali**:
   - un test da 181 s, che diventa il cammino critico con xdist: 242 s contro 183 s;
   - la harness F026 con `sleep` fissi (#528), che produce anche i falsi rossi misurati;
   - run di SHA superati che non si cancellano: 162 minuti-job su #535 [M].

### B. Benchmark

| Attività | Tempo attuale | Tempo ottimizzato stimato | Risparmio | Confidenza |
|---|---|---|---|---|
| **Pytest, suite completa** | CI 17,8 minuti in serie [M]; locale 19,7 minuti in serie [M] | locale 4,0 minuti [M] (3,0 senza il test da 181 s [M]); CI circa 4–6 minuti [S] | −75/−80% | alta in locale; media in CI (dipende dai vCPU del runner [NV]) |
| **Mutation** | CI 7,3–13,0 minuti ×3 per push [M]; locale 7,2 minuti [M] | PR GATE 1 volta per push, sharding: circa 2,5–4 minuti ([M] 4,0 per modulo); FAST incrementale <1,5 minuti [S] | −45/−80% in wall; −85% in minuti-job | alta per lo sharding (equivalenza provata); media per la selezione per hunk |
| **GitHub Actions per push** | wall 25–30 minuti; 342–391 minuti-job [M] | wall circa 10–12 minuti (resta il limite di Grok, ~9 minuti); circa 50–70 minuti-job [S] | −60% wall; −80/−85% minuti | media |
| **Review AI** | ~$1 per PR, Grok ~9 minuti in parallelo [M] | invariato: la review resta su ogni push | ~0 | alta: qui non c'è spreco reale da tagliare |
| **Ciclo completo PR** (fino a 25k caratteri) | 3,1 ore aperte, 6,7 round, ~28 minuti per round [M] | ~1,5–1,8 ore (round ~13–16 minuti) [S] | −40/−50% | media: il tempo di scrittura dei fix non cambia |

Sul piano di 100 PR eseguibili il tempo aperto scende da circa 310 ore a circa 160–180 ore [S]. Il costo dei revisori resta invariato.

### C. Strategia a tre livelli

| Livello | Quando si attiva | Contenuto esatto | Valore come evidenza |
|---|---|---|---|
| **FAST** (locale, durante RED→GREEN) | a ogni modifica locale, prima del commit | test toccati e test che importano i moduli cambiati; mutazioni a ±30 righe dagli hunk del diff; `-n 8 --dist worksteal` | **Nessuno.** Non sostituisce mai il PR GATE e non va mai citato come PASS di merge |
| **PR GATE** (locale prima del push, poi CI su ogni push) | ogni push su una PR; obbligatorio per il merge | 1 sola suite completa (tutti gli ~8 880 test, nessuna deselezione, xdist worksteal); test focalizzati di modulo e guardrail runner; stateful-integrity; live-sim-parity; **catalogo mutation completo una volta** (sharding, soglia 80%, aree obbligatorie); guard e full-diff; Merge Readiness; Sol e Grok sullo SHA del push | Sì, legata allo SHA del push. Un check fallito blocca, come oggi |
| **FULL CERTIFICATION** | push su `main` dopo ogni merge; prima di Gate A/B, #351 e di ogni campagna LIVE | suite completa **seriale** (scova i difetti mascherati dal parallelo); suite xdist ripetuta 3 volte (instabilità); mutation **seriale** completa; chaos (oggi solo a richiesta); GUI sotto Xvfb; build Linux e Windows | Dossier di certificazione con SHA, log e conteggi |

### D. Sicurezza: nessun controllo obbligatorio perso
1. **Stessi test, stesso esito.** Su 7 esecuzioni (seriale, `-n 4`/`-n 8` in `load` e in worksteal, 3 ripetizioni) l'insieme dei test eseguiti e degli esiti è identico. Nessun test diventa skipped e nessuno è deselezionato nel PR GATE.
2. **Mutation equivalente.** Seriale e parallelo danno lo stesso esito mutazione per mutazione (147 KILLED, 1 EQUIVALENT). Il PR GATE esegue il catalogo **completo**; la selezione per hunk vale solo per FAST.
3. **Il backstop resta.** Oggi `tests/testsuite/test_false_green_semantics.py:471` richiede lo step «Run full test suite (backstop)» in ogni ultra-check. La consolidazione aggiorna quel test perché richieda **esattamente un** backstop completo per push, bloccante e prerequisito (`needs`) degli ultra-check. Non si cancella l'asserzione: la si sposta.
4. **Cancellazione dei run superati** solo per gli eventi `pull_request` del medesimo PR e SHA superato (come già fa Merge Readiness). Mai sui push di `main`, mai sui reviewer. Merge Readiness giudica comunque lo SHA corrente.
5. **Nessuna cache dei test** (`-p no:cacheprovider` resta). La cache pip non serve: l'installazione costa 0,05–0,30 minuti. Niente artifact riusati fra SHA diversi.
6. **Test paralleli:**
   - i test GUI, Xvfb, processi OS, lock e SQLite su file vanno marcati `xdist_group` e girano seriali dentro il loro gruppo;
   - in FULL CERTIFICATION l'esecuzione seriale rileva eventuali dipendenze d'ordine;
   - la variante worksteal ha già passato 4 esecuzioni senza fallimenti spuri [M].
7. **Tracciabilità:** ogni report (junit, mutation JSON, decision.json) porta nel nome SHA e attempt, come oggi `mutation_guardrails_report.json`.
8. **Le modifiche ai workflow sono a merge owner** (`MANUAL_AUTHORITY_PATTERNS`), quindi nessuna auto-promozione.

### E. Piano operativo, in ordine di convenienza

Nessuna PR nuova fuori dal piano: tutto entra in schede già previste. La PF-074 (PR50, «CI: collection/workflow_run») si divide in due, quindi **+1 PR netta: 107 → 108 future**, e le schede si **anticipano** subito dopo #537, perché accelerano tutte le PR successive.

| Ordine | PR | Modifiche | Beneficio | Rischio | Test di equivalenza | Rollback | Gate |
|---|---|---|---|---|---|---|---|
| 0 (nessuna PR) | Procedura locale degli agenti | Regressione locale con `-n 8 --dist worksteal`; mutation con 4 shard paralleli | locale: 19,7 → 4,0 minuti; 7,2 → 4,0 minuti [M] | nullo | esiti identici già provati | tornare al seriale | nessuno; il PR GATE in CI resta invariato |
| 1 | **PF-022 esteso** (#528 + timing dei test) — solo `tests/`, mergiabile dall'agente | backoff a 0 nel test da 181 s (stessa logica, nessuna attesa); harness F026 senza `sleep` fissi; attese con condizione invece di `Event.wait(2)`; marcatori `xdist_group` per GUI, Xvfb, processi e lock | −181 s su ogni suite; meno falsi rossi | basso | RED→GREEN per i test toccati; 3 esecuzioni complete seriali e in parallelo con esito identico; mutation invariata | revert | nessun gate indebolito |
| 2 | **PF-074a** (merge owner) | togliere l'annidamento di `ci-dynamic-intelligent` dentro `ci-master-gate` (oppure il suo trigger autonomo); un solo job di backstop completo come `needs` degli ultra-check; `push` solo su `main` per i 12 workflow doppi; `concurrency` con `cancel-in-progress` per PR e SHA superati; aggiornare `test_false_green_semantics` e `test_orchestratori_ci_525` | −80% minuti-job per push; falsi rossi da instabilità ~17× meno probabili | medio: cambia la topologia della CI | test di policy sui workflow; confronto dei check prodotti su un push di prova contro l'elenco di oggi (nessun job obbligatorio scompare) | revert del commit sui workflow | resta 1 backstop completo bloccante |
| 3 | **PF-074b** (merge owner) | `pytest -n auto --dist worksteal` nel job di backstop; mutation una volta per push con sharding per mutazione (opzione `--jobs` nel runner); FAST incrementale (`--changed-since`) solo locale | wall CI 25–30 → ~10–12 minuti | medio | 3 esecuzioni CI parallele contro seriale sullo stesso SHA con insieme di esiti identico; mutation parallela contro seriale identica mutazione per mutazione | flag `-n` rimovibile; runner con `--jobs 1` = comportamento attuale | soglia 80% e aree obbligatorie invariate |
| 4 | dentro **PF-082** (strumenti di verifica) | job FULL CERTIFICATION su `main`: seriale, xdist ripetuto, mutation seriale | trova i difetti mascherati dal parallelo | basso | — | — | evidenze di certificazione |

**Costo di implementazione [S]:**
- 3 PR (1 solo test e 2 di workflow), circa 1 giornata di pipeline in totale;
- circa $3 di review;
- 2 merge owner.

Il risparmio di minuti stimato è di circa 300 minuti-job per push, cioè circa 200 000 minuti-job sulle ~670 push del piano. Che si traducano in denaro dipende dal piano GitHub del repository (pubblico o privato) [NV].

### F. Verdetto finale
1. **Quanto tempo perdiamo oggi?**
   - Circa 15–20 minuti per ogni push in attesa della CI (25–30 minuti contro circa 10–12 possibili);
   - circa 15 minuti per ciclo di fix in locale (regressione 8–16 minuti e mutation 7 minuti, contro 4 e 2–4 minuti);
   - circa 300 minuti-job per push di lavoro duplicato.
   
   Su una PR tipica di 6,7 round fanno circa 1,3–1,6 ore su 3,1 [S].
2. **Quanto possiamo risparmiare realisticamente?** −40/−50% del tempo per PR, circa −60% del wall della CI, circa −80% dei minuti-job. Sul piano residuo sono circa 130–150 ore aperte [S].
3. **Quali modifiche danno il beneficio maggiore?** xdist worksteal sulla suite (×4,9 misurato) insieme all'eliminazione delle 16 suite complete duplicate.
4. **Quali ottimizzazioni sono sicure?**
   - la procedura locale con xdist e mutation parallela (equivalenza misurata);
   - la deduplica dei job (stesso contenuto, una volta sola);
   - la cancellazione degli SHA superati sulle PR;
   - la correzione del test da 181 s.
5. **Quali richiedono ulteriori prove?**
   - xdist **in CI**: vCPU del runner e test GUI con display reale [NV];
   - la selezione mutation per hunk, perché può mancare effetti a distanza: resta solo FAST;
   - i marcatori `xdist_group` sui test con processi e lock;
   - la durata reale di Grok, che diventa il nuovo cammino critico.
6. **Quanto costerebbe implementarle?** 3 PR, circa una giornata di lavoro, circa $3 di review e 2 merge owner [S].
7. **Quale intervento conviene fare per primo?** Subito e a costo zero, la **procedura locale** (xdist worksteal e mutation in parallelo). Come PR, **PF-022 esteso** (solo test, nessun merge owner), poi **PF-074a** e **PF-074b**, anticipate in testa alla sequenza dopo #537.

**Review AI:** non conviene ridurle. Costano circa $1 per PR, girano in parallelo e trovano difetti reali. Conviene solo continuare ad accorpare i fix prima del push, cosa che la policy già prevede.

**File e dati grezzi:**
- `scratchpad/bench/` (serial.xml, xd*.log, ws*.log, mut_*.json);
- `scratchpad/ci/` (run e job di GitHub Actions).

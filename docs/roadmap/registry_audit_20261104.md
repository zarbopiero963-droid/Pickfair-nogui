# Audit di base per il registro persistente Roadmap V2 (F-01 → F-33)

Audit in sola lettura del 04/11/2026. `origin/main` = `ada40765e7949b8deb7661b3b5e9ddc8484adba3` (#607).
Fonti consultate: git locale (`git show/grep origin/main`, `git ls-remote origin`) e API GitHub pubbliche in GET
(#586 con tutti i 196 commenti, lista PR, label, issue con `roadmap-v2`). Non ho modificato nulla, non ho fatto commit
e non ho scritto su GitHub.

Convenzioni: **MERGED ≠ CERTIFIED**. Dove una cosa non è provata da una fonte verificabile scrivo `UNKNOWN`. Dove due
fonti non concordano scrivo `NEEDS_RECONCILIATION`.

---

## 1. Fonte autorevole dell'inventario F-01..F-33

### 1.1 Fonte primaria
Le schede sono **commenti della issue #586**, non issue separate e non file nel repo. Lo conferma anche Grok in
c.6583997783 §1.

| Ruolo | Commento #586 | Contenuto |
|---|---|---|
| **Indice ufficiale unico V2** | [c.6505613328](https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/586#issuecomment-6505613328) (29/10, 18:54Z) | «PICKFAIR_MASTER_EXECUTION_ROADMAP_V2 — 33 PR OFFICIAL»: §1 ordine esecutivo, §2 registro delle 33 schede (Ord., ID, titolo breve, prio, stato, dipendenze, merge, Betfair), §8 procedura `ROADMAP_UPDATE` |
| Schede v2, blocco 1 | [c.6504942027](https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/586#issuecomment-6504942027) | titoli completi F-01…F-15 |
| Schede v2, blocco 2 | [c.6504970288](https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/586#issuecomment-6504970288) | F-16…F-30, più F-31/32/33 ancora come «proposte» |
| Schede v2, blocco 3 | [c.6505576503](https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/586#issuecomment-6505576503) | **schede ufficiali** F-31, F-32, F-33 |
| Campi 1–24 di F-01…F-30 | [c.6502715406](https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/586#issuecomment-6502715406) (parte 4/7) | titoli con gli ID dei requisiti (CSH-01, RST-02…) |
| Delta V2 → V2.1 | [c.6505974575](https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/586#issuecomment-6505974575) | tracker UNVERIFIED (47 requisiti), assegnazione per scheda, rinomina di F-30 |
| Indice precedente (superato) | [c.6505088388](https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/586#issuecomment-6505088388) (V1) | conservato solo per tracciabilità |

Precedenza fra i documenti, dichiarata dalla V2 stessa: EXECUTION_LOCK (c.6504704386) → DECISIONS_RATIFIED
(c.6504686680) → MERGE_POLICY (c.6504721271) e CLAUDE_CERTIFICATION (c.6504736869) → FULL_SCOPE_TEST_PLAN e schede
v2 → riconciliazione parti 1–7. CLAUDE.md e AGENTS.md prevalgono dove sono più severi.

**Non esiste un `ROADMAP_UPDATE` né una V2.n ripubblicata.** La V2 §8 prescrive un commento `ROADMAP_UPDATE` e una
nuova versione dell'indice dopo ogni evento PR. Nei 196 commenti di #586 non c'è nessuna delle due: dopo il
29/10 lo stato vive solo nei rapporti narrativi degli agenti. Il più completo è l'audit di Claude B del 03/11
([c.6608348761](https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/586#issuecomment-6608348761)).

### 1.2 Fonti nel repo che citano le schede (ma non ne contengono l'inventario)
- `scripts/pr_autonomy_policy.py:301-309`: `OWNER_MERGE_DELEGATIONS["PF-ROADMAP-V2-MERGE-DELEGATION"]`, con issue
  586, granted 2026-11-01, `active: True`, `cards = F-01…F-33`, `_ROADMAP_CARD_RE = ^F-[0-9]{2}$`. Il codice conosce
  solo i numeri, non nomi, priorità o stato.
- `docs/auto_pr_flow_spec.md:369` (mandato precedente: F-05, F-08, F-01 a merge owner) e `:371-413` (§0.9-ter, delega
  temporanea di merge). Nel «Limite noto» (`:411-413`) la spec dice che la scheda la dichiara l'agente, che il codice
  non può verificare l'appartenenza alla roadmap e che ogni merge delegato va tracciato su #586.
- `.guardrails/allowed_scope.json:3131-3330`: le task key `audit_f33_fullscope_harness`, `f05_…`, `f08_…`, `f01_…`,
  `f09_…`, `f10_…`, `f06_…`, `f13a_…`. La `description` di ciascuna cita F-xx e #586.
- `CHANGELOG.md:69-125`: voci F-13a, F-06, F-10, F-09, F-01, F-08, F-05, F-33, con i test di ciascuna.
- `docs/testing/full_scope_harness.md`: tabella dei finding di F-33, che rinvia a F-05, F-01/F-02, F-18, F-10 e F-17.
- Docstring e commenti nel codice: `core/sl_tp_pnl.py:1` (F-01), `core/hedge_ref.py:1` (F-05),
  `core/order_ref.py:1` (F-13), `core/runtime_controller.py:2661,5751,6926`, `database_schema.py:299` (F-08) e altri.
- **Nessun F-xx** in `docs/traceability/requirements.json`, `tools/traceability/pf_trace.py`,
  `tools/agent_checkpoint/pfckpt.py`, `docs/agent_checkpoint_protocol.md` e `ops/roadmap_go_live.md`. Nel repo non
  esiste nessun file `roadmap*` o inventario delle schede.

### 1.3 Conflitti e incongruenze fra le fonti
1. **Ordine F-10/F-09.** La V2 §1 e §2 mettono F-10 (6a) prima di F-09 (6c), ma la stessa tabella dichiara
   F-10 ← F-08, F-09. L'ordine reale è stato F-09 (#597) e poi F-10 (#599), in base a un riordino owner del 31/10 e
   01/11 citato solo nelle `description` di `allowed_scope.json:3258` e `:3286`. Quel riordino non è ripubblicato
   nell'indice.
2. **Nome di F-30.** La V2 dice «Certificazione delle righe UNVERIFIED». La V2.1 §D.2 la rinomina in «FINAL
   UNVERIFIED REQUIREMENTS RECONCILIATION & RESIDUAL CERTIFICATION».
3. **Nomi di F-31, F-32 e F-33.** Il blocco 2 usa titoli italiani da «proposta». Il blocco 3, ufficiale, usa
   «Betfair Delayed Real-Data Feed», «Controlled LIVE Certification Campaign» e «Full-Scope Combinatorial & E2E Test
   Harness».
4. **Titoli diversi fra la parte 4 e le schede v2:**
   - F-03: «Trailing stop (CSH-04)» contro «Trailing stop (K-7)»;
   - F-22: «Messa in sicurezza dell'API legacy» contro «API legacy: esclusione e verifica non raggiungibilità (K-5)»;
   - F-24: «Ricontrollo fondi Microstake, OWNER C-01» contro «Guard esterna fondi Microstake (C-01 = ALTERNATIVA B)».

   La V2 §6 dichiara superata la parte 4 per F-22, F-24 e F-03.
5. **Sotto-ID non ufficiali.**
   - **F-13a/F-13b**: la divisione l'ha introdotta l'agente nel body di #601 («prima parte della scheda F-13»). Non
     sta nell'indice V2.
   - **«F-12a»** compare solo nel nome di un branch di Claude D (`claude/d-f12a-drawdown-parity`).
   - `_ROADMAP_CARD_RE` accetta solo `F-NN`, quindi una delega con card `F-13a` sarebbe `card_not_covered`.
6. **Mappatura dei difetti VPS.** Il registrar di Grok (c.6589579350) usa un `roadmap_map.json` che sta solo sulla
   VPS, fuori dal repo. Lì FI-03Z → F-07, FI-08B → F-16 e INV-RT-INFLIGHT-RESOLVED → F-13. L'audit di Claude B
   (c.6608348761 §1.a e §2 riga 14) tratta invece FI-03Z (#604) e FI-08B come «P1 fuori scheda». Su questo punto
   serve una riconciliazione (`NEEDS_RECONCILIATION`).
7. **Tre vocabolari di stato diversi:**
   - V2 §2: PLANNED · BLOCKED · AUTHORIZED · IN_PROGRESS · PR_OPEN · CI_PASS · REVIEW_PASS · MERGED · DEPLOYED ·
     CERTIFIED(1–6);
   - label del registrar e del dispatcher: BACKLOG · READY_FOR_CLAUDE · IN_PROGRESS · PR_OPEN · CERTIFICATION_RUNNING
     · BLOCKED · MERGED · VERIFIED;
   - `pf_trace`: PLANNED · IMPLEMENTED · TESTED · VERIFIED · CERTIFIED · BLOCKED · NOT VERIFIED.
8. **Agente responsabile.** L'audit del 03/11 §2 riga 13 assegna a Claude B F-11, F-12, F-16 e F-23. Il 04/11
   Claude D li ha implementati su branch propri (c.6636115622, c.6636874813, c.6638046063). Nessuna fonte owner
   verificabile riassegna le schede.
9. **«Branch locali non pushati».** F-13b, F-18 e F-02 risultano pushati come `keep/*` sul remoto (vedi §2):
   l'affermazione della cronologia non regge.
10. **Contatori di avanzamento.** L'audit del 03/11 dichiara 7,5/33 mergiate, con F-13 contata 0,5, P0 4/4 e P1
    2,5/16. È un conteggio dell'agente, senza una regola owner per le schede parziali.

---

## 2. Tabella F-01..F-33

Fonti:
- nome: titolo verbatim delle schede v2 (blocchi 1, 2, 3), con il titolo breve dell'indice V2 §2 dove serve;
- priorità e dipendenze: indice V2 §2;
- PR e SHA: API GitHub `pulls` (verificate);
- branch: `git ls-remote origin` (verificati).

Stato di certificazione: **UNKNOWN per tutte le schede**. Non ho trovato nessun report `CLAUDE_PR_FINAL_CERTIFICATION`
legato a una scheda, nessun livello CERTIFIED(1–6) e nessun `ROADMAP_UPDATE`.

Agente responsabile: i rapporti su #586 indicano Claude B per le schede mergiate. Tutte le PR però sono autore
`zarbopiero963-droid` (gli agenti pubblicano come owner, #472): GitHub non prova chi ha lavorato.

| ID | Nome ufficiale (verbatim) | Prio | Dipende da | PR / branch (verificati) | Stato verificato | Note |
|---|---|---|---|---|---|---|
| F-01 | Formula P&L del monitor SL/TP | P0 latente | — | **#596** merged 2026-11-01T13:44Z · merge `dc83b063ae5dfe06a2255d1527c1ae57163da53d` · head `aa184a2886201c3113d229410e19b5ea436aa6b3` · base `bb8d0285` · branch `f01-sltp-pnl` | MERGED (non CERTIFIED) | merge owner secondo c.6558691367/6559124329. Test `tests/core/test_sl_tp_pnl_sign.py`, mutazioni `f01_*`. Il monitor resta inerte fino a F-02 (CHANGELOG) |
| F-02 | Percorso SL/TP fino al monitor | P1 | F-01 | nessuna PR · `keep/f02-sl-tp-path-20261103` @ `80b00909a615` (impilato su F-18) | BRANCH_PUSHED, nessuna PR · test NOT_VERIFIED (audit 03/11: da ribasare) | c.6608487966 §4: conflitti con main dovuti allo squash di F-13a |
| F-03 | Trailing stop (K-7) | P2 | F-01, F-02, F-04, F-23 | nessuna | PLANNED (UNKNOWN se iniziata) | — |
| F-04 | Auto-close con lato e prezzo sotto barriera | P1 | F-02, F-05 | nessuna PR né branch trovati | PLANNED | Claude D dichiara di prepararla (c.6638046063): nessun branch verificato |
| F-05 | Identità e ledger dell'hedge; un solo cashout per posizione | P0 | — | **#593** merged 2026-11-01T09:25Z · `71ff2ab4bab7032f4b75a21932dd1b59eabe2c6f` · head `84fd4f7baa603ab308ed8eb30d2abe4f2c61ff05` · base `8de659ba` | MERGED (non CERTIFIED) | merge owner (c.6555582748). Test `tests/f05/`, mutazioni `f05_*`. Riga tracker SIG-03 (revisione A13): UNKNOWN |
| F-06 | Guard dell'hedge: lato/size, nessun default LAY | P1 | F-05 | **#600** merged 2026-11-02T09:47Z · `c7eff15d4f644e75c808e56356f734644ba0d927` · head `0f916f48f15cf7e4298e01b91c0d4bc16fbb3aec` · base `4531c638` | MERGED (non CERTIFIED) | merge owner (c.6574094726). Test `tests/f06/` |
| F-07 | Esposizione e residuo dopo cashout | P1/P2 | F-05 | nessuna | PLANNED · **NEEDS_RECONCILIATION** | il registrar VPS mappa FI-03Z (#604, merge `7a7006225277`) su F-07; l'audit di Claude B lo dichiara «fuori scheda» |
| F-08 | Schema `startup_orders`/`outbox` e ghost fail-closed | P0 | — | **#594** merged 2026-11-01T09:16Z · `1eed2fd2ecf5586af68be935e0c874b403109ec8` · head `6beacd4d1c5ed2e6fc2c1725add27359d8639080` · base `71ff2ab4` | MERGED (non CERTIFIED) | merge owner (c.6556164760). Test `tests/f08/` |
| F-09 | Esito di `recover()` gating | P1 | F-08 | **#597** merged 2026-11-01T14:46Z · `0d166934f84513103ccede4cf33271416184e503` · head `e5f23da2bf0861b42131ae602a3600f534d2a96e` · base `dc83b063` | MERGED (non CERTIFIED) | auto-merge certificato §0.9-bis (c.6559525216). Test `tests/f09/` |
| F-10 | Prova RST-03 (debito dopo crash) | P0 da provare | F-08, F-09 | **#599** merged 2026-11-01T22:56Z · `4531c638a98a720c1b30908cece96c162bef3118` · head `c18f2a2489bd33a9252b818560905e4114592844` · base `e139a5db` | MERGED (non CERTIFIED) | auto-merge certificato senza delega (c.6565139647). Test `tests/recovery/`. Ordine F-09/F-10: conflitto 1 |
| F-11 | Persistenza `equity_peak` e migrazione pre-v9 | P1 | F-08 | nessuna PR · `claude/d-f11-equity-peak` @ `8ce9e771a0aa` · `claude/d-f11-peak-reconstruction` @ `a87bdb279665` | BRANCH_PUSHED, nessuna PR (Claude D) | resta aperta la ratifica owner del picco ricostruito: le installazioni esistenti partono in PAUSED (c.6636115622) |
| F-12 | Parità drawdown % SIM/LIVE e rilascio guard | P1 | — | nessuna PR · `claude/d-f12a-drawdown-parity` @ `0c18198e6323` | BRANCH_PUSHED, nessuna PR · **NEEDS_RECONCILIATION** | il sotto-ID «F-12a» non è ufficiale: UNKNOWN se copre tutta la scheda |
| F-13 | `customerOrderRef` sugli ordini diretti e parità SIM | P1 | — | **F-13a #601** merged 2026-11-02T16:58Z · `a7b73efd35ae9882e296235c220caa5f8f3e862d` · head `bd327a4590abf454c992a7902c7e4f39a2201647` · base `c7eff15d` · **F-13b**: nessuna PR · `keep/f13b-order-identity-cashout-20261103` @ `4049b5c0db32` | PARTIAL: F-13a MERGED, F-13b BRANCH_PUSHED, test NOT_VERIFIED | la divisione a/b non è nell'indice V2. Test `tests/f13/`. PRE-LIVE SUCCESS su #601 (c.6583627989) |
| F-14 | Cancel single-shot, AMBIGUOUS e report per istruzione | P1 | — | nessuna | PLANNED | il P0 «placeOrders 2xx senza esito» di Claude D (`claude/pickfair-core-audit-slu9su` @ `47e2aaac86dd`) **non è associato a nessuna scheda**: UNKNOWN |
| F-15 | Gateway fail-closed su risposte malformate | P2 | — | nessuna | PLANNED | prerequisito di F-31 |
| F-16 | Freschezza fondi 30 s e auto-next | P1 | — | nessuna PR · `claude/d-f16-funds-freshness` @ `9767232f8f42` | BRANCH_PUSHED, nessuna PR (Claude D) | auto-next: decisione owner 04/11, nessuna scommessa generata (c.6636874813). FI-08B (`keep/fi08b-decimal-funds-20261103` @ `cee8eceb2055`) è mappato su F-16 solo dal registrar VPS: NEEDS_RECONCILIATION |
| F-17 | Commissione etichettata e default Roserpina espliciti | P1 | — | nessuna | PLANNED | finding xfail di F-33 (`commission_pct 0`) |
| F-18 | Validazione al salvataggio e loader fail-closed | P1 | — | nessuna PR · `keep/f18-config-validation-20261103` @ `94f52e14a552` (impilato su F-13b) | BRANCH_PUSHED, nessuna PR · test NOT_VERIFIED | tocca la configurazione: probabile esclusione dalla delega (`setting`/`config` non delegabili), da verificare sul diff |
| F-19 | GUI: `max_recovery_chase_abs`, controlli inerti, round-trip | P2 | — | nessuna | PLANNED | gate design handoff |
| F-20 | Confronti in Decimal | P2 | — | nessuna | PLANNED | — |
| F-21 | AEAD e migrazione segreti non distruttiva | P1 | — | nessuna | PLANNED | **merge owner** (manifest + credenziali non delegabili, §0.9-ter) |
| F-22 | API legacy: esclusione e verifica non raggiungibilità (K-5) | P2 | — | nessuna | PLANNED | owner se tocca il manifest |
| F-23 | PF-EXEC-05 all'ammissione | P1 | — | nessuna PR · `claude/d-f23-market-tradable` @ `a13714f85f8e` | BRANCH_PUSHED, nessuna PR (Claude D) | prerequisito di F-31 e F-03 |
| F-24 | Guard esterna fondi Microstake (C-01 = ALTERNATIVA B) | P2 | — | nessuna | PLANNED | titolo diverso nella parte 4 (conflitto 4) |
| F-25 | Igiene prove e test (TRACE-01) | P2 | — | nessuna | PLANNED | — |
| F-26 | Cablaggi mancanti | P2 | — | nessuna | PLANNED | — |
| F-27 | Operatività: path DB, log, installer, lockfile | P2 | — | nessuna | PLANNED | lockfile a merge owner |
| F-28 | Pulizia moduli orfani | P3 | — | nessuna | PLANNED | — |
| F-29 | Semantica `listClearedOrders` e riconciliazione commissione (GO-1) | P1 | F-31 | nessuna | **BLOCKED** (F-31 + GO-REALDATA) | — |
| F-30 | Certificazione delle righe UNVERIFIED (V2.1: FINAL UNVERIFIED REQUIREMENTS RECONCILIATION & RESIDUAL CERTIFICATION) | P0..P2 | F-33, F-31 | nessuna | PLANNED (continuo) | conflitto 2 |
| F-31 | Betfair Delayed Real-Data Feed (ambiente A GO-3) | P1 | F-23, F-15 | nessuna | **BLOCKED** (gateway attestato + GO-REALDATA) | piano GO-REALDATA in c.6608487966 §7 |
| F-32 | Controlled LIVE Certification Campaign (ambiente C GO-3) | — | P0/P1, F-31, F-21, F-30 | nessuna | **BLOCKED** (mandato finanziario + GO-LIVE-ORDER) | merge owner |
| F-33 | Full-Scope Combinatorial & E2E Test Harness | infra test | — | **#589** merged 2026-11-01T00:15Z · `8de659ba0d8739d431ebd3a2de2767a26fdba8f7` · head `4ab0fe3d289566d21c0dc24104edc005443be8fd` · base `b2c0947f` | MERGED (non CERTIFIED) | GO-MERGE condizionato (c.6551038375). `docs/testing/full_scope_harness.md`, `tests/fullscope/`, `tools/combi/` |

Verifica degli esempi indicati come «da verificare»:

| Esempio | Esito |
|---|---|
| F-01 #596, F-09 #597, F-10 #599, F-06 #600, F-13a #601, F-33 #589 | **confermati** |
| F-05 e F-08 | #593 e #594, merge owner secondo i rapporti su #586 |
| F-13b, F-18, F-02 «branch locali non pushati» | **falso**: sono sul remoto come `keep/*-20261103` |

Altre PR non di scheda, aperte nel periodo:
- governance: #592, #595, #598, #602, #603, #607, **#608 (aperta, head `dff58c3c25c5`)**;
- altre: #591 (fix #590), #604 (FI-03Z), #606 (chiusa senza merge);
- issue #605 con label `roadmap-v2` (`[PF-DEF][Roadmap V2] ext-mutation`).

---

## 3. Strumenti di tracciabilità e governance esistenti

| Strumento | Dove | Cosa fa | Utilità per il registro |
|---|---|---|---|
| Indice V2 + schede | #586 (commenti) | fonte normativa delle 33 schede | fonte dei campi statici; la V2 §8 prescrive già `ROADMAP_UPDATE` e V2.n, mai applicati |
| `pf_trace` | `tools/traceability/pf_trace.py`, registro `docs/traceability/requirements.json` (schema `pf-traceability/1`) | registro per requisito (decisione → codice → test → evidenza); evidenze append-only confrontate con `git show HEAD:`; `validate --strict`, `report` | **modello già pronto** per un registro versionato con validatore, ma è per requisito e non per scheda; nessun F-xx dentro |
| `pfckpt` | `tools/agent_checkpoint/pfckpt.py`, `docs/agent_checkpoint_protocol.md` | checkpoint atomico su branch di stato (`agent-state/*`): STATE.json, MANIFEST.sha256, lease/fencing, readback | il pattern «branch di stato» è riusabile. `agent-state/pf-claude-b` è fermo al seq 17 (01/11 13:38Z, F-01): **stale** |
| Delega di merge | `scripts/pr_autonomy_policy.py:301-309`, spec §0.9-ter | autorizza `card` F-01..F-33 con `head_sha` | consuma una scheda dichiarata dall'agente: un registro versionato la renderebbe verificabile |
| Allowed scope | `.guardrails/allowed_scope.json` (223 task key) | task key = file del diff, verificata da `pr-guard` (`guardrail_check.validate_declared_scope`) | collegamento scheda → PR oggi solo nella `description` (testo libero) |
| F-33 tracker | `tests/fullscope/requirements/pytest_plugin.py`, marker `pf_requirement` | `requirements_report.json` per SHA e seed | evidenza test per requisito |
| Attestazione review | `scripts/pr_review_attestation.py`, `docs/review_attestation.md` (PF-REVIEW-ATTESTATION v2, Sigstore) | review di Sol e Grok firmate sull'head | evidenza di review per head |
| Merge readiness | `scripts/pr_merge_readiness.py`, `.github/workflows/pr-merge-readiness.yml`, `pr-readiness-retrigger.yml` | gate di readiness; rerun su `check_run` del PRE-LIVE | stato dei check per head |
| PRE-LIVE VPS | check `pf-betfair-certification` (App «Pickfair Dedicated») | certificazione per SHA | evidenza di certificazione, non CERTIFIED V2 |
| Registrar VPS (Grok) | `/opt/pf-orch/registrar.py` + `roadmap_map.json` **sulla VPS, non nel repo** | digest dei difetti firmati su #586, issue `BACKLOG`, label di stato | ha già `issues:write` (c.6589579350); la mappa difetto → scheda non è versionata |
| Label | repo | BACKLOG, READY_FOR_CLAUDE, IN_PROGRESS, PR_OPEN, CERTIFICATION_RUNNING, BLOCKED, MERGED, VERIFIED, `roadmap-v2`, `pf-registrar`, `manual-review-required` | oggi usate solo dalla #605 |
| Dispatcher (non mergiato) | `keep/roadmap-dispatcher-20261103` @ `2ebb738`: `scripts/roadmap_dispatcher.py`, `scripts/claude_worker.py`, `docs/roadmap_dispatcher.md`, test | **definisce già** un registro su #586: commento con `<!-- pf-roadmap-registry:v1 -->` + JSON `{"cards":[{id,title,priority,status,deps,order,issue}]}`, ed eventi `<!-- pf-dispatch:v1 {...} -->`; accetta solo login fidati | formato di partenza riusabile. Nessun commento con quel marker esiste oggi su #586 |
| Dispatcher trusted CI | `scripts/pr_trusted_dispatcher.py`, `docs/trusted_dispatcher.md` | lane Buildkite (#562), non riguarda la roadmap | — |
| Coda task legacy | `.github/workflows/complete-task-on-merge.yml` (+ `.github/scripts/complete_current_task.py`, `next_task.py`) | su `pull_request: closed` (merged) fa **commit e push su `main`** di `ops/tasks*` con `contents: write`; poi issue con `issues: write` | precedente di scrittura su main da evento; da non replicare (vedi §5) |
| Issue master | #586 (governance #426, #461, #491) | tutto lo storico | vista umana |

### Workflow rilevanti per un aggiornamento da eventi (`origin/main`)

| Workflow | Trigger | Permessi |
|---|---|---|
| `pr-review-openrouter-gpt56-sol.yml`, `pr-review-xai-grok46.yml` | `pull_request_target` | contents:read, **pull-requests:write, issues:write**, id-token:write, attestations:write, actions:read |
| `pr-review-claude-fable5.yml`, `-fugu-ultra.yml`, `-gpt-astra.yml` (sospesi da P41) | `pull_request_target` | stessi, con `issues: write` |
| `ci-quarantine-guard.yml` | `pull_request_target`, push, dispatch | contents:read |
| `pr-readiness-retrigger.yml` | **`check_run: completed`** | actions:write, contents:read, pull-requests:read, checks:read |
| `pr-merge-readiness.yml` | pull_request, dispatch | contents/pull-requests/checks/statuses/actions/attestations: read |
| `pr-guard.yml`, `pr-overlap-guard.yml` | pull_request | default / contents:read |
| `complete-task-on-merge.yml` | pull_request closed (main) | job 1 **contents:write**; job 2 contents:read, issues:write |
| `queue-on-pr-closed.yml` | workflow_dispatch | contents:write, pull-requests:write, issues:write |
| `telegram-live-integration.yml` | dispatch, schedule | contents:read |

Nessun workflow su `main` scrive oggi un registro delle schede.

---

## 4. Dove vivono le istruzioni per gli agenti e i file a merge owner

**Istruzioni:**
- `CLAUDE.md`, con ingresso MCP da #491 e policy;
- `AGENTS.md` (Core rules, Persistent checkpoint);
- `docs/auto_pr_flow_spec.md` (§0 contratto di autonomia, §0.9 / 0.9-bis / 0.9-ter, §0.13 STOP, §0.14 PR successiva);
- `docs/hard_verify_spec.md`;
- `docs/mcp_operational_contract.md`;
- `docs/agent_checkpoint_protocol.md`;
- `docs/ai_audit_workflows.md`;
- `docs/microstake_native.md`.

Nel repo non ci sono `.claude/`, `.cursor` o copilot-instructions. Le istruzioni operative della roadmap stanno
solo nei commenti di #586 (V2 §0 «START HERE»).

**A merge owner** (`CLAUDE.md` §AUTO-MERGE, «UNICA ESCLUSIONE», righe ~477-510):
- documenti: `CLAUDE.md`, `AGENTS.md`, `docs/auto_pr_flow_spec.md`, `docs/hard_verify_spec.md`;
- workflow-gate: `pr-review-openrouter-gpt56-sol.yml`, `pr-review-xai-grok46.yml`,
  `pr-review-openrouter-fugu-ultra.yml`, `pr-review-claude-fable5.yml`, `pr-review-openrouter-gpt-astra.yml`,
  `ci-quarantine-guard.yml`, `pr-guard.yml`, `pr-merge-readiness.yml`;
- **ogni altro `.github/workflows/`** dal 08/10, e i manifest di dipendenze;
- script: `scripts/guardrail_check.py`, `scripts/pr_autonomy_policy.py`, `pr_fix_loop_policy.py`,
  `pr_flow_automation.py`, `pr_merge_readiness.py`, `pr_automation_controller.py`, `pr_clean_scope_rebuild.py`,
  `pr_refresh_self_checks.py`, `scripts/ci/check_ci_quarantine.py`, `scripts/ci/trusted_pr_guard.py`,
  `scripts/ci/pr_overlap_guard.sh`, `scripts/ci/pr_merge_readiness.sh` e, per fail-closed, ogni `scripts/pr_*.py`.

`.guardrails/allowed_scope.json` **non** è a merge owner, con invariante applicata da `guardrail_check`.

**Conseguenza per il registro:**
- qualsiasi workflow nuovo è merge owner;
- uno script che si chiami `scripts/pr_*.py` è merge owner;
- un file `docs/roadmap/registry.json` non lo è, a meno che non diventi input della delega.

---

## 5. Raccomandazione: struttura persistente meno invasiva

### Proposta: registro versionato a due strati, con la vista come derivato

1. **`docs/roadmap/registry.json` su `main`** (schema `pf-roadmap/1`). Contiene **solo i campi stabili e decisi
   dall'owner**:
   - `id` (`F-NN`), con `parts` opzionali (`F-13a`, `F-13b`) e campo `parts_source`; senza fonte owner,
     `parts_status: NEEDS_RECONCILIATION`;
   - `title_official` verbatim, `source_url` del commento #586, `title_history` per i conflitti 2–4;
   - `priority`, `deps`, `order_v2` e `order_override`, con la fonte (es. F-09 prima di F-10);
   - `merge_class` (agent / owner / owner-if-manifest);
   - `responsible_agent`, solo se c'è una fonte owner, altrimenti `UNKNOWN`.

   Più gli eventi **append-only**: PR, head, base, SHA di merge, data, check settled, test ed evidenze (comando + SHA
   + esito). Le evidenze seguono la stessa regola di `pf_trace`: mai rimosse o riscritte, confronto con
   `git show HEAD:`.

   Stati separati in tre assi:
   - `dev`: PLANNED, IN_PROGRESS, BRANCH_PUSHED, PR_OPEN, MERGED;
   - `integration`: CI post-merge su main;
   - `certification`: UNKNOWN, NOT_CERTIFIED, CERTIFIED(1–6) secondo la V2.

   La regola «MERGED non implica CERTIFIED» va codificata nel validatore.

2. **Validatore** sul modello di `tools/traceability/pf_trace.py`, ad esempio `tools/roadmap/pf_roadmap.py
   validate|report`. Gira come test pytest nella CI esistente, senza nessun workflow nuovo. Controlla:
   - 33 ID esatti;
   - dipendenze note;
   - SHA a 40 caratteri;
   - una scheda MERGED ha un merge SHA che `git merge-base --is-ancestor` trova su main;
   - CERTIFIED solo con un riferimento di evidenza owner;
   - evidenze append-only.

3. **Aggiornamento nello stesso PR della scheda.** Ogni PR F-xx aggiorna la propria riga: `PR_OPEN`, head, test.
   Applica la regola «docs nello stesso PR» e l'invariante di scope.

   Merge SHA e CI post-merge non si conoscono dentro la PR. Due opzioni:
   - li registra la **PR successiva** (procedura §0.14 «dopo ogni merge»);
   - oppure non si memorizzano: si derivano.

4. **Vista generata, non autorevole.** Un commento marcato su #586, compatibile con il formato
   `pf-roadmap-registry:v1` del dispatcher già scritto, generato da `report`. Pubblicarlo da evento richiede un
   workflow nuovo, quindi merge owner, con:
   - trigger `pull_request: closed` / `check_run: completed` / `schedule`;
   - permessi `issues: write`, `contents: read` e nient'altro;
   - eseguito dal branch di default, senza checkout del codice della PR;
   - un solo commento editato in place, con `concurrency`.

   Si può rimandare: finché il workflow non c'è, l'agente rigenera la vista a mano dopo ogni merge.

### Perché non le alternative
- **Solo issue master aggiornata da workflow.**
  - I commenti non sono autenticabili: gli account agente pubblicano come owner (#472), quindi la vista non può
    essere fonte per la delega.
  - Lo storico si perde a ogni edit.
  - Serve comunque un workflow a merge owner.
  - Un workflow su `pull_request_target` con `issues: write` è una superficie d'attacco se legge input della PR.
- **Workflow che committa su `main`** (stile `complete-task-on-merge.yml`):
  - scrive su main fuori dai gate e fuori da «una PR per repository»;
  - crea conflitti con la PR in corso;
  - `contents: write` sul branch protetto, e non ho verificato che funzioni con le protezioni attuali (UNKNOWN).
- **Branch di stato dedicato** (stile `pfckpt`, es. `roadmap-state`): atomico e senza conflitti, ma fuori dalla CI e
  dalla review. Il precedente `agent-state/pf-claude-b` è già stale dal 01/11. Va bene come cache, non come fonte.
- **Issue separate per scheda** (modello del registrar): 33 issue nuove, con un secondo vocabolario di label in
  conflitto con la V2.

### Rischi e punti da far decidere all'owner
1. **Autorità.**
   - Se il registro diventa input di `merge_decision` (scheda verificabile), il file va messo a merge owner: un
     agente che lo modifica potrebbe allargare la delega.
   - Se resta descrittivo, conviene dichiararlo esplicitamente «non autorevole per la delega».
2. **Precedenza.** Il registro non deve diventare «una seconda roadmap» (spec, regola #495). Va dichiarato come
   trascrizione della V2 di #586, con precedenza alla V2 e alle decisioni owner, e i conflitti 1–8 restano
   `NEEDS_RECONCILIATION` finché l'owner non decide.
3. **Sotto-schede** (F-13a/b, F-12a) e **mappe dei difetti VPS** (FI-03Z, FI-08B): servono una decisione owner o una
   fonte. Il codice della delega accetta solo `F-NN`.
4. **Responsabile.** Claude B o Claude D per F-11, F-12, F-16 e F-23: oggi non è deciso da una fonte verificabile.
5. **Tre vocabolari di stato** (V2, label del registrar, `pf_trace`): ne serve una mappatura unica, o il registro
   resta incoerente con le label.
6. **Doppio scrittore.** Il registrar VPS scrive già su #586, e il dispatcher non mergiato si aspetta un registro su
   #586. Serve una sola fonte primaria e marker distinti per non sovrascriversi.
7. **Merge SHA post-merge.** Con la soluzione «nella PR successiva», tra un merge e l'altro il registro è
   temporaneamente indietro. Il validatore deve tollerarlo (stato `MERGED_PENDING_RECORD`) senza dichiararlo PASS.
8. **Costo.** Un file modificato da ogni PR F-xx entra nel diff revisionato da Sol e Grok: pochi token, ma ogni
   modifica pesa sulla review.

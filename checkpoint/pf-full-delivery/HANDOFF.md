# HANDOFF — seq 12 (PF-CROSS-AGENT-HANDOFF-A06-01)

ChatGPT Work ha ripreso da clone/checkpoint remoto senza cronologia Claude: bootstrap `READY`, verifica `VALID`,
manifest 41/41. Il branch Gate-A concorrente `1734dee` è stato letto senza modificarlo e il suo manifest è 459/459.
Riconciliazione A06: 410 celle, 100 PASS, 0 FAIL, 233 NOT VERIFIED, 77 BLOCKED, 0 MISSING; nessun PASS trasferito
da un altro SHA. Dettagli e provenienza in `results/a06_reconciliation_seq12.{json,md}`.

Lotto indipendente F011/F025 pubblicato su `ccr-bcc81683-ud0hgc@c5e44221689c4e34c52cf6f55b69807cdac4c399`:
betId top-level/numerico recuperato; SUCCESS esplicito senza betId è AMBIGUOUS fail-closed. RED 4, mirati 76 PASS,
regressione 7563 PASS / 15 SKIP / 2 XFAIL / 8 subtest; guard e full-diff PASS. Gate A resta NON PASS, LIVE NO-GO.

Prossima azione atomica: vedi `STATE.json.next_atomic_action`. Non modificare main o PR #505; non inventare range.

---

# HANDOFF — seq 8 (PF-UNIVERSAL-CHECKPOINT-V1)

Stato: checkpoint migrato a schema `pf-checkpoint/3`, verificabile con `tools/pfckpt.py`.

Incidente INC-b45f528: il checkpoint seq 7 (b45f528) elencava nel MANIFEST tre log mai committati
(`*.log` in `.gitignore`). Originali ritrovati nel worktree locale con SHA-256 identici al manifest e committati
in questo checkpoint; b45f528 resta nella storia invariato. Prova: `results/incident_b45f528_verify.json`.
`tools/save_checkpoint.sh` ritirato (causa del difetto), sostituito da `tools/pfckpt.py`.

Prossima azione: vedi `STATE.json.next_atomic_action` (MD10, mutation d04 su HEAD, regressione completa, matrice A06).

---
# HANDOFF — PF-FULL-DELIVERY-01 (stato persistente, leggibile da qualsiasi agente)

**Fonte di verità:** questo branch (`agent-state/pf-full-delivery`) + branch di lavoro `ccr-bcc81683-ud0hgc`.
La chat e gli Artifact NON sono la fonte di verità.

## Ripresa («Riprendi Pickfair»)
1. `git ls-remote origin refs/heads/main refs/pull/505/head refs/heads/ccr-bcc81683-ud0hgc`:
   main deve essere `028cee9`, #505 `618884e`, branch di lavoro = `work.head_sha` di STATE.json. Altrimenti STOP.
2. `git checkout ccr-bcc81683-ud0hgc`; venv Python 3.12 da `requirements-lock` + pytest/xdist; Xvfb `:99` per i test GUI.
3. Rieseguire almeno `pytest -n 4 tests/f001 tests/f026` (atteso: tutti PASS) prima di proseguire.
4. Eseguire solo `next_atomic_action`.

## Fatto (verificato)
- F001/F023/F024 consolidati (`c92bb79`).
- F026 v2 + F2 + F018 (`afebec7`): grandezze del conto separate, base dello stake invariata, auto-reset sul regolato,
  rilascio tavolo al settlement, ricostruzione posizioni al riavvio con PAUSED se la riconciliazione è incompleta.
- Regressione completa 7363/0.

## Non fare
- Nessuna PR (una PR per repository: #505 è aperta), nessun merge, nessuna modifica a main o #505, nessun LIVE.
- Non integrare la patch F026 v1 (`b766261`): sostituita da v2.


## seq 2 (a2032fb)
- F029 corretto (TableManager.allocate: recovery solo se libero ed entro table_count), trovato dalla campagna.
- Regressione 7374/0; mutation 19/19 killed; guard + full-diff PASS sul range c92bb79..a2032fb.
- Campagna 41 parametri: 1684 scenari, 0 violazioni, 619 con difetti noti F027/F028. Non certificata.
- Codice della campagna in campaign/ (rieseguibile: generate -> run -> judge_all -> report).
- Aperti: F027 (P1), F028, F030, OBS-3, OBS-4. CONFIG-41 bloccato (#505, MCP assente).

## seq 3 (b33b54d) — mandato PF-ROSERPINA-FINANCIAL-CLOSURE-03
- Corretti con RED→GREEN: F027 (settlement in PAUSED/LOCKDOWN/emergency), F028 (rilascio ordini non abbinati confermati),
  F031 (parità SIM parziale annullato), F030 (liquidity guard ingressi singoli), stop esposizione sul candidato, etichetta Max Win.
- Regressione 7435/0; suite toccate 488 + 2 xfail (F032, F033); guard/full-diff PASS.
- Campagna su b33b54d: 1680/1680 PASS (base, stress, frontiera), zero KNOWN; durata e mutation MC10-MC17 da completare.
- Merge di prova con #505: compatibile (solo conflitti testuali), nessuna modifica a #505/main.
- Ripresa: vedi next_atomic_action in STATE.json.

## seq 4 (180f405) — mandato PF-ROSERPINA-CONTINUOUS-DELIVERY-04
- Lotto A: baseline verificata. Lotto B: F032 (baseline persistente del ciclo) e RSR-12 (recovery persistente) corretti RED→GREEN.
- Regressione 7530/0 su 180f405; guard/full-diff PASS. Parametri: 36 PASS, 1 NOT VERIFIED, 4 BLOCKED.
- Strumenti di ripresa: RUNBOOK.md, tools/pfckpt.py (bootstrap/verify/save con readback), mutation/, critical/, campaign/.

## seq 5 (f6ee25e)
- Campagna 1684/1684 su 180f405; scenario critico PASS; mutation c03 16/16 validi, d04 parziale (MD6 chiuso con test).

## seq 6 (f6ee25e) — chiusura sessione al 97% budget
- Pendenti: mutation MD6 (conferma), MD10, MD11 su f6ee25e; regressione completa su f6ee25e NOT VERIFIED.


## seq 52 — #529 mergiata (2026-10-20)

- **#529 (#523) MERGED:** squash `380157a200ae98b8b77034f15c232ed96d61bc79` dall'head `b902b00`; tree identico (diff 0). CI post-merge main 13/13 verdi.
- Hard verify: **IMPLEMENTED_WITH_NOTE**. PF-EXEC-02 NON completo (PR30/PR31). `BLOCKS_DUTCHING_ACTIVATION` resta: corsie Dutching/manual_bet dormienti.
- Prove: regressione 8650/0, mutation 67/67, Sol e Grok senza bloccanti, riesame indipendente pulito, 0 thread aperti.
- **Prossima voce (sequenza owner #461 c.6278391354):** allineamento fix loop a 10 cicli (`scripts/pr_fix_loop_policy.py` MAX=5 oggi; PF-QA-AUTONOMOUS-CONTINUATION-01). File di policy => merge manuale owner. Poi riconciliazione completa #461. Follow-up #528.
- NO LIVE.


## seq 53 — #530 mergiata (2026-10-20)

- **#530 MERGED** (merge manuale owner, squash `2141031a900f406b6a1b4e0325d26200eca97998`, head `2ecc4a4`, tree identico). CI post-merge 12/12 verdi.
- `MAX_FIX_LOOP_ITERATIONS_PER_PR = 10` su main; `max_total_controller_runs` = tetto + 3; docs normative allineate.
- **Prossima voce (sequenza owner #461 c.6278391354):** riconciliazione completa #461. Poi PR29/PR30. Follow-up #528. NO LIVE.

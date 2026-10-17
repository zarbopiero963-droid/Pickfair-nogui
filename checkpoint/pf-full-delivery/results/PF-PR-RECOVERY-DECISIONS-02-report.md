## PF-PR-RECOVERY-DECISIONS-02 — consegna finale del lotto (Claude Code, 17/10/2026)

Nessun merge, nessun deploy, nessun LIVE, nessuna spesa, nessuna cancellazione. Gate A #506 NON PASS, Gate B non certificato, PR #505 protetta e invariata (`618884e`), `main` invariata (`028cee9`).

### 1. Stato reale della #512 sul nuovo HEAD

| Commit | Contenuto |
|---|---|
| `5b4a589` | sei finding triagiati corretti con test RED→GREEN (`tests/test_pr512_recovery_fixes.py`, 18 RED su 339cd24) |
| `cad07e1` | evidenze di tracciabilità con lo SHA reale (registro PASS, 19 requisiti, 0 CERTIFIED) |
| `21aab95` | rilievo Sol sul lotto: esenzione scan segreti vincolata al percorso esatto `tools/pfckpt.py` |

HEAD `21aab95`, 18 commit, 69 file (= scope dichiarato, guard PASS, full-diff PASS, `owner_manual_merge_required` per AGENTS.md/CLAUDE.md). CI su `cad07e1`: tutti i check verdi tranne `overlap-check` (tre PR aperte, governance) e `Merge readiness` (UNSTABLE + thread attivi). Stato: **PR di implementazione attiva, in valutazione, NON certificata.**

### 2. I sei finding

| Finding | Esito | Test RED→GREEN |
|---|---|---|
| Codex 4303374299 `resume()` senza recovery Microstake | FIXED (5b4a589) | `test_resume_keeps_the_runtime_paused_while_a_microstake_saga_is_still_pending`, `test_resume_becomes_active_only_when_both_recovery_paths_succeed` |
| Codex 4303374312 precisione >2 decimali / residuo ≠ richiesto | FIXED (5b4a589): `invalid_size_precision` pre-invio; residuo diverso ⇒ AMBIGUOUS `unexpected_residual_size` | `test_stake_with_more_than_two_decimals_is_rejected_before_any_exchange_operation[3]`, `test_residual_different_from_the_requested_stake_is_ambiguous_never_active` |
| Codex 4303374328 ladder malformata | FIXED (5b4a589): liquidità ignota ⇒ RiskGate nega | `test_malformed_ladder_means_unknown_liquidity_not_an_exception[5]` |
| Codex 4303374317 scan segreti esenta i `.py` | FIXED (5b4a589 + 21aab95): esente solo `tools/pfckpt.py` | `test_integrity_violations_are_rejected[secret_py]`, `[secret_named_pfckpt]` |
| Codex 4303374326 SHA evidenza non risolto | FIXED (5b4a589): `git cat-file` per ogni evidenza; shallow ⇒ warning | `test_fabricated_evidence_sha_is_rejected_and_a_real_one_is_accepted`, `test_evidence_sha_outside_a_git_repository_is_not_verifiable` |
| Grok c.6236399723 snapshot fuori lock / False ignorato; `pre_send` dal log globale | FIXED (5b4a589): salvataggio sotto lock, `snapshot_persisted` dichiarato; `pre_send` per `customer_ref` | `test_sim_snapshot_is_written_while_the_broker_lock_is_held`, `test_snapshot_failure_is_declared_in_the_executor_result`, `test_pre_send_is_not_masked_by_a_concurrent_place_of_another_saga` |

Nessun finding inventato o sostituito; nessuno sospeso. Review sul lotto: Sol 1 bloccante (fixed 21aab95); Grok 2 bloccanti: uno falso positivo (`except` ritorna già `False`), uno THEORETICAL (controller senza engine non può emettere ordini) — risposte in c.6242069447.

### 3. Regressione completa (conteggi reali)

- `cad07e1`: **7740 passed, 14 skipped, 2 xfailed, 1 failed** (`tests/acceptance/test_issue461_pr04_quater.py::test_gui_save_reopen_file_pickers_and_live_boundary[True]`, `TclError: couldn't connect to display :99`: Xvfb morto durante la suite). Riprovato dopo il riavvio di Xvfb: 3 run su cad07e1 e 3 su 339cd24 con lo stesso esito (`RuntimeError: main thread is not in main loop`), quindi **ambientale**, non regressione (il diff 339cd24→21aab95 non tocca GUI/tk).
- `21aab95`: **7742 passed, 14 skipped, 2 xfailed, 0 failed** (15 min 22 s, Xvfb vivo per tutta la suite). Nessun test escluso.
- Suite mirate sul lotto (Microstake, core, f026, f029, f001, parity, guardrail no-bypass): 832 passed, 0 failed; nuovi test 78 passed; RED su 339cd24: 18 failed.

Comando: `PYTHONPATH=netguard DISPLAY=:99 venv312/bin/python -m pytest -q -p no:cacheprovider tests` (Python 3.12.3, Linux 6.18, rete non-loopback negata).

### 4. I 31 FAIL dell'integrazione #505 + #512 (decisione 3 / §7)

Riprodotti sull'albero scratch `f63cf3a` (#505 `618884e` + #512 `339cd24`, mai pubblicato): 31 failed, tutti `tests/f026/test_f026_financial.py`. Causa unica, verificata eseguendo il child: `SIGNAL_REJECTED max_order_exposure_exceeded:limit=1.0€` (cap assoluto per ordine della #505, default 1 € in `services/setting_service.py`).

Esperimento di attribuzione (albero scratch, cap assoluti alzati a 1 000 000 nel solo esperimento, nessun file pubblicato): **38 passed, 1 XPASS(strict)**. Il XPASS è `test_F033_event_cap_aggregates_markets_of_the_same_event`: con la #505 integrata, F033 (cap evento per identità canonica) **passa** — conferma che la #505 chiude F033 come previsto; il marker `xfail(strict)` andrà rimosso nella PR di integrazione.

Classificazione:
- **Conflitto contrattuale** (31/31): gli oracle F026 chiedono stake ≥ 1 € accettati; il default 1 € della #505 li rifiuta. Per la Decisione owner 1 («nessun limite universale arbitrario allo stake», la soglia 1 € non si applica come limite universale) **gli oracle F026 sono corretti** e il default è il cap storico superato. Nessun difetto di implementazione; nessun oracle obsoleto (i test non sono stati modificati, le evidenze RED restano).
- Da fare nella PR di integrazione (non ora, #505 protetta): riconciliare il default del cap per ordine con la Decisione 1 nella #505 (cap storico ⇒ limite di rischio configurabile, non universale), mantenere mercato/evento/drawdown assoluti come limiti validi, verificare esposizione massima, fondi, LAY liability, partial match, cancel/replace, restart e assenza di duplicati sull'albero integrato con cap configurati; fence cashout × saga Microstake in corso (test dedicato).
- Decisione 2 (stake > `max_single_bet`: vietato ridimensionare): **ancora da implementare** sulla #512 (oggi `_clamp` verso l'alto in `core/money_management.py`): lotto successivo autorizzabile, con test RED/GREEN; non incluso in questo lotto per rispettare il perimetro dei sei finding.

### 5. I tre rilievi #508 (decisione 4)

Push 11 `4476108` su `docs/od01-review-bf2-d03` (solo `docs/review/od01/`): DIFF-PF-BF-16-STATE (Sol) ACCETTATO e corretto (matched(old)+matched(new)+remaining(new)); DIFF-PF-BF-09-ERROR (Grok) ACCETTATO e corretto (errore/timeout ⇒ NO_ENTRY, nessun retry monetario; atteso di B_05 ancorato); DIFF-PF-BF-S02-SCHEMA (Grok) ACCETTATO: codice MISSING ⇒ nessuna conformità dichiarata, decisione owner necessaria. Registro `22_D03_LOTTO01_ERRATA_03.md`. Riesame Sol/Grok automatico sul push: Sol c.6241845836 e Grok c.6241849746, nessun bloccante («formula replaceOrders allineata, NO_ENTRY su listMarketBook in errore, Order Stream marcato MISSING»). La PR resta aperta come revisione documentale; chiusura senza merge solo dopo il verdetto owner per ID su #506.

### 6. Checkpoint durevole

seq 16 `__CKPT16__` su `agent-state/pf-full-delivery` (DURABLE, readback verificato, lease rilasciato): codice `21aab95`, prove in `results/pr_recovery_decisions_02/`.

### 7. Pubblicazioni

- #512: risposte nei 5 thread Codex (r4304674285…r4304675020), Grok 339cd24 c.6241828302, review su cad07e1 c.6242069447.
- #508: c.6241841538.
- #506 / #426 / #461: questo rapporto e il riepilogo.

### 8. Prossima azione autorizzabile (Gate A → Gate B → merge)

1. Decisione 2 sulla #512 (rifiuto esplicito sopra `max_single_bet`, nessun ridimensionamento): un lotto RED/GREEN sullo stesso branch.
2. Gate A globale #506 → Gate A #505 → riconciliazione del default cap-ordine con la Decisione 1 nella #505 (owner) → Gate B #505 → autorizzazione merge #505.
3. Poi integrazione #512 su `main` aggiornato: scenari F026 con cap espliciti, rimozione dell'xfail F033, test fence×saga, regressione completa + campagna, Gate B #512.

Nessuna nuova PR fino ad allora.

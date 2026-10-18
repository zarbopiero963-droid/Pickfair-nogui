# PR #512 ciclo 7 (deroga owner PF-PR512-CYCLE7-AUTHORIZATION, O1)

Commit `034af594a5bce4a121c87ec6aa57f959a1ab0208` su f62f0e3 (branch ccr-bcc81683-ud0hgc). 5 file: core/runtime_controller.py,
tests/test_pr512_codex_f62f0e3.py (nuovo, 21 test), docs/roserpina_bankroll_equity.md, CHANGELOG.md, .guardrails/allowed_scope.json.

Tre P1 Codex su f62f0e3: r4306544229 (lato mancante => non attribuito), r4306544236 (reset SIM solo senza tavoli
trattenuti, `sim_reset_skipped`), r4306544231 (baseline stantia rifiutata dopo reset durevole, marker
`roserpina.cycle_reset_state` nella stessa transazione RSR-12).

Evidenze:
- red_cycle7_f62f0e3.txt: RED dei 7 test iniziali sul head f62f0e3 (5 failed / 2 passed).
- independent_review_1_a8dbbd2.md + throwaway_review_test_1.py: riesame indipendente del primo albero (a8dbbd2):
  P1 ordine baseline/marker (baseline del reset scartata al riavvio), test con ordine invertito, P2 marker corrotto
  fail-open, docs. Tutti corretti; red_cycle7_review_a8dbbd2.txt = RED dei nuovi test sul core a8dbbd2 (7 failed).
- independent_review_2_fdf42bc.md + throwaway_review_test_2.py: secondo riesame (fdf42bc): quattro punti confermati
  corretti; P2 nuovo `set_at_ts` non finito nella baseline (NaN/inf) scavalca il confronto => corretto con
  `validators.finite_number` (red_review2_fdf42bc.txt: 3 failed sul core fdf42bc). NIT preesistente (esito False di
  `_persist_recovery_tables` ignorato nei percorsi di reset) registrato come GUARDRAIL_GAP follow-up, fuori perimetro.
- guard_final.txt / full_diff_final.json: guard locale PASS (scope == diff, 78 file), full-diff PASS,
  owner_manual_merge_required=true (AGENTS/CLAUDE nel diff).
- cycle7_034af59.diff: diff completo del ciclo; commit_034af59.txt.
- regression_034af59.txt: regressione completa sul commit pubblicato (aggiunta al termine).

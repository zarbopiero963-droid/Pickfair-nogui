# Esiti dei test su commit SOLO LOCALI (31/10/2026)

Ripristino: `git am patches/<area>/*` su main 6fa29b2.

- `59b05ae` — F-05 suite completa -n 4 (wt-f05): **FAIL** — 10647 passed, 1 failed (guardrail catalogo mutation, riga ambigua) su 59b05ae; corretto in 7876d64
- `7876d64` — F-05 mutation cashout_executor+core.runtime_controller: **PASS** — GATE PASS, f05_* killed
- `73c3da7` — #590 mutation scripts.pr_full_suite_pipeline: **PASS** — 8/8 killed
- `7876d64` — F-05 regressione mirata dopo il fix (catalogo, f05, f026, cashout, guardrails, acceptance) -n 4: **PASS** — 2510 passed
- `212d299` — F-05 tests/guardrails + tests/scripts -n 4: **PASS** — 3758 passed
- `ce06aac` — F-08 suite completa -n 4 (wt-f08): **PASS** — 10640 passed
- `889b9a9` — F-08 scripts+guardrails+f08+recovery -n 4: **PASS** — 3801 passed; mutation core.state_recovery+database_schema GATE PASS (f08_* killed)
- `dde85a5` — F-01 suite completa -n 4 (wt-f01): **PASS** — 10647 passed; mutation core.sl_tp_pnl+core.runtime_controller GATE PASS (113 killed, 3 equivalent, f01_* killed)
- `e1da865` — F-05 suite completa -n 4: **PASS** — 10655 passed; mutation cashout_executor GATE PASS (f05_duplicate_request_hedged_twice, f05_pending_hedge_not_recovered, f05_hedge_not_written_ahead killed); 16 test unita' F-05 (BLOCK: 5 FAIL su 212d299)

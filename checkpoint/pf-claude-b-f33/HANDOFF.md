# HANDOFF — Claude B (F-33 / F-05), checkpoint seq 2

Branch di stato: `agent-state/pf-claude-b`, directory `checkpoint/pf-claude-b-f33`.

- **PR #589 (F-33)**: head `a721e09` (3873eaa: bloccanti Sol/Grok + 3 P2 Codex; a721e09: xfail statico del test di confine). CI e review su a721e09 da leggere.
  `overlap-check` rosso finche' #587 e' aperta: blocco di governance registrato, **non aggirare**. Non mergiabile finche' il required check e' rosso.
- **#587**: standby VPS/Grok, gestita da Claude A; non modificarla.
- **F-05**: Phase 0 completa in `results/F05_PHASE0.md` (rischi: tavolo fantasma F018, regressione rilascio SIM, P&L hedge assente dal daily-loss LIVE, doppio hedge LIVE). Prossimo passo: test RED. Finanziaria critica ⇒ merge manuale owner.
- Roadmap e rapporti: issue #586 (PICKFAIR_MASTER_EXECUTION_ROADMAP_V2, tracker UNVERIFIED, F33_FINAL_LOCAL_HARDENING_READY, PICKFAIR_CLAUDE_B_SAFE_HANDOFF_F33_F05).
- Vietati: push su branch non autorizzati, nuove PR, merge forzati, modifiche a ruleset/required check/workflow, Betfair, VPS, LIVE.

## seq 12 (31/10/2026)
- #591 chiusa DEFERRED_GOVERNANCE_DEPENDENCY senza merge; branch `fix-590-tg-guard-sigpipe` (0d1f466) preservato.
- #592 (governance, `gov-gha-primary-buildkite-deferred`) aperta, head 990b99d, audit Claude A integrato.
- Readiness 990b99d: status Buildkite ignorati come `deferred: buildkite`, pending vuoto. Thread Codex risolto con evidenza.
- Blocco: mutation-guardrails rosse per fsp_* INCONCLUSIVE (fix #590 non su main). Decisione owner richiesta su #586 c.6549919101. Nessun push su #592 fino alla decisione.

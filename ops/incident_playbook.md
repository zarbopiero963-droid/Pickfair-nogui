# Incident Playbook (Operational, Manual)

This playbook is for operator-driven incident handling.

## Non-negotiable safety constraints
- No autonomous close/cashout/liquidation actions.
- No autonomous resume or live activation.
- Any progression remains blocked until operator review is complete.

## Immediate operator actions
1. Declare incident status and timestamp in the ops log.
2. Keep live progression blocked.
3. Capture local evidence snapshot via `scripts/incident_snapshot.py`.
4. Record snapshot JSON path with incident notes.

## Minimum evidence set
- `ops/readiness_checklist.md`
- `ops/rollback_checklist.md`
- `ops/paper_trading_gate.md`
- `ops/live_microstake_gate.md`
- `ops/observability_minimum.md`

## Incident handling checklist
- [ ] Incident declared and timestamp recorded
- [ ] Live progression explicitly blocked
- [ ] Local evidence snapshot captured
- [ ] Missing evidence reviewed
- [ ] Operator acknowledgement recorded
- [ ] Explicit manual decision documented (remain blocked / proceed)

## Exit criteria (manual only)
All must be true before considering progression:
- snapshot `status` is `PASS`
- required evidence files are present
- operator acknowledgement is recorded
- explicit manual go/no-go decision documented

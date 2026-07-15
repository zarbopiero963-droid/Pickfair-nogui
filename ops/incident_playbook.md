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

## Telegram runtime diagnostics (dove guardare)

Se il listener Telegram ha un problema, i log applicativi ora riportano il
motivo (prima molti fallimenti erano silenziosi). Cerca questi prefissi:
- `[TelegramListener] mark_failed: <reason>` — **ogni** fallimento terminale
  (`connect_timeout`, `session_not_authorized`, `disconnected_unexpectedly`,
  `runtime_error: …`, `reconnect_failed`, ecc.), livello ERROR.
- `[TelegramListener] runtime thread crashed` — crash del thread runtime con
  traceback completo (livello ERROR/exception).
- `[TelegramListener] stato X -> Y` — transizioni di stato
  (CONNECTING/CONNECTED/RECONNECTING/STOPPED/FAILED), livello INFO.
- `[TelegramListener] reconnect tentativo #N avviato` / `reconnect riuscito`
  — ciclo di riconnessione.
- `[TelegramService] autoheal ENTER_FAILED_LOCKOUT` — recovery **sospeso**
  (niente più restart automatici), livello ERROR; `autoheal SCHEDULE_RESTART`
  — restart automatico in corso (WARNING).

`last_error` (operator-facing via `status()`/telemetria) riporta un reason
**whitelisted**: per i crash runtime è `runtime_error: <TipoEccezione>` (solo il
tipo, mai il messaggio raw), gli altri sono codici fissi (`connect_timeout`,
`session_not_authorized`, …). Così nessun segreto — nemmeno di terzi (URL DB con
password in un'eccezione) — può raggiungere l'operatore via `last_error`. Il
**dettaglio completo** del crash (traceback) resta nel log ERROR, ulteriormente
redatto dalle credenziali NOTE al listener via `_redact_sensitive` (difesa in
profondità). Sono diagnostica; non sostituiscono lo snapshot operator-facing di
`ops/observability_minimum.md`.

## Exit criteria (manual only)
All must be true before considering progression:
- snapshot `status` is `PASS`
- required evidence files are present
- operator acknowledgement is recorded
- explicit manual go/no-go decision documented

## Ratifica owner t80u: chiavi assenti o non valide (PF-OWNER-CLOSE-ALL-02)
**Fonte:** messaggio owner del 2026-10-13 12:33 (ora di Roma), ratifica t80u. Riporto le regole **verbatim** (testo originale dell'owner, in inglese):

> (1) max_single_bet_pct, max_total_exposure_pct, max_event_exposure_pct, max_stake_abs absent/invalid/unverifiable => block all new risk; no code default (18/35/18/10000) approved.
> (2) max_recovery_tables, allow_recovery absent/invalid => no new recovery; absence never True, no invented max.
> (3) commission_pct: only verified applicable rate; else explicitly estimated vs verified P/L; no risk-increasing decision on invented/estimated commission; risk-reducing ops only under conservative verifiable conditions.
> Not an approval of 18%,35%,18%,10000€,2,True,4.5%.

**Chiavi interessate:**
- regola 1: `roserpina.max_single_bet_pct` (RSR-02), `roserpina.max_total_exposure_pct` (RSR-03), `roserpina.max_event_exposure_pct` (RSR-04), `roserpina.max_stake_abs` (RSR-15);
- regola 2: `roserpina.max_recovery_tables` (RSR-12), `roserpina.allow_recovery` (RSR-31);
- regola 3: `roserpina.commission_pct` (RSR-13).

**Note di registrazione:**
- Questa ratifica sostituisce lo stato PENDING_OWNER di queste 7 chiavi.
- I default del codice (18 / 35 / 18 / 10000 / 2 / True / 4,5) restano **comportamento attuale non ratificato** e non diventano valori normativi.
- Documentato non significa implementato: la conformità del codice si verifica con prove separate su #506.
- Nessun merge, deploy o LIVE. Gate A: NON PASS.

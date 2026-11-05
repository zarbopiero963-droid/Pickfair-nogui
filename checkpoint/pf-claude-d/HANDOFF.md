# HANDOFF — Claude D (motore finanziario Pickfair)

Catena: `agent-state/pf-claude-d`, directory `checkpoint/pf-claude-d`. Lease `claude-d`.
Fonte di verità: questo checkpoint + i branch pubblicati elencati in `STATE.json.branches` + #586.

- Coda d'integrazione (una PR alla volta, aperte da Claude B): vedi `STATE.json.integration_queue`.
- Non pushare su `claude/pickfair-core-audit-slu9su` (#609), `claude/d-f16-funds-freshness`,
  `claude/d-g7-r06-settled-ambiguous` senza coordinamento con Claude B.
- Commit locali NON pubblicati: `STATE.json.local_unpublished` (non durable).
- Nessun LIVE, nessun ordine reale, nessuna modifica VPS.

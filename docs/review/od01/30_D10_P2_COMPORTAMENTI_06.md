# D10 parte 2 — 80 comportamenti (6/6)

> Revisione indipendente BF2/D03 (OD-01, #426). Istruzioni: `00_ISTRUZIONI_REVISORE.md`. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO + motivo.

- [ ] ✅ **D10-BF13** — Bet-to-payout/profit (`betTargetType`) · **non** abilitato · abilitato UK/Int. · CONFIRMED ⇒ non usare
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF14** — Min Bet Payout · **non** abilitato · abilitato · CONFIRMED (BF-16 storico, superato per l'account dall'owner solo sul minimo)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF15** — Commissione · **4,5% fissa** sulle vincite nette per mercato, nessuno sconto, nessuna commissione su perdita netta; arrotondamento a 2 decimali, **0,005 per eccesso** · MBR variabile + sconti · CONFIRMED (sito/help) ⇒ `BETFAIR_ITALY_COMMISSION_PCT = 4.5` (`trading_config.py:21`) coerente
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF16** — Valuta / wallet · EUR (Currency Parameters EUR); wallet unico (la doc non indica wallet AU per IT) · multi · CONFIRMED per EUR; il wallet si verifica con getAccountDetails
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF17** — Errori regolatore · REJECTED_BY_REGULATOR, REGULATOR_IS_NOT_AVAILABLE ⇒ fail-closed (il secondo è un esito ambiguo se arriva dopo l'invio?) · — · CONFIRMED codici; semantica di esecuzione UNRESOLVED (D09-F2)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF18** — In-play, bet delay, stream, settlement · nessuna differenza IT documentata · — · NOT DOCUMENTED ⇒ si applicano le regole globali (C/E)
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF19** — Limiti API specifici IT (oltre 50 istruzioni) · nessuno documentato · Market Data Request Limits · NOT DOCUMENTED
  - [ ] ❌ RESPINGI · NOTA: 

- [ ] ✅ **D10-BF20** — Requisiti commerciali/SaaS · — · — · N/A (fuori scope per mandato)
  - [ ] ❌ RESPINGI · NOTA: 


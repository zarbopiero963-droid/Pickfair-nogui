# Conservazione PR #505 prima della sequenza #512 -> #505 (PF-PR-SEQUENCE-RECOVERY-01, owner 19/10/2026)

- PR: #505 "[TASK: issue461-pr28d-absolute-caps] PR28-d absolute Roserpina caps and fail-closed side"
- Branch originale: feat/issue461-pr28d-absolute-caps -> 618884e6c0aa82342bb98ad7c81a4cf8963f1fa4 (NON cancellare)
- Branch di conservazione: archive/pf-505-pre-512 -> 618884e6c0aa82342bb98ad7c81a4cf8963f1fa4 (verificato con git ls-remote)
- Base: main 028cee9074c3ab6c2a8014449a1897ae8faebc93; merge-base #505/main = 028cee9; merge-base #505/#512 = 028cee9
- 9 commit, 22 file, +1299/-127
- Patch isolata (NON nella PR, preparata in worktree wt_cap505 / branch lots/pf-cap-505-reconciliation):
  archive/pf-505-cap-reconciliation-prep -> 022ee9d619f04de65ea38166705dc499adbbdcac (6 commit sopra 618884e: PF-CAP-01..04, 10, 10b, 11);
  copia durevole anche in ../cap_505_reconciliation_prep/ (patches/*.patch, review indipendenti 1-3, RED/GREEN).
  Verifica: git am delle 6 patch su 618884e => albero aaa60bc78d1ef656a737e60ff47f2e159c5d4a27 == albero di 022ee9d.
- Contenuto di questo archivio: pr.json, commits.json, files.json, reviews.json (33), review_comments.json (49),
  issue_comments.json (30; anche issue_comments_mcp.json), check_runs.json, pr505_full.diff, pr505_patches/ (9 patch).
- Thread di review aperti su 618884e (5, NON risolti, debito da integrare alla ripresa):
  r4230527614 (P1, side non valido in auto-trade => ValueError), r4230677535 (P1, drawdown assoluto senza PnL non realizzato:
  decisione owner), r4230677545 (P1, cashout normalizzati non instradati: preesistente/Telegram sospeso),
  r4231703506 (P1, esposizione rilasciata su QUICK_BET_SUCCESS/AMBIGUOUS), r4231703513 (P1, drawdown assoluto non ricontrollato
  dopo best price).
- Decisioni owner da integrare alla ripresa: 18/10 nessun cap monetario fisso arbitrario attivo di default (cap opzionali
  configurabili e disattivabili; PF-CAP-01..12, #426 c.6250405396); protezioni finanziarie obbligatorie, LAY liability, fondi,
  riserve atomiche invariate.
- Scansione segreti dell'archivio: nessun pattern trovato.
- Ripristino: git fetch origin archive/pf-505-pre-512 && git checkout -B <nuovo-branch> origin/archive/pf-505-pre-512;
  patch correttiva: git cherry-pick 618884e..origin/archive/pf-505-cap-reconciliation-prep (oppure git am ../cap_505_reconciliation_prep/patches/*.patch);
  poi rebase/merge su main aggiornato dopo #512, riapertura #505 o PR sostitutiva con riferimenti bidirezionali.

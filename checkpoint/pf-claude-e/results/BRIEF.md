# PF-ISSUE-CLEANUP-01 — brief per gli auditor (Fase 1, SOLA LETTURA)

Repo: zarbopiero963-droid/Pickfair-nogui. Clone locale su main @ a09d8d6116aa234e838186f807f62470873def50
(= origin/main, 08/11/2026) in /home/user/Pickfair-nogui. Nessuna PR aperta al momento (#609 e #610 mergiate).

DIVIETI ASSOLUTI: non commentare, non chiudere, non etichettare, non modificare nessuna issue/PR; non
modificare file del repo; non fare push. Solo lettura. Scrivi solo nella cartella audit/ dello scratchpad.

Dati locali (scratchpad = /tmp/claude-0/-home-user-Pickfair-nogui/252a0a92-dbb8-5bbe-b0a0-df69d1356e55/scratchpad):
- open_issues.json: le 82 issue aperte con corpo completo (campo body), labels, n. commenti.
- closed_issues.txt: 27 issue chiuse. prs.txt: tutte le PR (#num, stato, MERGED data, titolo).
- Commenti: usa il tool MCP mcp__github__issue_read (method get_comments, perPage<=30, pagina per pagina;
  owner zarbopiero963-droid, repo Pickfair-nogui). Per issue con centinaia di commenti leggi le ultime pagine
  e cerca i riferimenti pertinenti, non serve tutto.
- Ricerca: `gh api -X GET search/issues -f q='repo:zarbopiero963-droid/Pickfair-nogui <termini>'` e
  `search/commits`. Non usare gh per altro.
- Codice: grep/git log nel clone (git log --oneline --grep, git log -S).

Registri di riferimento (NON da chiudere): #586 (registro di coordinamento, indice in c.6497837005),
#588 (ricerca adapter), #461 (roadmap, 60 schede), #489 (promemoria: issue superate/rinviate/requisiti
validi rispetto a #461 — LEGGI IL SUO CORPO in open_issues.json, è la fonte principale sulle supersessioni),
#426 (decisioni owner), #453 (piano operativo), #351 (collaudo reale), #491 (MCP), #506 (master Gate A/B),
#582 (collaudo Betfair A–D), #562 (CI VPS).

Classificazione per OGNI issue assegnata (una sola):
- KEEP: lavoro realmente ancora da fare / registro vivo / requisito attivo.
- CLOSE_VERIFIED: risolta con prova su MAIN (commit/PR mergiata + file:riga o test su main). Un fix su un
  branch non integrato o in una PR non mergiata NON basta.
- DUPLICATE: stessa cosa di un'altra issue (indicare quale, che resta aperta e copre tutto il contenuto).
- SUPERSEDED: sostituita da un'altra issue/registro/decisione owner che ne contiene i requisiti ancora validi
  (indicare dove sono ora i requisiti; se un requisito non è riportato altrove, NON è SUPERSEDED).
- CONSOLIDATE: va accorpata in un'altra issue (requisiti da trasferire esplicitamente prima di chiudere).
- NEEDS_REVIEW: ambigua, contiene requisiti forse attivi, serve decisione owner.

Regole: mai CLOSE/DUPLICATE/SUPERSEDED per issue con P0/P1 aperti, certificazioni incomplete, Gate A,
sicurezza finanziaria o attività ancora necessarie. Nel dubbio: KEEP o NEEDS_REVIEW. Le issue-report/dump
(audit esterni, report finali, note personali dell'owner) possono essere SUPERSEDED solo se i loro rilievi
sono tracciati altrove (es. #489, #461, #586) — indica dove; altrimenti NEEDS_REVIEW con l'elenco dei
rilievi non tracciati.

OUTPUT: scrivi audit/<gruppo>.json = lista di oggetti:
{"number":N,"title":"...","class":"KEEP|CLOSE_VERIFIED|DUPLICATE|SUPERSEDED|CONSOLIDATE|NEEDS_REVIEW",
 "priority":"P0|P1|P2|P3|n/a","reason":"motivo in italiano, 1-3 frasi","replacement":"#N o null",
 "evidence":["PR #x merged <data>", "commit <sha> ", "file:riga su main", "#489 sezione ..."],
 "deps":["#N", "PR #N"], "open_requirements":"requisiti ancora attivi non coperti altrove (o vuoto)",
 "owner_question":"domanda specifica all'owner se NEEDS_REVIEW (o null)"}
Poi rispondi con un riepilogo compatto (una riga per issue: numero, classe, motivo breve).

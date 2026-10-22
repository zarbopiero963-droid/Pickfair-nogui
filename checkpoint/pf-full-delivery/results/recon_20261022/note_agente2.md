# note_agente2.md — raccolta read-only per PF-POST-PR29C-GLOBAL-RECONCILIATION-01

Data lettura: 2026-10-22 (UTC). Nessun commento, nessuna modifica su GitHub o nel repo locale.
Strumenti: solo mcp__github__* (issue_read get/get_comments, list_pull_requests, pull_request_read get, list_commits).

## Fonti lette

| Fonte | Corpo | Commenti | Note |
|---|---|---|---|
| #453 | letto integralmente (corpo 63k char) | 101/101 (2 pagine: 100 + 1) | ultimo c.6320059392 (21/10) |
| #506 | letto integralmente | 447/447 (5 pagine) | ultimo c.6320057119 (21/10); ~200 commenti sono lotti PF-PRE505-P01-M12/M13 e archivi base64 L03, letti solo per ID/intestazione |
| #351 | letto integralmente | 84/84 | 33 commenti sono parti base64 dell'archivio PF-DOC-4 |
| PR | list_pull_requests pagina 1 (50) copre fino a #451 (27/08); 22 PR mergiate >= 01/10 verificate con `get` | | merge SHA da list_commits main since 2026-10-01 (22 commit, 1:1) |

## Rischio «cancellazione manuale di .runtime.lock» / esclusivita' runtime

- La stringa `runtime.lock` **non compare** in nessun corpo/commento di #453, #506, #351.
- Il lock di istanza `<db_path>.runtime.lock` (`core/runtime_controller.py::_acquire_runtime_instance_lock`) e' arrivato su main con **#535 PR29-c, merge 152613b (22/10 07:37)**. Il limite «rimozione manuale del file di lock mentre il runtime e' attivo permette un secondo lock» e' dichiarato **solo** in `docs/roserpina_bankroll_equity.md` (riga ~144), non come finding tracciato.
- Finding/prerequisiti piu' vicini (esclusivita'/multiprocesso), nessuno FIXED:
  - #453 corpo, sottogate S1 **B7**: «due processi/restart concorrenti sul medesimo DB -> un solo owner valido/fencing persistente o esclusione single-instance provata; crash rilascia ownership» — checkbox aperta.
  - #506 c.6123962107 **L08-F1** (PF-C32-01, P1 cand.): «nessun lock fra processi e nessuna garanzia di istanza unica; INSTANCE_LOCK_CONFLICT dichiarato mai emesso» + L08-F2/F3 fencing; decisione owner aperta **OWNER-MULTIPROCESS-03** («il deployment ammette piu' processi sullo stesso DB o account?»).
  - #506 c.6315251036: coordinamento durevole fra processi (OWNER-MULTIPROCESS-03, T-F03/T-F06) rinviato a **29-g1/29-g2**.
  - #506 c.6125417791: TS-D14 (2 processi DB condiviso), TS-D15 (perdita lease) NOT_RUN, dest. PR32.
  - #453 c.6162830558: T-F03/T-F06 «anche a due processi sullo stesso DB» come test di chiusura FIN-FUNDS.
  - #351 corpo: T21 («due istanze concorrenti sul DB test… fencing/lock efficaci»), T29 («restore non avvia due trader sullo stesso account»), gate «concurrency» NOT_RUN; c.6085517762 E2E-11 «due processi concorrenti» NOT VERIFIED.
- Conclusione: il rischio specifico (lock file rimosso a mano -> seconda istanza) **non e' coperto** da un finding in #453 ne' da un prerequisito in #351; l'esclusivita' in generale e' tracciata solo come L08-F1/B7/OWNER-MULTIPROCESS-03, ancora OPEN, e nessun commento registra il lock di #535 come evidenza parziale.

## Incertezze

1. `list_pull_requests` restituisce `merged:false` anche per PR con `merged_at`: ho usato `merged_at` + `pull_request_read get` (merged=true su tutte e 22). Il campo `merge_commit_sha` non e' esposto dal tool: ricavato dai commit squash su main (`(#NNN)` nel titolo), corrispondenza 1:1 con 22 commit dal 01/10.
2. Gli stati in finding_453.json sono la mia sintesi dell'ultimo commento che cita il finding; dove una PR mergiata chiude un finding senza che #453/#506 lo registrino (es. PF-C29-01.11/.18 e L08-F1 dopo #535), l'ho scritto esplicitamente come «non ancora registrato». Non esiste in #453 un registro unico con etichette FIXED_WITH_EVIDENCE / DEFERRED_BY_POLICY / KNOWN_LIMITATION: le etichette usate sono eterogenee (FAIL UNIT, OPEN/NON CONFORME, CHIUSO, IMPLEMENTED_WITH_NOTE, PARZIALE, STATIC/NOT VERIFIED).
3. #453 corpo: conteggio 35 righe (4/23/8) e' del 13/09 e non e' stato aggiornato; i commenti successivi non rifanno il conteggio.
4. #506: gli item A01-A13 compaiono due volte nel corpo (FASE 1 e sezione G1 PF-506-ATOMIC) con formulazioni equivalenti; nessuna checkbox spuntata; i commenti M14-A01..A13 (c.6111761822…6111763587) sono **batch di atomizzazione**, non lo stato degli item A01-A13 (omonimia).
5. Il mandato chiede ID «F001..F0xx, OBS-x, PF-C29-xx, G7»: in #453 convivono tre numerazioni (F1-F15 FOLLOW-UP sett/ott; F01-F11 PF-DOC v1; PF-ALL-F001-F033). Le ho tenute separate col campo `famiglia`.
6. PF-ALL-F029: citato solo nel corpo di #512 come consolidato; nessun commento #453 ne descrive il contenuto.
7. L'ultimo stato del Gate B per #531/#533/#534/#535 e' nei commenti PR e in #461 (non letto: fuori mandato); #506 registra solo c.6304344711 (#531), c.6315251036 (#533), c.6320057119 (#534). Nessun commento post-merge #535 in #453/#506/#351 alla data di lettura.
8. Nella cartella `recon/` erano gia' presenti `dump.py` e `raw/` non creati da me: non toccati.

## Comment id delle fonti principali

- #453: 5877897525 (F1-F5), 5881918056 (F6), 5893389383 (F7-F9), 5893543850 (F10-F11), 5893922854 (F12-F13), 5899903839 (F14-F15), 6085517147 (F01-F11), 6087858130 (D09), 6162762000 (AUD-01..05, FIN-*), 6162830558 (FIN-FUNDS oracolo), 6163146564 (PF-ALL-F001..F015), 6163919308 (F016-F022), 6167128500 (F023-F025), 6170832671 (F026), 6174471178 (OBS-1/2), 6187670007 (F027-F033), 6250410915/6250528652 (PF-CAP), 6304345652 (PF-CAP-09/19 chiusi), 6315251624, 6320059392 (famiglia F02 dopo #534).
- #506: 6109174764 (PF-DOC-4 NON PASS), 6109349651 (B1-B5), 6111763865 (M14 consolidato), 6112705592 (decisioni BLOCKED_SPEC), 6123640661 (L05/PF-C29-01), 6123962107 (L07/L08, PF-C31/C32), 6131986497 (Gate A prep closeout), 6162791592 (A10 input), 6225690392, 6270749054 (riconciliazione Gate A 19/10), 6272366442, 6275844253, 6278393693, 6284060186, 6304344711, 6315251036, 6320057119.
- #351: 6063073039 (matrice OS), 6067168898 (2 VPS), 6068311872, 6085517762 (tests), 6085542238, 6242905583 (PF-351-INTEGRATED-CERTIFICATION-01), 6250413342, 6272369752, 6275845616, 6278395720, 6284060752.

## File prodotti

- /tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/recon/finding_453.json
- /tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/recon/gate_a_506.json
- /tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/recon/cert_351.json
- /tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/recon/pr_merged.json
- supporto: recon/src/all_comments.json (tutti i commenti grezzi), recon/src/main_commits.json, recon/src/dump_{453,506,351}.md

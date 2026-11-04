# Control API v1 — sola lettura (PF-API-1, preparazione)

Stato: **PROVISIONAL**, sviluppata su branch con GO controllato della direzione tecnica (03/11/2026).
Non è cablata in `headless_main`, non è inclusa nel packaging (`pyproject.toml`, `pickfair.spec`) e non
modifica il comportamento del motore. Nessuna route di comando. Non sostituisce né tocca
`betfair_market_api.py` (API legacy di sincronizzazione catalogo).

Riferimenti: `docs/mcp_operational_contract.md` (sezione «Control API e adapter»), pickfair-mcp- #1
(25 route approvate), roadmap MCP + Control API (pickfair-mcp-, `docs/roadmap/`), schede PF-MCP-004 e PF-MCP-008.

## Route servite

| Metodo | Route | Sorgente nel core | Note |
|---|---|---|---|
| GET | `/v1/meta` | `control_api/contract.py` | versione, capability, git SHA del checkout, schema hash condiviso con il MCP |
| GET | `/v1/status` | `control_api/views.py::status_view` | stato runtime, modalità SIM/LIVE, readiness e blocker, STOP/emergenza |
| GET | `/v1/reconciliation/status` | `control_api/views.py::reconciliation_view` | esito della riconciliazione d'avvio, ambiguità, ghost, registro hedge, recovery |

Ogni altro metodo: `405 METHOD_NOT_ALLOWED`. Ogni altro percorso (incluse query string, slash finale,
`//` iniziale): `404 NOT_FOUND`.

## Sicurezza

- Bind solo su `127.0.0.1` letterale con porta esplicita (`HOST_NOT_LOOPBACK_LITERAL`, `PORT_NOT_EXPLICIT`).
- `Authorization: Bearer <token>` obbligatorio, confronto a tempo costante; token ≥ 32 caratteri.
  Il token è generato per avvio e consegnato al MCP con `issue_token_file` (file nuovo, `0600`,
  mai sovrascritto, mai symlink). Modello PROVISIONAL: decisione D-11 aperta.
- `Host: 127.0.0.1:<porta>` obbligatorio (difesa da DNS rebinding); GET con corpo rifiutato.
- Corpi di errore fissi `{"error": {"code": ...}}`; nessun log per richiesta, token e header mai loggati.
- Contatori per esito (`request_counts()`), senza dati della richiesta.

## Contratto delle letture

- `source` vale sempre `"core"`; il MCP rifiuta una risposta la cui sorgente non coincide con l'adapter.
- Un dato che il runtime non possiede è `null` **ed** è elencato in `unavailable` (mai un default).
  Esempio: prima di `start()` la riconciliazione d'avvio è `NOT_RUN` e `recovery_state` è indisponibile.
- I motivi testuali del core diventano token `MAIUSCOLI` (prefisso prima di `:`); un testo libero diventa
  `UNSTRUCTURED`. Il dettaglio libero non esce mai.
- `execution_mode`: `SIMULATION` del core → `SIM`; `LIVE` → `LIVE`; qualsiasi altro valore → indisponibile.
- `readiness.mcp_aware` è `false`: la readiness MCP-aware (Telegram non blocker quando l'ingresso è MCP)
  appartiene a PF-API-0 e non esiste ancora.

## Assenza di effetti collaterali

Le viste **non** chiamano `RuntimeController.get_status()`: quel metodo legge i fondi Betfair, aggiorna
lo stato della cassa e fa girare il monitor della perdita giornaliera. Le viste leggono attributi
d'istanza con controllo di tipo e chiamano solo `evaluate_live_readiness()` (lettura di config e flag),
`_is_kill_switch_active()`, `ambiguity_snapshot()` e `ghost_evidence_snapshot()` (copie difensive).
Provato da `tests/control_api/test_views_real_runtime.py::test_views_have_no_side_effects_on_the_real_runtime`:
DB, stato del runtime e bus invariati dopo 50 letture, nessun metodo con effetti chiamato.

Limite noto: i campi non sono letti in un'unica sezione critica del runtime (prenderne il lock
rallenterebbe il motore); `observed_at` dichiara l'istante della lettura.

## Prove

| Test | Livello | Cosa prova |
|---|---|---|
| `tests/control_api/test_views_real_runtime.py` | core reale in-process | proiezioni, indisponibilità, codici, nessun effetto, letture concorrenti |
| `tests/control_api/test_server.py` | server reale su loopback | auth, Host, metodi, percorsi, configurazione, file del token, concorrenza |
| `tests/control_api/test_mcp_chain_crossrepo.py` | cross-repo | client MCP → server pickfair-mcp → Control API → RuntimeController; richiede `pickfair_mcp` installato, altrimenti SKIPPED |
| `guardrails/mutation_catalog.json` area `control_api` | mutation | 18 guardie |

Nella catena cross-repo il trasporto HTTP lato MCP è un **harness di test**: l'adapter HTTP di
pickfair-mcp (MCP-01d) non esiste ancora. La prova non certifica MCP-01d né l'esecuzione di ordini.

## Fuori perimetro (non implementato)

Comandi (PF-API commands), readiness MCP-aware e command contract neutro (PF-API-0), cablaggio in
`headless_main`, packaging, le altre 11 letture approvate, hosting remoto o endpoint pubblici.

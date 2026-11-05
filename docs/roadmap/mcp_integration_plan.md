# Integrazione delle patch MCP-01d…01n in Pickfair-nogui (PF-MCP-INTEGRATION-NEXT-01)

Stato al 05/11/2026. Sorgente durevole: branch `handoff/claude-c/mcp-core-20261105` @ `f22d6bfb`. Le 12 patch sono `DURABLE`: checkpoint di Claude C seq 8 `d6d1cda0`, prova 12/12, bootstrap READY. Matrice di Claude C: `pickfair-mcp-` `docs/handoff/INTEGRATION_MATRIX_MCP01d-n.md` @ `fcde366`.

Questo documento è un piano. Nessuna patch è integrata finché la sua PR non supera CI, review e gate. Il branch di handoff resta finché ogni patch non è `INTEGRATED_VERIFIED`.

## 1. Struttura verificata (clone GitHub pulito)

| Gruppo | Patch | Base | Codice toccato (esclusi test e registri) | Dipende da |
|---|---|---|---|---|
| A | 01d | `ada40765` (= main #607) | `control_api/` (nuovo), `headless_main.py` (avvio opzionale), `scripts/ci_changed_modules.py`, `docs/control_api.md` | main |
| A | 01e, 01f, 01g, 01h, 01j, 01k, 01l | catena lineare sopra 01d | `control_api/bridge.py`, `control_api/views.py`, doc | patch precedente |
| A | sec01 | 01l | `control_api/bridge.py`, doc | 01l |
| A | 13 `f16-compat` | `f22d6bfb` | solo test del ponte | F-16 (#610) |
| B | 01i-r2 | `772ab581` (F-04, non su main) | `control_api/` | **F-04** |
| B | 01m | `a0a60137` (F-03 + F-04, fusione locale) | `control_api/`, `cashout_router.py` (−9: duplicato della fusione locale) | **F-02, F-03, F-04, F-18** |
| B | 01n | `2ea5187a` (+ `claude/d-market-book-age`) | `control_api/` | **market-book-age** |

Gruppo A non tocca il motore finanziario: niente `core/`, ordini, money management, riconciliazione o DB. Le sole superfici condivise sono `headless_main.py` (la Control API è opzionale e spenta senza flag espliciti) e `scripts/ci_changed_modules.py`.

## 2. Prova di compatibilità del gruppo A con main + F-16

Merge di prova di `4525b28c` (sec01) sopra l'head di F-16 `7fae18d4`, fatto in locale, mai pubblicato:
- conflitti **solo** nei registri (`.guardrails/allowed_scope.json`, `guardrails/mutation_catalog.json`, `CHANGELOG.md`), risolti per unione; nessun conflitto nel codice;
- `tests/control_api`: 188 passed, 14 failed, 6 skipped. Sulla catena originale, senza main né F-16: 191 passed, 11 failed;
- **11 fallimenti identici nei due casi:** il package `pickfair_control_api` pinnato (1.9.0, schema hash `5a5c912c…`) non è installato. Non dipendono dall'integrazione;
- **3 fallimenti in più dovuti a F-16** (`test_funds_limit_on_main_is_the_reservation_ttl`, `test_funds_limit_not_declared_when_f16_threshold_exists_without_accessor`, `test_funds_freshness_follows_the_core_gate`): i test codificavano il main precedente (TTL 120 s). La patch 13 li allinea, dato riportato da Claude C (215 passed sullo stack); va riverificato dopo la pubblicazione della 13.

## 3. Blocco reale prima della PR: il package pinnato in CI

Senza `pickfair_control_api`, 11 test del ponte falliscono. Non usano `importorskip`, quindi la CI sarebbe rossa. Le strade sono tre:
1. **installare il package pinnato nella CI**: tocca workflow e/o manifest di dipendenze, cioè file a merge owner (supply chain, `MANUAL_AUTHORITY_PATTERNS`), e serve una sorgente durevole del package;
2. far saltare in modo esplicito i test che richiedono il package quando manca. È **sconsigliato**: un test di contratto saltato in CI non prova niente;
3. una suite dedicata del catalogo #606 (G8 `control-api`) che installa il package nella sandbox.

Decisione owner richiesta prima della PR del gruppo A: la 1, la 3, o entrambe.

## 4. Ordine di integrazione proposto (una PR alla volta)

1. **#610 F-16**, in corso.
2. **F-R06**, poi **pfckpt hardening**: già in coda, indipendenti da MCP.
3. **PR MCP-A**: 01d…sec01 + patch 13, merge preservando gli SHA di handoff. Contenuto:
   - voce G8 `control-api` nel catalogo e il suo id in `campaigns.all`;
   - registrazione della task key;
   - test `tests/control_api` + regressione completa;
   - prova che la Control API resta spenta senza flag e che `headless_main` parte senza il package.

   Merge owner se la soluzione del §3 tocca workflow o manifest.
4. **F-02 → F-04 → F-03 → F-18** (corsia finanziaria, con D-C4-01 e #612), poi **PR MCP-B1** (01i-r2 + 01m). In 01m si scarta l'hunk duplicato di `cashout_router.py` se main ha già la versione di F-03.
5. **market-book-age**, poi **PR MCP-B2** (01n).

Dopo ogni merge: `pfckpt integrate` / record `INTEGRATED_VERIFIED`, tree confrontato con il commit di integrazione specifico; il branch di handoff si cancella solo dopo l'ultima.

## 5. Rischi

- **Gruppo B su basi locali mai pubblicate** (fusioni locali di Claude C): si reintegra solo sopra i commit reali di F-02/F-03/F-04/market-book-age dopo il loro merge, mai sulla fusione locale.
- **Pin del package:** una versione diversa ⇒ `CONTRACT_PIN_MISMATCH`, la Control API non parte (fail-closed, corretto). Ogni aggiornamento del package richiede un cambio di pin, in una PR.
- **Nessun ordine dalla Control API:** va riverificato nella PR MCP-A (`test_side_effects`) con il package installato.

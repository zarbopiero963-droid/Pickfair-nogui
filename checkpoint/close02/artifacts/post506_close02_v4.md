## PF-OWNER-CLOSE-ALL-02 — aggiornamento v4 (aggiunte owner t74u): gap candidati, ENV, lotto R-L1

Spesa AI: **0 €** (residuo 16,7883 €). Nessuna modifica a #505, nessun push, merge, deploy o LIVE, nessun ordine Betfair. Il tree di Pickfair-nogui è pulito. **Gate A resta NON PASS.**

### 1. Classificazione dei 66 gap candidati
Registro: `close02/findings_register.json`, sha `26d1fb45…`.

| Classe | N. | Dettaglio |
|---|---|---|
| REALE | 9 | 5 BF + 4 radici di caricamento RSR |
| DUPLICATO | 23 | — |
| GIÀ COPERTO | 11 | — |
| NON DIMOSTRATO | 1 | — |
| BLOCCATO (dominio PENDING_OWNER) | 22 | — |

**REALI, tutti riprodotti con test RED a 028cee9** (`red_gaps.txt`, 28 failed / 3 passed):
- **CL02-BF-001**: `betfair_market_api` list_*. Un result di tipo inatteso diventa `[]`. Radice di 002-004.
- **CL02-BF-005**: `get_market_book`. Un tipo inatteso diventa `None`, cioè "nessun book".
- **CL02-BF-006**: `get_current_orders`. Una risposta senza `currentOrders` diventa `[]`, cioè "nessun ordine".
- **CL02-BF-008**: `cancel_orders`. Una risposta senza status dà `ok=True` con 0 annullati.
- **CL02-BF-010**: `get_account_funds`. Un saldo assente diventa **0.00**.
- **Radici di caricamento RSR** (float/int/enum/bool):
  - un valore presente ma corrotto (testo, vuoto, NaN, inf) produce un default silenzioso o propaga NaN;
  - `_b()` porta un valore corrotto a False, quindi fail-open sulle guardie anti-duplicazione/liquidità.

**GIÀ COPERTI:**
- **CL02-BF-009** (replace senza ok): `order_manager` verifica `instructionReports[0].status` ⇒ REPLACE_REJECTED.
- **Chiavi hard-stop** corrotte o a -1: NaN o limite negativo producono una negazione (fail-closed).

**NON DIMOSTRATO — CL02-BF-007** (SIM SUCCESS su mercato sconosciuto):
- il SIM non ha un'anagrafica dei mercati, quindi "sconosciuto" ≠ "invalido";
- il fix tentato rompeva oltre 30 test del contratto F8 (#453), per cui è stato ritirato;
- resta un test xfail strict; serve una decisione.

### 2. Proposta v4 (worktree isolato da 028cee9, NON applicata)
Patch `patches/close02_findings_proposal_v4.patch`, sha `9398d1d4…`.

- **Betfair:**
  - un tipo inatteso solleva `UNEXPECTED_RESULT_TYPE`;
  - un vuoto legittimo resta vuoto (`_post_jsonrpc` converte `[]` in `{}`);
  - un saldo assente o non finito solleva `INVALID_ACCOUNT_FUNDS`;
  - una cancel con risposta senza status dà `ok=False`.
- **Caricamento:**
  - "assente" e "corrotto" sono distinti;
  - le chiavi corrotte vanno in `RoserpinaConfig.corrupt_keys`;
  - un booleano corrotto prende il default, non False;
  - il runtime rifiuta nuovo rischio con `config_corrotta:<chiavi>`.
- **Test esistente aggiornato:** `test_get_current_orders_empty_result_returns_empty_list` ora usa `currentOrders: []`. In più c'è un test per la risposta malformata.

### 3. CL02-ENV-001 (fallback tkinter)
- **Problema:** senza customtkinter, `CTkComboBox(command=)` cade su `ttk.Combobox` e dà TclError `-command` (mini_gui.py:182-184, chiamata :919-925).
- **Lacune successive:** opzioni solo-CTk (`progress_color`) e `Tabview.set`.
- **Dipendenza:** customtkinter è assente dal venv di test (sta solo in requirements-build-linux) e DISPLAY è impostato.
- **Repro:** `tests/close02/test_close02_tk_fallback.py`, RED a 028cee9.
- **Proposta:**
  - `command` diventa `<<ComboboxSelected>>`;
  - le opzioni CTk vengono scartate;
  - `pr04_quater[True]` viene saltato con BLOCKED_ENV quando customtkinter manca.

### 4. Test
- RED gap: 28 failed / 3 passed a 028cee9.
- GREEN: `tests/close02` 81 passed, 1 skipped, 1 xfailed.
- **Suite completa con la proposta v4:** 7318 passed, **0 failed**, 15 skipped, 2 xfailed (riferimento: ≥7275). Log `patched_full_v8.txt`, sha `c021e953…`.
- test_preflight: 37/37 PASS.

### 5. Lotto Betfair R-L1 (22 record): pronto, NON chiamato
Il controllo semantico (`lot_check_L1.py`) dà **22/22 OK** su:
- fonti complete (verbatim + sha);
- oracoli REQUIREMENT;
- fixture concrete (payload);
- controlli negativi;
- preflight C1–C14;
- comportamento osservato solo in impl_attuale.

**Preventivo finale:** ~49k token, **~0,21 €**, tetto 0,30 €. Serve l'autorizzazione owner.

### 6. Verifica di provenienza (Opzione B)
- Dei 119 record (L1+L2), **22 avevano oracoli OBSERVED_ONLY** (tutto L1 v2).
- 20 record L2 avevano oracoli VALID-positivi UNSOURCED.
- 97 record L2 avevano il campo osservato fuori da impl_attuale (problema strutturale).
- Dopo la ricostruzione: OBSERVED_ONLY = 0.

### SHA (prefisso 12)
| File | SHA |
|---|---|
| record_specific_L1 | e7e75c825c19 |
| record_specific_L2 | 692fe873050d |
| findings_register | 26d1fb456907 |
| lot_check_L1 | 58fd05743dd0 |
| review_digest_L1 | 97fb4985670f |
| review_request | e054417d39a8 |
| semantic_preflight | 291f57958922 |
| test_preflight | 4736632bcd53 |

Elenco completo: `SHA256SUMS_v4.txt`.

**Nessun finding dichiarato risolto. Accettati/chiusi: 0.**

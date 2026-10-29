# Osservabilità Buildkite in sola lettura (#562)

`scripts/ci/bk_observe.py` legge build, step, log e artifact della pipeline trusted `pf-dedicated-ci` tramite l'API REST di Buildkite.
- Non dipende da GitHub Actions né da Grok.
- Non scrive niente: fa solo richieste `GET`, con host e pipeline fissi.

## Credenziale
- **Ambiente cloud di Claude.** Il token lo aggiunge il proxy di rete dell'ambiente (Network secret). Non si trova in nessuna variabile d'ambiente, nel repository o nei log.
- **Altrove.** Lo script legge `BUILDKITE_API_TOKEN` e non lo stampa mai.
- **Scope ammessi:** `read_pipelines`, `read_builds`, `read_build_logs`, `read_artifacts`. Il comando `whoami` esce con codice 2 se il token ha uno scope `write_*` o `delete_*`.
- **Avvio e retry dei job.** Sono un'altra cosa: richiedono un permesso separato e il launcher a catalogo, mai questo token.

## Comandi
| Comando | Uso |
|---|---|
| `whoami` | scope e scadenza del token, senza il valore |
| `find --sha <40 hex>` / `find --intent <pf_intent>` | build per SHA esatto o per intento del dispatcher |
| `report --build N --sha … --intent … --expect-steps 49\|52 --budget 7200` | tabella degli step e `VERDETTO_BUILD` |
| `log --build N --step <step_key> [--tail 200]` | log testuale di uno step |
| `artifacts --build N` | artifact con `sha1sum` |

## Verdetto di `report` (codice di uscita)
- **PASS (0)** solo se valgono tutte queste condizioni:
  - la build è `passed`;
  - lo SHA e il `pf_intent` sono quelli attesi;
  - è l'unica build per quello SHA e per quell'intento;
  - il numero di step, escluso il launcher, è quello atteso;
  - tutti gli step sono `passed`;
  - la durata non supera il budget.
- **FAIL (1)** se c'è uno SHA o un intento diverso, una build duplicata, uno step fallito o non eseguito (`canceled`, `skipped`, ecc.), un numero di step sbagliato, un budget superato o una build `failed`/`canceled`. Uno step fallito rende FAIL anche una build ancora in corso.
- **PENDING (3)** se la build o uno step sono ancora attivi.
- **UNKNOWN (4)** se una risposta è assente o illeggibile, oppure se mancano i job o i tempi.

Un PENDING o un UNKNOWN non vale mai come PASS. Il verdetto riguarda solo la build indicata e non certifica gli head successivi.

## Esempio
```bash
python3 scripts/ci/bk_observe.py report --build 31 \
  --sha 185078e42f30c9e9f29ad2858b73fd27fb6ff70a --intent p585-t3-185078e42f30 --expect-steps 49
```

Test: `tests/scripts/test_bk_observe.py`, eseguiti su risposte API simulate e senza rete.

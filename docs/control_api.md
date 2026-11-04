# Control API v1 — ponte di sola lettura (proposta per Claude B)

Stato: **PROVISIONAL**, consegnato da Claude C come handoff (pickfair-mcp-, `docs/handoff/`).
Il server, l'autenticazione e il contratto sono nel package `pickfair_control_api` del repository
pickfair-mcp- (solo libreria standard). In questo repository stanno soltanto il ponte che legge il
`RuntimeController` e l'aggancio opzionale a `headless_main`.

## Componenti

| File | Ruolo |
|---|---|
| `control_api/views.py` | letture pure del runtime (mai `get_status()`, che legge i fondi Betfair, aggiorna la cassa e fa girare il monitor della perdita giornaliera) |
| `control_api/bridge.py` | collega le viste al server `pickfair_control_api` |
| `control_api/launcher.py` | avvio opzionale; **disattivato per impostazione predefinita** |
| `headless_main.py` | 16 righe: avvio dopo un `runtime.start()` riuscito, arresto per primo nello `stop()` |

Route servite: `GET /v1/meta`, `GET /v1/status`, `GET /v1/reconciliation/status`. Nessun comando.

## Attivazione

Solo con entrambi i flag espliciti:

```bash
mkdir -m 700 -p ~/.pickfair
python headless_main.py --sim --control-api-port 8765 --control-api-token-file ~/.pickfair/control_api.token
```

Nessuna variabile d'ambiente e nessuna chiave di `config.json` attivano la Control API. Senza flag:
nessun import di `pickfair_control_api`, nessun socket, nessun file (provato su processo reale).

Prerequisiti (altrimenti la Control API NON parte e il motore continua, causa nel log
`CONTROL_API_NOT_STARTED: <codice>`):
- `pickfair_control_api` installato ESATTAMENTE alla versione e allo schema hash attesi da
  `control_api/bridge.py` (`PACKAGE_MISSING`, `CONTRACT_PIN_MISMATCH`); nessuna risoluzione di
  dipendenze all'avvio: il package è solo importato e verificato;
- porta 1–65535 libera (`PORT_INVALID`, `BIND_FAILED`);
- percorso del token assoluto, cartella privata dell'utente, non scrivibile da altri (`TOKEN_DIR_UNSAFE`);
- SHA del codice dimostrabile: `.git` del checkout o file `BUILD_SHA` scritto dal packaging (`GIT_SHA_UNRESOLVABLE`).

Ordine di avvio, senza servizi parziali: pin del package → token nuovo in memoria → bind del
listener → pubblicazione atomica del token (`0600`) → servizio. Pin errato: nessun socket e nessun
file; bind fallito: nessun file; pubblicazione fallita: listener chiuso. Il token di un'esecuzione
precedente non resta mai valido. Il MCP si collega con
`pickfair-mcp serve --adapter http --control-api-port 8765 --control-api-token-file ~/.pickfair/control_api.token`.

## Arresto

`HeadlessApp.stop()` ferma la Control API **prima** del runtime: nessuna lettura durante il teardown.
Il processo che termina chiude la porta (provato: connessione rifiutata dopo SIGTERM).

## Isolamento

- Bind solo su `127.0.0.1` letterale; nessuna risoluzione di nomi (anche il DNS inverso di
  `HTTPServer.server_bind` è disattivato); Host e token Bearer obbligatori.
- Letture senza effetti: provato su runtime avviato in SIM (DB, bus, saldi, chiamate a
  Betfair/broker, monitor di rischio, stato profondo invariati) e senza attesa sui lock di ammissione
  e dei tavoli. Le viste prendono solo per un istante il lock del registro delle ambiguità (copia).
- Ogni risposta è validata contro lo schema del contratto prima dell'invio
  (`CONTRACT_SELF_CHECK`).
- La Control API non può fermare il motore: ogni suo errore all'avvio è registrato e ignorato.

## Rollback

1. Togliere i flag `--control-api-*`: comportamento identico a prima (nessuna porta).
2. Rollback del codice: revert del commit del ponte; `headless_main.py` torna alla versione precedente.
3. Rimuovere il file del token (`~/.pickfair/control_api.token`); non contiene altri dati.

## Limiti noti

- Lettura non atomica fra campi (`consistency = PER_FIELD_NON_ATOMIC`), istante dichiarato in `observed_at`.
- `recovery_state` resta indisponibile finché il runtime non scrive per la prima volta lo stato di
  recovery: `_recovery_persist_failed` nasce solo lì (proposta: inizializzarlo nel costruttore).
- Token bearer locale: chi legge il file come lo stesso utente ottiene l'accesso in lettura.
- Packaging non incluso: `pyproject.toml` e `pickfair.spec` non distribuiscono `control_api/`.

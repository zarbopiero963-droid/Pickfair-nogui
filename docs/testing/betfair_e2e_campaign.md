# Campagna Betfair Exchange Italia — PF-BETFAIR-E2E-01

Obiettivo: separare ciò che Pickfair ha dimostrato contro le **API Betfair reali** da ciò che è dimostrato solo contro
**mock** o il **broker simulato**. Ogni prova dichiara la modalità:

| Modalità | Significato | Cosa NON dimostra |
|---|---|---|
| `MOCK` | risposte scritte a mano nei test | forma reale delle risposte, latenze, errori reali |
| `SIMULATION` | `SimulationBroker` (matching interno) | comportamento dell'exchange, regole Betfair |
| `RECORDED` | fixture catturate da una lettura reale, con provenienza e hash | comportamento dinamico |
| `REAL_READ_ONLY` | chiamate reali di sola lettura sul canale autorizzato | invio/cancellazione ordini |
| `REAL_ORDER` | ordini reali con denaro | **non autorizzato** (livello C) |

## Livello A — offline (in corso)

Test riproducibili con `MOCK`/`SIMULATION`; mappatura degli scenari su `PF-BETFAIR-E2E-MATRIX.md`. Nessuna rete.

## Livello B — API reali, sola lettura

**Stato: BLOCKED_ACCESS nelle sessioni di sviluppo** (nessuna credenziale né canale Betfair; gli host Betfair non sono
raggiungibili dal proxy della sessione). Il canale autorizzato è la lane `bf-readonly` della VPS (gestione Grok /
Claude B). Lo strumento è pronto e provato offline:

- `tools/bf_readonly/guard.py` — `ReadOnlyTransport`: sessione del `BetfairClient` di produzione che lascia passare solo
  login/keepAlive/logout identitysso e i metodi JSON-RPC di lettura (`ALLOWED_METHODS`). `placeOrders`,
  `cancelOrders`, `replaceOrders`, `updateOrders`, ogni metodo sconosciuto, URL fuori dagli endpoint noti, http, GET e
  altri verbi ⇒ `ReadOnlyViolation` **prima della rete**, registrata in `violations` anche se il client la inghiotte.
- `tools/bf_readonly/campaign.py` — `run_campaign`: getAccountFunds, listEvents, listMarketCatalogue, listMarketBook
  con prezzi (stato mercato/selezioni, ladder BACK/LAY), listCurrentOrders, listClearedOrders. Report JSON con
  `mode`, `commit_sha`, esito per passo, forma delle risposte; **senza** token, app key, username, importi o id in
  chiaro (id di mercato come hash). Verdetto FAIL se una lettura fallisce o la guardia ha bloccato qualcosa.
- Esecuzione (solo sul canale autorizzato, credenziali fornite dall'operatore in `PF_BF_USERNAME`, `PF_BF_APP_KEY`,
  `PF_BF_CERT_PEM`, `PF_BF_KEY_PEM`, `PF_BF_PASSWORD`; assenti ⇒ `BLOCKED_ACCESS`, exit 2):
  `python -c "from tools.bf_readonly.campaign import main; raise SystemExit(main(['--out','report.json']))"`.

Verifiche che il livello B deve chiudere (oggi UNVERIFIED):

1. **Endpoint Betting/Account per l'Exchange italiano.** Il client usa login `.it` e Betting/Account
   `api.betfair.com` (#351: «la pagina IT indica i .com; da verificare»). La campagna lo prova con una sessione `.it`.
2. Forma reale di `listMarketBook` con `EX_BEST_OFFERS` (campi letti da F030/F-23: `status`, `runners[].status`,
   `ex.availableToBack/availableToLay`).
3. Forma reale di `getAccountFunds` (`availableToBetBalance`, `exposure`, `retainedCommission`): base di F-16 e PFC-008.
4. Paginazione reale di `listCurrentOrders`/`listClearedOrders` (`moreAvailable`).
5. Latenza delle letture (il gate fondi richiede conferme ≤ 30 s, F-16).
6. Stream (MCM/OCM): non coperto dal runner REST; da aggiungere se la lane ha lo stream abilitato.
7. Estratto conto per la ricostruzione del picco F-11: `export_account_statement(client, from_iso=..., to_iso=...)`
   (`getAccountStatement`, tutte le pagine; troncato o malformato ⇒ errore). Il file contiene importi: è un'evidenza
   privata da passare a `tools/equity_peak_reconstruction`, mai al report pubblicabile.

## Livello C — ordini reali (NON AUTORIZZATO)

Solo piano e criteri d'ammissione. Nessun ordine, nemmeno da 0,10 €.

Criteri minimi prima di chiedere l'autorizzazione owner specifica:

- correzioni finanziarie P0/P1 mergiate e certificate (P0 placeOrders, F-16, F-11, F-12, F-23 e successive);
- livello B PASS sullo stesso SHA, con l'endpoint italiano verificato;
- PRE-LIVE della certificazione verde sullo SHA; kill switch e risk-stop provati;
- mandato finanziario owner: importo massimo, mercati, durata, responsabile, condizioni di arresto;
- runner dedicato con allowlist di mercato/importo e registrazione completa (richiesta, risposta, riconciliazione);
- piano di riconciliazione post-campagna (listCurrentOrders/listClearedOrders) e di rollback.

Scenari candidati (dopo autorizzazione): un BACK al minimo exchange non abbinabile e cancellato (prova di
place/cancel/riconciliazione), un ordine Microstake nativo, un esito ignoto simulato lato client con riconciliazione.

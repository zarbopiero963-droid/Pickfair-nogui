# Registro Roadmap V2 (PF-GOV-PERSISTENT-ROADMAP-01)

Direttiva permanente owner del 04/11/2026: nessun lavoro è completato finché non è **eseguito e verificato** e
**registrato su GitHub** con prove, SHA, stato e collegamenti. Flusso per ogni agente:
`WORK → TEST → EVIDENCE → GITHUB → HANDOFF`.
Ordine owner PF-FINISH-THE-PRODUCT-01 (stesso giorno): il registro resta **minimo**; il tempo va alle 33 schede.

## Cosa c'è
- `registry.json`: le 33 schede F-01…F-33 con nome, priorità e dipendenze trascritti dalle fonti ufficiali su #586
  (indice c.6505613328, schede v2), collegamenti verificati a PR e branch, blocchi e punti da riconciliare. Si
  modifica solo con una PR. Nessuno stato si scrive a mano.
- `../../tools/roadmap/pf_roadmap.py`: deriva gli stati dai soli fatti di GitHub (PR, merge SHA nella storia di main,
  branch, check). Tre dimensioni: sviluppo, integrazione, certificazione. **MERGED non è CERTIFIED**: CERTIFIED solo
  con una voce in `certifications` che cita evidenza firmata e SHA. GitHub non leggibile => errore, mai uno stato.
- `registry_audit_20261104.md`: audit delle fonti e dei conflitti da cui è nato il registro.

## Uso
```
python3 tools/roadmap/pf_roadmap.py validate                       # completezza del registro
python3 tools/roadmap/pf_roadmap.py derive --out snap.json         # stati da GitHub
python3 tools/roadmap/pf_roadmap.py render --snapshot snap.json    # dashboard Markdown
python3 tools/roadmap/pf_roadmap.py publish --snapshot snap.json --issue 586   # commento-registro + transizioni
```

## Regole per gli agenti
- Ogni PR dichiara la scheda nel body: `Roadmap-Card: F-xx` oppure `Roadmap-Card: none <motivo>`. Una PR aperta
  senza scheda compare nella dashboard come problema di tracciabilità (non come difetto del codice).
- Una nuova PR o un nuovo branch di una scheda si aggiunge ai `links` della scheda nella stessa PR.
- Dove la fonte manca o confligge: `needs_reconciliation`, mai un'associazione inventata.

## Limite attuale (dichiarato)
L'aggiornamento è **su comando** (`derive` + `publish`). L'aggiornamento automatico a ogni evento (workflow con solo
`issues:write`, da main, concorrenza unica) richiede un file in `.github/workflows/`, quindi una PR a merge owner:
arriva quando la corsia è libera. Fino ad allora Claude B esegue `derive`/`publish` a ogni evento di PR.

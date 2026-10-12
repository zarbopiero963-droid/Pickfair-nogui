# D10 — criteri di accettazione (1/1)

> Revisione indipendente BF2/D03 (OD-01, #426). Istruzioni: `00_ISTRUZIONI_REVISORE.md`. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO + motivo.

# BF2-D10 — Pacchetto verificabile per revisore indipendente

**Stato: REVIEW_PENDING.** Le caselle già pubblicate restano all'owner/revisore: nessuna è stata spuntata dall'agente. Un controllo fatto dallo stesso sistema che ha prodotto il dossier **non** è indipendente.

**Oggetto della revisione (180 voci, ID stabili):** [parte 1](https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/506#issuecomment-6125596930) (5 finding, 10 errata, 15 contraddizioni, 10 customerOrderRef) · [parte 2](https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/506#issuecomment-6125597245) (80 comportamenti) · [parte 3](https://github.com/zarbopiero963-droid/Pickfair-nogui/issues/506#issuecomment-6125597587) (59 righe di fonti).

## Requisiti del revisore
Distinto dal produttore (non questo agente né lo stesso sistema). Accesso in sola lettura a GitHub e alla documentazione Betfair pubblica. Nessuna credenziale Betfair, nessun LIVE.

## Procedura e criteri di accettazione per classe di voce
| Classe | Cosa verificare | Accetta se | Respingi se |
|---|---|---|---|
| D10-F (finding) | Apri il permalink a `028cee9` | Il codice fa ciò che il finding dice | Il comportamento descritto non c'è o le righe non corrispondono |
| D10-E (errata) | Ricontare dal checkpoint indicato | Il conteggio o la correzione è esatto | Il conteggio diverge |
| D10-K (contraddizioni) | Leggi le due fonti in conflitto | La regola prudente non è meno sicura di nessuna delle due fonti | La regola rilascia esposizione o aumenta lo stake |
| D10-I (customerOrderRef) | Pagine placeOrders, TypeDefinitions, listCurrentOrders/listClearedOrders, Stream API | Ogni OFFICIAL è citabile testualmente; ogni UNVERIFIED è davvero assente dalla doc | Garanzia inventata o classe sbagliata |
| D10-B (comportamenti) | Fonte S-xx indicata | Affermazione presente nella fonte, classe corretta (OFFICIAL/STAFF/COMMUNITY) | Affermazione assente o classe gonfiata |
| D10-S (fonti) | Link, data di versione, giurisdizione | Il link esiste e la versione o data coincide | Link morto, data sbagliata, alias non marcato |

## Permalink per i finding (baseline 028cee9)
- F01: [betfair_client.py#L1686-L1691](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/betfair_client.py#L1686-L1691) e [#L1724-L1730](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/betfair_client.py#L1724-L1730)
- F02: [core/reconciliation_engine.py#L714-L719](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/core/reconciliation_engine.py#L714-L719), [#L986-L998](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/core/reconciliation_engine.py#L986-L998)
- F03: [services/streaming_feed.py#L264-L270](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/services/streaming_feed.py#L264-L270)
- F04: [services/streaming_feed.py#L341-L352](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/services/streaming_feed.py#L341-L352)
- F05: [core/runtime_controller.py#L776-L786](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/core/runtime_controller.py#L776-L786), [#L840](https://github.com/zarbopiero963-droid/Pickfair-nogui/blob/028cee9074c3ab6c2a8014449a1897ae8faebc93/core/runtime_controller.py#L840)

## Fonti ufficiali usate (versione, sha256 del testo salvato sul box)
placeOrders 2026-07-31 · replaceOrders 2024-06-04 · cancelOrders 2024-06-04 · updateOrders 2024-06-19 · listCurrentOrders 2024-11-05 · listClearedOrders 2026-05-18 · getAccountFunds 2024-06-11 · listMarketProfitAndLoss 2025-09-02 · listMarketBook 2024-06-04 · Exchange Stream API 2026-05-11. Gli hash per file sono in `review/SHA256SUMS` sul box. Il forum risponde 403 a curl, **nessun bypass**: le voci STAFF/COMMUNITY vanno verificate dal revisore con un browser.

## Esito richiesto
Per ogni ID: ACCETTA / RESPINGI + nota. Verdetto D10 = PASS solo se tutte le voci delle Parti 1–4 sono accettate e nessuna voce delle Parti 5–6 respinta resta non corretta. Altrimenti PARTIAL o FAIL, con la lista degli ID.

# Ricalcolo dei fondi del SimulationBroker (#543)

Follow-up prestazionale di PFC-005 (#542), priorità P2, non di correttezza. Va chiuso prima dei collaudi SIM
prolungati (H24, multi-mercato). Criteri dell'owner: #543, commento del 23/10/2026.

## Problema

`get_account_funds` e il controllo dei fondi di ogni `place_bet` / `place_orders` ricalcolavano **tutti** i
mercati aperti. Per ciascuno:
- `_esposizione_mercato_locked` riscorreva **l'intero** registro degli ordini;
- `_ricostruisci_fill_mercato_locked` riscorreva l'intero registro dei fill e degli ordini e rigiocava i fill su
  ledger nuovi.

Il costo per chiamata era O(mercati × (ordini + fill)), tutto sotto `_lock`. Rallentava quindi anche il thread
dei book. In più `_apply_balance_effect` costruiva a ogni fill l'insieme di tutti gli id del registro.

## Correzione

Tre interventi. Nessun limite al numero di ordini o mercati, e nessun mercato escluso.

1. **Raggruppamento in una passata.** `_ordini_per_mercato_locked` raggruppa ordini e fill per mercato una sola
   volta per chiamata, conservando l'ordine di inserimento e quello cronologico. Le funzioni per mercato
   ricevono il sottoinsieme già filtrato e mantengono il filtro sul mercato: stesso calcolo, stessi elementi,
   stesso ordine.
2. **Memo per mercato indirizzato dal contenuto.** `_esposizione_mercato_memo_locked` e
   `_ricostruisci_fill_mercato_memo_locked` riusano il risultato di un mercato solo se la chiave coincide. La
   chiave è fatta di **tutti** i campi che il calcolo legge:
   - per l'esposizione: per ogni ordine bet_id, selezione, lato, quota, importo, abbinato, quota media e stato,
     più il numero di vincitori del book;
   - per la cassa: per ogni fill fill_id, bet_id, mercato, selezione, lato, quota e importo, più per ogni ordine
     bet_id, mercato, selezione, lato, abbinato e quota media.

   Non esiste un'invalidazione esplicita da poter dimenticare: un input cambiato, anche fuori dalle API,
   cambia la chiave e il mercato si ricalcola. Regole del memo:
   - non si usa mai con candidati o esclusioni (controllo del singolo ordine);
   - non si usa con registro dei fill corrotto;
   - non si usa su un campo non hashabile;
   - non memorizza mai un'eccezione;
   - una chiave con NaN non coincide mai, quindi si ricalcola.

   I mercati regolati escono dal memo, che quindi non cresce con la storia della sessione.
3. **Id del fill.** `_apply_balance_effect` considera solo gli id che cominciano con il bet_id dell'ordine.
   Ogni id candidato comincia così, quindi il risultato è identico.

## Misure

Benchmark riproducibile: `python tests/stress/bench_sim_funds.py <ordini> <mercati>`.
- Carico: ordini piazzati a turno sui mercati, a 2 runner e vincitore unico, con poca liquidità. Si ottengono
  fill, parziali e ordini a riposo.
- Saldo ampio, quindi nessun rifiuto.
- Lo script stampa tempi e un'impronta contabile.

Condizioni: macchina di sviluppo a 4 CPU, 3 esecuzioni per lato. "Prima" è un worktree di main `e9d0705`, "dopo"
il codice di questa modifica.

| Carico (ordini / mercati) | Totale piazzamenti prima | dopo | Ultimo `place_bet` prima | dopo | `get_account_funds` prima | dopo |
|---|---|---|---|---|---|---|
| 1000 / 50 | 2,83–2,98 s | 0,40–0,43 s | 3,9–5,6 ms | 0,58–0,65 ms | 5,6–7,1 ms | 1,3–1,5 ms |
| 2000 / 100 | 16,1–17,2 s | 1,64–1,78 s | 13,5–14,2 ms | 1,3–1,9 ms | 20,1–22,3 ms | 3,2–4,5 ms |

L'impronta contabile è identica prima e dopo in tutte le esecuzioni (`270c1f449cf9dfb6`, `4d8db80d319eb4cc`):
fondi, abbinato, numero di fill e stati degli ordini.

Il costo resta lineare negli ordini per chiamata, per raggruppamento e chiavi, quindi non è costante. I numeri
valgono su questa macchina; non sono un obiettivo né una garanzia per altri carichi.

## Prove di equivalenza

`tests/unit/test_543_sim_funds_recompute.py`:
- **Test differenziale.** Un broker di riferimento con il codice di prima, copiato verbatim da main `e9d0705`,
  e il broker ottimizzato eseguono la stessa sequenza deterministica: 12 semi × 260 passi. Dopo **ogni**
  operazione esiti, fondi e stati degli ordini devono coincidere. La sequenza contiene:
  - BACK/LAY a riposo, abbinati e parziali;
  - batch Dutching;
  - cancellazioni e sostituzioni;
  - book che abbinano ordini a riposo;
  - settlement;
  - riavvii da snapshot.
- **Casi limite:**
  - rifiuto per fondi identico al limite del disponibile;
  - registro dei fill corrotto;
  - campi modificati direttamente;
  - abbinato incoerente con i fill (deve restare non verificabile, mai la cassa di prima);
  - numero di vincitori cambiato sullo stesso mercato;
  - memo senza crescita sui mercati regolati;
  - piazzamenti e book da più thread, senza stalli e con fondi finali uguali al riferimento.
- **Mutazioni `i543_*` nel catalogo.** Coprono memo che ignora la chiave e chiavi senza abbinato o senza numero
  di vincitori. Sono tutte uccise.

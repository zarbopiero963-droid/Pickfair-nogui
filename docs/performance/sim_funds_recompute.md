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
   cambia la chiave e il mercato si ricalcola.

   Il memo si usa solo nel **caso normale**:
   - nei fill sono presenti tutte le chiavi lette;
   - id, mercato, lato e stato sono `str`, cioè proprio i valori che il calcolo legge con `str()` o confronta;
   - i numeri entrano nella chiave con il loro tipo, così 1, 1.0 e True restano distinti.

   Qualunque altro valore (id numerico, chiave assente, book malformato) fa calcolare senza memo, come prima.
   Questa regola viene dalla review indipendente su `5392139`, che aveva trovato due casi:
   - `1.0 == 1` e `None` contro una chiave assente davano un falso riuso;
   - un book non dizionario sollevava `AttributeError`.

   Altre regole del memo:
   - non si usa mai con candidati o esclusioni (controllo del singolo ordine);
   - non si usa con registro dei fill corrotto;
   - non memorizza mai un'eccezione;
   - una chiave con un NaN nuovo non coincide, quindi si ricalcola. Lo **stesso** oggetto NaN invece coincide
     con se stesso, perché il confronto fra tuple controlla prima l'identità, e il memo si riusa. È innocuo:
     gli oggetti in ingresso sono gli stessi, il calcolo è deterministico e un'eccezione non si memorizza mai.
     Rettifica della review completa su `699d4b0`.

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
| 1000 / 50 | 2,93–3,42 s | 0,50–0,81 s | 4,5–6,1 ms | 0,90–1,29 ms | 6,4–8,9 ms | 1,9–2,7 ms |
| 2000 / 100 | 18,1–19,0 s | 2,24–3,57 s | 10,6–23,7 ms | 2,3–3,2 ms | 16,3–20,5 ms | 6,3–7,2 ms |

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
  - piazzamenti e book da più thread, senza stalli e con fondi finali uguali al riferimento;
  - book non dizionario su un mercato senza esposizione;
  - valori uguali per `==` ma letti in modo diverso (`fill_id` 1.0→1, `bet_id` 1→1.0, `fill_id` None→assente);
  - NaN: lo stesso oggetto riusa il memo con il risultato del riferimento, un NaN nuovo ricalcola;
  - riuso effettivo del memo nel caso normale, perché una chiave che fallisce sempre farebbe sparire
    l'ottimizzazione in silenzio.
- **Mutazioni `i543_*` nel catalogo.** Coprono:
  - memo che ignora la chiave;
  - chiavi senza abbinato o senza numero di vincitori;
  - id non stringa accettati nel memo.

  Sono tutte uccise: 21/21 sul modulo.

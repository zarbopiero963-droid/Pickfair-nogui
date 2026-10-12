# P35 e coda segnali — domanda al revisore: P35 si applica a core/signal_queue.py?

> Revisione indipendente BF2/D03 (OD-01, #426). Istruzioni: `00_ISTRUZIONI_REVISORE.md`. Rispondi per ID: ACCETTA / RESPINGI / DUBBIO + motivo.

## Decisione OD-03 (owner 2026-10-12, #426/6126847863)
Nessuna sostituzione automatica; concorrenza solo entro limiti di esposizione e dedup; force mai oltre tetto/riserve/STOP/gate LIVE; a capienza: rifiuto (attesa solo se coda e scadenza lo consentono); nessuna cancellazione implicita.

## P35 (testo owner, #426/5934562905, estratto)
## DEC-426-P35-B/C — Nessuna sostituzione e scadenza alla chiusura del mercato

**Fonte owner — 01/10/2026, 17:23 Europe/Rome:** «1 non deve sostituire, 2 si alla chiusura del mercato».

**P35 ora interamente CONFERMATA:**
- **P35-A, attesa:** il comando valido sotto soglia rimane pendente e si esegue automaticamente quando la condizione è raggiunta, attraverso i gate previsti.
- **P35-B, nessuna sostituzione:** l’arrivo di un nuovo comando non sostituisce, modifica né cancella automaticamente quello già in attesa. Non applicare una policy “vince l’ultimo”. La risposta non autorizza implicitamente FIFO, concorrenza di uscite, doppia chiusura o scarto silenzioso del nuovo comando: usare identità, deduplicazione, stato posizione e contratti vigenti per trattarlo; eventuale lacuna reale si circoscrive dopo aver letto le fonti.
- **P35-C, scadenza:** alla **chiusura del mercato di riferimento**, l’intento ancora pendente scade e non deve generare nuovi invii, anche dopo restart. Non confondere la chiusura con SUSPENDED, la fine del tempo regolamentare o l’attesa dell’accredito/settlement. Gli ordini già inviati e gli esiti incerti restano da riconciliare: scadere un intento non significa falsificare il risultato o annullare una bet matched.

**Schede coinvolte:** PR25 persistenza/attesa/scadenza; PR44 soglia/frazione/esecuzione; PR42 replica; PR45 reconciliation. Prove: nuovo messaggio preserva intento precedente; mercato CLOSED disarma e persiste scadenza; restart non riesuma intento scaduto; SUSPENDED non equivale a CLOSED; esiti già inviati ancora riconciliati. Nessun PASS attestato e nessun codice modificato da questa annotazione.

### Stato delle altre decisioni

Le scelte del chiarimento P33–P40 sono tutte risolte; le decisioni già confermate non vanno richieste di nuovo. Restano distinti:
1. **Dati per il collaudo #351:** macchine/versioni e distribuzione Linux, alias non segreti account/bot/chat/canali, disponibilità effettiva Telethon, quantità/alias follower, tipi di mercato calcio da usare nella campagna. Lo SHA/build è congelato al momento della consegna; non è una scelta di prodotto mancante.
2. **Ratifica già prevista P05:** l’agente misura freshness/tolleranze Delayed e Live e propone i valori finali all’owner dopo le prove.
3. **Possibili chiarimenti futuri, soltanto con evidenza:** formule MM, base percentuale, condizionata o interazione fra int

## Codice osservato `core/signal_queue.py:169-214` @028cee9 (citazione, non modifica)
```python
    def add(self, row: dict, *, signal_id: str = None, now: float = None,
            timeout: float = None, force: bool = False, dedup_key: str = None):
        """Aggiunge un segnale e ritorna il suo `signal_id`, oppure ``None`` se è stato
        BLOCCATO dal tetto `max_active` (#136 punto 5).

        - ``OVERWRITE_LAST``: sostituisce tutti i segnali attivi con questo (mai bloccato);
        - altre modalità: aggiorna se `signal_id` è già presente, altrimenti accoda — ma un
          NUOVO segnale che porterebbe oltre `max_active` (se > 0) viene **bloccato**
          (ritorna ``None``, coda invariata) così non si superano N scommesse simultanee.

        `force=True` (auto-raise del tetto, decisione proprietario #192): accoda il segnale
        SENZA applicare il tetto `max_active`. Serve al commit MULTI-RIGA (`commit_signals`) per
        NON spezzare mai il blocco coerente di UN singolo messaggio: tutte le righe di
        quell'istruzione restano attive insieme invece di essere troncate al tetto (che
        lascerebbe righe fuori in silenzio). Non incide sull'aggiornamento di un segnale già
        attivo (mai bloccato comunque) né su `OVERWRITE_LAST`.


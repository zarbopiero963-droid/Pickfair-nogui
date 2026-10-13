## PF-OWNER-CLOSE-ALL-02 — Classificazione delle decisioni (owner t72u, 2026-10-13 10:23 ora di Roma)

Testo trascritto fedelmente dal mandato owner PF-OWNER-CLOSE-ALL-02, come ricevuto dall'agente. Le formulazioni restano quelle del mandato. Non sono introdotti nuovi valori: l'unico importo, 10,00 €, è già vigente.

### OWNER_RATIFIED
- **Q3 — Limite di esposizione:**
  - esposizione uguale al cap: **consentita**;
  - esposizione oltre il cap: **bloccata**;
  - nessuna tolleranza numerica; calcolo in Decimal con unità coerenti.
- **Q3 — Drawdown:** è un controllo separato. Lo stop scatta quando la perdita è **>= 10,00 €**.
- **Q4 — Rischio di un ordine LAY:**
  - rischio = liability = stake × (quota − 1);
  - l'aggregato comprende matched, unmatched, pendenti e incerti, senza doppi conteggi;
  - nessun nuovo ordine che aumenta il rischio quando l'esposizione non è verificabile.
- **Q5 e Q6:** confermate (#426/6141136157).
- **Q7 — Solo i criteri approvati:**
  - **T5:** confronto esatto degli importi in forma canonica, senza tolleranza di 0,01. Le differenze non spiegate bloccano nuovo rischio. Le differenze di rappresentazione si distinguono da quelle economiche.
  - **T7:** calcolo in Decimal; nessuna tolleranza implicita nelle decisioni di rischio. La visualizzazione a 2 decimali non cambia la precisione interna e non anticipa regole di settlement non verificate.
  - **T4:** solo verifica in sola lettura delle regole Betfair Italia e delle funzioni dell'account. Nessun ordine di prova; nessuno step definitivo senza evidenza.
  - **T6:** la formula si definisce solo dopo T4.

### TECNICA_DELEGATA
- Lavoro tecnico su Q1 e Q2.
- **Q1 e Q2 non sono approvazioni generali di intervalli.**

### PENDING_OWNER
- Tutto ciò che non è determinabile dalle fonti.

Gate A: NON PASS.

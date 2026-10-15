# A06 — riconciliazione per cella

Fonti immutabili:

- crosswalk Roserpina: codice `82f499c852f4bdb38db2183c4fae8cb099910c9a`;
- evidenze concorrenti: `checkpoint/gate-a-close02@1734dee1f170bd8e5ea3c7eff6c602bf7e8c306f`;
- manifest Gate-A: 459/459 SHA-256 OK.

Risultato prudenziale sulle 410 celle (41 parametri × 10 assi):

| Stato | Celle |
|---|---:|
| PASS | 100 |
| FAIL | 0 |
| NOT VERIFIED | 233 |
| BLOCKED | 77 |
| MISSING | 0 |

La scansione strutturata ha recuperato record concorrenti per 368/410 celle in 25 file JSON. Le 42 celle senza record concorrente sono i 41 assi GUI e `PF-RSR-01-VALID`; restano comunque classificate nel crosswalk sorgente, quindi non sono MISSING. Ogni cella, incluse quelle senza record concorrente, è presente in `a06_reconciliation_seq12.json` con requisito, asse, evidenza originaria, SHA e motivazione.

Nessun PASS è stato trasferito da alberi patch v20–v24 al codice `82f499c` o all'albero corrente `c5e4422`: un risultato eseguito su un altro SHA non certifica il codice corrente. Le 37 celle VALID che dipendono da range numerici generali restano bloccate in assenza di una decisione owner; non sono stati inventati range.

Decisioni finanziarie già ricostruite e mantenute: uguaglianza ai cap consentita, oltre-cap bloccato, Decimal senza tolleranza, rischio LAY uguale alla liability, drawdown assoluto €10 con stop a `>=10`, cap obbligatori assenti/invalidi fail-closed, recovery e commissioni secondo le decisioni t80u. Gate A resta **NON PASS**.

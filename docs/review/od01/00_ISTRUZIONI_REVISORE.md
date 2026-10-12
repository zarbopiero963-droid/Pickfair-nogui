# 00 — Istruzioni per il revisore indipendente (Sol primario, Grok secondo)

Questa PR contiene **solo documentazione** da revisionare. Non è codice e non va mergiata. Fonti di riferimento: #506 (master), #426 (decisioni owner), baseline main `028cee9`.

**Cosa produrre:** per OGNI ID presente nei file di questo push, una riga nella forma:
`ID — ACCETTA | RESPINGI | DUBBIO — motivo (fonte o file:riga)`

**Criteri:**
- **Finding D10-F*:** il codice descritto a `028cee9` fa davvero ciò che si afferma. Se non lo puoi verificare dal diff, rispondi DUBBIO.
- **Contraddizioni D10-K*:** la regola prudente non è meno sicura di nessuna fonte.
- **D10-I* (customerOrderRef):** OFFICIAL solo se citabile dalla doc Betfair; nessuna garanzia inventata.
- **D10-B* e D10-S*:** l'affermazione è coerente con la fonte e la classe (OFFICIAL / STAFF / COMMUNITY) non è gonfiata.
- **D03 DIFF-*:** l'interpretazione è atomica e fedele alla fonte A; l'oracle discrimina, cioè il negativo fallirebbe su un'implementazione errata; la citazione B è plausibile per la versione indicata.
- **Metodo P1–P7** (`D03_METODO_OWNER_RECONSTRUCTED.md`): confrontalo con il metodo pubblicato (#506 commento 6124224080, riassunto in quel file) e segnala ogni incompatibilità.
- **P35** (`P35_E_SIGNAL_QUEUE.md`): rispondi esplicitamente **P35-APPLICA: SÌ/NO** ai segnali di `core/signal_queue.py`, con motivo.

Non spuntare caselle. Non proporre merge.

# Rollback del vincolo single-account e del SafetyStateOwner (procedura B, M155)

Decisione owner M154-bis/M155: **nessun ritorno normale** a una versione che consenta il LIVE senza controlli equivalenti.
Questa procedura è l'unico modo ammesso per tornare a una versione precedente della PR #638 (es. `cef002ba`), ed è
eseguita **solo dall'operatore**, con il programma fermo, con **LIVE OFF** e con una verifica indipendente prima di
riaprire qualsiasi cosa. Nessun percorso automatico.

## Che cosa si perde tornando indietro

| Ritorno a | Cosa si perde | Perché la procedura B è obbligatoria |
|---|---|---|
| commit 1 senza commit 2 (rollback (A)) | il proprietario unico dello stato di sicurezza: tornano M1 (scritture concorrenti del giornale), M2 (reset dell'ES che toglie la barriera C3), C02 (ES perso dopo una rimozione del giornale), C12 (reset senza ES che toglie il pending-stop) | (A) conserva le garanzie d'identità, **non** quelle di concorrenza: non è un rollback «senza perdita di sicurezza» |
| `cef002ba` o precedente | anche l'identità attestata e `betfair_authorized_identity`: torna il ripiego sullo username di configurazione e del client | la chiave resta nel db ma è inerte per quel codice: il vincolo single-account non è più applicato |

Il kill switch (`safe_mode.SafeModeManager`) è solo in memoria: dopo un riavvio con la versione vecchia **non** protegge.
La protezione deve quindi essere **durevole nel db** prima di installare la versione vecchia.

## Passi (programma fermo, LIVE OFF)

1. **Fermare** Pickfair e verificare che nessun processo resti attivo (lock del runtime libero).
2. **Copia e hash** del db e dei file accanto (`pickfair.db`, `pickfair.db.safety_pending.json`,
   `pickfair.db.daily_loss_pending.json`, marker dello stop di rischio), come al passo 2 di
   `ops/live_registry_recovery.md`. Le copie sono evidenze: non si modificano.
3. **Emergency Stop durevole** nella copia di lavoro del db, con motivo `ROLLBACK_SINGLE_ACCOUNT` e provenienza LIVE:
   `emergency_stopped = "1"`, `emergency_reason = "ROLLBACK_SINGLE_ACCOUNT"`, `emergency_settlement_source = "LIVE"`.
4. **Registro LIVE con la sentinella**: nella chiave `emergency_live_accounts` (registro v2) la voce
   `betfair|LIVE|<conto_non_identificato>|EUR` in stato `PENDING`. La versione vecchia riparte in emergenza e rifiuta il
   reset finché non si esegue la procedura M148 con la doppia conferma.
5. **Obblighi conservati**: nessuna chiave di sicurezza esistente si cancella (stop di rischio, marker, esposizione
   irrisolta, `betfair_authorized_identity`). Il giornale di sicurezza resta al suo posto: la versione vecchia legge lo
   stesso formato (chiavi in piano; la chiave `journal_seq` è ignorata).
6. **Validare** la copia di lavoro (riletta: ES attivo, sentinella PENDING) e applicarla come ai passi 5 e 6 di
   `ops/live_registry_recovery.md`, con una voce di audit.
7. **Installare** la release dedicata del rollback e avviarla **solo in SIM**. Verifica indipendente (Claude D o
   operatore diverso): ES attivo, reset rifiutato, nessun ingresso LIVE, nessuna certificazione LIVE.
8. Ogni riapertura del LIVE richiede la procedura M148 completa (riconciliazione reale del conto, doppia conferma) e una
   nuova decisione dell'owner.

## Prova

La prova R-ROLLBACK (Claude D) crea lo stato reale con il codice nuovo, applica questi passi e apre lo stesso db con il
codice di `cef002ba` su un banco completo. Esiti richiesti: (a) nessun ingresso, sweep o certificazione LIVE; (b) nessuna
riscrittura delle chiavi di sicurezza; (c) SIM usabile, oppure il limite dichiarato all'owner. La misura di B su
`cef002ba` (M154, c.7004803621) ha già mostrato che un registro illeggibile o la sentinella bloccano sweep e
certificazione, e che il reset dell'ES è rifiutato.

## Divieti

- Nessun downgrade automatico o silenzioso. Saltare i passi 3–5 equivale a un downgrade di sicurezza vietato.
- Nessun ritorno a una versione che consenta il LIVE senza controlli equivalenti.
- Il kill switch in memoria non sostituisce l'ES durevole.

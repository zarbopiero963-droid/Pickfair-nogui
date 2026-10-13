## CLOSE-02 v10: primo traguardo (t75u/t76u/t77u), 13/10 12:06
1. **Lavoro svolto**
   - Stream A: lotto L7-LOAD20.
     - Sonda dinamica `probe_load_v2` a 028cee9, con DB sqlite reale nuovo per ogni scenario: assente, override, riavvio, vuoto, abc, nan, inf, più cattura dei log.
     - `build_L7.py`: oracoli per chiave, fase e clausola, ancorati a citazioni verbatim con SHA ricalcolato.
     - `verify_L7.py`: verifica indipendente. Risonda in un processo separato e controlla la riproducibilità, gli sha, la copertura positivo/negativo/limite per clausola, l'assenza di €/cap sulle chiavi non-EUR e i verdetti ricalcolati.
   - Stream B: patch RACE-001 v6, con 11 test race001.
   - CASH: casi suddivisi in ENTRY 4, HOLDING 4, RISK_REDUCTION 1, CASHOUT 6. Le regole di cap sono applicabili solo a ENTRY: 0 violazioni.
2. **Gap chiusi**: nessuno dichiarato risolto (restano aperti fino alla sequenza #505 autorizzata).
3. **Test**
   - RACE-001 RED a 028cee9: 8 falliti, 3 passati (`red_race_v2.txt`). GREEN nel wt: 11/11.
   - Regressione completa con la patch v6: **7329 passati, 0 falliti**, 15 saltati, 2 xfail. Baseline 028cee9: 7253 passati, 1 fallito (pr04_quater, ambiente CL02-ENV-001).
   - I test RACE dimostrano:
     - chiavi diverse concorrenti, in thread, tra connessioni e dopo riavvio: entrambe persistono;
     - stessa chiave: `CONFIG_CONFLICT` tutto-o-niente, settings byte-identici e versione invariata;
     - crash a metà salvataggio: rollback, riavvio coerente, salvataggio successivo OK;
     - config_version +1 per salvataggio riuscito;
     - round-trip di tipi e valori;
     - una chiave corrotta non viene sovrascritta da un salvataggio non correlato e resta in `corrupt_keys`.
   - Due istanze Database sullo stesso file, stessa chiave: esito esplicito `OperationalError: database is locked`, mai una perdita silenziosa. Non è però `CONFIG_CONFLICT` semantico: è un residuo (possibile `BEGIN IMMEDIATE` + mappatura).
4. **Finding**: RACE-001 corretto SOLO in proposta (`close02_findings_proposal_v6.patch`, sha 367858ba…). Nei 20 record LOAD, gli esiti osservati a 028cee9 sono documentati come NON_CONFORME in 180 casi (es. corrotto `abc`→default silenzioso, `nan`/`inf` accettati, chiavi RSR-38..41 assenti in RoserpinaConfig).
5. **Denominatore e blocchi**
   - Denominatore: 316 record / 4146 casi (L1 22/513, L2b 97/1478, L6 192/2140, CASH 5/15). Il lotto L7 riformula le 21 righe LOAD in 293 casi.
   - **Record interi verificati: 20** (RSR-02, 03, 04, 12-19, 23, 27, 29, 31, 36, 38-41 LOAD); **casi verificati: 280** (180 NON_CONFORME, 90 CONFORME, 10 NON_APPLICABILE con sonda e ancora verbatim). Verifica indipendente 20/20.
   - Bloccato: RSR-20-LOAD, perché la clausola owner sul drawdown di 10 € non è osservabile in LOAD (serve una sonda APPLY/FAILSAFE).
6. **Costi**: 0 € (residuo 16.7883 €).
   - Lotto di revisione proposto e **NON chiamato**: `lot_R_L7_LOAD20.json` (sha f8e543f3…), stima ~0.44 €, **tetto 0.66 €**.
   - Tempi reali: agente 11:53–12:04 per 20 record (~33 s/record). Macchina per record: sonda 0.08 s, build 0.01 s, verifica 0.02 s.
   - Cause di rilavorazione (`timing_L7.json`):
     - fonte P1/P2 nella riga di fase, non nella sezione citata;
     - id di clausola sfalsati (RSR-29);
     - sonda inadatta per le chiavi assenti dalla config;
     - predicati permissivi (bool `nan`, riavvio NA);
     - clausole D ri-ancorate a citazioni verbatim;
     - casi NA senza sonda.
7. **Prossima attività**
   - Altri 296 record: stima dai tempi misurati ≥ ~2.7 h se omogenei. Realistica 8–14 h, perché le fasi SAVE/RESTART/VALID/FAILSAFE/APPLY e i record BF/CASH richiedono nuove famiglie di sonde.
   - Serve l'autorizzazione owner per il lotto L7 (nessuna chiamata eseguita).

SHA256SUMS_v10 (verificati 16/16). Backup: /home/box/pickfair_backup/close02_v10_1206.tgz + patch v6.

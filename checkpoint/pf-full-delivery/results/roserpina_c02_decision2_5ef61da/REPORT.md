# Campagna Roserpina C02 con oracle indipendente — head 5ef61da (codice = ad13383), Decisione owner 2

- Data: 2026-10-17 23:40Z → 2026-10-18 00:00Z (1147 s, 4 worker). Ambiente: HeadlessApp reale (child_campaign.py), SQLite reale,
  SimulationBroker reale, netguard, Linux 6.18, Python 3.12.3. Scenari: base.jsonl (1152: capitali 10/50/100/250, profili, recovery,
  sequenze win/loss/adversarial, 192 con crash+riavvio). Oracle: oracle.py INVARIATO (indipendente, Decimal, non importa Pickfair).
- Oracle base: 1072 PASS / 80 VIOLATION. Violazioni solo su C01_no_duplicate_order (80), C01b_unique_customer_ref (80),
  R2_no_replay_after_restart (1, S00191). TUTTI gli altri controlli PASS (A1-A7 contabilita', C02 cap singolo 591/591,
  C03 max win, C04-C12 limiti e gate, C14 liquidita', H0/H1 harness, R1 ricostruzione 192/192).
- Lettura complementare (footprint_check.py, NON modifica l'oracle): 124 gruppi multiriga, 124 = impronta del Microstake nativo
  (stub CANCELLED con matched 0 + UNA riga viva, stesso customer_ref = stessa saga, matched == stake richiesto), 0 duplicati reali.
  L'oracle base conta le righe dell'exchange per (mercato, selezione, lato): con il Microstake (stub + replace con nuovo betId)
  l'impronta legittima e' di due righe. Da ratificare: aggiornare C01/C01b/R2 dell'oracle a "righe vive" (decisione owner su
  oracle, PF-MASTER §3: non si cambiano oracle senza giustificazione tracciata).
- Decisione 2 misurata: 1994 rifiuti `supera_max_single_bet` (prima: clamp silenzioso al cap), 1091 `stake_sotto_min_stake`,
  178 ordini vivi < 2 EUR eseguiti esattamente via Microstake, 1845 ordini vivi totali, C02_single_cap 591/591 PASS.
- Esito: PASS_SIM_NON_CERTIFICANTE sugli invarianti finanziari; nessun ordine sopra il cap, nessun duplicato reale, nessuna
  perdita contabile. Limiti: SIM/OFFLINE, nessun exchange reale, nessun Windows.

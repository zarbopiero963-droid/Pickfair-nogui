# Campagna — 1684 scenari eseguiti

Verdetti: {'PASS': 1065, 'PASS_WITH_KNOWN': 619}

## Per capitale

| capitale | totale | PASS | VIOLATION | BLOCKED |
|---|---|---|---|---|
| 2.0 | 144 | 128 | 0 | 0 |
| 3.4 | 144 | 129 | 0 | 0 |
| 6.7 | 144 | 113 | 0 | 0 |
| 10 | 156 | 112 | 0 | 0 |
| 50 | 156 | 94 | 0 | 0 |
| 100 | 156 | 80 | 0 | 0 |
| 250 | 156 | 79 | 0 | 0 |
| 500 | 156 | 87 | 0 | 0 |
| 1000 | 160 | 77 | 0 | 0 |
| 2500 | 156 | 86 | 0 | 0 |
| 5000 | 156 | 80 | 0 | 0 |

## Per controllo

| controllo | PASS | KNOWN | VIOLATION | BLOCKED |
|---|---|---|---|---|
| A1_gross_matches_oracle | 1045 | 0 | 0 | 0 |
| A2_commission_once_4_5_on_positive_net | 1045 | 0 | 0 | 0 |
| A3_conservation_total | 1684 | 0 | 0 | 0 |
| A4_settlement_booked_by_bot | 1003 | 42 | 0 | 0 |
| A5_runtime_realized_equals_exchange_net | 1003 | 0 | 0 | 0 |
| A6_stake_base_is_available | 1684 | 0 | 0 | 0 |
| A7_no_leaked_table_after_settlement | 1065 | 619 | 0 | 0 |
| C01_no_duplicate_order | 1684 | 0 | 0 | 0 |
| C01b_unique_customer_ref | 1684 | 0 | 0 | 0 |
| C02_single_cap | 1188 | 0 | 0 | 0 |
| C03_max_win_hard | 1188 | 0 | 0 | 0 |
| C04_min_price | 1188 | 0 | 0 | 0 |
| C05_total_exposure_cap | 1188 | 0 | 0 | 0 |
| C06_event_exposure_cap | 1188 | 0 | 0 | 0 |
| C07_max_open_exposure_abs | 1188 | 0 | 0 | 0 |
| C08_exposure_stop_before_submit | 531 | 0 | 0 | 0 |
| C09_tables_bound | 1188 | 0 | 0 | 0 |
| C10_daily_loss_stop | 585 | 0 | 0 | 0 |
| C10b_session_loss_stop | 600 | 0 | 0 | 0 |
| C11_no_entry_when_not_active | 1188 | 0 | 0 | 0 |
| C12_no_entry_after_risk_stop | 1188 | 0 | 0 | 0 |
| H0_harness | 1684 | 0 | 0 | 0 |
| H1_clean_stop | 1684 | 0 | 0 | 0 |
| R1_restart_rebuild_ok | 266 | 0 | 0 | 0 |
| R2_no_replay_after_restart | 266 | 0 | 0 | 0 |

## Copertura percorsi

- entry_placed: 1188
- entry_blocked_mm: 745
- entry_denied_engine: 503
- duplicate_blocked: 905
- settle_win: 455
- settle_loss: 795
- commission_charged: 455
- partial_match: 215
- unmatched_lapsed_or_cancelled: 621
- rejected_by_exchange: 80
- ambiguous_outcome: 76
- cashout_hedge: 186
- risk_stop: 394
- lockdown: 95
- auto_reset: 1
- crash_restart: 266
- no_table_available: 632
- daily_or_session_stop: 123

## Matrice RSR x percorsi

| RSR | parametro | esito campagna | influenza | rumore p95 | livelli |
|---|---|---|---|---|---|
| RSR-01 | Target Profit Cycle % | VARIED_INFLUENCE_OBSERVED | 0.281 | 0.104 | {'CONSERVATIVE': 384, 'BALANCED': 384, 'AGGRESSIVE': 384} |
| RSR-02 | Max Single Bet % | VARIED_INFLUENCE_OBSERVED | 0.281 | 0.107 | {'CONSERVATIVE': 384, 'BALANCED': 384, 'AGGRESSIVE': 384} |
| RSR-03 | Max Total Exposure % | VARIED_INFLUENCE_OBSERVED | 0.281 | 0.109 | {'CONSERVATIVE': 384, 'BALANCED': 384, 'AGGRESSIVE': 384} |
| RSR-04 | Max Event Exposure % | VARIED_INFLUENCE_OBSERVED | 0.281 | 0.096 | {'CONSERVATIVE': 384, 'BALANCED': 384, 'AGGRESSIVE': 384} |
| RSR-05 | Auto Reset Drawdown % | VARIED_INFLUENCE_OBSERVED | 0.281 | 0.107 | {'CONSERVATIVE': 384, 'BALANCED': 384, 'AGGRESSIVE': 384} |
| RSR-06 | Defense Drawdown % | VARIED_INFLUENCE_OBSERVED | 0.281 | 0.107 | {'CONSERVATIVE': 384, 'BALANCED': 384, 'AGGRESSIVE': 384} |
| RSR-07 | Lockdown Drawdown % | VARIED_INFLUENCE_OBSERVED | 0.281 | 0.102 | {'CONSERVATIVE': 384, 'BALANCED': 384, 'AGGRESSIVE': 384} |
| RSR-08 | Expansion Profit % | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.044 | 0.073 | {'L1': 566, 'L0': 586} |
| RSR-09 | Expansion Multiplier | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.062 | 0.075 | {'L1': 565, 'L0': 587} |
| RSR-10 | Defense Multiplier | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.055 | 0.071 | {'L0': 561, 'L1': 591} |
| RSR-11 | Table Count | VARIED_INFLUENCE_OBSERVED | 0.736 | 0.075 | {'1': 576, '5': 576} |
| RSR-12 | Max Recovery Tables | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.076 | 0.08 | {'True': 576, 'False': 576} |
| RSR-13 | Commission % | FIXED_SINGLE_VALUE |  |  | {'fixed:4.5': 1152} |
| RSR-14 | Min Stake | VARIED_INFLUENCE_OBSERVED | 0.209 | 0.073 | {'L1': 609, 'L0': 543} |
| RSR-15 | Max Stake Assoluto | VARIED_INFLUENCE_OBSERVED | 0.297 | 0.069 | {'pct': 576, 'abs': 576} |
| RSR-16 | Hard-stop: Perdita Giornaliera Max | VARIED_INFLUENCE_OBSERVED | 0.094 | 0.075 | {'L1': 567, 'L0': 585} |
| RSR-17 | Hard-stop: Esposizione Aperta Max | VARIED_INFLUENCE_OBSERVED | 0.297 | 0.069 | {'pct': 576, 'abs': 576} |
| RSR-18 | Stop: Perdita Sessione Max | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.073 | 0.073 | {'L0': 576, 'L1': 576} |
| RSR-19 | Stop: Esposizione Complessiva | VARIED_INFLUENCE_OBSERVED | 0.113 | 0.072 | {'L0': 579, 'L1': 573} |
| RSR-20 | Hard-stop: Drawdown Max % | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.045 | 0.067 | {'L0': 593, 'L1': 559} |
| RSR-21 | Book Warning % | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.056 | 0.071 | {'L1': 573, 'L0': 579} |
| RSR-22 | Book Block % | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.054 | 0.072 | {'L1': 578, 'L0': 574} |
| RSR-23 | Liquidity Moltiplicatore | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.056 | 0.067 | {'L1': 601, 'L0': 551} |
| RSR-24 | Liquidity Floor assoluto € | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.059 | 0.077 | {'L1': 574, 'L0': 578} |
| RSR-25 | Quota minima / Min Price | VARIED_INFLUENCE_OBSERVED | 0.467 | 0.075 | {'L0': 561, 'L1': 591} |
| RSR-26 | Max Win € | VARIED_INFLUENCE_OBSERVED | 0.248 | 0.068 | {'L0': 555, 'L1': 597} |
| RSR-27 | Max Stake % (avviso) | VARIED_INFLUENCE_OBSERVED | 0.281 | 0.104 | {'CONSERVATIVE': 384, 'BALANCED': 384, 'AGGRESSIVE': 384} |
| RSR-28 | Profit epsilon € | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.055 | 0.072 | {'L0': 572, 'L1': 580} |
| RSR-29 | Auto-green delay (s) | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.056 | 0.075 | {'L1': 589, 'L0': 563} |
| RSR-30 | Risk Profile | VARIED_INFLUENCE_OBSERVED | 0.281 | 0.099 | {'CONSERVATIVE': 384, 'BALANCED': 384, 'AGGRESSIVE': 384} |
| RSR-31 | Allow Recovery | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.076 | 0.078 | {'True': 576, 'False': 576} |
| RSR-32 | Anti Duplication Enabled | FIXED_SINGLE_VALUE |  |  | {'fixed:True': 1152} |
| RSR-33 | Liquidity Guard attivo | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.04 | 0.072 | {'L1': 604, 'L0': 548} |
| RSR-34 | Liquidity: solo avviso | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.067 | 0.069 | {'L1': 577, 'L0': 575} |
| RSR-35 | Max Win: solo avviso | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.048 | 0.084 | {'L1': 572, 'L0': 580} |
| RSR-36 | Auto-green delay enabled | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.047 | 0.075 | {'L1': 589, 'L0': 563} |
| RSR-37 | Profit epsilon enabled | VARIED_NO_INFLUENCE_ABOVE_NOISE | 0.054 | 0.07 | {'L1': 540, 'L0': 612} |
| RSR-38 | Cap assoluto: Singolo ordine € | BLOCKED |  |  | parametro assente al HEAD (solo PR #505): non configurabile, non testabile |
| RSR-39 | Cap assoluto: Singolo mercato € | BLOCKED |  |  | parametro assente al HEAD (solo PR #505): non configurabile, non testabile |
| RSR-40 | Cap assoluto: Singolo evento € | BLOCKED |  |  | parametro assente al HEAD (solo PR #505): non configurabile, non testabile |
| RSR-41 | Hard-stop: Drawdown assoluto € | BLOCKED |  |  | parametro assente al HEAD (solo PR #505): non configurabile, non testabile |

## Violazioni / bloccati


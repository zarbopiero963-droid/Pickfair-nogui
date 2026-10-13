"""close02 — script di mutazione ricostruito (M1..M79) sul patch finale.
Ogni mutante: una sostituzione testuale verificata (il pattern DEVE esistere una volta)."""
import subprocess, shutil, sys, os
P = '/workspace/pickfair/venv311/bin/python'
RC = 'core/runtime_controller.py'; SR = 'core/state_recovery.py'; RP = 'core/risk_persistence.py'
SS = 'services/setting_service.py'; VA = 'core/validators.py'; PE = 'core/pnl_engine.py'; PL = 'core/pnl_label.py'
DS = 'database_schema.py'; GUI = 'mini_gui.py'; TG = 'services/telegram_alerts_service.py'
MG = 'core/pre_v9_migration.py'; PR = 'core/peak_reconstruction.py'; BC = 'betfair_client.py'
M = [
("M1_start_attivo_anche_se_recovery_ko", RC, 'if not (isinstance(recovery_result, dict) and recovery_result.get("ok") is True):', 'if False:'),
("M2_schema_startup_orders_rimosso", DS, 'CREATE TABLE IF NOT EXISTS startup_orders (', 'CREATE TABLE IF NOT EXISTS startup_orders_mut ('),
("M3_errore_lettura_ordini_ignorato", SR, 'raise _StartupOrderCheckError("startup_order_exists") from exc', 'return False'),
("M4_picco_non_ripristinato", RC, '        self._restore_and_persist_equity_peak()\n', '        self._equity_peak_restored = True\n'),
("M5_riga_corrotta_ignorata", RP, '''    if _UNREADABLE in (a, b):
        return None, "equity_peak_illeggibile"
    vals = [v for v in (a, b) if v is not None]
    if vals:''', '''    a = None if a is _UNREADABLE else a
    b = None if b is _UNREADABLE else b
    vals = [v for v in (a, b) if v is not None]
    if vals:'''),
("M6_mancante_come_primo_avvio", RP, '''    if was_written(db, "equity_peak"):
        return None, "equity_peak_mancante"''', '''    if False:
        return None, "equity_peak_mancante"'''),
("M7_risk_stop_mancante_riapre", RC, '            self._risk_stop_reason = "risk_stop_state_mancante"', '            pass'),
("M8_gate_segnale_rimosso", RC, '''        if getattr(self, "_startup_recovery_blocked", ""):
            self._reject_signal(signal, f"startup_recovery_blocked:{self._startup_recovery_blocked}")
            return''', '''        if False:
            return'''),
("M9_resume_scavalca", RC, '''    def resume(self) -> dict:
        if getattr(self, "_startup_recovery_blocked", ""):''', '''    def resume(self) -> dict:
        if False:'''),
("M10_daily_loss_mancante_ignorato", RC, '''            logger.critical("DAILY LOSS: stato persistito mancante, fail-closed")
            self._daily_loss_state_missing = True''', '''            logger.critical("DAILY LOSS: stato persistito mancante, fail-closed")
            pass'''),
("M11_gate_t80u_segnale_rimosso", RC, '        if _t80u:  # owner t80u', '        if False:  # owner t80u'),
("M12_t80u_fuori_da_loss_block", RC, '                or validators.owner_t80u_new_risk_block_reason(getattr(self, "config", None)))', '                or "")'),
("M13_assenza_non_segnalata", SS, '        cfg.absent_keys = tuple(absent)', '        cfg.absent_keys = ()'),
("M14_recovery_assente_resta_true", SS, '            cfg.allow_recovery = False  # (2) mai True per assenza', '            pass'),
("M15_rsr38_non_caricata", SS, 'max_order_exposure=self._optional_hard_stop_value(data, "roserpina.max_order_exposure"),', 'max_order_exposure=None,'),
("M16_hook_nuovo_massimo_rimosso", RC, '        self.risk_desk.on_new_peak = lambda _p: self._persist_equity_peak()', '        self.risk_desk.on_new_peak = None'),
("M17_registro_sparito_ignorato", RP, '    if any(s.get(k) not in (None, "") for k in HISTORY_KEYS):\n        return True', '    if False:\n        return True'),
("M18_blocco_registro_non_durevole", RC, '            risk_persistence.mark(self.db, risk_persistence.REINIT_REQUIRED)', '            pass'),
("M19_emergenza_mancante_ignorata", RC, 'if flag == "" and risk_persistence.was_written(self.db, "emergency"):', 'if False:'),
("M20_picco_scritto_prima_del_ripristino", RC, '''        if not getattr(self, "_equity_peak_restored", False):
            return  # il picco''', '''        if False:
            return  # il picco'''),
("M21_blocco_daily_loss_non_durevole", RC, '            risk_persistence.mark(self.db, "daily_loss_missing")', '            pass'),
("M22_gate_daily_loss_mancante_rimosso", RC, '''        if getattr(self, "_daily_loss_state_missing", False):  # v9: perdita del giorno non verificabile
            self._reject_signal(signal, "daily_loss_state_mancante")
            return''', '''        if False:
            return'''),
("M23_commissione_inventata", SS, '            cfg.commission_pct = None  # (3)', '            cfg.commission_pct = 5.0  # (3)'),
("M24_massimo_recovery_azzerato_per_altra_chiave", SS, '        if "roserpina.max_recovery_tables" in bad:', '        if bad & set(self.OWNER_T80U_RECOVERY_KEYS):'),
("M25_verificata_sempre", PE, '    verified = commission_verified is True', '    verified = True'),
("M26_verifica_senza_confronto", PE, '''                if math.isfinite(reported_commission_f) and abs(
                        reported_commission_f - float(payload["market_commission_amount_total"])) < 0.005:''', '''                if True:'''),
("M27_etichetta_non_propagata", PE, '                "pnl_status": str(realized.get("pnl_status", "STIMATO")),', '                "pnl_status": "STIMATO",'),
("M28_vuoto_non_segnalato", SS, '                empty.append(k)', '                absent_opt.append(k)'),
("M29_corrotto_opzionale_non_segnalato", SS, '''                if not ok and k not in corrupt:
                    corrupt.append(k)''', '''                if False:
                    corrupt.append(k)'''),
("M30_assente_opzionale_non_segnalato", SS, '                absent_opt.append(k)\n                logger.warning("CONFIG: cap opzionale assente', '                pass\n                logger.warning("CONFIG: cap opzionale assente'),
("M31_default_non_segnalato", SS, '                defaulted.append(k)', '                pass'),
("M32_corrotto_non_blocca", RC, '''        if _corrupt:
            self._reject_signal(signal, "config_corrotta:" + ",".join(_corrupt))''', '''        if False:
            self._reject_signal(signal, "config_corrotta:" + ",".join(_corrupt))'''),
("M33_gate_pl_stimato_rimosso", RC, '        if not pnl_label.allows_risk_increase(payload if isinstance(payload, dict) else None):', '        if False:'),
("M34_stimato_ammesso", PL, '    if pnl_label(data) in (VERIFIED, SIMULATED):\n        return True', '    if True:\n        return True'),
("M35_commissione_illeggibile_ammessa", PL, '    return amount is not None and total is not None and amount == 0.0 and total == 0.0', '    return (amount or 0.0) == 0.0 and (total or 0.0) == 0.0'),
("M36_rollup_non_chiesto", RC, '''    def _market_commission_for_verification(self, rows, bet_to_market, settled_after) -> dict:''', '''    def _market_commission_for_verification(self, rows, bet_to_market, settled_after) -> dict:
        return {}'''),
("M37_betcount_ignorato", RC, '            if isinstance(n, bool) or not isinstance(n, int) or n != len(e["bets"]):', '            if False:'),
("M38_profit_rollup_ignorato", RC, '            if isinstance(p, bool) or not isinstance(p, (int, float)) or abs(float(p) - e["profit"]) >= 0.005:', '            if False:'),
("M39_commissione_su_ogni_riga", RC, 'if mc is not None and mc["last_bet"] == bet_id:', 'if mc is not None:'),
("M40_commissione_bet_level_usata", RC, '''            reported_commission = None
            mc = market_commission.get(market_id)''', '''            reported_commission = row.get("commission")
            mc = market_commission.get(market_id)'''),
("M41_sim_etichetta_stimata", PE, '                payload["pnl_status"] = "SIMULATO"', '                payload["pnl_status"] = "STIMATO"'),
("M42_gui_senza_etichetta", GUI, '    return _format_pnl(data.get("net_pnl", 0.0), data)', '    return f"{float(data.get(\'net_pnl\', 0.0) or 0.0):.2f}"'),
("M43_telegram_senza_pl", TG, '                    lines.append(f"• P/L: {_fmt(details.get(_k), details)}")', '                    pass'),
("M44_gui_ignora_payload_json", GUI, '    if not data.get("pnl_status"):\n        try:', '    if False:\n        try:'),
# v12
("M45_proiezione_netto_stimato", RC, 'projected_close = self._projected_daily_loss_breach(pnl_protection)', 'projected_close = self._projected_daily_loss_breach(pnl)'),
("M46_apply_netto_stimato", RC, 'self._apply_realized_pnl_without_mutating_bankroll(pnl_protection)', 'self._apply_realized_pnl_without_mutating_bankroll(pnl)'),
("M47_senza_margine_arrotondamento", PL, 'worst = gross * BETFAIR_ITALY_MAX_COMMISSION_PCT / 100.0 + ROUNDING_MARGIN_EUR', 'worst = gross * BETFAIR_ITALY_MAX_COMMISSION_PCT / 100.0'),
("M48_senza_commissione_massima", PL, 'worst = gross * BETFAIR_ITALY_MAX_COMMISSION_PCT / 100.0 + ROUNDING_MARGIN_EUR', 'worst = ROUNDING_MARGIN_EUR'),
("M49_fallback_netto_invece_di_zero", PL, "        return 0.0  # TECNICA_DELEGATA", "        return net  # TECNICA_DELEGATA"),
("M50_picco_non_ripristinato_apply", RC, '            self.risk_desk.equity_peak = peak_before\n', '            pass\n'),
("M51_hook_picco_attivo", RC, '        self.risk_desk.on_new_peak = None\n', '        pass\n'),
("M52_rollup_attivo_di_default", RC, 'CLEARED_MARKET_ROLLUP_VERIFIES = False', 'CLEARED_MARKET_ROLLUP_VERIFIES = True'),
("M53_stimato_non_conservativo", PL, '    if net <= 0.0 or allows_risk_increase(data):', '    if True:'),
# v13
("M54_nessun_blocco_pre_migrazione", RP, '    if reinit_required(db):\n        # v13', '    if False:\n        # v13'),
("M55_picco_mancante_ammesso", MG, '            out.append(f"{name}_mancante")', '            pass'),
("M56_copie_incoerenti_ammesse", MG, '        elif len(ok) == 2 and ok[0]["valore"] != ok[1]["valore"]:', '        elif False:'),
("M57_ordini_incerti_ignorati", MG, '    elif rep["ordini_incerti"]:', '    elif False:'),
("M58_digest_ignorato", MG, '    elif digest != rep["digest"]:', '    elif False:'),
("M59_migrazione_disarma_risk_stop", MG, '        db.save_settings({rp.LEDGER_KEY: new})          # commit 1/2', '        db.save_settings({rp.LEDGER_KEY: new, "risk_stop_state": ""})'),
("M60_illeggibile_ammesso", MG, '            out.append(f"{name}_illeggibile")', '            pass'),
("M61_una_tantum_rimossa", MG, '    elif rep["gia_migrata"] and not rep["migrazione_richiesta"]:\n        out.append("migrazione_gia_eseguita")\n    elif not rep["migrazione_richiesta"]:\n        out.append("migrazione_non_richiesta")', '    pass'),
("M62_audit_recupero_rimosso", MG, '        _audit(db, dict(base, esito="COMPLETATA_RECUPERATA"))', '        pass'),
("M63_operatore_non_obbligatorio", MG, '    if not str(operatore or "").strip():\n        motivo = "operatore_mancante"\n    elif', '    if'),
("M64_azzera_perdita_giorno", MG, '        db.save_settings({rp.LEDGER_KEY: new})          # commit 1/2', '        db.save_settings({rp.LEDGER_KEY: new, "daily_loss_state": ""})'),
("M65_commit_solo_db", MG, '            atomic_write_text(rp._ledger_file(db), new)  # commit 2/2', '            pass'),
("M66_audit_intento_non_scritto", MG, '    if not _audit(db, dict(base, esito="INTENTO", report=rep)):', '    if False:'),
# v14
("M67_includeitem_non_forzato", BC, '        body["includeItem"] = "ALL"\n', ''),
("M68_troncamento_ignorato", PR, '            reasons.append("storico_troncato_moreAvailable")', '            pass'),
("M69_copertura_periodo_ignorata", PR, '            reasons.append("intervallo_non_copre_periodo_rilevante")  # massimo parziale', '            pass'),
("M70_catena_non_verificata", PR, '                reasons.append(f"catena_incoerente:{r[\'refId\']}")', '                pass'),
("M71_saldo_attuale_non_confrontato", PR, '            reasons.append("saldo_finale_diverso_dal_saldo_attuale")', '            pass'),
("M72_picco_al_ribasso", PR, '    new = max(v for v in (prop["picco_proposto"], stored) if v is not None)', '    new = prop["picco_proposto"]'),
("M73_sha_ignorato", PR, '    elif sha_digitato != prop["sha256"]:', '    elif False:'),
("M74_bloccata_ratificabile", PR, '    elif prop["stato"] != "VERIFICATA_DA_RATIFICARE" or prop["picco_proposto"] is None:', '    elif False:'),
("M75_manomissione_non_rilevata", PR, '    if _sha(prop) != sha or _sha(data["pagine_grezze"]) != prop["evidenze_sha256_totale"]:', '    if False:'),
("M76_copia_illeggibile_sovrascritta", PR, '    if err:\n        _audit(db, {"esito": "RATIFICA_RIFIUTATA", "motivo": err', '    if False:\n        _audit(db, {"esito": "RATIFICA_RIFIUTATA", "motivo": err'),
("M77_saldo_attuale_come_picco", PR, '        elif cur > peak:\n            peak = cur', '        else:\n            peak = cur'),
("M78_duplicati_ammessi", PR, '        if key in seen:', '        if False:'),
("M79_pagine_non_contigue_ammesse", PR, '            reasons.append(f"pagina_{i}_fromRecord_non_contiguo")', '            pass'),
("M80_owner_non_obbligatorio", PR, '    if not str(owner or "").strip() or not str(owner_ref or "").strip():', '    if False:'),
]
T = ['tests'] if os.environ.get('FULL') else ['tests/close02', 'tests/core', 'tests/integration', 'tests/services', 'tests/unit']
only = set(sys.argv[1:])
for name, f, a, b in M:
    if only and name.split('_')[0] not in only:
        continue
    s = open(f).read()
    if s.count(a) != 1:
        print('==', name); print('PATTERN_NON_UNICO count=%d' % s.count(a), flush=True); continue
    shutil.copy(f, '/tmp/mbak_all')
    open(f, 'w').write(s.replace(a, b, 1))
    try:
        r = subprocess.run([P, '-m', 'pytest', '-q', '-x', '-p', 'no:cacheprovider', '-o', 'addopts=', '-n', '8'] + T,
                           capture_output=True, text=True)
        last = (r.stdout.strip().splitlines() or ['?'])[-1]
        print('==', name); print(last, flush=True)
    finally:
        shutil.copy('/tmp/mbak_all', f)

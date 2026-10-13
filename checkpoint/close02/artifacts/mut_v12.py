import subprocess,shutil,sys
P='/workspace/pickfair/venv311/bin/python'
RC='core/runtime_controller.py';PL='core/pnl_label.py'
M=[("M39_commissione_su_ogni_riga",RC,'if mc is not None and mc["last_bet"] == bet_id:','if mc is not None:'),
("M45_proiezione_netto_stimato",RC,'projected_close = self._projected_daily_loss_breach(pnl_protection)','projected_close = self._projected_daily_loss_breach(pnl)'),
("M46_apply_netto_stimato",RC,'self._apply_realized_pnl_without_mutating_bankroll(pnl_protection)','self._apply_realized_pnl_without_mutating_bankroll(pnl)'),
("M47_senza_margine_arrotondamento",PL,'worst = gross * BETFAIR_ITALY_MAX_COMMISSION_PCT / 100.0 + ROUNDING_MARGIN_EUR','worst = gross * BETFAIR_ITALY_MAX_COMMISSION_PCT / 100.0'),
("M48_senza_commissione_massima",PL,'worst = gross * BETFAIR_ITALY_MAX_COMMISSION_PCT / 100.0 + ROUNDING_MARGIN_EUR','worst = ROUNDING_MARGIN_EUR'),
("M49_fallback_netto_invece_di_zero",PL,"        return 0.0  # TECNICA_DELEGATA","        return net  # TECNICA_DELEGATA"),
("M50_picco_non_ripristinato",RC,'            self.risk_desk.equity_peak = peak_before\n','            pass\n'),
("M51_hook_picco_attivo",RC,'        self.risk_desk.on_new_peak = None\n','        pass\n'),
("M52_rollup_attivo_di_default",RC,'CLEARED_MARKET_ROLLUP_VERIFIES = False','CLEARED_MARKET_ROLLUP_VERIFIES = True'),
("M53_stimato_non_conservativo",PL,'    if net <= 0.0 or allows_risk_increase(data):','    if True:'),
]
T=sys.argv[1:]
for name,f,a,b in M:
    s=open(f).read(); assert s.count(a)>=1,name; shutil.copy(f,'/tmp/mbak')
    open(f,'w').write(s.replace(a,b,1))
    try:
        r=subprocess.run([P,'-m','pytest','-q','-x','-p','no:cacheprovider','-o','addopts=','-n','4']+T,capture_output=True,text=True)
        print('==',name);print(r.stdout.strip().splitlines()[-1],flush=True)
    finally: shutil.copy('/tmp/mbak',f)

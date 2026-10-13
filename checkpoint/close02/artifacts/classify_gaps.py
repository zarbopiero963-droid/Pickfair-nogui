import json,re,collections
R=json.load(open('findings_register.json'))
HARD={'max_daily_loss','max_open_exposure','max_session_loss','max_exposure_stop','max_drawdown_hard_stop_pct'}
BOOL=('auto_green_delay_enabled','allow_recovery','anti_duplication_enabled','liquidity_guard_enabled','liquidity_warning_only','max_win_warning_only','profit_epsilon_enabled')
root={}
for g in R['candidate_gaps']:
    d=g['descrizione']; i=g['id']
    if i.startswith('CL02-BF'):
        n=int(i[-3:])
        cls={1:('REAL','root BF-LIST: test_bf001_*'),2:('DUPLICATE','di CL02-BF-001 (stessa radice)'),3:('DUPLICATE','di CL02-BF-001'),4:('DUPLICATE','di CL02-BF-001'),
             5:('REAL','test_bf005_*'),6:('REAL','test_bf006_* + test_get_current_orders_missing_current_orders_is_malformed'),
             7:('NOT_DEMONSTRATED','SIM senza anagrafica mercati: sconosciuto != invalido; fix tentato rompeva 30+ test del contratto F8 (#453); serve decisione; xfail strict test_bf007'),
             8:('REAL','test_bf008_*'),9:('ALREADY_COVERED','order_manager.replace verifica instructionReports[0].status ⇒ REPLACE_REJECTED'),
             10:('REAL','test_bf010_*')}[n]
    else:
        key=re.search(r'roserpina\.(\w+)',d).group(1)
        if "'-1' accettato" in d:
            cls=('ALREADY_COVERED','limite -1 ⇒ superato sempre ⇒ blocco (fail-closed)') if key in HARD else ('BLOCKED_PENDING_OWNER_DOMAIN','dominio non definito da fonte (Q1/Q2)')
        elif key in HARD:
            cls=('ALREADY_COVERED',"corrotto ⇒ NaN ⇒ cap/stop non valido nega (PR27); '' ⇒ None (per max_open_exposure blocca per D2)")
        else:
            kind='enum' if key=='risk_profile' else 'bool' if key in BOOL else 'int' if key in ('table_count','max_recovery_tables') else 'float'
            if kind not in root: root[kind]=i; cls=('REAL',f'root LOAD-{kind}: test_rsr_corrupt_*; = finding S (_f/_b) Q1/Q2')
            else: cls=('DUPLICATE',f'di {root[kind]}')
    g['classificazione'],g['nota']=cls
    g['stato']={'REAL':'OPEN — RED riprodotto; fix in proposta v3','DUPLICATE':'OPEN (segue la radice)','ALREADY_COVERED':'COPERTO (nessuna modifica)','NOT_DEMONSTRATED':'OPEN — da decidere','BLOCKED_PENDING_OWNER_DOMAIN':'BLOCKED'}[cls[0]]
R['env_findings']=[{'id':'CL02-ENV-001','descrizione':'Fallback Tk senza customtkinter: CTkComboBox(command=) su ttk.Combobox ⇒ TclError "-command" (mini_gui.py:182-184, chiamata :919-925); poi opzioni solo-CTk (progress_color) e Tabview.set mancanti','dipendenza_ambiente':'customtkinter assente dal venv (solo requirements-build-linux) + DISPLAY=:7','repro':'tests/close02/test_close02_tk_fallback.py RED a 028cee9; senza DISPLAY 1 passed 1 skipped','proposta':'command→<<ComboboxSelected>>, opzioni CTk scartate; pr04_quater[True] skip BLOCKED_ENV senza customtkinter','stato':'OPEN — proposta'}]
json.dump(R,open('findings_register.json','w'),ensure_ascii=False,indent=1)
print(collections.Counter(g['classificazione'] for g in R['candidate_gaps']))

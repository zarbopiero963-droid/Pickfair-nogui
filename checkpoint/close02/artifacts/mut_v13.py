import subprocess,shutil,sys
P='/workspace/pickfair/venv311/bin/python'
RP='core/risk_persistence.py';MG='core/pre_v9_migration.py'
M=[("M54_nessun_blocco_pre_migrazione",RP,'    if reinit_required(db):\n        # v13','    if False:\n        # v13'),
("M55_picco_mancante_ammesso",MG,'            out.append(f"{name}_mancante")','            pass'),
("M56_copie_incoerenti_ammesse",MG,'        elif len(ok) == 2 and ok[0]["valore"] != ok[1]["valore"]:','        elif False:'),
("M57_ordini_incerti_ignorati",MG,'    elif rep["ordini_incerti"]:','    elif False:'),
("M58_digest_ignorato",MG,'    elif digest != rep["digest"]:','    elif False:'),
("M59_migrazione_disarma_risk_stop",MG,'        db.save_settings({rp.LEDGER_KEY: new})          # commit 1/2','        db.save_settings({rp.LEDGER_KEY: new, "risk_stop_state": ""})'),
("M60_illeggibile_ammesso",MG,'            out.append(f"{name}_illeggibile")','            pass'),
("M61_una_tantum_rimossa",MG,'    elif rep["gia_migrata"] and not rep["migrazione_richiesta"]:\n        out.append("migrazione_gia_eseguita")\n    elif not rep["migrazione_richiesta"]:\n        out.append("migrazione_non_richiesta")','    pass'),
("M62_audit_recupero_rimosso",MG,'        _audit(db, dict(base, esito="COMPLETATA_RECUPERATA"))','        pass'),
("M63_operatore_non_obbligatorio",MG,'    if not str(operatore or "").strip():\n        motivo = "operatore_mancante"\n    elif','    if'),
("M64_azzera_perdita_giorno",MG,'        db.save_settings({rp.LEDGER_KEY: new})          # commit 1/2','        db.save_settings({rp.LEDGER_KEY: new, "daily_loss_state": ""})'),
("M65_commit_solo_db",MG,'            atomic_write_text(rp._ledger_file(db), new)  # commit 2/2','            pass'),
("M66_audit_intento_non_scritto",MG,'    if not _audit(db, dict(base, esito="INTENTO", report=rep)):','    if False:'),
]
T=sys.argv[1:]
for name,f,a,b in M:
    s=open(f).read(); assert s.count(a)>=1,name; shutil.copy(f,'/tmp/mbak13')
    open(f,'w').write(s.replace(a,b,1))
    try:
        r=subprocess.run([P,'-m','pytest','-q','-p','no:cacheprovider','-o','addopts=','-n','4']+T,capture_output=True,text=True)
        print('==',name);print(r.stdout.strip().splitlines()[-1],flush=True)
    finally: shutil.copy('/tmp/mbak13',f)

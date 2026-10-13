"""L2b: L2v3 + fixture strutturate (config) + sonde SAVE per scenario (028cee9, solo impl)."""
import json
B='/workspace/pickfair/bf2/close02/'
L=json.load(open(B+'record_specific_L2.json')); PR={v['key']:(k,v) for k,v in json.load(open(B+'probe_rsr.json')).items()}
PRS=json.load(open(B+'probe_restart.json')); PSS=json.load(open(B+'probe_save_same.json'))
for y in L:
    k=y['record_specific'].get('key') or (y.get('control') or {}).get('key'); i,p=PR.get(k,(None,{}))
    for c in y['cases']:
        c['fixtures']['config']={k:str(y['record_specific'].get('fixture')),'db':'sqlite temporaneo reale'}
        if y['phase']=='SAVE' and i:
            base=c['norm_vs_impl']['impl_attuale'].split(' | sonda SAVE')[0]
            pr={'positive':{'stored_raw':p.get('stored_raw'),'load':p.get('load_same')},'negative':{'crash_a_meta_salvataggio':PRS.get(i,{}).get('crash'),'stato_misto':PRS.get(i,{}).get('stato_misto')},'boundary':PSS.get(i)}[c['kind']]
            c['norm_vs_impl']['impl_attuale']=base+' | sonda SAVE 028cee9 (NON requisito, per scenario): '+json.dumps(pr,ensure_ascii=False)
    y['record_specific']['batch']='close02-L2b'
json.dump(L,open(B+'record_specific_L2b.json','w'),ensure_ascii=False,indent=1); print(len(L))

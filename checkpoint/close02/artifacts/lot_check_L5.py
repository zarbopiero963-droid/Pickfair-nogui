"""Prontezza 'livello L1' per i record L5 (stessi 6 criteri di lot_check_L1)."""
import json,sys,re,collections
sys.path.insert(0,'/workspace/pickfair/bf2/repair01'); from semantic_preflight import check
L=json.load(open('/workspace/pickfair/bf2/close02/record_specific_L5.json')); rows=[]
for x in L:
    src=x['record_specific']['sources']; cs=x['cases']
    c1=bool(src.get('source_quote_verbatim')) and bool(re.search(r'[0-9a-f]{64}',str(src.get('R1_source',''))+str(x['R1'].get('sha256',''))+str((x.get('control') or {}).get('src_sha',''))))
    c2=all(c.get('oracle') and all(f['provenance'] in ('REQUIREMENT','OWNER_DECISION','TECNICA_DELEGATA') for f in c['oracle']) for c in cs)
    c3=all((c.get('fixtures') or {}).get('config') for c in cs)
    neg=[c for c in cs if c['kind']=='negative']; c4=bool(neg) and all(c.get('oracle') for c in neg)
    c5=not check(x)
    c6=all('osservato 028cee9' in c['norm_vs_impl']['impl_attuale'] for c in cs)
    # C7 specificita': testo dei casi diverso da quello di un altro record della stessa fase
    #    dopo aver tolto chiave e valori di test (se identico = template di fase condiviso)
    k=x['record_specific']['key']; v=x['record_specific']['test_values']
    def sig(r):
        t=json.dumps([[c.get('pre'),c.get('input'),[f['text'] for f in c.get('oracle',[])]] for c in r['cases']],ensure_ascii=False)
        t=t.replace(r['record_specific']['key'],'K')
        for vv in r['record_specific']['test_values']: t=t.replace(vv,'V')
        return re.sub(r'RSR-\d+','R',t)
    c7=sum(sig(o)==sig(x) for o in L if o['phase']==x['phase'])==1
    ok=all((c1,c2,c3,c4,c5,c6,c7))
    rows.append(dict(id=x['difference_id'],phase=x['phase'],fonti_complete=c1,oracoli_tutti_i_casi=c2,fixture_concrete=c3,negativi=c4,preflight_C1_C15=c5,sonda_impl_tutti_i_casi=c6,specifico_non_template=c7,READY_L1_LEVEL=ok))
json.dump(rows,open('/workspace/pickfair/bf2/close02/lot_check_L5.json','w'),indent=1)
print('READY',sum(r['READY_L1_LEVEL'] for r in rows),'/',len(rows))
print(collections.Counter((r['phase'],r['READY_L1_LEVEL']) for r in rows))
print({k:sum(not r[k] for r in rows) for k in ('fonti_complete','oracoli_tutti_i_casi','fixture_concrete','negativi','preflight_C1_C15','sonda_impl_tutti_i_casi','specifico_non_template')})

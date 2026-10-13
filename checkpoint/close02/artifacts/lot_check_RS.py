"""Prontezza 'livello L1' per i record L5 (stessi 6 criteri di lot_check_L1)."""
import json,sys,re,collections
sys.path.insert(0,'/workspace/pickfair/bf2/repair01'); from semantic_preflight import check
IN=sys.argv[1] if len(sys.argv)>1 else 'L5'
L=json.load(open(f'/workspace/pickfair/bf2/close02/record_specific_{IN}.json')); rows=[]
for x in L:
    src=x['record_specific'].get('sources') or {'R1_source':str(x['R1'].get('source'))+' '+str(x['R1'].get('sha256')),'source_quote_verbatim':' | '.join(c['text'] for c in x['R1']['primary_clauses'])}; cs=x['cases']
    c1=bool(src.get('source_quote_verbatim')) and bool(re.search(r'[0-9a-f]{64}',str(src.get('R1_source',''))+str(x['R1'].get('sha256',''))+str((x.get('control') or {}).get('src_sha',''))))
    c2=all(c.get('oracle') and all(f['provenance'] in ('REQUIREMENT','OWNER_DECISION','TECNICA_DELEGATA') for f in c['oracle']) for c in cs)
    c3=all((c.get('fixtures') or {}).get('config') or (c.get('fixtures') or {}).get('scenario') for c in cs)
    neg=[c for c in cs if c['kind']=='negative']; c4=bool(neg) and all(c.get('oracle') for c in neg)
    c5=not check(x)
    c6=all(('osservato 028cee9' in c['norm_vs_impl']['impl_attuale'] or 'sonda 028cee9' in c['norm_vs_impl']['impl_attuale']) and 'non sondato' not in c['norm_vs_impl']['impl_attuale'] and 'non disponibile' not in c['norm_vs_impl']['impl_attuale'] and 'assente in RoserpinaConfig' not in c['norm_vs_impl']['impl_attuale'] for c in cs)
    # C7 specificita': testo dei casi diverso da quello di un altro record della stessa fase
    #    dopo aver tolto chiave e valori di test (se identico = template di fase condiviso)
    def sig(r):
        t=json.dumps([[c.get('pre'),c.get('input'),[f['text'] for f in c.get('oracle',[])]] for c in r['cases']],ensure_ascii=False)
        t=t.replace(r['record_specific'].get('key') or '§§','K')
        for vv in (r['record_specific'].get('test_values') or [str(r['record_specific'].get('fixture',''))]): t=t.replace(vv,'V')
        t=t.replace(r['control'].get('name',''),'N')
        return re.sub(r'RSR-\d+','R',t)
    c7=True if 'key' not in x['record_specific'] else 0==sum(sig(o)==sig(x) for o in L if o['phase']==x['phase'] and (o.get('control') or {}).get('id')!=(x.get('control') or {}).get('id'))
    # C8 la decisione della chiave deve venire da una clausola (non dal solo titolo)
    c8=not any(f['provenance']=='TECNICA_DELEGATA' and 'titolo del controllo' in f['source'] for c in cs for f in c.get('oracle',[]))
    # C9 la sonda deve esercitare lo scenario di OGNI caso (non solo citare la chiave)
    PROBED={'LOAD':{'positive','negative','boundary'},'RESTART':{'positive','negative','boundary'},'FAILSAFE':{'negative'},'RACE':{'positive','boundary'},'SAVE':{'positive','negative','boundary'}}
    MARK={'SAVE':'sonda SAVE 028cee9','RESTART':'restart_valore'}
    c9=all(('O1' not in c.get('covers',[])) and c['kind'] in PROBED.get(x['phase'],set()) and MARK.get(x['phase'],'') in c['norm_vs_impl']['impl_attuale'] for c in cs) if x['requirement_id'].startswith('PF-RSR') else c6
    ok=all((c1,c2,c3,c4,c5,c6,c7,c8,c9))
    import re as _re; _rx=_re.compile(r'SUCCESS|\bUI\b|(?i:\bmessagg|\blog\b|\baudit|\bevento|\bnotific|\btoast)')
    c10=not any(_rx.search(c['text']) for c in x['R1']['primary_clauses']+x['R1']['derived_clauses']) or any('sonda_UI' in c['norm_vs_impl']['impl_attuale'] for c in cs)
    ok=ok and c10
    import re as _r2
    _src=x.get('sources') or []
    c1s=bool(_src) and all(_r2.fullmatch(r'[0-9a-f]{64}',str(z.get('sha256',z.get('sha','')) if isinstance(z,dict) else '')) for z in _src)
    _cl=[c['cid'] for c in x['R1']['primary_clauses']+x['R1']['derived_clauses']]
    _per={k:tuple(sorted(f['text'] for c in cs if k in c.get('covers',[]) for f in c.get('oracle',[]))) for k in _cl}
    c11=len(_cl)<=1 or len(set(_per.values()))==len(_per)
    _subj=' '.join(c['text'] for c in x['R1']['primary_clauses']+x['R1']['derived_clauses'])+' '+str(x.get('requirement_id',''))
    _money=bool(_r2.search(r'€|import|stake|liabil|cash|saldo|bankroll|exposure_eur|cap',_subj,_r2.I))
    c12=_money or not any('importi €' in str(c.get('fixtures',{}).get('precision','')) for c in cs)
    c13=all(c.get('verdetto_osservato_vs_oracolo') in ('CONFORME','NON_CONFORME') for c in cs)
    ok=ok and c1s and c11 and c12 and c13
    rows.append(dict(fonti_sha_verificate=c1s,oracolo_distinto_per_clausola=c11,no_template_money=c12,verdetto_confronto=c13,clausole_UI_sondate=c10,id=x['difference_id'],phase=x['phase'],fonti_complete=c1,oracoli_tutti_i_casi=c2,fixture_concrete=c3,negativi=c4,preflight_C1_C15=c5,sonda_impl_tutti_i_casi=c6,specifico_non_template=c7,decisione_da_fonte=c8,sonda_per_scenario=c9,READY_L1_LEVEL=ok))
json.dump(rows,open(f'/workspace/pickfair/bf2/close02/lot_check_{IN}.json','w'),indent=1)
print('READY',sum(r['READY_L1_LEVEL'] for r in rows),'/',len(rows))
print(collections.Counter((r['phase'],r['READY_L1_LEVEL']) for r in rows))
print({k:sum(not r[k] for r in rows) for k in ('fonti_complete','oracoli_tutti_i_casi','fixture_concrete','negativi','preflight_C1_C15','sonda_impl_tutti_i_casi','specifico_non_template','decisione_da_fonte','sonda_per_scenario')})

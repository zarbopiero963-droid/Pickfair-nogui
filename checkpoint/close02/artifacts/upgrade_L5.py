"""close02 L5 — P0 RSR resi specifici per chiave (owner 13/10 11:37).
- fixture dalla clausola e dalla chiave: valori di TEST per unità (non owner, non default);
- scenari/calcoli monetari del template rimossi se la chiave non ha soggetto (C15);
- oracoli: REQUIREMENT con clausola verbatim per OGNI caso; OWNER_DECISION solo se
  il record ne ha il soggetto (stesse regex di C15);
- impl_attuale: sonda probe_rsr.json (028cee9) solo come comportamento attuale."""
import json,copy,re,sys,collections
B='/workspace/pickfair/bf2/'
sys.path.insert(0,B+'repair01'); import semantic_preflight as SP
CTL={c['key']:c for c in json.load(open(B+'close02/rsr_controls.json'))}
PRB={v['key']:v for v in json.load(open(B+'close02/probe_rsr.json')).values()}
L3=json.load(open(B+'close02/record_specific_L3.json'))
L4=[y for y in json.load(open(B+'close02/record_specific_L4.json')) if y['record_specific']['priority']=='P0' and y['requirement_id'].startswith('PF-RSR')]
def frag(t,p,s): return {'text':t,'provenance':p,'source':s}
def vals(c):
    u=(c.get('unit') or '').lower(); k=c['key']
    if k.endswith(('_enabled','_only')) or k.startswith('roserpina.allow'): return ('true','false')
    if u=='%': return ('12.5','7.5')
    if '€' in u or 'eur' in u: return ('25.00','15.00')
    if k.endswith(('_count','_tables')): return ('3','2')
    if k.endswith('_sec'): return ('2.0','4.0')
    if k.endswith('risk_profile'): return ('CONSERVATIVE','AGGRESSIVE')
    return ('1.20','0.90')
PH={
 'APPLY':{'positive':("{k}={v1} salvato (versione N)","segnale valido: mercato 1.230000001, selezione 47999, BACK"),
          'negative':("{k}={v1} al punto decisionale; salvato {k}={v2} (versione N+1) prima dell'effetto","stesso segnale"),
          'boundary':("{k} assente o corrotto ('abc') nel momento dell'effetto","stesso segnale")},
 'RACE':{'positive':("{k}={v1} (versione N)","salvataggi concorrenti {k}={v2} da GUI e headless + 2 segnali su 2 tavoli"),
         'negative':("{k}={v1} (versione N)","salvataggio {k}={v2} durante una prenotazione aperta"),
         'boundary':("{k}={v1} (versione N)","due salvataggi nello stesso istante con versione attesa N")},
 'AUDIT':{'positive':("{k}={v1} (versione N), correlation_id c-1","decisione su segnale con {k}"),
          'negative':("sorgente con credenziali nel contesto, {k}={v1}","decisione registrata"),
          'boundary':("{k} cambiato {v1}→{v2} durante la decisione","decisione e recovery")},
 'FAILSAFE':{'positive':("{k}={v1} valido, book disponibile","segnale valido"),
             'negative':("{k} memorizzato come 'abc' (corrotto) o book/provider assente","segnale valido"),
             'boundary':("esito ordine ambiguo (timeout dopo invio) con {k}={v1}","nuovo segnale sullo stesso mercato")},
 'PARITY':{'positive':("{k}={v1}","stesso segnale BACK e LAY da GUI, headless e MCP"),
           'negative':("{k}={v1}, modalità SIM","stesso segnale in LIVE"),
           'boundary':("{k}={v1}","LAY con quota 3.00 (liability ≠ stake)")},
 'OBSERVABLE':{'positive':("{k}={v1} salvato","lettura dello stato da GUI/headless"),'negative':("{k} corrotto ('abc')","lettura dello stato"),'boundary':("{k} assente","lettura dello stato")},
 'S1':{'positive':("{k}={v1}","uso del campo secondo la sua semantica"),'negative':("{k}={v2} non coerente con il contesto","uso del campo"),'boundary':("{k} al bordo del dominio (dominio PENDING_OWNER: nessun bordo inventato)","uso del campo")},
 'LOAD':{'positive':("{k}={v1} persistito","ricarica"),'negative':("{k}='abc' persistito","ricarica"),'boundary':("{k} assente","ricarica")},
 'RESTART':{'positive':("{k}={v1} persistito","riavvio processo"),'negative':("{k}='abc' persistito","riavvio"),'boundary':("{k}={v1}, crash durante il salvataggio di {v2}","riavvio")},
}
KT={'positive':"con {k}={v1} la clausola {cid} è soddisfatta usando {v1} e la versione corrente; esito riportato come successo",
    'negative':"scenario che viola la clausola {cid} per {k}: rilevato, nessun effetto monetario non autorizzato, esito non riportato come successo",
    'boundary':"al bordo per {k} la clausola {cid} vale ancora: assente distinto da corrotto, nessun valore fantasma, nessun effetto non autorizzato"}
def impl(k,phase):
    p=PRB.get(k)
    if not p: return "sonda non disponibile per la chiave: da verificare"
    if phase in ('LOAD','RESTART','FAILSAFE','OBSERVABLE','S1'):
        return "osservato 028cee9 (sonda, NON requisito): "+json.dumps({x:p.get(x) for x in ('stored_raw','load_same','load_restart','corrupt_abc','corrupt_nan','corrupt_inf','corrupt_empty')},ensure_ascii=False)
    return "sonda probe_rsr non copre la fase "+phase+": da verificare nel codice"
SKIP=[];OUT=[];stats=collections.Counter()
for x in L3+L4:
    y=copy.deepcopy(x); k=y['control']['key']; c=CTL.get(k,{'key':k}); v1,v2=vals(c)
    if y['phase'] not in PH: stats['fase_senza_template']+=1; SKIP.append(y['difference_id']+':'+y['phase']); continue
    cl={q['cid']:q['text'] for q in y['R1']['primary_clauses']+y['R1'].get('derived_clauses',[])}
    subj=(" ".join(cl.values())+" "+json.dumps(y['control'],ensure_ascii=False)+" "+y['requirement_id']).lower()
    money=bool(SP.CAP_RX.search(subj)); moneyfx=bool(SP.MONEY_RX.search(subj))
    for cs in y['cases']:
        kind=cs.get('kind','positive'); pre,inp=PH[y['phase']][kind]
        F=dict(k=k,v1=v1,v2=v2); owner_case='O1' in cs.get('covers',[])
        fx=cs.get('fixtures') or {}
        if not owner_case:
            cs['pre']=pre.format(**F)+('; stato di rischio: '+(cs['pre'] if money else re.sub(r';?\s*totale capitale a rischio[^;]*','',cs['pre'])) if cs.get('pre') else '')+('; importo stake 2.00 € registrato' if moneyfx and y['phase']=='AUDIT' else '')
            cs['input']=inp.format(**F)+(' — '+cs['input'] if money and cs.get('input') else '')
            if not moneyfx:
                if fx.get('amounts') or str(fx.get('calc','n/a'))!='n/a': stats['fixture_monetarie_rimosse']+=1
                fx.update(amounts=[],calc='n/a',limit='n/a')
            elif not money and str(fx.get('calc','n/a'))!='n/a':
                stats['calcolo_cap_rimosso']+=1; fx.update(calc='n/a',limit='n/a')
        fx['config']={k:v1,'versione':'N'} | ({'nuovo_valore':v2} if '{v2}' in pre+inp else {}) | ({'corrotto':'abc'} if "'abc'" in pre+inp else {})
        fx['valori_di_test']='valori di TEST per unità, non valori owner né default'
        cs['fixtures']=fx
        fr=[frag(KT[kind].format(cid=cid,**F),'REQUIREMENT',f'{cid} «{cl[cid]}»') for cid in cs.get('covers',[]) if cid in cl]
        if owner_case or (y.get('q5_drawdown') and SP.DD_RX.search(subj)):
            fr.append(frag("drawdown: soglia effettiva = min(configurata validata, 10.00 €) in SIM e LIVE; stop a perdita ≥ soglia",'OWNER_DECISION','#426/6141902809 D1; #426/6141136157 Q5'))
        if money and kind!='positive' and y['phase'] in ('APPLY','FAILSAFE'): fr.append(frag("cap assente/non valido ⇒ blocco nuovo rischio; esposizione = cap ammessa, oltre bloccata, Decimal",'OWNER_DECISION','#426/6141902809 D2; #426/6141450045 Q3'))
        if y['phase']=='PARITY' and SP.LAY_RX.search(subj): fr.append(frag("LAY: rischio = liability stake×(quota−1)",'OWNER_DECISION','#426/6141450045 Q4'))
        if not fr: stats['caso_senza_oracolo']+=1
        cs['oracle']=fr
        base=re.split(r' — ORACOLO',cs['expected'])[0]
        if not owner_case:
            base='; '.join(f'requisito «{cl[cid]}»' for cid in cs.get('covers',[]) if cid in cl)
            if money and 'ammess' in str(fx.get('calc','')): base+='; '+str(fx['calc'])+' (ammesso)'
        cs['expected']=base+' — ORACOLO: '+'; '.join(f['text'] for f in fr)
        cs['norm_vs_impl']['impl_attuale']=impl(k,y['phase'])
        cs.pop('observed_028cee9',None)
    y['record_specific']={'batch':'close02-L5','priority':'P0','provenance_rule':'Opzione B + C15','level':'RECORD_SPECIFIC_PER_CHIAVE','key':k,'test_values':[v1,v2],'cap_subject':money,'money_subject':moneyfx,
        'sources':{'R1_source':y['R1']['source'],'source_quote_verbatim':y['R1'].get('source_quote') or ' | '.join(cl.values())}}
    OUT.append(y)
json.dump(OUT,open(B+'close02/record_specific_L5.json','w'),ensure_ascii=False,indent=1)
print(SKIP);print(len(OUT),dict(stats),collections.Counter(y['phase'] for y in OUT))

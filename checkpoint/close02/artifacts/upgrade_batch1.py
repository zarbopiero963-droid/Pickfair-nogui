"""close02 — upgrade record-specific lotto 1 (P0, record BF con riferimenti codice).
Per ogni riferimento file:riga (pin 618884e) risolve la def che contiene la riga,
la ritrova a 028cee9, e sostituisce gli input generici con input specifici
dell'operazione Betfair del record. Nessun valore owner inventato."""
import json,re,copy,hashlib,subprocess
B='/workspace/pickfair/bf2/'
OLD='/tmp/s618/'; NEW=B+'close02/wt_base/'
M={x['id']:x for x in json.load(open(B+'repair01/master1035.json'))}
R=json.load(open(B+'repair01/rebuilt570.json'))
def enclosing(path,line):
    L=open(path).read().split('\n'); 
    for i in range(min(line,len(L))-1,-1,-1):
        m=re.match(r'\s*(?:async\s+)?def\s+(\w+)',L[i])
        if m: return m.group(1),i+1
    return None,None
def find_def(path,name):
    for i,l in enumerate(open(path).read().split('\n')):
        if re.match(r'\s*(?:async\s+)?def\s+%s\b'%name,l): return i+1
# input specifici per fase (operazione = nome API Betfair dalla clausola P1)
NEG_AUTH='risposta Betfair con errore di sessione (INVALID_SESSION_INFORMATION / NO_SESSION) o app key non valida'
out=[];log=[]
for x in R:
    if M.get(x['difference_id'],{}).get('priority')!='P0' or x['hand_built']: continue
    refs=re.findall(r'\[([\w/]+\.py):(\d+)\]',x['R1'].get('source_quote') or '')
    if not refs: continue
    op=re.match(r'BF-\d+\s+(\w+)',x['R1']['primary_clauses'][0]['text']).group(1)
    anchors=[]
    for f,l in refs:
        fn,dl=enclosing(OLD+f,int(l)); nl=find_def(NEW+f,fn) if fn else None
        anchors.append({'ref_618884e':f'{f}:{l}','def':fn,'def_line_618884e':dl,'def_line_028cee9':nl,'present_028cee9':nl is not None})
    y=copy.deepcopy(x)
    a_txt='; '.join(f"{a['ref_618884e']} → def {a['def']} (028cee9: {a['ref_618884e'].split(':')[0]}:{a['def_line_028cee9']})" if a['present_028cee9'] else f"{a['ref_618884e']} → def {a['def']} NON TROVATA a 028cee9" for a in anchors)
    for c in y['cases']:
        g=c['input']
        if 'P1' not in c.get('covers',[]):
            continue
        if c['kind']=='positive':
            c['input']=f"chiamata {op} con sessione valida e app key presente (fixture SIM / doppio del client)"
        elif c['kind']=='negative':
            c['input']=f"chiamata {op}: "+(NEG_AUTH if y['phase']=='AUTH' else 'risposta di errore della controparte (eccezione / APINGException) o payload non conforme')
        elif c['kind']=='boundary':
            c['input']=f"chiamata {op} con risultato vuoto (lista vuota) o sessione che scade durante la chiamata"
        c['norm_vs_impl']['impl_attuale']=f"ancore verificate: {a_txt} — comportamento da verificare con test, non certificato"
        if c['input']!=g: log.append(c['case_id'])
    y['record_specific']={'batch':'close02-L1','anchors':anchors,'note':'input specifici per operazione; expected invariato (verbatim); nessun valore owner introdotto'}
    out.append(y)
json.dump(out,open(B+'close02/record_specific_L1.json','w'),ensure_ascii=False,indent=1)
print(len(out),'record',len(log),'casi con input specifico', sum(1 for y in out for a in y['record_specific']['anchors'] if not a['present_028cee9']),'ancore mancanti')

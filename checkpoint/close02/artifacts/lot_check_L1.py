"""Controllo semantico del lotto Betfair L1 (22 record) prima della richiesta di revisione."""
import json,sys,re
sys.path.insert(0,'/workspace/pickfair/bf2/repair01')
from semantic_preflight import check
L=json.load(open('/workspace/pickfair/bf2/close02/record_specific_L1.json'))
rows=[];ok_all=True
for x in L:
    src=x['record_specific'].get('sources',{})
    c1=bool(src.get('source_quote_verbatim')) and 'sha256' in str(src.get('R1_source',''))
    p1=[c for c in x['cases'] if 'P1' in c.get('covers',[])]
    c2=all(c.get('oracle') and all(f['provenance'] in ('REQUIREMENT','OWNER_DECISION','TECNICA_DELEGATA') for f in c['oracle']) for c in p1)
    c3=all(c['fixtures'].get('stub_payload') for c in p1)
    neg=[c for c in p1 if c['kind']=='negative']
    c4=bool(neg) and all(c['fixtures']['stub_payload'] and c.get('oracle') for c in neg)
    c5=not check(x)
    c6=all('osservato 028cee9' in c['norm_vs_impl']['impl_attuale'] for c in p1) and not any(re.search(r'\b(ritorna|solleva)\b',f['text']) for c in p1 for f in c['oracle'])
    ok=all((c1,c2,c3,c4,c5,c6)); ok_all&=ok
    rows.append({'id':x['difference_id'],'fonti_complete':c1,'oracoli_da_fonte':c2,'fixture_concrete':c3,'controlli_negativi':c4,'preflight_C1_C14':c5,'osservato_solo_in_impl':c6,'OK':ok})
json.dump(rows,open('/workspace/pickfair/bf2/close02/lot_check_L1.json','w'),indent=1)
print(sum(r['OK'] for r in rows),'/',len(rows),'OK')
for r in rows:
    if not r['OK']: print(r)

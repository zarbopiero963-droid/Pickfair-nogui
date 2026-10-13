"""Test negativi SAVE: ogni rilievo re-iniettato deve essere intercettato (preflight) e le omissioni dal controllo indipendente."""
import json,copy,os,subprocess,sys
os.chdir('/workspace/pickfair/bf2/close02')
from preflight_L8b import check,probes
R=json.load(open('record_specific_L8b.json')); PP,DD=probes()
def rec(i='DIFF-PF-RSR-03-SAVE'): return copy.deepcopy(next(r for r in R if r['difference_id']==i))
def run(r): return check(r,PP('RSR-'+r['difference_id'].split('-')[3]),DD)
def case(r,s): return next(c for c in r['cases'] if c['cid'].endswith(s))
def test_all_pass(): assert all(not run(r) for r in R)
def test_S1_default_codice():
    r=rec(); c=case(r,'P1-boundary'); c['oracle'][0]['text']='atteso il default del codice 35.0'; assert any(x.startswith('S1') for x in run(r))
def test_S2_inf():
    r=rec(); case(r,'P1-negative')['oracle'][0]['text']+=' (anche inf)'; assert any(x.startswith('S2') for x in run(r))
def test_S3_riavvio():
    r=rec(); case(r,'D2-positive')['fixtures']['scenario']['nome']='riavvio'; assert any(x.startswith('S3') for x in run(r))
def test_S4_invariato():
    r=rec(); r['cases']=[c for c in r['cases'] if c['fixtures']['scenario'].get('nome')!='invariato']; assert any(x.startswith('S4') for x in run(r))
def test_S5_origine():
    r=rec(); r['clauses'][0]['origin']='verbatim #461/6085510893'; assert any(x.startswith('S5') for x in run(r))
def test_S6_covers():
    r=rec('DIFF-PF-RSR-29-SAVE'); case(r,'D1-positive')['covers']=['D4']; assert any(x.startswith('S6') for x in run(r))
def test_S7_verdetto():
    r=rec('DIFF-PF-RSR-31-SAVE'); c=case(r,'D3-positive'); c['verdetto_osservato_vs_oracolo']={'CONFORME':'NON_CONFORME','NON_CONFORME':'CONFORME'}[c['verdetto_osservato_vs_oracolo']]; assert any(x.startswith('S7') for x in run(r))
def test_S8_sentinel():
    r=rec('DIFF-PF-RSR-38-SAVE'); case(r,'P1-positive')['atteso_predicato']['atteso']='<attributo assente in RoserpinaConfig>'; assert any(x.startswith('S8') or x.startswith('S7') for x in run(r))
def test_S9_fixture_non_inseribile():
    r=rec('DIFF-PF-RSR-38-SAVE'); c=case(r,'P1-positive'); c['oracle'][0]['text']=c['oracle'][0]['text'].replace(' (nessun campo GUI per la chiave: il valore non può essere inserito)',''); assert any(x.startswith('S9') for x in run(r))
def test_money():
    r=rec(); case(r,'P1-positive')['oracle'][0]['text']+=' entro il cap di 10 €'; assert any('money' in x for x in run(r))
def test_sha():
    r=rec(); r['sources'][0]['sha256']='0'*64; assert any(x.startswith('sha') for x in run(r))
def test_indipendente_intercetta_scenario_mancante(tmp_path):
    R2=copy.deepcopy(R); R2[0]['cases']=[c for c in R2[0]['cases'] if c['fixtures']['scenario'].get('nome')!='versione_fallita']
    f=tmp_path/'m_L8b.json'; f.write_text(json.dumps(R2,ensure_ascii=False))
    o=subprocess.run([sys.executable,'indep_check_L8b.py',str(f)],capture_output=True,text=True).stdout
    assert '19 / 20' in o and 'versione_fallita' in json.dumps(json.load(open('indep_L8b.json')))
    subprocess.run([sys.executable,'indep_check_L8b.py','record_specific_L8b.json'],capture_output=True)

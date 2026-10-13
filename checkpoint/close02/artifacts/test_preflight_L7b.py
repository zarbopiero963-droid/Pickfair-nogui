"""Test negativi: ogni rilievo Sol re-introdotto per mutazione deve essere intercettato dal preflight."""
import json,copy,pytest,os
os.chdir('/workspace/pickfair/bf2/close02')
from preflight_L7b import check
R=json.load(open('record_specific_L7b.json')); P=json.load(open('probe_load_v3.json')); DD=json.load(open('probe_dd_apply.json'))
def rec(i='DIFF-PF-RSR-04-LOAD'): return copy.deepcopy(next(r for r in R if r['difference_id']==i))
def run(r): return check(r,P,DD)
def case(r,suffix): return next(c for c in r['cases'] if c['cid'].endswith(suffix))
def test_all_pass(): assert all(not run(r) for r in R)
def test_S1_default_codice():
    r=rec(); c=case(r,'P2-positive'); c['atteso_predicato']={'op':'valore_eq','scenario':'assente','atteso':'18.0'}; c['oracle'][0]['text']='assente ⇒ default di classe 18.0'
    assert any(x.startswith('S1') for x in run(r))
def test_S2_inf_non_ancorato():
    r=rec(); case(r,'D1-negative')['oracle'][0]['text']+=' e "inf" segnalato'; assert any(x.startswith('S2') for x in run(r))
def test_S3_riavvio_senza_tipo():
    r=rec(); case(r,'D1-boundary')['atteso_predicato'].pop('atteso_tipo'); assert any(x.startswith('S3') for x in run(r))
def test_S4_manca_override_uguale_default():
    r=rec(); r['cases']=[c for c in r['cases'] if c['fixtures']['scenario'].get('nome')!='override_uguale_default']; assert any(x.startswith('S4') for x in run(r))
def test_S5_origine_errata():
    r=rec('DIFF-PF-RSR-23-LOAD'); r['clauses'][0]['origin']='verbatim #461/6085510893'; assert any(x.startswith('S5') for x in run(r))
def test_S6_covers_inesistente():
    r=rec('DIFF-PF-RSR-29-LOAD'); case(r,'D1-positive')['covers']=['D4']; assert any(x.startswith('S6') for x in run(r))
def test_S6_clausola_scoperta():
    r=rec(); r['cases']=[c for c in r['cases'] if c['covers']!=['D2']]; assert any(x.startswith('S6 copertura D2') for x in run(r))
def test_S7_verdetto_incoerente():
    r=rec('DIFF-PF-RSR-31-LOAD'); c=case(r,'D2-positive'); c['verdetto_osservato_vs_oracolo']={'CONFORME':'NON_CONFORME','NON_CONFORME':'CONFORME'}[c['verdetto_osservato_vs_oracolo']]
    assert any(x.startswith('S7') for x in run(r))
def test_S8_sentinel_come_atteso():
    r=rec('DIFF-PF-RSR-38-LOAD'); c=case(r,'P2-positive'); c['atteso_predicato']={'op':'valore_eq','scenario':'assente','atteso':'<attributo assente in RoserpinaConfig>'}
    assert any(x.startswith('S8') for x in run(r))
def test_money_su_non_eur():
    r=rec(); case(r,'P1-positive')['oracle'][0]['text']+=' entro il cap di 10 €'; assert any('money' in x for x in run(r))
def test_sha_alterato():
    r=rec(); r['sources'][0]['sha256']='0'*64; assert any(x.startswith('sha') for x in run(r))

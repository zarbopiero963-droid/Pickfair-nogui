import json,copy,subprocess,sys,pytest
from preflight_L7b import check
R=json.load(open('record_specific_L7b.json')); P=json.load(open('probe_load_v3.json')); DD=json.load(open('probe_dd_apply.json'))
def run(f,rid='DIFF-PF-RSR-02-LOAD'):
    X=copy.deepcopy(R); r=[x for x in X if x['difference_id']==rid][0]; f(r); return check(r,P,DD)
def o2(r,k): return [c for c in r['cases'] if 'O2' in c['covers'] and c['kind']==k][0]
M={'provvisorio':lambda r: r['cases'][0].__setitem__('stato_oracolo','PROVVISORIO_PENDING_OWNER'),
   'owner_sha':lambda r: r['owner_decision'].__setitem__('sha256','0'*64),
   'provenienza':lambda r: [c['oracle'][0].__setitem__('provenance','REQUIREMENT') for c in r['cases'] if c['fixtures']['scenario'].get('nome')=='assente'],
   'O2_verdetto':lambda r: o2(r,'negative').__setitem__('verdetto_osservato_vs_oracolo','CONFORME'),
   'O2_ancora':lambda r: o2(r,'negative')['ancora_normativa'][0].__setitem__('citazione_verbatim','block nothing'),
   'O2_boundary':lambda r: r.__setitem__('cases',[c for c in r['cases'] if not('O2' in c['covers'] and c['kind']=='boundary')]),
   'O2_clausola':lambda r: r.__setitem__('clauses',[c for c in r['clauses'] if c['cid']!='O2'])}
def test_pulito(): assert all(not check(r,P,DD) for r in R)
@pytest.mark.parametrize('k',sorted(M))
def test_mut(k): assert run(M[k])
def test_indip_rileva(tmp_path):
    X=copy.deepcopy(R); r=[x for x in X if x['difference_id']=='DIFF-PF-RSR-13-LOAD'][0]; r['clauses']=[c for c in r['clauses'] if c['cid']!='O2']
    p=tmp_path/'m_L7b.json'; p.write_text(json.dumps(X)); o=subprocess.run([sys.executable,'indep_check_L7b.py',str(p)],capture_output=True,text=True).stdout
    assert 't80u owner: 6 / 7' in o

import json,copy,subprocess,sys,pytest
from preflight_L9 import check
R=json.load(open('record_specific_L9.json'))
def mut(f,rid='DIFF-PF-RSR-20-RESTART'):
    X=copy.deepcopy(R); r=[x for x in X if x['difference_id']==rid][0]; f(r); return check(X)[rid]
def test_base_pulito(): assert all(not v for v in check(R).values())
def c(r,op): return [x for x in r['cases'] if x['predicato']['op']==op][0]
MUT={
 'sha':lambda r: r['sources'][0].__setitem__('sha256','0'*64),
 'S1_atteso_da_codice':lambda r: c(r,'riavvio_eq').__setitem__('atteso',10.0),
 'S3_senza_sigkill':lambda r: c(r,'riavvio_eq')['osservato_028cee9'].__setitem__('sigkill',False),
 'S3_metodo_assente':lambda r: r.__setitem__('cases',[x for x in r['cases'] if x['predicato']['op']!='metodo_db_reale']),
 'S5_origine':lambda r: r['clauses'][0].__setitem__('text','testo inventato'),
 'S6_covers':lambda r: r['cases'][0].__setitem__('covers',['ZZ']),
 'S6_PNB':lambda r: r.__setitem__('cases',[x for x in r['cases'] if not (x['kind']=='boundary' and 'D3' in x['covers'])]),
 'S7_verdetto_D2':lambda r: c(r,'nessuna_ammissione').__setitem__('verdetto_osservato_vs_oracolo','CONFORME'),
 'S7_verdetto_fantasma':lambda r: c(r,'fantasma_registrato_o_bloccato').__setitem__('verdetto_osservato_vs_oracolo','CONFORME'),
 'S7_verdetto_dd':lambda r: c(r,'dd_blocca_dopo_riavvio').__setitem__('verdetto_osservato_vs_oracolo','CONFORME'),
 'S8_sentinella':lambda r: c(r,'riavvio_eq').__setitem__('atteso','<assente>'),
 'O1_senza_owner':lambda r: [x.__setitem__('stato_oracolo','DEFINITIVO') for x in r['cases'] if 'O1' in x['covers']],
 'cid_duplicato':lambda r: r['cases'][1].__setitem__('cid',r['cases'][0]['cid']),
}
@pytest.mark.parametrize('k',sorted(MUT))
def test_mutazione_rilevata(k): assert mut(MUT[k])
INDEP={
 'scenario_incerto_rimosso':lambda r: r.__setitem__('cases',[x for x in r['cases'] if 'incerto' not in x['fixtures']['scenario']]),
 'dd_999_rimosso':lambda r: r.__setitem__('cases',[x for x in r['cases'] if '9,99' not in x['fixtures']['scenario']]),
 'riga_fase_non_citata':lambda r: r.__setitem__('sources',[s for s in r['sources'] if s['ref']!='#461/6085510337']),
 'clausola_P2_rimossa':lambda r: r.__setitem__('clauses',[x for x in r['clauses'] if x['cid']!='P2']),
}
@pytest.mark.parametrize('k',sorted(INDEP))
def test_mutazione_indipendente(k,tmp_path):
    X=copy.deepcopy(R); r=[x for x in X if x['difference_id']=='DIFF-PF-RSR-20-RESTART'][0]; INDEP[k](r)
    p=tmp_path/'m.json'; p.write_text(json.dumps(X))
    out=subprocess.run([sys.executable,'indep_check_L9.py',str(p)],capture_output=True,text=True,cwd='/workspace/pickfair/bf2/close02').stdout
    assert '37 / 38 completi' in out

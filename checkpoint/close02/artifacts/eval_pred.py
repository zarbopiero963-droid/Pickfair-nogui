"""Valutatore dei predicati dichiarativi (osservato a 028cee9 vs atteso)."""
import json
NA='<attributo assente in RoserpinaConfig>'
def sig(o): return (tuple(map(tuple,o.get('segnali',[]))),tuple(sorted(o.get('flag',{}).items())))
def segnalato(o): return bool(o.get('segnali')) or any(v not in ('()','{}','','set()','frozenset()') for v in o.get('flag',{}).values())
def num(a):
    try: return float(a)
    except Exception: return a
OW=None
def _ow():
    global OW
    if OW is None: OW=json.load(open(__import__('os').environ.get('EV_OWNER','/workspace/pickfair/bf2/close02/probe_owner_t80u.json')))
    return OW
def evaluate(pred,P,DD):
    op=pred['op']
    if op.startswith('owner_'):
        o=_ow()[pred['sonda']]; adm=bool(o['nuovo_rischio_arrivato_al_sizing']); v=o['valore_effettivo']
        if op=='owner_blocca_nuovo_rischio': ok=not adm
        elif op=='owner_controllo_ammesso': ok=adm
        elif op=='owner_no_recovery': ok=v not in ('True',) and not (pred.get('vietato') and v==pred['vietato'])
        elif op=='owner_commissione_non_inventata': ok=v!=pred['vietato'] and not adm
        else: raise ValueError(op)
        return ('CONFORME' if ok else 'NON_CONFORME'),o
    if op=='non_applicabile': return 'NON_APPLICABILE',{'motivo':pred['motivo']}
    if op.startswith('dd_'):
        o=DD[pred['scenario']]; blk=any('drawdown' in str(x) for x in o.get('rifiuti',[]))
        return ('CONFORME' if blk==(op=='dd_blocca') else 'NON_CONFORME'),o
    if op=='riavvio_eq':
        o=P['riavvio']; d=o['dopo']; ok=o['processo_nuovo'] and num(d['valore'])==num(pred['atteso_valore']) and d['tipo']==pred['atteso_tipo']
        return ('CONFORME' if ok else 'NON_CONFORME'),{k:o[k] for k in ('sequenza','processo_nuovo','prima','dopo')}
    if op=='stati_distinti':
        a,b=P[pred['a']],P[pred['b']]; return ('CONFORME' if sig(a)!=sig(b) else 'NON_CONFORME'),{pred['a']:a,pred['b']:b}
    o=P[pred['scenario']]
    if op=='valore_eq': ok=o['valore']!=NA and (num(o['valore'])==num(pred['atteso']) or str(o['valore'])==str(pred['atteso']))
    elif op=='tipo_eq': ok=o['tipo']==pred['atteso']
    elif op=='segnalato': ok=segnalato(o)
    elif op=='non_silenzioso': ok=segnalato(o) or o['valore'] in ('None',)
    elif op=='assente_onesto': ok=segnalato(o) or o['valore']=='None'
    else: raise ValueError(op)
    return ('CONFORME' if ok else 'NON_CONFORME'),o
if __name__=='__main__':
    import sys
    f=sys.argv[1]; R=json.load(open(f)); import os; P=json.load(open(os.environ.get('EV_LOAD','probe_load_v3.json'))); DD=json.load(open(os.environ.get('EV_DD','probe_dd_apply.json')))
    for r in R:
        rid='RSR-'+r['difference_id'].split('-')[3]
        for c in r['cases']:
            v,o=evaluate(c['atteso_predicato'],P[rid],DD); c['verdetto_osservato_vs_oracolo']=v; c['osservato_028cee9']=o
            c['norm_vs_impl']={'impl_attuale':'sonda 028cee9: '+json.dumps(o,ensure_ascii=False)[:500]}
    json.dump(R,open(f,'w'),indent=1,ensure_ascii=False)
    import collections; print(collections.Counter(c['verdetto_osservato_vs_oracolo'] for r in R for c in r['cases']))

"""Valutatore predicati SAVE (osservato 028cee9: probe_gui_save, probe_save_same_v2, probe_dd_apply)."""
import json
def info(o): return [e for e in o['eventi'] if e[0]=='INFO']
def num(a):
    try: return float(a)
    except Exception: return a
def evaluate(pred,P,DD):
    op=pred['op']
    if op.startswith('dd_'):
        o=DD[pred['scenario']]; blk=any('drawdown' in str(x) for x in o.get('rifiuti',[]))
        return ('CONFORME' if blk==(op=='dd_blocca') else 'NON_CONFORME'),o
    if op=='same_invariato_con_versione':
        o=P['same']; return ('CONFORME' if o['raw_dopo']==o['raw_prima'] and not o['altre_chiavi_cambiate'] and o['versione_dopo'] is not None else 'NON_CONFORME'),o
    o=P['gui'][pred['scenario']]
    if pred['scenario']!='ok' and not any(e[0]=='ERROR' and 'iniettato' in e[2] for e in o['eventi']):
        return 'NON_ESERCITABILE',dict(o,motivo='guasto iniettato mai raggiunto: la chiave non viene scritta dal percorso di salvataggio')
    if op=='db_eq': ok=o['db_finale'] is not None and (num(o['db_finale'])==num(pred['atteso']) or str(o['db_finale'])==str(pred['atteso']))
    elif op=='db_eq_con_versione': ok=o['db_finale'] is not None and num(o['db_finale'])==num(pred['atteso']) and o['versione_finale'] is not None
    elif op=='nessuna_scrittura': ok=o['righe_dopo']==o['righe_prima'] and o['db_finale']==pred.get('atteso_db')
    elif op=='no_successo': ok=not info(o) and any(e[0]=='ERROR' for e in o['eventi'])
    elif op=='successo_dopo_commit':
        i=info(o); ok=bool(i) and i[0][3]['valore_db_al_messaggio'] is not None and num(i[0][3]['valore_db_al_messaggio'])==num(pred['atteso']) and i[0][3]['versione_db'] is not None
    else: raise ValueError(op)
    return ('CONFORME' if ok else 'NON_CONFORME'),o
if __name__=='__main__':
    import sys
    f=sys.argv[1]; R=json.load(open(f)); G=json.load(open('probe_gui_save.json')); SS=json.load(open('probe_save_same_v2.json')); DD=json.load(open('probe_dd_apply.json'))
    for r in R:
        rid='RSR-'+r['difference_id'].split('-')[3]
        for c in r['cases']:
            v,o=evaluate(c['atteso_predicato'],{'gui':G[rid],'same':SS[rid]},DD); c['verdetto_osservato_vs_oracolo']=v; c['osservato_028cee9']=o
            c['norm_vs_impl']={'impl_attuale':'sonda 028cee9: '+json.dumps(o,ensure_ascii=False)[:500]}
    json.dump(R,open(f,'w'),indent=1,ensure_ascii=False)
    import collections; print(collections.Counter(c['verdetto_osservato_vs_oracolo'] for r in R for c in r['cases']))

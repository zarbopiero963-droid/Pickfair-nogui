def ev(c):
    p=c['predicato']['op']; a=c['atteso']; o=c['osservato_028cee9']
    if p=='riavvio_eq': return o.get('valore')==a and o.get('sigkill',True) is not False
    if p=='non_committato_invisibile': return o.get('raw_db')!=a['diverso_da'] and (o.get('valore') in (a['uguale'],None))
    if p=='protezione_ordine_aperto': return all(o.get(k)==v for k,v in a.items())
    if p=='metodo_db_reale': return o['returncode_A']==-9 and o['pid_B'] is not None
    if p=='config_prima_di_readiness':
        cr=o['config_runtime']
        if o['recovery']!='RECOVERED' or cr in ('<assente>',None): return False
        return cr==repr(a) or (cr.startswith('<') and cr.split('.')[1].split(':')[0]==a)
    if p=='nessuna_ammissione': return not (o['modo']=='RuntimeMode.ACTIVE' or o['segnale_arrivato_al_sizing'])
    if p=='fantasma_registrato_o_bloccato': return o['recovery'].get('ghost_orders_recovered',0)>=1 or not o['segnale_arrivato_al_sizing']
    if p=='ammissione_dopo_recovery_ok': return o['recovery']=='RECOVERED' and bool(o['segnale_arrivato_al_sizing'])
    if p=='dd_blocca_dopo_riavvio': return not o['segnale_arrivato_al_sizing']
    if p=='dd_non_blocca_dopo_riavvio': return bool(o['segnale_arrivato_al_sizing'])
    raise ValueError(p)
def verdetto(c): return 'CONFORME' if ev(c) else 'NON_CONFORME'

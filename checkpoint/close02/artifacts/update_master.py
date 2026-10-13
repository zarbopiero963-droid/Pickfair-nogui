import json,collections
B="/workspace/pickfair/bf2/"
M=json.load(open(B+'repair01/master1035.json')); O={r['difference_id']:r for r in json.load(open(B+'repair01/rebuilt570.json'))}; R=json.load(open(B+'repair01/sp570.json'))
Q=json.load(open(B+'repair02/q6_split_verification.json')); P=json.load(open(B+'repair02/packs_q6.json'))
EXTRA={}
try: EXTRA=json.load(open(B+'close02/record_specific_status.json'))
except FileNotFoundError: pass
for m in M:
    i=m['id']; r=O.get(i)
    if r:
        ok=not R[i]; pk=",".join(sorted({p['key'] for p in r['pending_owner']}))
        if not ok: m['status']="REJECTED_SEMANTIC_PREFLIGHT"; m['blocker']="; ".join(R[i])[:200]
        elif pk: m['status']="PREFLIGHT_PASS_PENDING_OWNER"; m['blocker']="PENDING_OWNER: "+pk+"; poi revisione indipendente"
        else: m['status']="PREFLIGHT_PASS_AWAITING_INDEPENDENT_REVIEW"; m['blocker']="revisione indipendente non disponibile senza spesa; PF-QA-02 non iniziato"
        if r.get('q5_drawdown'): m['blocker']+="; Q5 (#426/6141136157) e Q3 (#426/6141450045); finding F-DRAWDOWN-UNIT/F-DRAWDOWN-LIVEONLY aperti per #505/Gate B"
        m['preflight']="PASS" if ok else "FAIL"; m['pending_owner']=pk or None
        m['pack_kind']="manuale" if r.get('hand_built') else "generato per fase"
    if i in EXTRA: m.update(EXTRA[i])
    if i in Q:
        q=Q[i]; ch=[o for o in P if o['parent']==i]
        if q['parent_state']=="ATOMIC_NO_SPLIT":
            ok=all(not o['preflight'] for o in ch)
            m['status']="ATOMIC_NO_SPLIT_PREFLIGHT_PASS_AWAITING_INDEPENDENT_REVIEW" if ok else "ATOMIC_NO_SPLIT_PREFLIGHT_FAIL"; m['preflight']="PASS" if ok else "FAIL"
        else:
            m['status']="SPLIT_PARENT_"+q['parent_state']; m['preflight']="N/A_PARENT (figli: %d/%d PASS)"%(sum(not o['preflight'] for o in ch),len(ch)); m['children']=[o['requirement_id'] for o in ch]
ids=[m['id'] for m in M]; assert len(ids)==len(set(ids))==1035
json.dump(M,open(B+'repair01/master1035.json','w'),ensure_ascii=False,indent=0)
c=collections.Counter(m['status'] for m in M); print(sorted(c.items()),sum(c.values()),"preflight PASS",sum(m.get('preflight')=='PASS' for m in M))

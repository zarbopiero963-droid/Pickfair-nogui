"""Classifica i casi CASH per tipo di operazione (t75u): ENTRY (aumenta rischio), HOLDING, RISK_REDUCTION, CASHOUT.
Regole cap Q3/D2 ammesse solo su ENTRY; su RISK_REDUCTION/CASHOUT vale D2 seconda parte (cancel/cashout consentiti)."""
import json,re
D=json.load(open('record_specific_CASH.json'))
M={('DIFF-PF-CASH-04','positive'):'HOLDING',('DIFF-PF-CASH-04','negative'):'CASHOUT',('DIFF-PF-CASH-04','boundary'):'HOLDING',
   ('DIFF-PF-CASH-19','positive'):'CASHOUT',('DIFF-PF-CASH-19','negative'):'CASHOUT',('DIFF-PF-CASH-19','boundary'):'CASHOUT',
   ('DIFF-PF-CASH-21','positive'):'CASHOUT',('DIFF-PF-CASH-21','negative'):'RISK_REDUCTION',('DIFF-PF-CASH-21','boundary'):'CASHOUT',
   ('DIFF-PF-CASH-23','positive'):'ENTRY',('DIFF-PF-CASH-23','negative'):'ENTRY',('DIFF-PF-CASH-23','boundary'):'HOLDING',
   ('DIFF-PF-CASH-25','positive'):'ENTRY',('DIFF-PF-CASH-25','negative'):'ENTRY',('DIFF-PF-CASH-25','boundary'):'HOLDING'}
CAP=re.compile(r'\bcap\b|10[.,]00 ?€|blocca(re|to)? nuovo rischio|Q3|D2',re.I); bad=[]
for x in D:
    for c in x['cases']:
        op=M[(x['difference_id'],c['kind'])]; c['operazione']=op
        c['regole_cap_applicabili']= op=='ENTRY'
        txt=' '.join(f['text'] for f in c['oracle'])
        if op!='ENTRY' and CAP.search(txt): bad.append((x['difference_id'],c['kind']))
json.dump(D,open('record_specific_CASH_split.json','w'),indent=1,ensure_ascii=False)
import collections
print(collections.Counter(c['operazione'] for x in D for c in x['cases']),'cap su non-ENTRY:',bad)

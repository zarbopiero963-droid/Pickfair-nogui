import sys,json,types
sys.path.insert(0,'/workspace/pickfair/bf2/close02/wt_base')
import betfair_client as bc, simulation_broker as sb
from probe import mk
ERR={"faultcode":"Client","detail":{"APINGException":{"errorCode":"INVALID_SESSION_INFORMATION"}}}
EXC=RuntimeError("INVALID_SESSION_INFORMATION")
C={
 'place_bet':(dict(market_id="1.230000001",selection_id=47999,side="BACK",price=2.0,size=2.0,customer_ref="cf-1"),
   {"status":"SUCCESS","instructionReports":[{"status":"SUCCESS","betId":"3001","sizeMatched":2.0,"averagePriceMatched":2.0}]}),
 'cancel_orders':(dict(market_id="1.230000001",bet_ids=["3001"]),
   {"status":"SUCCESS","instructionReports":[{"status":"SUCCESS","sizeCancelled":2.0,"instruction":{"betId":"3001"}}]}),
 'replace_orders':(dict(market_id="1.230000001",bet_id="3001",new_price=2.2),
   {"status":"SUCCESS","instructionReports":[{"status":"SUCCESS","placeInstructionReport":{"status":"SUCCESS","betId":"3002"},"cancelInstructionReport":{"status":"SUCCESS"}}]}),
}
out={}
for name,(kw,good) in C.items():
    r={}
    for tag,ret in (('valid',good),('error_payload',ERR),('exception',EXC),('failure_report',{"status":"FAILURE","errorCode":"INSUFFICIENT_FUNDS","instructionReports":[{"status":"FAILURE","errorCode":"INSUFFICIENT_FUNDS"}]})):
        o=mk(bc.BetfairClient,ret)
        try: v=getattr(o,name)(**kw); r[tag]={'ret':repr(v)[:200],'calls':o._calls}
        except Exception as e: r[tag]={'raise':f'{type(e).__name__}: {e}'[:200],'calls':o._calls}
    out['betfair_client.'+name]=r
# boundary inputs client place_bet
for tag,kw in (('nan_price',dict(market_id="1.2",selection_id=1,side="BACK",price=float('nan'),size=2.0)),('size_zero',dict(market_id="1.2",selection_id=1,side="BACK",price=2.0,size=0)),('bad_side',dict(market_id="1.2",selection_id=1,side="X",price=2.0,size=2.0))):
    o=mk(bc.BetfairClient,C['place_bet'][1])
    try: v=o.place_bet(**kw); out.setdefault('betfair_client.place_bet',{})[tag]={'ret':repr(v)[:200],'calls':o._calls}
    except Exception as e: out['betfair_client.place_bet'][tag]={'raise':f'{type(e).__name__}: {e}'[:200],'calls':o._calls}
# SimulationBroker
def S(): return sb.SimulationBroker(starting_balance=100.0,commission_pct=4.5)
def run(tag,f):
    try: v=f(); return {'ret':repr(v)[:200]}
    except Exception as e: return {'raise':f'{type(e).__name__}: {e}'[:200]}
b=S()
out['simulation_broker.get_account_funds']={'fresh':run('',lambda:S().get_account_funds())}
out['simulation_broker.list_events']={'valid':run('',lambda:S().list_events(["1"])),'empty_ids':run('',lambda:S().list_events([]))}
out['simulation_broker.list_market_catalogue']={'valid':run('',lambda:S().list_market_catalogue(["1"])),'empty_ids':run('',lambda:S().list_market_catalogue([]))}
out['simulation_broker.get_market_book']={'unknown':run('',lambda:S().get_market_book("1.999"))}
out['simulation_broker.get_current_orders']={'fresh':run('',lambda:S().get_current_orders())}
out['simulation_broker.place_bet']={'unknown_market':run('',lambda:S().place_bet(market_id="1.999",selection_id=1,side="BACK",price=2.0,size=2.0)),
  'nan_price':run('',lambda:S().place_bet(market_id="1.999",selection_id=1,side="BACK",price=float('nan'),size=2.0))}
out['simulation_broker.cancel_orders']={'unknown_bet':run('',lambda:S().cancel_orders(market_id="1.999",instructions=[{"betId":"nope"}])),'none':run('',lambda:S().cancel_orders())}
out['simulation_broker.replace_orders']={'unknown_bet':run('',lambda:S().replace_orders(market_id="1.999",bet_id="nope",new_price=2.2)),'none':run('',lambda:S().replace_orders())}
p=json.load(open('probe_out.json')); p.update(out); json.dump(p,open('probe_all.json','w'),indent=1,ensure_ascii=False)
for k,v in out.items(): print(k,{t:(x.get('ret') or x.get('raise'))[:90] for t,x in v.items()})

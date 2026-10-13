import sys,json,types
sys.path.insert(0,'/workspace/pickfair/bf2/close02/wt_base')
import betfair_client as bc
import betfair_market_api as bm
FIX={
 'list_event_types':([{"eventType":{"id":"1","name":"Soccer"},"marketCount":120}],()),
 'list_competitions':([{"competition":{"id":"81","name":"Serie A"},"marketCount":40}],(["1"],)),
 'list_events':([{"event":{"id":"34000001","name":"Inter v Milan"},"marketCount":60}],(["1"],)),
 'list_market_catalogue':([{"marketId":"1.230000001","marketName":"Match Odds","runners":[{"selectionId":47999,"runnerName":"Inter"}]}],(["1"],)),
 'get_market_book':([{"marketId":"1.230000001","status":"OPEN","runners":[{"selectionId":47999,"status":"ACTIVE"}]}],("1.230000001",)),
 'get_current_orders':({"currentOrders":[{"betId":"3001","marketId":"1.230000001","selectionId":47999,"side":"BACK","priceSize":{"price":2.0,"size":1.0},"status":"EXECUTABLE"}],"moreAvailable":False},()),
 'list_cleared_orders':({"clearedOrders":[{"betId":"3001","profit":1.0,"betOutcome":"WON"}],"moreAvailable":False},()),
 'get_account_funds':({"availableToBetBalance":100.0,"exposure":0.0},()),
}
def mk(cls,ret):
    o=object.__new__(cls)
    def post(self,url,method,params,*a,**k):
        o._calls.append(method)
        if isinstance(ret,Exception): raise ret
        return ret
    o._calls=[]; o._post_jsonrpc=types.MethodType(post,o)
    o.BETTING_URL='https://betting.invalid'; o.ACCOUNT_URL='https://account.invalid'
    return o
out={}
for cls in (bm.MarketBetfairClient,):
  for name,(good,args) in FIX.items():
    if not hasattr(cls,name): continue
    r={}
    for tag,ret in (('valid',good),('error_payload',{"faultcode":"Client","detail":{"APINGException":{"errorCode":"INVALID_SESSION_INFORMATION"}}}),('exception',RuntimeError("INVALID_SESSION_INFORMATION")),('empty',[] if isinstance(good,list) else {})):
        o=mk(cls,ret)
        try: v=getattr(o,name)(*args); r[tag]={'ret':repr(v)[:160],'calls':o._calls}
        except Exception as e: r[tag]={'raise':f'{type(e).__name__}: {e}'[:160],'calls':o._calls}
    out[f'{cls.__module__}.{name}']=r
json.dump(out,open('probe_out.json','w'),indent=1,ensure_ascii=False)
for k,v in out.items(): print(k, {t:(x.get('ret') or x.get('raise'))[:70] for t,x in v.items()})

import collections, heapq, json
exec(open('plan2.py').read())
micro={m[0]:dict(id=m[0],title=m[1],scope=m[2],deps=[d.strip() for d in m[3].replace(' + tutte le P0/P1','').split(',') if d.strip() not in ('','-','#537')],tests=m[4],prio=m[5],size=m[6],kind=m[7],orig=o) for o,_,ms in P for m in ms}
origname={o:n for o,n,_ in P}
W={}
for k in "M025 M158 M156 M130 M157".split(): W[k]=0
for k in "M008 M009 M010 M011 M012 M013 M014 M015 M016 M017 M018 M019 M020 M021 M022 M085 M088 M097 M098 M030 M031 M032 M033 M034 M035".split(): W[k]=1
for k in "M001 M002 M003 M004 M006 M023 M024 M028 M029 M047".split(): W[k]=2
for k in "M154 M005 M007 M026 M027 M065 M066 M067 M068 M069 M099 M100 M101 M102 M045 M063 M064".split(): W[k]=3
for k in "M048 M049 M050 M051 M052 M053 M054 M055 M056 M057 M058 M059 M060 M061 M062 M106 M107".split(): W[k]=4
for k in "M036 M079 M080 M081 M082 M089 M090 M091 M092 M093 M094 M095 M096 M070 M071 M072 M073 M074 M075 M076 M077 M078 M083 M084 M086 M087".split(): W[k]=5
for k in "M037 M038 M039 M040 M041 M042 M043 M044 M046 M103 M104 M105 M108 M109 M110 M111 M112 M113 M114 M115 M116 M117 M123 M127 M128 M129 M131 M132 M133 M134 M135 M136 M137 M151 M152 M153".split(): W[k]=6
for k in "M118 M119 M120 M121 M122 M124 M125 M126".split(): W[k]=7
for k in "M138 M139 M140 M141 M142 M143 M144 M145 M146 M147 M148 M149 M150".split(): W[k]=8
act=[k for k,v in micro.items() if v['kind']!='SUSP']
missing=[k for k in act if k not in W]; assert not missing, missing
# Scenario B grouping
G={1:[["M001"]],2:[["M002","M003"]],3:[["M004","M006"],["M005","M007"]],4:[["M008"],["M009"],["M010"]],5:[["M011"],["M012","M013"]],
6:[["M014"]],7:[["M015"]],8:[["M016"]],9:[["M017"]],10:[["M018"]],11:[["M019","M020"],["M021","M022"]],12:[["M023","M024"]],
13:[["M025","M158"],["M026","M027"]],14:[["M028"],["M029"]],15:[["M030","M031"]],16:[["M032"],["M033","M034"]],17:[["M035"]],18:[["M036"]],
19:[["M037"]],20:[["M038"]],21:[["M039","M040"]],22:[["M041","M042","M043"]],23:[["M045"],["M044","M046"]],24:[["M047","M048"]],
25:[["M049"],["M050","M051"]],26:[["M052","M053"]],27:[["M054"]],28:[["M055","M056"]],29:[["M057","M058"]],30:[["M059"]],
31:[["M060"],["M061","M062"]],32:[["M063","M064"]],33:[["M065","M067"],["M066","M154"],["M068","M069"]],34:[["M070"],["M071","M074"],["M072","M073"]],
35:[["M075","M076","M077"],["M078"]],36:[["M079","M080"]],37:[["M081","M082"]],38:[["M083","M084"]],39:[["M086","M087"]],40:[["M085","M088"]],
41:[["M089"],["M090","M091"]],42:[["M092","M093"],["M094"]],43:[["M095","M096"]],44:[["M097","M098"]],45:[["M099"],["M100","M101","M102"]],
46:[["M103","M104","M105"]],47:[["M106","M107"]],48:[["M108","M109","M110"]],49:[["M111","M112"]],50:[["M113","M114"],["M115"]],
51:[["M116"]],52:[["M117"]],53:[["M118"]],54:[["M119"],["M120"]],55:[["M121","M122"]],56:[["M123","M124"]],57:[["M125","M126"]],
58:[["M127","M128"]],59:[["M129"]],60:[["M156","M130"],["M157"]],61:[["M131"]],62:[["M132"]],63:[["M133"]],64:[["M134"]],65:[["M135"]],66:[["M136","M137"]],
67:[["M138","M139"]],68:[["M140"]],69:[["M141"],["M142"]],70:[["M143","M144"]],71:[["M145"]],72:[["M146"]],73:[["M147"]],74:[["M148"]],
75:[["M149"]],76:[["M150"]],77:[["M151","M152"]],78:[["M153"]]}
for o in range(79,86): G[o]=[[m[0] for m in P[o-1][2]]]
used=[x for o in G for g in G[o] for x in g]
assert sorted(used)==sorted(micro), (set(micro)-set(used), [k for k,v in collections.Counter(used).items() if v>1])
for o in G:
  for g in G[o]: assert all(micro[x]['orig']==o for x in g),(o,g)
groups=[]
for o in sorted(G):
  for i,g in enumerate(G[o]): groups.append(dict(orig=o,members=g,susp=micro[g[0]]['kind']=='SUSP'))
owner={x:gi for gi,gr in enumerate(groups) for x in gr['members']}
prank={'P0':0,'P1':1,'P2':2,'P3':3}
def order(units, depsof, keyof):
  indeg={u:0 for u in units}; out=collections.defaultdict(list)
  for u in units:
    for d in depsof(u):
      if d!=u: indeg[u]+=1; out[d].append(u)
  h=[(keyof(u),u) for u in units if indeg[u]==0]; heapq.heapify(h); res=[]
  while h:
    _,u=heapq.heappop(h); res.append(u)
    for v in out[u]:
      indeg[v]-=1
      if indeg[v]==0: heapq.heappush(h,(keyof(v),v))
  assert len(res)==len(units),"cycle"
  return res
actg=[i for i,g in enumerate(groups) if not g['susp']]
def gdeps(i): return sorted({owner[d] for x in groups[i]['members'] for d in micro[x]['deps'] if owner[d]!=i})
def gkey(i): ms=groups[i]['members']; return (min(W[x] for x in ms), min(prank[micro[x]['prio']] for x in ms), i)
seqB=order(actg,gdeps,gkey)
seqC=order(act,lambda u:micro[u]['deps'],lambda u:(W[u],prank[micro[u]['prio']],u))
json.dump(dict(micro=micro,groups=groups,seqB=seqB,seqC=seqC,W=W,origname=origname),open('out2.json','w'),ensure_ascii=False)
print('B groups',len(groups),'active',len(actg),'split origs',sum(1 for o in G if len(G[o])>1 and o<79))
roots=[i for i in actg if not gdeps(i)]; print('B immediately executable',len(roots))

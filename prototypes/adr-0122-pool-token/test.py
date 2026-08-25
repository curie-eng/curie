import json, urllib.request, urllib.error
B="BOOTSTRAP-AAA"; C="CONV-BBB"
G,R,X="\033[32m","\033[31m","\033[0m"
def call(path, bearer=None, body=None):
    h={"Content-Type":"application/json"}
    if bearer: h["Authorization"]=f"Bearer {bearer}"
    d=json.dumps(body if body is not None else
        {"kind":"event","type":"message","text":"p","ts":"1","user":"U"}).encode()
    r=urllib.request.Request(f"http://localhost:7300{path}",data=d,headers=h,method="POST")
    try:
        resp=urllib.request.urlopen(r,timeout=60); return resp.status, resp.read().decode()[:70]
    except urllib.error.HTTPError as e: return e.code, e.read().decode()[:70]
    except Exception as e: return 0, type(e).__name__
def chk(label, got, want):
    ok = got==want
    print(f"  {(G+'PASS'+X) if ok else (R+'FAIL'+X)}  {label:52} -> {got} (want {want})")
    return ok
res=[]
print("=== before adoption ===")
res.append(chk("event, no auth",            call('/v1/event')[0], 401))
res.append(chk("event, bootstrap token",    call('/v1/event', B)[0], 200))
res.append(chk("event, wrong token",        call('/v1/event','nope')[0], 401))
print("=== adoption ===")
s,b = call('/v1/adopt', B, {"token": C})
res.append(chk("adopt with bootstrap",      s, 200)); print(f"        body: {b}")
print("=== after adoption ===")
res.append(chk("event, BOOTSTRAP (retired)",call('/v1/event', B)[0], 401))
res.append(chk("event, new conv token",     call('/v1/event', C)[0], 200))
res.append(chk("adopt again w/ bootstrap",  call('/v1/adopt', B, {"token":"ZZZ"})[0], 401))
res.append(chk("event, no auth",            call('/v1/event')[0], 401))
print(f"\n  {(G+'ALL PASS'+X) if all(res) else (R+'SOME FAILED'+X)}  ({sum(res)}/{len(res)})")

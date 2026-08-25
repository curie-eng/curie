import json, urllib.request, urllib.error
C="CONV-BBB"; G,R,X="\033[32m","\033[31m","\033[0m"
def call(path, bearer=None, body=None, raw=None):
    h={"Content-Type":"application/json"}
    if bearer: h["Authorization"]=f"Bearer {bearer}"
    d = raw if raw is not None else json.dumps(body if body is not None else
        {"kind":"event","type":"message","text":"p","ts":"1","user":"U"}).encode()
    r=urllib.request.Request(f"http://localhost:7300{path}",data=d,headers=h,method="POST")
    try:
        resp=urllib.request.urlopen(r,timeout=60); return resp.status
    except urllib.error.HTTPError as e: return e.code
    except Exception as e: return 0
def chk(l,g,w):
    ok=g==w; print(f"  {(G+'PASS'+X) if ok else (R+'FAIL'+X)}  {l:50} -> {g} (want {w})"); return ok
res=[]
print("=== what does a FAILED adoption leave behind? ===")
res.append(chk("adopt with malformed json",      call('/v1/adopt', C, raw=b'{not json'), 400))
res.append(chk("  -> current token still works", call('/v1/event', C), 200))
res.append(chk("adopt with no token field",      call('/v1/adopt', C, {"nope":1}), 400))
res.append(chk("  -> current token still works", call('/v1/event', C), 200))
res.append(chk("adopt with empty token",         call('/v1/adopt', C, {"token":""}), 400))
res.append(chk("  -> current token still works", call('/v1/event', C), 200))
res.append(chk("adopt with non-string token",    call('/v1/adopt', C, {"token":123}), 400))
res.append(chk("  -> current token still works", call('/v1/event', C), 200))
print(f"\n  {(G+'a failed adoption is inert'+X) if all(res) else (R+'FAILED ADOPTION BREAKS STATE'+X)}  ({sum(res)}/{len(res)})")

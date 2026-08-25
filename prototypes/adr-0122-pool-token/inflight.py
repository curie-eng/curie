import json, threading, time, urllib.request, urllib.error
B="BOOTSTRAP-AAA"; C="CONV-BBB"; D="ROTATED-CCC"
G,R,Y,X="\033[32m","\033[31m","\033[33m","\033[0m"
def post(path, bearer, body, timeout=180):
    h={"Content-Type":"application/json","Authorization":f"Bearer {bearer}"}
    r=urllib.request.Request(f"http://localhost:7300{path}",data=json.dumps(body).encode(),headers=h,method="POST")
    try:
        resp=urllib.request.urlopen(r,timeout=timeout)
        n=sum(1 for _ in resp)
        return resp.status, n, None
    except urllib.error.HTTPError as e: return e.code, 0, None
    except Exception as e: return 0, 0, type(e).__name__

# adopt first so we hold a conversation credential
s,_,_ = post('/v1/adopt', B, {"token": C}); print(f"  setup: adopt bootstrap -> {s}")

out={}
def long_turn():
    t0=time.time()
    out['turn'] = post('/v1/event', C,
        {"kind":"event","type":"message","text":"Write 300 words about TCP congestion control.","ts":"1","user":"U"})
    out['secs'] = time.time()-t0

print("=== start a long turn with the current token, then rotate mid-flight ===")
th=threading.Thread(target=long_turn); th.start()
time.sleep(2.5)
st=json.loads(urllib.request.urlopen("http://localhost:7300/status",timeout=8).read())
print(f"  at t=2.5s  turn_active={st.get('turn_active')}  <- rotating now")
s2,_,_ = post('/v1/adopt', C, {"token": D})
print(f"  rotate during in-flight turn -> {s2}")
th.join(timeout=200)
code,frames,err = out.get('turn',(0,0,'no-result'))
print(f"  in-flight turn finished: HTTP {code}, {frames} frames, {out.get('secs',0):.1f}s, err={err}")

ok_inflight = code==200
print(f"  {(G+'PASS: the in-flight turn survived the rotation'+X) if ok_inflight else (R+'FAIL: rotation killed the in-flight turn'+X)}")
print("=== and afterwards ===")
a,_,_ = post('/v1/event', C, {"kind":"event","type":"message","text":"x","ts":"1","user":"U"})
b,_,_ = post('/v1/event', D, {"kind":"event","type":"message","text":"x","ts":"1","user":"U"})
print(f"  {(G+'PASS'+X) if a==401 else (R+'FAIL'+X)}  old token after rotation -> {a} (want 401)")
print(f"  {(G+'PASS'+X) if b==200 else (R+'FAIL'+X)}  new token after rotation -> {b} (want 200)")

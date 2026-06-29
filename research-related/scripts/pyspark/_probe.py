"""Run a one-off python snippet on the existing remote kernel and print output."""
import json, uuid, base64, requests, websocket

BASE = "http://static.250.169.217.95.clients.your-server.de/jupyter"
WS_BASE = "ws://static.250.169.217.95.clients.your-server.de/jupyter"
AUTH = ("admin", "ceras")
KID = "31693baa-2660-4d67-9405-d7044d0791f4"   # q9 kernel (idle)
SID = "a14c82cf-55d5-4d05-8dfa-4ee6312995d1"

CODE = r"""
import os
base = '/mnt/bench'
print('== /mnt/bench ==')
for d in sorted(os.listdir(base)):
    print(' ', d)
tp = os.path.join(base, 'tpch-scale-200')
print('== tpch-scale-200 exists:', os.path.isdir(tp), '==')
if os.path.isdir(tp):
    tables = sorted(os.listdir(tp))
    print('  table dirs (%d):' % len(tables), tables)
    # spot-check a couple have parquet files
    for t in ('lineitem', 'orders', 'nation'):
        p = os.path.join(tp, t)
        if os.path.isdir(p):
            files = os.listdir(p)
            pq = [f for f in files if f.endswith('.parquet')]
            print('   %-10s entries=%d parquet=%d' % (t, len(files), len(pq)))
        else:
            print('   %-10s MISSING' % t)
"""

s = requests.Session(); s.auth = AUTH
s.get(f"{BASE}/lab", timeout=20)
auth_b64 = base64.b64encode(f"{AUTH[0]}:{AUTH[1]}".encode()).decode()
cookie = "; ".join(f"{c.name}={c.value}" for c in s.cookies)
ws = websocket.create_connection(
    f"{WS_BASE}/api/kernels/{KID}/channels?session_id={SID}",
    header=[f"Authorization: Basic {auth_b64}", f"Cookie: {cookie}"], timeout=60)

mid = uuid.uuid4().hex
ws.send(json.dumps({
    "header": {"msg_id": mid, "username": "p", "session": SID,
               "msg_type": "execute_request", "version": "5.3"},
    "parent_header": {}, "metadata": {},
    "content": {"code": CODE, "silent": False, "store_history": False,
                "user_expressions": {}, "allow_stdin": False, "stop_on_error": True},
    "channel": "shell"}))

while True:
    m = json.loads(ws.recv())
    if m.get("parent_header", {}).get("msg_id") != mid:
        continue
    if m.get("channel") == "iopub" and m.get("msg_type") == "stream":
        print(m["content"]["text"], end="")
    if m.get("channel") == "iopub" and m.get("msg_type") == "error":
        print("\n".join(m["content"]["traceback"]))
    if m.get("channel") == "shell" and m.get("msg_type") == "execute_reply":
        break
ws.close()

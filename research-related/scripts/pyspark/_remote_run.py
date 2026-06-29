"""Execute run_screening_q9.ipynb on the remote Jupyter (pod) kernel over the
Jupyter Server REST + websocket API, streaming cell output to stdout.

Creates its OWN session/kernel (does not touch the user's open notebook kernel),
runs every code cell except the multi-config sweep cell, then exits.
"""
import json
import sys
import time
import uuid
import requests
import websocket

BASE = "http://static.250.169.217.95.clients.your-server.de/jupyter"
WS_BASE = "ws://static.250.169.217.95.clients.your-server.de/jupyter"
AUTH = ("admin", "ceras")
NB_LOCAL = "research-related/scripts/pyspark/run_screening.ipynb"
NB_REMOTE = "run_screening_q9.ipynb"
PER_CELL_TIMEOUT = 900   # seconds to wait for one cell to finish

s = requests.Session()
s.auth = AUTH
# Prime XSRF cookie.
s.get(f"{BASE}/lab", timeout=20)
xsrf = s.cookies.get("_xsrf")
hdrs = {"X-XSRFToken": xsrf} if xsrf else {}

# --- Create a fresh session+kernel bound to the q9 notebook ----------------
session_name = str(uuid.uuid4())
r = s.post(
    f"{BASE}/api/sessions",
    headers=hdrs,
    json={
        "path": NB_REMOTE,
        "name": NB_REMOTE,
        "type": "notebook",
        "kernel": {"name": "python3"},
    },
    timeout=30,
)
r.raise_for_status()
sess = r.json()
kid = sess["kernel"]["id"]
sid = sess["id"]
print(f"[remote] created session {sid}  kernel {kid}", flush=True)

# --- Load code cells (skip markdown + the sweep cell) ----------------------
nb = json.load(open(NB_LOCAL, encoding="utf-8"))
cells = [
    "".join(c["source"])
    for c in nb["cells"]
    if c["cell_type"] == "code" and "for cfg_name" not in "".join(c["source"])
]
print(f"[remote] {len(cells)} code cells to execute", flush=True)

# --- Connect websocket -----------------------------------------------------
import base64
auth_b64 = base64.b64encode(f"{AUTH[0]}:{AUTH[1]}".encode()).decode()
cookie = "; ".join(f"{c.name}={c.value}" for c in s.cookies)
ws = websocket.create_connection(
    f"{WS_BASE}/api/kernels/{kid}/channels?session_id={sid}",
    header=[f"Authorization: Basic {auth_b64}", f"Cookie: {cookie}"],
    timeout=PER_CELL_TIMEOUT,
)
print("[remote] websocket connected\n", flush=True)


def make_msg(code):
    msg_id = uuid.uuid4().hex
    return msg_id, {
        "header": {
            "msg_id": msg_id,
            "username": "runner",
            "session": sid,
            "msg_type": "execute_request",
            "version": "5.3",
        },
        "parent_header": {},
        "metadata": {},
        "content": {
            "code": code,
            "silent": False,
            "store_history": True,
            "user_expressions": {},
            "allow_stdin": False,
            "stop_on_error": True,
        },
        "channel": "shell",
    }


def run_cell(idx, code):
    msg_id, msg = make_msg(code)
    ws.send(json.dumps(msg))
    deadline = time.time() + PER_CELL_TIMEOUT
    got_error = False
    while time.time() < deadline:
        try:
            raw = ws.recv()
        except websocket.WebSocketTimeoutException:
            print(f"\n[remote] cell {idx} TIMEOUT after {PER_CELL_TIMEOUT}s", flush=True)
            return False
        m = json.loads(raw)
        parent = m.get("parent_header", {}).get("msg_id")
        if parent != msg_id:
            continue
        ch = m.get("channel")
        mt = m.get("msg_type")
        content = m.get("content", {})
        if ch == "iopub":
            if mt == "stream":
                sys.stdout.write(content.get("text", ""))
                sys.stdout.flush()
            elif mt in ("execute_result", "display_data"):
                txt = content.get("data", {}).get("text/plain", "")
                if txt:
                    print(txt, flush=True)
            elif mt == "error":
                got_error = True
                print("\n".join(content.get("traceback", [])), flush=True)
        elif ch == "shell" and mt == "execute_reply":
            status = content.get("status")
            if status == "error":
                got_error = True
            return not got_error
    print(f"\n[remote] cell {idx} did not finish before deadline", flush=True)
    return False


ok = True
for i, code in enumerate(cells):
    head = code.strip().splitlines()[0] if code.strip() else ""
    print(f"\n########## CELL {i}  |  {head[:70]} ##########", flush=True)
    ok = run_cell(i, code)
    if not ok:
        print(f"[remote] stopping: cell {i} failed", flush=True)
        break

ws.close()
print("\n[remote] done. kernel left running (session id "
      f"{sid}). Delete via DELETE /api/sessions/{sid} if desired.", flush=True)

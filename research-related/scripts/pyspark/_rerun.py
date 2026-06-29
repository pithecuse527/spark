"""Re-run run_screening on the live q9 kernel with data_base overridden to S3,
then stop the session. Streams output."""
import json, uuid, base64, requests, websocket, sys

BASE = "http://static.250.169.217.95.clients.your-server.de/jupyter"
WS_BASE = "ws://static.250.169.217.95.clients.your-server.de/jupyter"
AUTH = ("admin", "ceras")
KID = "31693baa-2660-4d67-9405-d7044d0791f4"
SID = "a14c82cf-55d5-4d05-8dfa-4ee6312995d1"
TIMEOUT = 1200

CELLS = [
    # reuse already-built `spark` session + earlier vars; only override data_base
    "result = screening.run_screening(spark, SQL, CFG, label=LABEL, "
    "benchmark=BENCHMARK, scale=SCALE, data_base='s3a://spark-obj-storage', "
    "register=REGISTER, show_plans=SHOW_PLANS, show_rows=SHOW_ROWS)\nresult",
    "spark.stop()\nprint('SPARK_STOPPED')",
]

s = requests.Session(); s.auth = AUTH
s.get(f"{BASE}/lab", timeout=20)
auth_b64 = base64.b64encode(f"{AUTH[0]}:{AUTH[1]}".encode()).decode()
cookie = "; ".join(f"{c.name}={c.value}" for c in s.cookies)
ws = websocket.create_connection(
    f"{WS_BASE}/api/kernels/{KID}/channels?session_id={SID}",
    header=[f"Authorization: Basic {auth_b64}", f"Cookie: {cookie}"], timeout=TIMEOUT)


def run(code):
    mid = uuid.uuid4().hex
    ws.send(json.dumps({
        "header": {"msg_id": mid, "username": "r", "session": SID,
                   "msg_type": "execute_request", "version": "5.3"},
        "parent_header": {}, "metadata": {},
        "content": {"code": code, "silent": False, "store_history": True,
                    "user_expressions": {}, "allow_stdin": False, "stop_on_error": True},
        "channel": "shell"}))
    err = False
    while True:
        m = json.loads(ws.recv())
        if m.get("parent_header", {}).get("msg_id") != mid:
            continue
        ch, mt, c = m.get("channel"), m.get("msg_type"), m.get("content", {})
        if ch == "iopub" and mt == "stream":
            sys.stdout.write(c.get("text", "")); sys.stdout.flush()
        elif ch == "iopub" and mt in ("execute_result", "display_data"):
            t = c.get("data", {}).get("text/plain", "")
            if t: print(t, flush=True)
        elif ch == "iopub" and mt == "error":
            err = True; print("\n".join(c.get("traceback", [])), flush=True)
        elif ch == "shell" and mt == "execute_reply":
            return c.get("status") != "error" and not err


for i, code in enumerate(CELLS):
    print(f"\n########## RERUN CELL {i} ##########", flush=True)
    if not run(code):
        print(f"[rerun] cell {i} failed; stopping", flush=True); break
ws.close()
print("\n[rerun] finished", flush=True)

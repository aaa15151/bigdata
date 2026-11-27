import argparse
import json
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

parser = argparse.ArgumentParser()
parser.add_argument("--control-dir", required=True)
parser.add_argument("--port", type=int, default=8090)
args = parser.parse_args()

root = os.path.realpath(args.control_dir)
status_path = os.path.join(root, "status.json")
command_path = os.path.join(root, "command.txt")
lock = threading.Lock()

if not os.path.isfile(status_path):
    raise RuntimeError("Producer status.json does not exist")

def read_status():
    with open(status_path, "r", encoding="utf-8") as source:
        data = json.load(source)
    if not re.match(r"^[A-Za-z0-9_-]{1,80}$", data.get("run_id", "")):
        raise ValueError("Invalid run ID")
    return data

def write_command(command):
    temporary = os.path.join(root, "command.http.tmp")
    with open(temporary, "w", encoding="utf-8") as output:
        output.write(command + "\n")
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, command_path)

class Handler(BaseHTTPRequestHandler):
    def reply(self, code, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path != "/status":
            self.reply(404, {"error": "Not found"})
            return
        try:
            self.reply(200, read_status())
        except Exception:
            self.reply(503, {"error": "Cannot read producer status"})

    def do_POST(self):
        # Browser requests must go through the Windows backend.
        if self.headers.get("Origin") is not None:
            self.reply(403, {"error": "Direct browser access is disabled"})
            return

        if self.headers.get("X-Replay-Control") != "1":
            self.reply(403, {"error": "Missing control header"})
            return

        actions = {
            "/resume": "RUN",
            "/pause": "PAUSE",
            "/stop": "STOP"
        }
        command = actions.get(self.path)
        if command is None:
            self.reply(404, {"error": "Not found"})
            return

        try:
            with lock:
                state = read_status()
                if state["state"] in (
                    "STOPPED", "COMPLETED", "ERROR", "SOURCE_EXHAUSTED"
                ):
                    self.reply(409, {
                        "error": "Run has ended; prepare a new run",
                        "status": state
                    })
                    return

                # Refuse commands if the producer has stopped updating status.
                import time
                age = time.time() * 1000 - state["updated_at_ms"]
                if age > 45000:
                    self.reply(409, {
                        "error": "Producer status is stale",
                        "status": state
                    })
                    return

                write_command(command)
                self.reply(202, {
                    "accepted": True,
                    "requested": command,
                    "status": state
                })
        except Exception:
            self.reply(503, {"error": "Cannot control producer"})

server = HTTPServer(("127.0.0.1", args.port), Handler)
print("CONTROL_API_READY http://127.0.0.1:{0}".format(args.port),
      flush=True)
print("Control directory: " + root, flush=True)

try:
    server.serve_forever()
except KeyboardInterrupt:
    pass
finally:
    server.server_close()

"""Local HTTP controller. Bind to loopback only; this prototype has no authentication."""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json

from .core import Scheduler

engine = Scheduler()


class Handler(BaseHTTPRequestHandler):
    def reply(self, status, value):
        body = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        try:
            with engine.lock:
                engine._expire()
                if self.path == "/jobs":
                    return self.reply(200, [engine.snapshot(j) for j in engine.jobs])
                if self.path.startswith("/jobs/"):
                    return self.reply(200, engine.snapshot(self.path.split("/")[2]))
            self.reply(404, {"error": "not found"})
        except KeyError:
            self.reply(404, {"error": "not found"})

    def do_POST(self):
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 65536:
                raise ValueError("body must be 1..65536 bytes")
            body = json.loads(self.rfile.read(size))
            if not isinstance(body, dict):
                raise ValueError("expected JSON object")
            parts = self.path.strip("/").split("/")
            if parts == ["jobs"]:
                return self.reply(201, engine.submit(body.get("task"), body.get("payload", {}),
                    body.get("dependencies", []), body.get("priority", 0), body.get("max_retries", 2)))
            if parts == ["workers"]:
                return self.reply(201, {"id": engine.register(body.get("name"))})
            if len(parts) == 3 and parts[0] == "workers" and parts[2] == "heartbeat":
                engine.heartbeat(parts[1]); return self.reply(200, {"ok": True})
            if len(parts) == 3 and parts[0] == "workers" and parts[2] == "lease":
                return self.reply(200, engine.lease(parts[1]))
            if len(parts) == 3 and parts[0] == "jobs" and parts[2] == "complete":
                return self.reply(200, engine.complete(parts[1], body.get("worker_id"),
                    body.get("lease_token"), body.get("success") is True, body.get("result")))
            self.reply(404, {"error": "not found"})
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            self.reply(400, {"error": str(exc)})
        except KeyError:
            self.reply(404, {"error": "not found"})


if __name__ == "__main__":
    print("ThreadForge listening on http://127.0.0.1:8765", flush=True)
    ThreadingHTTPServer(("127.0.0.1", 8765), Handler).serve_forever()

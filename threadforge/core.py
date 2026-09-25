"""Thread-safe, in-memory scheduler with fenced execution leases."""
from __future__ import annotations

import threading
import time
import uuid


class Scheduler:
    def __init__(self, clock=time.monotonic, lease_seconds=10, heartbeat_seconds=15):
        self.clock = clock
        self.lease_seconds = lease_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.jobs = {}
        self.workers = {}
        self.lock = threading.RLock()

    def submit(self, task, payload=None, dependencies=None, priority=0, max_retries=2):
        if task not in ("echo", "sleep"):
            raise ValueError("unsupported task")
        if not isinstance(payload, dict) or isinstance(priority, bool) or not isinstance(priority, int):
            raise ValueError("invalid payload or priority")
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or not 0 <= max_retries <= 20:
            raise ValueError("max_retries must be 0..20")
        dependencies = dependencies or []
        if not isinstance(dependencies, list) or len(set(dependencies)) != len(dependencies):
            raise ValueError("dependencies must be unique")
        with self.lock:
            if any(d not in self.jobs for d in dependencies):
                raise ValueError("unknown dependency")
            job_id = str(uuid.uuid4())
            self.jobs[job_id] = dict(id=job_id, task=task, payload=payload,
                dependencies=dependencies, priority=priority, max_retries=max_retries,
                attempts=0, state="QUEUED", result=None, worker_id=None,
                lease_token=None, lease_until=None, created=self.clock())
            return self.snapshot(job_id)

    def snapshot(self, job_id):
        with self.lock:
            job = self.jobs[job_id]
            return {k: v for k, v in job.items() if k not in ("lease_token", "lease_until", "created")}

    def register(self, name):
        if not isinstance(name, str) or not name.strip() or len(name) > 100:
            raise ValueError("invalid worker name")
        with self.lock:
            worker_id = str(uuid.uuid4())
            self.workers[worker_id] = dict(name=name, heartbeat=self.clock())
            return worker_id

    def heartbeat(self, worker_id):
        with self.lock:
            if worker_id not in self.workers:
                raise KeyError(worker_id)
            self.workers[worker_id]["heartbeat"] = self.clock()

    def _expire(self):
        now = self.clock()
        for job in self.jobs.values():
            if job["state"] == "RUNNING" and (job["lease_until"] <= now or
                now - self.workers[job["worker_id"]]["heartbeat"] >= self.heartbeat_seconds):
                job["state"] = "QUEUED" if job["attempts"] <= job["max_retries"] else "FAILED"
                job["worker_id"] = job["lease_token"] = job["lease_until"] = None
        for job in self.jobs.values():
            if job["state"] == "QUEUED" and any(self.jobs[d]["state"] in ("FAILED", "BLOCKED") for d in job["dependencies"]):
                job["state"] = "BLOCKED"

    def lease(self, worker_id):
        with self.lock:
            self._expire()
            if worker_id not in self.workers:
                raise KeyError(worker_id)
            if self.clock() - self.workers[worker_id]["heartbeat"] >= self.heartbeat_seconds:
                raise ValueError("worker heartbeat expired")
            if any(j["state"] == "RUNNING" and j["worker_id"] == worker_id for j in self.jobs.values()):
                return None
            ready = [j for j in self.jobs.values() if j["state"] == "QUEUED" and
                all(self.jobs[d]["state"] == "SUCCEEDED" for d in j["dependencies"])]
            if not ready:
                return None
            # Inherit priority from queued descendants, avoiding inversion in dependency chains.
            def effective(job_id, visited=None):
                visited = visited or set()
                if job_id in visited:
                    raise ValueError("dependency cycle")
                visited = visited | {job_id}
                return max([self.jobs[job_id]["priority"]] + [effective(j["id"], visited)
                    for j in self.jobs.values() if j["state"] == "QUEUED" and job_id in j["dependencies"]])
            job = min(ready, key=lambda j: (-effective(j["id"]), j["created"]))
            job["state"] = "RUNNING"
            job["attempts"] += 1
            job["worker_id"] = worker_id
            job["lease_token"] = str(uuid.uuid4())
            job["lease_until"] = self.clock() + self.lease_seconds
            return {"id": job["id"], "task": job["task"], "payload": job["payload"],
                    "lease_token": job["lease_token"], "attempt": job["attempts"]}

    def complete(self, job_id, worker_id, lease_token, success, result):
        with self.lock:
            self._expire()
            job = self.jobs[job_id]
            if job["state"] != "RUNNING" or job["worker_id"] != worker_id or job["lease_token"] != lease_token:
                raise ValueError("stale or invalid lease")
            job["result"] = result
            job["state"] = "SUCCEEDED" if success else ("QUEUED" if job["attempts"] <= job["max_retries"] else "FAILED")
            job["worker_id"] = job["lease_token"] = job["lease_until"] = None
            return self.snapshot(job_id)

"""Worker for built-in tasks; never executes submitted shell commands."""
import argparse
import json
import time
import urllib.request


def post(path, body):
    request = urllib.request.Request("http://127.0.0.1:8765" + path,
        data=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


def execute(job):
    if job["task"] == "echo":
        return str(job["payload"].get("text", ""))
    if job["task"] == "sleep":
        seconds = float(job["payload"].get("seconds", 0))
        if not 0 <= seconds <= 5:
            raise ValueError("sleep seconds must be 0..5")
        time.sleep(seconds)
        return f"slept {seconds} seconds"
    raise ValueError("unsupported task")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="local-worker")
    args = parser.parse_args()
    worker_id = post("/workers", {"name": args.name})["id"]
    print(f"worker {worker_id} registered", flush=True)
    while True:
        post(f"/workers/{worker_id}/heartbeat", {})
        job = post(f"/workers/{worker_id}/lease", {})
        if job:
            try:
                result, success = execute(job), True
            except Exception as exc:
                result, success = str(exc), False
            try:
                post(f"/jobs/{job['id']}/complete", {"worker_id": worker_id,
                    "lease_token": job["lease_token"], "success": success, "result": result})
            except Exception as exc:
                print(f"completion rejected: {exc}", flush=True)
        time.sleep(0.5)


if __name__ == "__main__":
    main()

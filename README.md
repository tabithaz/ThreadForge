# ThreadForge

An early, runnable foundation for a distributed job execution platform. This first slice provides a controller, HTTP API, worker registration and heartbeats, dependency-aware scheduling, lease recovery, and an executable worker. It is a prototype: state is in memory and workers execute only built-in safe tasks (`echo` and `sleep`), not arbitrary user commands.

## Run

Requires Python 3.10+; no third-party packages.

```sh
python -m threadforge.controller
# another terminal
python -m threadforge.worker --name worker-1
# another terminal
python -m threadforge.cli submit --task echo --text hello
python -m threadforge.cli list
```

The controller listens on `127.0.0.1:8765`. For a dependency chain, submit an `echo` job, then submit another with `--depends-on JOB_ID`. The worker heartbeats and polls for a lease, executes a task, then reports completion with the lease token. Restarting the controller currently loses jobs. A later milestone will persist job state and lease fencing in PostgreSQL.

## Contracts and failure behavior

- `POST /jobs`: `{ "task": "echo", "payload": {"text":"hello"}, "dependencies": [], "priority": 0, "max_retries": 2 }`
- `GET /jobs`, `GET /jobs/{id}`: inspect state, result, and attempt count.
- `POST /workers`: register `{ "name": "worker-1" }`.
- `POST /workers/{id}/heartbeat`: extend worker liveness.
- `POST /workers/{id}/lease`: claim the highest effective priority ready job.
- `POST /jobs/{id}/complete`: report `{ "worker_id": "...", "lease_token": "...", "success": true, "result": "..." }`.

Dependencies must already exist, and cycles are rejected by the scheduler's graph validator. A failed dependency blocks its descendants. Workers have one active lease; a timed-out lease gets requeued or fails when retries are exhausted. Old completion tokens are rejected. Successful completion is committed once by the controller; a task with external side effects can still run more than once after lease expiry. Do not claim exactly-once execution.

## Next milestones

1. Persist jobs, attempts, and worker leases in PostgreSQL with transactional claims and startup recovery.
2. Add controller/worker authentication and a container runtime with limits; do not expose the prototype on an untrusted network.
3. Implement cancellation, structured logs, metrics, benchmarks, and fault injection.
4. Introduce a versioned gRPC worker protocol and benchmark scheduling policies before considering a Go controller or C++ worker.

# Distributed Task Scheduler with Fault-Tolerant Execution

A job scheduler in **C++17**: submit tasks over a REST API, and a pool of workers runs them with **priorities, retries, heartbeats and task leases**. If a worker crashes mid-task, another worker picks the task up automatically.

```
           POST /tasks                       claim (atomic Lua)            heartbeat every 1s
client ───────────────▶ API ──┬──▶ PostgreSQL ◀─────── worker ×N ──────────────────────────▶ Redis
                              │    (source of truth)     │  run task                       (lease renewed)
                              └──▶ Redis queue ──────────┘  ack / retry with backoff / dead-letter
                                   (priority sorted set)
```

## How it works
| Concern | Mechanism |
|---|---|
| **Durable state** | Every task is a row in PostgreSQL (status, attempts, result, last error, timestamps). Redis holds only the scheduling state, with AOF persistence on. |
| **Priority + FIFO** | Redis sorted set scored `(9 − priority) × 10¹³ + enqueue_ms`. Higher priority pops first; ties keep arrival order. |
| **No double-claiming** | Claiming is one Lua script, `ZPOPMIN` + lease + owner, so it runs atomically inside Redis. |
| **Task leases** | A claimed task is leased for 5s. The worker renews the lease every 1s from a heartbeat thread. |
| **Crash recovery** | If a worker dies, its heartbeats stop and the lease expires. Any worker's reaper moves expired tasks back to the queue. |
| **Stale-worker fencing** | Results are only written `WHERE worker_id = me AND status = 'running'`, and a worker that lost its lease discards its result, so a slow "zombie" worker can't overwrite the real one. |
| **Retries** | A failed task goes to a delayed set with exponential backoff (500 ms, 1 s, 2 s… capped at 30 s), up to `max_retries`. After that it's marked `failed` (dead letter). |
| **Acknowledgement** | Success removes the lease and records the result. Delivery is **at-least-once**, so tasks should be idempotent. |
| **Reconciliation** | If Redis was down at submit time, workers find `queued` rows that Redis doesn't know about and re-enqueue them. |
| **Graceful shutdown** | On SIGTERM, a worker finishes its current task before exiting. |

## API
| Method | Path | Notes |
|---|---|---|
| `POST` | `/tasks` | `{"type": "fib", "payload": {"n": 40}, "priority": 0-9, "max_retries": 0-10}` → `201 {"id": 17}` |
| `GET` | `/tasks/{id}` | status, attempts, worker, result, last error, timestamps |
| `DELETE` | `/tasks/{id}` | cancels a task that hasn't started; `409` if it's running or finished |
| `GET` | `/stats` | counts per status, and ready / delayed / leased queue depths |
| `GET` | `/health` | |

Built-in task types for demos and tests: `sleep {ms}` (I/O-like), `fib {n}` (CPU-bound), `echo`, `fail` (always fails), and `flaky {fail_times}` (fails *n* times, then succeeds).

## Run it
```bash
docker compose up --build                 # Redis, PostgreSQL, API on :8080, 3 workers
curl -X POST localhost:8080/tasks -d '{"type":"fib","payload":{"n":40},"priority":7}'
curl 127.0.0.1:8080/tasks/1
docker compose up --scale worker=5        # scale workers horizontally
python scripts/docker_check.py            # end-to-end check incl. killing a worker container
```

## Tests
- **Unit (C++, 26 checks):** queue ordering, backoff, retry budget, input validation, task execution.
- **Integration (`tests/integration_test.py`, 8 scenarios):** runs real Redis, PostgreSQL, the API and 3 workers.
  - 60 tasks spread across workers
  - priority order (a high-priority task overtakes an earlier low-priority one)
  - retry with backoff, then success
  - dead-letter after max retries
  - cancelling a queued task
  - **a worker killed with SIGKILL mid-task, with the task finished by another worker on attempt 2**
  - stats

  Passed 10 consecutive runs, and also passed with the API and workers built under AddressSanitizer and UBSan.
- **Docker Compose check (`scripts/docker_check.py`):** run on Windows with Docker Desktop. 30 tasks ran across 3 worker containers, a flaky task succeeded on attempt 3, and after `docker kill` on the container running a long task, another worker finished it on attempt 2.

```bash
cmake -S . -B build && cmake --build build && ./build/test_task_logic
REDIS_PORT=6379 PG_DSN=postgresql://user@localhost/scheduler BUILD_DIR=build python3 tests/integration_test.py
```

## Trade-offs and limits
- **At-least-once, not exactly-once:** a task can run twice if a worker finishes but crashes before acknowledging. Exactly-once would need idempotency keys or transactional outbox patterns.
- **Lease length is a trade-off:** shorter leases recover faster but risk false expiry during long GC pauses or network blips. Heartbeats make long tasks safe.
- **Redis is a single node here.** Production would use Redis Sentinel or Cluster. The Lua scripts touch several keys, so in a cluster those keys would need a shared hash tag.

"""End-to-end tests against a real Redis, PostgreSQL, API and worker processes.

Starts its own API and workers (from BUILD_DIR) against Redis and PostgreSQL given by env vars,
then checks: throughput, priority order, retries with backoff, dead-lettering, cancellation,
and recovery when a worker is killed mid-task.

    REDIS_PORT=6379 PG_DSN=postgresql://... BUILD_DIR=build python3 tests/integration_test.py
Uses only the Python standard library.
"""
import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

BUILD = os.environ.get("BUILD_DIR", "build")
API_PORT = int(os.environ.get("API_PORT", "18080"))
API = f"http://127.0.0.1:{API_PORT}"
ENV = {**os.environ, "API_PORT": str(API_PORT), "LEASE_MS": "1500", "HEARTBEAT_MS": "300",
       "BACKOFF_BASE_MS": "200", "POLL_MS": "50"}

procs = []
results = []


def http(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def submit(**task):
    status, body = http("POST", "/tasks", task)
    assert status == 201, (status, body)
    return body["id"]


def task(tid):
    return http("GET", f"/tasks/{tid}")[1]


def wait_for(tids, statuses=("succeeded", "failed", "cancelled"), timeout=30):
    end = time.time() + timeout
    while time.time() < end:
        tasks = [task(t) for t in tids]
        if all(t["status"] in statuses for t in tasks):
            return tasks
        time.sleep(0.1)
    raise AssertionError(f"timed out; statuses: {[t['status'] for t in tasks]}")


def start(binary, name):
    log = open(f"/tmp/{name}.log", "w")
    p = subprocess.Popen([os.path.join(BUILD, binary)], env={**ENV, "WORKER_ID": name},
                         stdout=log, stderr=subprocess.STDOUT)
    procs.append(p)
    return p


def check(name, fn):
    t0 = time.time()
    try:
        fn()
        results.append((name, True, ""))
        print(f"PASS  {name} ({time.time() - t0:.1f}s)")
    except Exception as e:  # noqa: BLE001 - report every failure
        results.append((name, False, str(e)))
        print(f"FAIL  {name}: {e}")


# ---------------------------------------------------------------- tests

def test_validation():
    assert http("POST", "/tasks", {"type": "nope"})[0] == 422
    assert http("POST", "/tasks", {"type": "fib", "priority": 99})[0] == 422
    assert http("GET", "/tasks/999999999")[0] == 404
    assert http("GET", "/tasks/abc")[0] == 400


def test_many_tasks_all_succeed():
    ids = [submit(type="fib", payload={"n": 30 + i % 10}) for i in range(60)]
    tasks = wait_for(ids)
    assert all(t["status"] == "succeeded" for t in tasks)
    assert len({t["worker_id"] for t in tasks}) >= 2, "work should spread across workers"
    assert task(ids[0])["result"] == "832040"  # fib(30)


def test_priority_order():
    # Occupy all 3 workers; one blocker is much shorter, so exactly one worker frees up first.
    # That worker must take the high-priority task before the low one, even though low was queued first.
    blockers = [submit(type="sleep", payload={"ms": ms}, priority=9) for ms in (800, 3000, 3000)]
    time.sleep(0.3)
    low = submit(type="echo", payload={"p": "low"}, priority=1)
    high = submit(type="echo", payload={"p": "high"}, priority=9)
    wait_for([low, high])
    lo, hi = task(low), task(high)
    assert hi["worker_id"] == lo["worker_id"], "both should run on the first worker to free up"
    assert hi["finished_at"] < lo["finished_at"], (hi["finished_at"], lo["finished_at"])
    wait_for(blockers)


def test_retry_then_succeed():
    tid = submit(type="flaky", payload={"fail_times": 2}, max_retries=3)
    t = wait_for([tid])[0]
    assert t["status"] == "succeeded" and t["attempts"] == 3, t
    assert "attempt 2" in (t["last_error"] or "")


def test_dead_letter_after_max_retries():
    tid = submit(type="fail", max_retries=2)
    t = wait_for([tid])[0]
    assert t["status"] == "failed" and t["attempts"] == 3, t


def test_cancel_queued_task():
    blockers = [submit(type="sleep", payload={"ms": 1000}, priority=9) for _ in range(3)]
    time.sleep(0.3)
    tid = submit(type="echo", priority=0)
    status, body = http("DELETE", f"/tasks/{tid}")
    assert status == 200 and body["status"] == "cancelled", (status, body)
    assert task(tid)["status"] == "cancelled"
    wait_for(blockers)
    assert task(tid)["status"] == "cancelled", "cancelled task must never run"
    status, _ = http("DELETE", f"/tasks/{blockers[0]}")
    assert status == 409  # finished tasks can't be cancelled


def test_worker_crash_recovery():
    # A long task starts on some worker; we SIGKILL that worker mid-run. Its lease expires,
    # another worker picks the task up, and it still succeeds.
    tid = submit(type="sleep", payload={"ms": 2500}, priority=9, max_retries=3)
    end = time.time() + 10
    while task(tid)["status"] != "running":
        assert time.time() < end, "task never started"
        time.sleep(0.05)
    victim = task(tid)["worker_id"]
    proc = next(p for p, name in zip(procs[1:], WORKERS) if name == victim)
    proc.send_signal(signal.SIGKILL)
    proc.wait()
    t = wait_for([tid], timeout=30)[0]
    assert t["status"] == "succeeded", t
    assert t["worker_id"] != victim, "a different worker should have finished it"
    assert t["attempts"] == 2, t
    # bring a replacement worker back for the remaining tests
    new_name = victim + "-restarted"
    p = start("scheduler_worker", new_name)
    procs[procs.index(proc)] = p
    WORKERS[WORKERS.index(victim)] = new_name


def test_stats():
    status, body = http("GET", "/stats")
    assert status == 200
    assert body["tasks"]["succeeded"] >= 60
    assert body["queue"]["leased"] == 0 and body["queue"]["ready"] == 0


WORKERS = ["worker-1", "worker-2", "worker-3"]


def main():
    try:
        start("scheduler_api", "api")
        for name in WORKERS:
            start("scheduler_worker", name)
        end = time.time() + 15
        while True:
            try:
                if http("GET", "/health")[0] == 200:
                    break
            except Exception:  # noqa: BLE001 - not up yet
                pass
            if time.time() > end:
                sys.exit("API did not start")
            time.sleep(0.2)

        for name, fn in [
            ("rejects invalid input", test_validation),
            ("60 tasks across 3 workers", test_many_tasks_all_succeed),
            ("priority order", test_priority_order),
            ("retry with backoff then succeed", test_retry_then_succeed),
            ("dead-letter after max retries", test_dead_letter_after_max_retries),
            ("cancel a queued task", test_cancel_queued_task),
            ("worker killed mid-task -> recovered", test_worker_crash_recovery),
            ("stats", test_stats),
        ]:
            check(name, fn)
    finally:
        for p in procs:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        for p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()

    passed = sum(ok for _, ok, _ in results)
    print(f"\n{passed}/{len(results)} integration tests passed")
    sys.exit(0 if passed == len(results) else 1)


if __name__ == "__main__":
    main()

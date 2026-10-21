"""Checks a running `docker compose` stack end to end, including killing a worker container.

    docker compose up -d --build
    python scripts/docker_check.py
Uses only the Python standard library plus the docker CLI.
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request

API = f"http://127.0.0.1:{os.environ.get('API_PORT', '8080')}"


def http(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(API + path, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def wait(ids, timeout=60):
    end = time.time() + timeout
    while time.time() < end:
        tasks = [http("GET", f"/tasks/{i}")[1] for i in ids]
        if all(t["status"] in ("succeeded", "failed", "cancelled") for t in tasks):
            return tasks
        time.sleep(0.2)
    raise SystemExit("timed out waiting for tasks")


def main():
    end = time.time() + 120
    while True:
        try:
            if http("GET", "/health")[0] == 200:
                break
        except Exception:  # noqa: BLE001 - not up yet
            pass
        if time.time() > end:
            raise SystemExit("API not reachable")
        time.sleep(1)
    print("API is up")

    ids = [http("POST", "/tasks", {"type": "fib", "payload": {"n": 35}})[1]["id"] for _ in range(30)]
    tasks = wait(ids)
    workers = {t["worker_id"] for t in tasks}
    ok = all(t["status"] == "succeeded" for t in tasks)
    print(f"30 tasks: {'all succeeded' if ok else 'FAILURES'}, spread over {len(workers)} workers")

    tid = http("POST", "/tasks", {"type": "flaky", "payload": {"fail_times": 2}})[1]["id"]
    t = wait([tid])[0]
    retry_ok = t["status"] == "succeeded" and t["attempts"] == 3
    print(f"flaky task: {t['status']} after {t['attempts']} attempts")

    # Kill the container running a long task and check another worker finishes it.
    tid = http("POST", "/tasks", {"type": "sleep", "payload": {"ms": 6000}, "priority": 9})[1]["id"]
    while http("GET", f"/tasks/{tid}")[1]["status"] != "running":
        time.sleep(0.1)
    victim = http("GET", f"/tasks/{tid}")[1]["worker_id"]
    container = victim.rsplit("-", 1)[0]  # worker id is "<container hostname>-<pid>"
    subprocess.run(["docker", "kill", container], check=True, capture_output=True)
    print(f"killed worker container {container} mid-task")
    t = wait([tid], timeout=90)[0]
    crash_ok = t["status"] == "succeeded" and t["worker_id"] != victim
    print(f"long task: {t['status']} on {t['worker_id']} after {t['attempts']} attempts")

    print("stats:", http("GET", "/stats")[1])
    passed = ok and retry_ok and crash_ok and len(workers) >= 2
    print("DOCKER CHECK PASSED" if passed else "DOCKER CHECK FAILED")
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()

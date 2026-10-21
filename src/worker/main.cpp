// Worker: claims tasks, runs them with a heartbeat, and acknowledges, retries or dead-letters them.
// Every worker also runs the "reaper" that recovers tasks from crashed workers; Redis Lua scripts
// make that safe to run in several workers at once.
#include <atomic>
#include <csignal>
#include <iostream>
#include <thread>
#include <unistd.h>

#include "../common/config.hpp"
#include "../common/task_logic.hpp"
#include "../common/task_queue.hpp"
#include "../common/task_store.hpp"

using namespace dts;

namespace {

std::atomic<bool> stopping{false};

void on_signal(int) { stopping = true; }  // finish the current task, then exit

void log(const std::string& worker, const std::string& msg) {
    std::cout << "[" << worker << "] " << msg << std::endl;
}

std::string worker_name() {
    char host[256] = {};
    gethostname(host, sizeof(host) - 1);
    return env("WORKER_ID", std::string(host) + "-" + std::to_string(getpid()));
}

}  // namespace

int main() {
    std::signal(SIGTERM, on_signal);
    std::signal(SIGINT, on_signal);
    const Config cfg;
    const std::string me = worker_name();

    RedisClient redis(cfg.redis_host, cfg.redis_port);
    TaskQueue queue(redis);
    TaskStore store(cfg.pg_dsn);
    store.migrate();
    log(me, "started");

    std::int64_t next_maintenance = 0, next_reconcile = 0;
    while (!stopping) {
        const std::int64_t now = now_ms();

        // Maintenance: recover expired leases and release delayed retries.
        if (now >= next_maintenance) {
            for (const auto& id : queue.requeue_expired(now)) log(me, "lease expired, re-queued task " + id);
            queue.promote_due(now);
            next_maintenance = now + 500;
        }
        // Reconcile: tasks saved in PostgreSQL but never enqueued (e.g. Redis was down at submit).
        if (now >= next_reconcile) {
            for (const auto& id : store.queued_older_than(10)) {
                if (!queue.is_tracked(id)) {
                    if (auto t = store.get(id)) {
                        queue.enqueue(id, queue_score((*t)["priority"].get<int>(), now));
                        log(me, "reconciled untracked task " + id);
                    }
                }
            }
            next_reconcile = now + 10'000;
        }

        auto claimed_id = queue.claim(me, now + cfg.lease_ms);
        if (!claimed_id) {
            std::this_thread::sleep_for(std::chrono::milliseconds(cfg.poll_ms));
            continue;
        }
        const std::string id = *claimed_id;
        auto task = store.start(id, me);
        if (!task) {  // cancelled or already finished: drop it from Redis
            queue.ack(id, me);
            continue;
        }
        if (task->attempt > task->max_retries + 1) {  // kept dying mid-run (e.g. crashes every time)
            store.fail(id, me, "gave up after " + std::to_string(task->attempt - 1) + " attempts");
            queue.ack(id, me);
            log(me, "task " + id + " failed permanently (too many attempts)");
            continue;
        }
        log(me, "running task " + id + " (" + task->type + ", attempt " + std::to_string(task->attempt) + ")");

        // Heartbeat on a separate thread with its own Redis connection, renewing the lease.
        std::atomic<bool> done{false}, lease_lost{false};
        std::thread heartbeat([&] {
            RedisClient hb_redis(cfg.redis_host, cfg.redis_port);
            TaskQueue hb_queue(hb_redis);
            while (!done) {
                for (int waited = 0; waited < cfg.heartbeat_ms && !done; waited += 50) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
                if (done) break;
                if (!hb_queue.heartbeat(id, me, now_ms() + cfg.lease_ms)) {
                    lease_lost = true;
                    break;
                }
            }
        });

        std::string result, error;
        bool ok = true;
        try {
            result = execute_task(task->type, task->payload, task->attempt);
        } catch (const std::exception& e) {
            ok = false;
            error = e.what();
        }
        done = true;
        heartbeat.join();

        if (lease_lost) {  // someone else may own this task now; our result must not win
            log(me, "lost lease on task " + id + ", discarding result");
            continue;
        }
        if (ok) {
            store.succeed(id, me, result);
            queue.ack(id, me);
            log(me, "task " + id + " succeeded");
        } else if (should_retry(task->attempt, task->max_retries)) {
            const auto delay = backoff_ms(task->attempt, cfg.backoff_base_ms);
            store.requeue(id, me, error);
            queue.retry_later(id, me, now_ms() + delay);
            log(me, "task " + id + " failed (" + error + "), retrying in " + std::to_string(delay) + " ms");
        } else {
            store.fail(id, me, error);
            queue.ack(id, me);
            log(me, "task " + id + " failed permanently: " + error);
        }
    }
    log(me, "stopped");
}

// Redis-backed task queue. Every multi-step operation is one Lua script, so it runs
// atomically on the Redis server and two workers can never claim the same task.
//
// Keys:
//   dts:queue    sorted set  task id -> priority/FIFO score     (ready to run)
//   dts:delayed  sorted set  task id -> time it becomes ready   (retry backoff)
//   dts:leases   sorted set  task id -> lease expiry time (ms)  (being run)
//   dts:owners   hash        task id -> worker id holding the lease
//   dts:scores   hash        task id -> its queue score (to re-queue at the same priority)
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "redis_client.hpp"

namespace dts {

struct QueueStats {
    long long ready = 0, delayed = 0, leased = 0;
};

class TaskQueue {
public:
    explicit TaskQueue(RedisClient& redis) : redis_(redis) {}

    void enqueue(const std::string& id, double score);

    // Atomically pops the highest-priority ready task and leases it to `worker` until `lease_until_ms`.
    std::optional<std::string> claim(const std::string& worker, std::int64_t lease_until_ms);

    // Extends the lease, but only if `worker` still owns it. False means the lease was lost.
    bool heartbeat(const std::string& id, const std::string& worker, std::int64_t lease_until_ms);

    // Acknowledges completion: releases the lease if `worker` still owns it.
    bool ack(const std::string& id, const std::string& worker);

    // Releases the lease and schedules the task to become ready again at `ready_at_ms`.
    bool retry_later(const std::string& id, const std::string& worker, std::int64_t ready_at_ms);

    // Moves tasks whose lease expired (worker crashed or hung) back to the ready queue.
    std::vector<std::string> requeue_expired(std::int64_t now_ms);

    // Moves delayed tasks whose backoff has elapsed into the ready queue.
    std::vector<std::string> promote_due(std::int64_t now_ms);

    // Removes a task that hasn't started yet. False if it isn't waiting (running or finished).
    bool cancel(const std::string& id);

    // True if the task is anywhere in Redis (ready, delayed or leased).
    bool is_tracked(const std::string& id);

    QueueStats stats();

private:
    RedisClient& redis_;
};

}  // namespace dts

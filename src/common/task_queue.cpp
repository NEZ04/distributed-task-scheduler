#include "task_queue.hpp"

namespace dts {

namespace {

const std::string kQueue = "dts:queue";
const std::string kDelayed = "dts:delayed";
const std::string kLeases = "dts:leases";
const std::string kOwners = "dts:owners";
const std::string kScores = "dts:scores";

const char* kClaim = R"lua(
local popped = redis.call('ZPOPMIN', KEYS[1])
if #popped == 0 then return false end
local id = popped[1]
redis.call('ZADD', KEYS[2], ARGV[1], id)
redis.call('HSET', KEYS[3], id, ARGV[2])
return id
)lua";

const char* kHeartbeat = R"lua(
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('ZADD', KEYS[1], 'XX', ARGV[3], ARGV[1])
return 1
)lua";

const char* kAck = R"lua(
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
return 1
)lua";

const char* kRetryLater = R"lua(
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[2] then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1])
return 1
)lua";

// Shared by "expired leases -> queue" and "due delayed tasks -> queue".
const char* kMoveDue = R"lua(
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 100)
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  if KEYS[4] ~= '' then redis.call('HDEL', KEYS[4], id) end
  local score = redis.call('HGET', KEYS[3], id)
  if score then redis.call('ZADD', KEYS[2], score, id) end
end
return ids
)lua";

const char* kCancel = R"lua(
local removed = redis.call('ZREM', KEYS[1], ARGV[1]) + redis.call('ZREM', KEYS[2], ARGV[1])
if removed > 0 then redis.call('HDEL', KEYS[3], ARGV[1]) end
return removed
)lua";

std::vector<std::string> string_array(const Reply& r) {
    std::vector<std::string> out;
    if (r && r->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i < r->elements; ++i) out.emplace_back(r->element[i]->str, r->element[i]->len);
    }
    return out;
}

}  // namespace

void TaskQueue::enqueue(const std::string& id, double score) {
    const std::string s = std::to_string(static_cast<long long>(score));
    redis_.command({"HSET", kScores, id, s});
    redis_.command({"ZADD", kQueue, s, id});
}

std::optional<std::string> TaskQueue::claim(const std::string& worker, std::int64_t lease_until_ms) {
    auto r = redis_.eval(kClaim, {kQueue, kLeases, kOwners}, {std::to_string(lease_until_ms), worker});
    if (r->type != REDIS_REPLY_STRING) return std::nullopt;  // Lua false -> nil: queue empty
    return std::string(r->str, r->len);
}

bool TaskQueue::heartbeat(const std::string& id, const std::string& worker, std::int64_t lease_until_ms) {
    return redis_.eval(kHeartbeat, {kLeases, kOwners}, {id, worker, std::to_string(lease_until_ms)})->integer == 1;
}

bool TaskQueue::ack(const std::string& id, const std::string& worker) {
    return redis_.eval(kAck, {kLeases, kOwners, kScores}, {id, worker})->integer == 1;
}

bool TaskQueue::retry_later(const std::string& id, const std::string& worker, std::int64_t ready_at_ms) {
    return redis_.eval(kRetryLater, {kLeases, kOwners, kDelayed}, {id, worker, std::to_string(ready_at_ms)})
               ->integer == 1;
}

std::vector<std::string> TaskQueue::requeue_expired(std::int64_t now_ms) {
    return string_array(redis_.eval(kMoveDue, {kLeases, kQueue, kScores, kOwners}, {std::to_string(now_ms)}));
}

std::vector<std::string> TaskQueue::promote_due(std::int64_t now_ms) {
    return string_array(redis_.eval(kMoveDue, {kDelayed, kQueue, kScores, ""}, {std::to_string(now_ms)}));
}

bool TaskQueue::cancel(const std::string& id) {
    return redis_.eval(kCancel, {kQueue, kDelayed, kScores}, {id})->integer > 0;
}

bool TaskQueue::is_tracked(const std::string& id) {
    for (const auto& key : {kQueue, kDelayed, kLeases}) {
        if (redis_.command({"ZSCORE", key, id})->type != REDIS_REPLY_NIL) return true;
    }
    return false;
}

QueueStats TaskQueue::stats() {
    return {redis_.command({"ZCARD", kQueue})->integer, redis_.command({"ZCARD", kDelayed})->integer,
            redis_.command({"ZCARD", kLeases})->integer};
}

}  // namespace dts

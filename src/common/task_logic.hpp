// Pure scheduling logic with no Redis or PostgreSQL dependency, so it can be unit-tested directly.
#pragma once

#include <cstdint>
#include <string>

#include <nlohmann/json.hpp>

namespace dts {

constexpr int kMinPriority = 0;
constexpr int kMaxPriority = 9;

// Queue score: lower pops first. Higher priority sorts ahead; within a priority, earlier
// tasks go first (FIFO). Fits in a double's 53-bit integer range: 9e13 + 2e12 < 9e15.
inline double queue_score(int priority, std::int64_t enqueued_ms) {
    return static_cast<double>(kMaxPriority - priority) * 1e13 + static_cast<double>(enqueued_ms);
}

// Exponential backoff: base * 2^(attempt-1), capped. attempt is 1-based.
inline std::int64_t backoff_ms(int attempt, std::int64_t base_ms = 500, std::int64_t cap_ms = 30'000) {
    if (attempt < 1) attempt = 1;
    std::int64_t delay = base_ms;
    for (int i = 1; i < attempt && delay < cap_ms; ++i) delay *= 2;
    return delay < cap_ms ? delay : cap_ms;
}

// Whether a task that just failed on `attempt` gets another try.
inline bool should_retry(int attempt, int max_retries) { return attempt <= max_retries; }

struct TaskSpec {
    std::string type;
    nlohmann::json payload;
    int priority;
    int max_retries;
};

// Validates a submitted task. Throws std::invalid_argument with a readable message.
TaskSpec parse_task_spec(const nlohmann::json& body);

// Runs a built-in task type and returns its result. Throws on failure, which triggers a retry.
//   sleep {ms}           sleeps, simulating I/O-bound work
//   fib {n}              computes a Fibonacci number, simulating CPU-bound work
//   echo {...}           returns the payload
//   fail {}              always fails, to show retries ending in a dead letter
//   flaky {fail_times}   fails on the first `fail_times` attempts, then succeeds
std::string execute_task(const std::string& type, const nlohmann::json& payload, int attempt);

}  // namespace dts

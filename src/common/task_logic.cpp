#include "task_logic.hpp"

#include <chrono>
#include <stdexcept>
#include <thread>

namespace dts {

using nlohmann::json;

namespace {

int int_field(const json& obj, const char* key, int fallback, int lo, int hi) {
    if (!obj.contains(key)) return fallback;
    if (!obj[key].is_number_integer()) throw std::invalid_argument(std::string(key) + " must be an integer");
    const int v = obj[key].get<int>();
    if (v < lo || v > hi) {
        throw std::invalid_argument(std::string(key) + " must be between " + std::to_string(lo) + " and " +
                                    std::to_string(hi));
    }
    return v;
}

}  // namespace

TaskSpec parse_task_spec(const json& body) {
    if (!body.is_object()) throw std::invalid_argument("body must be a JSON object");
    if (!body.contains("type") || !body["type"].is_string()) throw std::invalid_argument("type is required");
    TaskSpec spec;
    spec.type = body["type"].get<std::string>();
    static const char* known[] = {"sleep", "fib", "echo", "fail", "flaky"};
    bool ok = false;
    for (const char* k : known) ok = ok || spec.type == k;
    if (!ok) throw std::invalid_argument("unknown task type '" + spec.type + "'");
    spec.payload = body.value("payload", json::object());
    if (!spec.payload.is_object()) throw std::invalid_argument("payload must be an object");
    spec.priority = int_field(body, "priority", 5, kMinPriority, kMaxPriority);
    spec.max_retries = int_field(body, "max_retries", 3, 0, 10);
    if (spec.type == "sleep") int_field(spec.payload, "ms", 100, 0, 600'000);
    if (spec.type == "fib") int_field(spec.payload, "n", 30, 0, 90);
    if (spec.type == "flaky") int_field(spec.payload, "fail_times", 1, 0, 10);
    return spec;
}

std::string execute_task(const std::string& type, const json& payload, int attempt) {
    if (type == "sleep") {
        std::this_thread::sleep_for(std::chrono::milliseconds(payload.value("ms", 100)));
        return "slept " + std::to_string(payload.value("ms", 100)) + " ms";
    }
    if (type == "fib") {
        const int n = payload.value("n", 30);
        unsigned long long a = 0, b = 1;
        for (int i = 0; i < n; ++i) {
            const auto next = a + b;
            a = b;
            b = next;
        }
        return std::to_string(a);
    }
    if (type == "echo") return payload.dump();
    if (type == "fail") throw std::runtime_error("task type 'fail' always fails");
    if (type == "flaky") {
        if (attempt <= payload.value("fail_times", 1)) {
            throw std::runtime_error("flaky failure on attempt " + std::to_string(attempt));
        }
        return "succeeded on attempt " + std::to_string(attempt);
    }
    throw std::runtime_error("unknown task type '" + type + "'");
}

}  // namespace dts

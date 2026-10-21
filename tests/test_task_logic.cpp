// Unit tests for the pure scheduling logic (no Redis/PostgreSQL needed).
#include <iostream>
#include <stdexcept>

#include "../src/common/task_logic.hpp"

using namespace dts;
using nlohmann::json;

static int failures = 0, checks = 0;
#define CHECK(cond)                                                                    \
    do {                                                                               \
        ++checks;                                                                      \
        if (!(cond)) {                                                                 \
            ++failures;                                                                \
            std::cerr << "  FAIL line " << __LINE__ << ": " #cond "\n";                \
        }                                                                              \
    } while (0)

template <typename F>
static bool throws(F f) {
    try { f(); } catch (const std::exception&) { return true; }
    return false;
}

int main() {
    // Higher priority pops first (lower score); FIFO within the same priority.
    CHECK(queue_score(9, 2'000) < queue_score(1, 1'000));
    CHECK(queue_score(5, 1'000) < queue_score(5, 2'000));
    CHECK(queue_score(0, 1'900'000'000'000) < 9.007e15);  // stays exact in a double

    // Exponential backoff, capped.
    CHECK(backoff_ms(1, 500) == 500);
    CHECK(backoff_ms(2, 500) == 1000);
    CHECK(backoff_ms(4, 500) == 4000);
    CHECK(backoff_ms(20, 500, 30'000) == 30'000);
    CHECK(backoff_ms(0, 500) == 500);

    // Retry budget: max_retries=2 means attempts 1, 2, 3.
    CHECK(should_retry(1, 2));
    CHECK(should_retry(2, 2));
    CHECK(!should_retry(3, 2));
    CHECK(!should_retry(1, 0));

    // Validation.
    auto spec = parse_task_spec(json{{"type", "fib"}, {"payload", {{"n", 10}}}});
    CHECK(spec.priority == 5 && spec.max_retries == 3);
    CHECK(throws([] { parse_task_spec(json{{"payload", json::object()}}); }));        // no type
    CHECK(throws([] { parse_task_spec(json{{"type", "rm -rf"}}); }));                   // unknown type
    CHECK(throws([] { parse_task_spec(json{{"type", "fib"}, {"priority", 42}}); }));   // out of range
    CHECK(throws([] { parse_task_spec(json{{"type", "fib"}, {"priority", "high"}}); }));
    CHECK(throws([] { parse_task_spec(json{{"type", "fib"}, {"payload", {{"n", 500}}}}); }));
    CHECK(throws([] { parse_task_spec(json{{"type", "sleep"}, {"payload", "x"}}); }));
    CHECK(throws([] { parse_task_spec(json::array()); }));

    // Execution.
    CHECK(execute_task("fib", {{"n", 10}}, 1) == "55");
    CHECK(execute_task("fib", {{"n", 90}}, 1) == "2880067194370816120");
    CHECK(execute_task("echo", {{"a", 1}}, 1) == R"({"a":1})");
    CHECK(throws([] { execute_task("fail", json::object(), 1); }));
    CHECK(throws([] { execute_task("flaky", {{"fail_times", 2}}, 2); }));
    CHECK(execute_task("flaky", {{"fail_times", 2}}, 3) == "succeeded on attempt 3");

    std::cout << checks - failures << "/" << checks << " checks passed\n";
    return failures ? 1 : 0;
}

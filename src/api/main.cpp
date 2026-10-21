// REST API: submit, inspect and cancel tasks.
//
//   POST   /tasks       {"type": "fib", "payload": {"n": 40}, "priority": 7, "max_retries": 3}
//   GET    /tasks/:id
//   DELETE /tasks/:id   cancels a task that hasn't started yet
//   GET    /stats       counts by status + queue depths
//   GET    /health
#include <iostream>
#include <memory>

#include <httplib.h>
#include <nlohmann/json.hpp>

#include "../common/config.hpp"
#include "../common/task_logic.hpp"
#include "../common/task_queue.hpp"
#include "../common/task_store.hpp"

using nlohmann::json;
using namespace dts;

namespace {

const Config cfg;

// httplib serves requests on a thread pool; each thread keeps its own connections.
TaskStore& store() {
    thread_local std::unique_ptr<TaskStore> s;
    if (!s) s = std::make_unique<TaskStore>(cfg.pg_dsn);
    return *s;
}

RedisClient& redis() {
    thread_local std::unique_ptr<RedisClient> r;
    if (!r) r = std::make_unique<RedisClient>(cfg.redis_host, cfg.redis_port);
    return *r;
}

void reply(httplib::Response& res, int status, const json& body) {
    res.status = status;
    res.set_content(body.dump(), "application/json");
}

bool valid_id(const std::string& id) {
    return !id.empty() && id.size() < 19 && id.find_first_not_of("0123456789") == std::string::npos;
}

}  // namespace

int main() {
    store().migrate();
    httplib::Server server;

    server.Get("/health", [](const httplib::Request&, httplib::Response& res) { reply(res, 200, {{"status", "ok"}}); });

    server.Post("/tasks", [](const httplib::Request& req, httplib::Response& res) {
        json body = json::parse(req.body, nullptr, false);
        if (body.is_discarded()) return reply(res, 400, {{"error", "invalid JSON"}});
        TaskSpec spec;
        try {
            spec = parse_task_spec(body);
        } catch (const std::invalid_argument& e) {
            return reply(res, 422, {{"error", e.what()}});
        }
        // Write to PostgreSQL first (the source of truth), then make it runnable in Redis.
        // If Redis is briefly unavailable, workers' reconciliation re-queues it later.
        const std::string id = store().insert(spec);
        try {
            TaskQueue(redis()).enqueue(id, queue_score(spec.priority, now_ms()));
        } catch (const std::exception& e) {
            std::cerr << "enqueue of task " << id << " deferred: " << e.what() << "\n";
        }
        reply(res, 201, {{"id", std::stoll(id)}, {"status", "queued"}});
    });

    server.Get(R"(/tasks/(\w+))", [](const httplib::Request& req, httplib::Response& res) {
        const std::string id = req.matches[1];
        if (!valid_id(id)) return reply(res, 400, {{"error", "invalid id"}});
        auto task = store().get(id);
        if (!task) return reply(res, 404, {{"error", "task not found"}});
        reply(res, 200, *task);
    });

    server.Delete(R"(/tasks/(\w+))", [](const httplib::Request& req, httplib::Response& res) {
        const std::string id = req.matches[1];
        if (!valid_id(id)) return reply(res, 400, {{"error", "invalid id"}});
        auto task = store().get(id);
        if (!task) return reply(res, 404, {{"error", "task not found"}});
        if (TaskQueue(redis()).cancel(id) && store().cancel_if_queued(id)) {
            return reply(res, 200, {{"id", std::stoll(id)}, {"status", "cancelled"}});
        }
        reply(res, 409, {{"error", "task is " + (*task)["status"].get<std::string>() + " and can't be cancelled"}});
    });

    server.Get("/stats", [](const httplib::Request&, httplib::Response& res) {
        const QueueStats q = TaskQueue(redis()).stats();
        reply(res, 200, {{"tasks", store().counts_by_status()},
                         {"queue", {{"ready", q.ready}, {"delayed", q.delayed}, {"leased", q.leased}}}});
    });

    server.set_exception_handler([](const httplib::Request&, httplib::Response& res, std::exception_ptr ep) {
        try {
            std::rethrow_exception(ep);
        } catch (const std::exception& e) {
            reply(res, 503, {{"error", std::string("service unavailable: ") + e.what()}});
        }
    });

    std::cout << "API listening on :" << cfg.api_port << std::endl;
    server.listen("0.0.0.0", cfg.api_port);
}

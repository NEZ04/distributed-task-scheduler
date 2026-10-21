// PostgreSQL task store: the durable source of truth for every task's state and history.
#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include "task_logic.hpp"

namespace dts {

struct ClaimedTask {
    std::string id;
    std::string type;
    nlohmann::json payload;
    int attempt;
    int max_retries;
};

class TaskStore {
public:
    explicit TaskStore(std::string dsn);
    ~TaskStore();
    TaskStore(const TaskStore&) = delete;
    TaskStore& operator=(const TaskStore&) = delete;

    // Creates the schema if needed. Safe to call from several processes at once.
    void migrate();

    std::string insert(const TaskSpec& spec);  // returns the new task id
    std::optional<nlohmann::json> get(const std::string& id);

    // Marks a task running for `worker` and bumps its attempt counter.
    // Returns nothing if the task is no longer runnable (e.g. cancelled).
    std::optional<ClaimedTask> start(const std::string& id, const std::string& worker);

    // These only apply if `worker` is still the task's owner and it's still running, so a
    // worker whose lease expired can't overwrite the result of the worker that took over.
    bool succeed(const std::string& id, const std::string& worker, const std::string& result);
    bool requeue(const std::string& id, const std::string& worker, const std::string& error);
    bool fail(const std::string& id, const std::string& worker, const std::string& error);

    bool cancel_if_queued(const std::string& id);
    nlohmann::json counts_by_status();
    std::vector<std::string> queued_older_than(int seconds);  // for reconciling with Redis

private:
    void connect();
    struct ResultDeleter {
        void operator()(PGresult* r) const { PQclear(r); }
    };
    using Result = std::unique_ptr<PGresult, ResultDeleter>;
    Result exec(const std::string& sql, const std::vector<std::string>& params = {});

    std::string dsn_;
    PGconn* conn_ = nullptr;
};

}  // namespace dts

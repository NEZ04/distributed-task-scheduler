#include "task_store.hpp"

#include <stdexcept>

namespace dts {

using nlohmann::json;

TaskStore::TaskStore(std::string dsn) : dsn_(std::move(dsn)) { connect(); }

TaskStore::~TaskStore() {
    if (conn_) PQfinish(conn_);
}

void TaskStore::connect() {
    if (conn_) PQfinish(conn_);
    conn_ = PQconnectdb(dsn_.c_str());
    if (PQstatus(conn_) != CONNECTION_OK) {
        throw std::runtime_error(std::string("postgres connect failed: ") + PQerrorMessage(conn_));
    }
}

TaskStore::Result TaskStore::exec(const std::string& sql, const std::vector<std::string>& params) {
    std::vector<const char*> values;
    for (const auto& p : params) values.push_back(p.c_str());
    for (int attempt = 0; attempt < 2; ++attempt) {
        Result r(PQexecParams(conn_, sql.c_str(), static_cast<int>(values.size()), nullptr, values.data(),
                              nullptr, nullptr, 0));
        const auto status = PQresultStatus(r.get());
        if (status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK) return r;
        if (PQstatus(conn_) != CONNECTION_OK && attempt == 0) {  // dropped connection: reconnect once
            PQreset(conn_);
            continue;
        }
        throw std::runtime_error(std::string("postgres: ") + PQresultErrorMessage(r.get()));
    }
    throw std::runtime_error("postgres: connection lost");
}

void TaskStore::migrate() {
    // The advisory lock stops several processes racing to create the same table on startup.
    exec("SELECT pg_advisory_lock(424242)");
    try {
        exec(R"sql(
            CREATE TABLE IF NOT EXISTS tasks (
                id          BIGSERIAL PRIMARY KEY,
                type        TEXT        NOT NULL,
                payload     JSONB       NOT NULL DEFAULT '{}',
                priority    SMALLINT    NOT NULL DEFAULT 5,
                status      TEXT        NOT NULL DEFAULT 'queued',
                attempts    INT         NOT NULL DEFAULT 0,
                max_retries INT         NOT NULL DEFAULT 3,
                worker_id   TEXT,
                result      TEXT,
                last_error  TEXT,
                created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                finished_at TIMESTAMPTZ,
                CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled'))
            ))sql");
        exec("CREATE INDEX IF NOT EXISTS tasks_status_idx ON tasks (status)");
    } catch (...) {
        exec("SELECT pg_advisory_unlock(424242)");
        throw;
    }
    exec("SELECT pg_advisory_unlock(424242)");
}

std::string TaskStore::insert(const TaskSpec& spec) {
    auto r = exec("INSERT INTO tasks (type, payload, priority, max_retries) VALUES ($1, $2::jsonb, $3, $4) "
                  "RETURNING id",
                  {spec.type, spec.payload.dump(), std::to_string(spec.priority), std::to_string(spec.max_retries)});
    return PQgetvalue(r.get(), 0, 0);
}

std::optional<json> TaskStore::get(const std::string& id) {
    auto r = exec(R"sql(
        SELECT id, type, payload::text, priority, status, attempts, max_retries, worker_id, result,
               last_error, to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
               to_char(finished_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
        FROM tasks WHERE id = $1)sql",
                  {id});
    if (PQntuples(r.get()) == 0) return std::nullopt;
    auto text = [&](int col) -> json {
        return PQgetisnull(r.get(), 0, col) ? json(nullptr) : json(PQgetvalue(r.get(), 0, col));
    };
    return json{{"id", std::stoll(PQgetvalue(r.get(), 0, 0))},
                {"type", text(1)},
                {"payload", json::parse(PQgetvalue(r.get(), 0, 2))},
                {"priority", std::stoi(PQgetvalue(r.get(), 0, 3))},
                {"status", text(4)},
                {"attempts", std::stoi(PQgetvalue(r.get(), 0, 5))},
                {"max_retries", std::stoi(PQgetvalue(r.get(), 0, 6))},
                {"worker_id", text(7)},
                {"result", text(8)},
                {"last_error", text(9)},
                {"created_at", text(10)},
                {"finished_at", text(11)}};
}

std::optional<ClaimedTask> TaskStore::start(const std::string& id, const std::string& worker) {
    // 'running' is allowed too: that's a task re-queued after its previous worker's lease expired.
    auto r = exec(R"sql(
        UPDATE tasks SET status = 'running', attempts = attempts + 1, worker_id = $2, updated_at = now()
        WHERE id = $1 AND status IN ('queued', 'running')
        RETURNING type, payload::text, attempts, max_retries)sql",
                  {id, worker});
    if (PQntuples(r.get()) == 0) return std::nullopt;
    return ClaimedTask{id, PQgetvalue(r.get(), 0, 0), json::parse(PQgetvalue(r.get(), 0, 1)),
                       std::stoi(PQgetvalue(r.get(), 0, 2)), std::stoi(PQgetvalue(r.get(), 0, 3))};
}

bool TaskStore::succeed(const std::string& id, const std::string& worker, const std::string& result) {
    auto r = exec("UPDATE tasks SET status = 'succeeded', result = $3, updated_at = now(), finished_at = now() "
                  "WHERE id = $1 AND worker_id = $2 AND status = 'running'",
                  {id, worker, result});
    return std::string(PQcmdTuples(r.get())) == "1";
}

bool TaskStore::requeue(const std::string& id, const std::string& worker, const std::string& error) {
    auto r = exec("UPDATE tasks SET status = 'queued', last_error = $3, updated_at = now() "
                  "WHERE id = $1 AND worker_id = $2 AND status = 'running'",
                  {id, worker, error});
    return std::string(PQcmdTuples(r.get())) == "1";
}

bool TaskStore::fail(const std::string& id, const std::string& worker, const std::string& error) {
    auto r = exec("UPDATE tasks SET status = 'failed', last_error = $3, updated_at = now(), finished_at = now() "
                  "WHERE id = $1 AND worker_id = $2 AND status = 'running'",
                  {id, worker, error});
    return std::string(PQcmdTuples(r.get())) == "1";
}

bool TaskStore::cancel_if_queued(const std::string& id) {
    auto r = exec("UPDATE tasks SET status = 'cancelled', updated_at = now(), finished_at = now() "
                  "WHERE id = $1 AND status = 'queued'",
                  {id});
    return std::string(PQcmdTuples(r.get())) == "1";
}

json TaskStore::counts_by_status() {
    json out = {{"queued", 0}, {"running", 0}, {"succeeded", 0}, {"failed", 0}, {"cancelled", 0}};
    auto r = exec("SELECT status, count(*) FROM tasks GROUP BY status");
    for (int i = 0; i < PQntuples(r.get()); ++i) out[PQgetvalue(r.get(), i, 0)] = std::stoll(PQgetvalue(r.get(), i, 1));
    return out;
}

std::vector<std::string> TaskStore::queued_older_than(int seconds) {
    auto r = exec("SELECT id FROM tasks WHERE status = 'queued' AND updated_at < now() - make_interval(secs => $1) "
                  "ORDER BY id LIMIT 500",
                  {std::to_string(seconds)});
    std::vector<std::string> ids;
    for (int i = 0; i < PQntuples(r.get()); ++i) ids.emplace_back(PQgetvalue(r.get(), i, 0));
    return ids;
}

}  // namespace dts

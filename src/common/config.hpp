#pragma once

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <string>

namespace dts {

inline std::string env(const char* name, const std::string& fallback) {
    const char* v = std::getenv(name);
    return v && *v ? v : fallback;
}

inline int env_int(const char* name, int fallback) {
    const char* v = std::getenv(name);
    return v && *v ? std::atoi(v) : fallback;
}

inline std::int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

struct Config {
    std::string redis_host = env("REDIS_HOST", "localhost");
    int redis_port = env_int("REDIS_PORT", 6379);
    std::string pg_dsn = env("PG_DSN", "postgresql://scheduler:scheduler@localhost:5432/scheduler");
    int api_port = env_int("API_PORT", 8080);
    int lease_ms = env_int("LEASE_MS", 5000);          // how long a claim lasts without a heartbeat
    int heartbeat_ms = env_int("HEARTBEAT_MS", 1000);  // how often a running task renews its lease
    int poll_ms = env_int("POLL_MS", 100);             // idle wait between claim attempts
    int backoff_base_ms = env_int("BACKOFF_BASE_MS", 500);
};

}  // namespace dts

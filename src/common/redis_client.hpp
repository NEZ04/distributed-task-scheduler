// Thin RAII wrapper around hiredis.
#pragma once

#include <memory>
#include <string>
#include <vector>

#include <hiredis/hiredis.h>

namespace dts {

struct ReplyDeleter {
    void operator()(redisReply* r) const { if (r) freeReplyObject(r); }
};
using Reply = std::unique_ptr<redisReply, ReplyDeleter>;

class RedisClient {
public:
    RedisClient(std::string host, int port);
    ~RedisClient();
    RedisClient(const RedisClient&) = delete;
    RedisClient& operator=(const RedisClient&) = delete;

    // Runs a command given as separate arguments (binary-safe; no string formatting).
    // Reconnects once if the connection dropped. Throws on error replies.
    Reply command(const std::vector<std::string>& args);

    // EVAL helper for the atomic Lua scripts.
    Reply eval(const std::string& script, const std::vector<std::string>& keys,
               const std::vector<std::string>& args);

private:
    void connect();
    std::string host_;
    int port_;
    redisContext* ctx_ = nullptr;
};

}  // namespace dts

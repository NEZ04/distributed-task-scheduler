#include "redis_client.hpp"

#include <stdexcept>

namespace dts {

RedisClient::RedisClient(std::string host, int port) : host_(std::move(host)), port_(port) { connect(); }

RedisClient::~RedisClient() {
    if (ctx_) redisFree(ctx_);
}

void RedisClient::connect() {
    if (ctx_) redisFree(ctx_);
    timeval timeout{5, 0};
    ctx_ = redisConnectWithTimeout(host_.c_str(), port_, timeout);
    if (!ctx_ || ctx_->err) {
        std::string msg = ctx_ ? ctx_->errstr : "allocation failed";
        throw std::runtime_error("redis connect to " + host_ + ":" + std::to_string(port_) + " failed: " + msg);
    }
}

Reply RedisClient::command(const std::vector<std::string>& args) {
    std::vector<const char*> argv;
    std::vector<size_t> lens;
    for (const auto& a : args) {
        argv.push_back(a.data());
        lens.push_back(a.size());
    }
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto* raw = static_cast<redisReply*>(
            redisCommandArgv(ctx_, static_cast<int>(argv.size()), argv.data(), lens.data()));
        if (raw) {
            Reply reply(raw);
            if (reply->type == REDIS_REPLY_ERROR) throw std::runtime_error("redis: " + std::string(reply->str));
            return reply;
        }
        if (attempt == 0) connect();  // connection dropped: reconnect and retry once
    }
    throw std::runtime_error("redis command failed: " + std::string(ctx_ ? ctx_->errstr : "no connection"));
}

Reply RedisClient::eval(const std::string& script, const std::vector<std::string>& keys,
                        const std::vector<std::string>& args) {
    std::vector<std::string> cmd{"EVAL", script, std::to_string(keys.size())};
    cmd.insert(cmd.end(), keys.begin(), keys.end());
    cmd.insert(cmd.end(), args.begin(), args.end());
    return command(cmd);
}

}  // namespace dts

/*
 *Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 *
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *THE POSSIBILITY OF SUCH DAMAGE.
 */
#include "comms.hpp"
#include "network.hpp"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>

OP_NAMESPACE_BEGIN
    Comms* g_db = NULL;

    size_t Comms::RedisCommandHash::operator ()(const std::string& t) const
    {
        unsigned int hash = 5381;
        size_t len = t.size();
        const char* buf = t.data();
        while (len--)
            hash = ((hash << 5) + hash) + (tolower(*buf++)); /* hash * 33 + c */
        return hash;
    }
    bool Comms::RedisCommandEqual::operator()(const std::string& s1, const std::string& s2) const
    {
        return strcasecmp(s1.c_str(), s2.c_str()) == 0 ? true : false;
    }

    Comms::Comms() :
            m_kv_store(NULL), m_service(NULL), m_starttime(0)
    {
        g_db = this;
        m_settings.set_empty_key("");
        m_settings.set_deleted_key("\n");
        struct RedisCommandHandlerSetting settingTable[] =
            {
                { "ping", REDIS_CMD_PING, &Comms::Ping, 0, 0, "r", 0, 0, 0 },
                { "multi", REDIS_CMD_MULTI, &Comms::Multi, 0, 0, "rs", 0, 0, 0 },
                { "discard", REDIS_CMD_DISCARD, &Comms::Discard, 0, 0, "r", 0, 0, 0 },
                { "exec", REDIS_CMD_EXEC, &Comms::Exec, 0, 0, "r", 0, 0, 0 },
                { "watch", REDIS_CMD_WATCH, &Comms::Watch, 0, -1, "rs", 0, 0, 0 },
                { "unwatch", REDIS_CMD_UNWATCH, &Comms::UnWatch, 0, 0, "rs", 0, 0, 0 },
                { "subscribe", REDIS_CMD_SUBSCRIBE, &Comms::Subscribe, 1, -1, "pr", 0, 0, 0 },
                { "psubscribe", REDIS_CMD_PSUBSCRIBE, &Comms::PSubscribe, 1, -1, "pr", 0, 0, 0 },
                { "unsubscribe", REDIS_CMD_UNSUBSCRIBE, &Comms::UnSubscribe, 0, -1, "pr", 0, 0, 0 },
                { "punsubscribe", REDIS_CMD_PUNSUBSCRIBE, &Comms::PUnSubscribe, 0, -1, "pr", 0, 0, 0 },
                { "publish", REDIS_CMD_PUBLISH, &Comms::Publish, 2, 2, "pr", 0, 0, 0 },
                { "info", REDIS_CMD_INFO, &Comms::Info, 0, 1, "r", 0, 0, 0 },
                { "save", REDIS_CMD_SAVE, &Comms::Save, 0, 1, "ars", 0, 0, 0 },
                { "bgsave", REDIS_CMD_BGSAVE, &Comms::BGSave, 0, 1, "ar", 0, 0, 0 },
                { "import", REDIS_CMD_IMPORT, &Comms::Import, 0, 1, "ars", 0, 0, 0 },
                { "lastsave", REDIS_CMD_LASTSAVE, &Comms::LastSave, 0, 0, "r", 0, 0, 0 },
                { "slowlog", REDIS_CMD_SLOWLOG, &Comms::SlowLog, 1, 2, "r", 0, 0, 0 },
                { "dbsize", REDIS_CMD_DBSIZE, &Comms::DBSize, 0, 0, "r", 0, 0, 0 },
                { "config", REDIS_CMD_CONFIG, &Comms::Config, 1, 3, "ar", 0, 0, 0 },
                { "client", REDIS_CMD_CLIENT, &Comms::Client, 1, 3, "ar", 0, 0, 0 },
                { "flushdb", REDIS_CMD_FLUSHDB, &Comms::FlushDB, 0, 0, "w", 0, 0, 0 },
                { "flushall", REDIS_CMD_FLUSHALL, &Comms::FlushAll, 0, 0, "w", 0, 0, 0 },
                { "time", REDIS_CMD_TIME, &Comms::Time, 0, 0, "ar", 0, 0, 0 },
                { "echo", REDIS_CMD_ECHO, &Comms::Echo, 1, 1, "r", 0, 0, 0 },
                { "quit", REDIS_CMD_QUIT, &Comms::Quit, 0, 0, "rs", 0, 0, 0 },
                { "shutdown", REDIS_CMD_SHUTDOWN, &Comms::Shutdown, 0, 1, "ar", 0, 0, 0 },
                { "slaveof", REDIS_CMD_SLAVEOF, &Comms::Slaveof, 2, -1, "as", 0, 0, 0 },
                { "replconf", REDIS_CMD_REPLCONF, &Comms::ReplConf, 0, -1, "ars", 0, 0, 0 },
                { "sync", EWDIS_CMD_SYNC, &Comms::Sync, 0, 2, "ars", 0, 0, 0 },
                { "psync", REDIS_CMD_PSYNC, &Comms::PSync, 2, 4, "ars", 0, 0, 0 },
                { "apsync", REDIS_CMD_PSYNC, &Comms::PSync, 2, -1, "ars", 0, 0, 0 },
                { "select", REDIS_CMD_SELECT, &Comms::Select, 1, 1, "r", 0, 0, 0 },
                { "append", REDIS_CMD_APPEND, &Comms::Append, 2, 2, "w", 0, 0, 0 },
                { "get", REDIS_CMD_GET, &Comms::Get, 1, 1, "r", 0, 0, 0 },
                { "set", REDIS_CMD_SET, &Comms::Set, 2, 7, "w", 0, 0, 0 },
                { "del", REDIS_CMD_DEL, &Comms::Del, 1, -1, "w", 0, 0, 0 },
                { "exists", REDIS_CMD_EXISTS, &Comms::Exists, 1, 1, "r", 0, 0, 0 },
                { "expire", REDIS_CMD_EXPIRE, &Comms::Expire, 2, 2, "w", 0, 0, 0 },
                { "pexpire", REDIS_CMD_PEXPIRE, &Comms::PExpire, 2, 2, "w", 0, 0, 0 },
                { "expireat", REDIS_CMD_EXPIREAT, &Comms::Expireat, 2, 2, "w", 0, 0, 0 },
                { "pexpireat", REDIS_CMD_PEXPIREAT, &Comms::PExpireat, 2, 2, "w", 0, 0, 0 },
                { "persist", REDIS_CMD_PERSIST, &Comms::Persist, 1, 1, "w", 1, 0, 0 },
                { "ttl", REDIS_CMD_TTL, &Comms::TTL, 1, 1, "r", 0, 0, 0 },
                { "pttl", REDIS_CMD_PTTL, &Comms::PTTL, 1, 1, "r", 0, 0, 0 },
                { "type", REDIS_CMD_TYPE, &Comms::Type, 1, 1, "r", 0, 0, 0 },
                { "bitcount", REDIS_CMD_BITCOUNT, &Comms::Bitcount, 1, 3, "r", 0, 0, 0 },
                { "bitop", REDIS_CMD_BITOP, &Comms::Bitop, 3, -1, "w", 1, 0, 0 },
                { "decr", REDIS_CMD_DECR, &Comms::Decr, 1, 1, "w", 1, 0, 0 },
                { "decrby", REDIS_CMD_DECRBY, &Comms::Decrby, 2, 2, "w", 1, 0, 0 },
                { "getbit", REDIS_CMD_GETBIT, &Comms::GetBit, 2, 2, "r", 0, 0, 0 },
                { "getrange", REDIS_CMD_GETRANGE, &Comms::GetRange, 3, 3, "r", 0, 0, 0 },
                { "substr", REDIS_CMD_GETRANGE, &Comms::GetRange, 3, 3, "r", 0, 0, 0 },
                { "getset", REDIS_CMD_GETSET, &Comms::GetSet, 2, 2, "w", 1, 0, 0 },
                { "incr", REDIS_CMD_INCR, &Comms::Incr, 1, 1, "w", 1, 0, 0 },
                { "incrby", REDIS_CMD_INCRBY, &Comms::Incrby, 2, 2, "w", 1, 0, 0 },
                { "incrbyfloat", REDIS_CMD_INCRBYFLOAT, &Comms::IncrbyFloat, 2, 2, "w", 0, 0, 0 },
                { "mget", REDIS_CMD_MGET, &Comms::MGet, 1, -1, "w", 0, 0, 0 },
                { "mset", REDIS_CMD_MSET, &Comms::MSet, 2, -1, "w", 0, 0, 0 },
                { "msetnx", REDIS_CMD_MSETNX, &Comms::MSetNX, 2, -1, "w", 0, 0, 0 },
                { "psetex", REDIS_CMD_PSETEX, &Comms::PSetEX, 3, 3, "w", 0, 0, 0 },
                { "setbit", REDIS_CMD_SETBIT, &Comms::SetBit, 3, 3, "w", 0, 0, 0 },
                { "bitpos", REDIS_CMD_SETBIT, &Comms::Bitpos, 2, 4, "r", 0, 0, 0 },
                { "setex", REDIS_CMD_SETEX, &Comms::SetEX, 3, 3, "w", 0, 0, 0 },
                { "setnx", REDIS_CMD_SETNX, &Comms::SetNX, 2, 2, "w", 0, 0, 0 },
                { "setrange", REDIS_CMD_SETEANGE, &Comms::SetRange, 3, 3, "w", 0, 0, 0 },
                { "strlen", REDIS_CMD_STRLEN, &Comms::Strlen, 1, 1, "r", 0, 0, 0 },
                { "hdel", REDIS_CMD_HDEL, &Comms::HDel, 2, -1, "w", 0, 0, 0 },
                { "hexists", REDIS_CMD_HEXISTS, &Comms::HExists, 2, 2, "r", 0, 0, 0 },
                { "hget", REDIS_CMD_HGET, &Comms::HGet, 2, 2, "r", 0, 0, 0 },
                { "hgetall", REDIS_CMD_HGETALL, &Comms::HGetAll, 1, 1, "r", 0, 0, 0 },
                { "hincrby", REDIS_CMD_HINCR, &Comms::HIncrby, 3, 3, "w", 0, 0, 0 },
                { "hincrbyfloat", REDIS_CMD_HINCRBYFLOAT, &Comms::HIncrbyFloat, 3, 3, "w", 0, 0, 0 },
                { "hkeys", REDIS_CMD_HKEYS, &Comms::HKeys, 1, 1, "r", 0, 0, 0 },
                { "hlen", REDIS_CMD_HLEN, &Comms::HLen, 1, 1, "r", 0, 0, 0 },
                { "hvals", REDIS_CMD_HVALS, &Comms::HVals, 1, 1, "r", 0, 0, 0 },
                { "hmget", REDIS_CMD_HMGET, &Comms::HMGet, 2, -1, "r", 0, 0, 0 },
                { "hset", REDIS_CMD_HSET, &Comms::HSet, 3, 3, "w", 0, 0, 0 },
                { "hsetnx", REDIS_CMD_HSETNX, &Comms::HSetNX, 3, 3, "w", 0, 0, 0 },
                { "hmset", REDIS_CMD_HMSET, &Comms::HMSet, 3, -1, "w", 0, 0, 0 },
                { "hscan", REDIS_CMD_HSCAN, &Comms::HScan, 2, 6, "r", 0, 0, 0 },
                { "hstrlen", REDIS_CMD_HSTRLEN, &Comms::HStrlen, 2, 2, "r", 0, 0, 0 },
                { "scard", REDIS_CMD_SCARD, &Comms::SCard, 1, 1, "r", 0, 0, 0 },
                { "sadd", REDIS_CMD_SADD, &Comms::SAdd, 2, -1, "w", 0, 0, 0 },
                { "sdiff", REDIS_CMD_SDIFF, &Comms::SDiff, 2, -1, "r", 0, 0, 0 },
                { "sdiffstore", REDIS_CMD_SDIFFSTORE, &Comms::SDiffStore, 3, -1, "w", 0, 0, 0 },
                { "sinter", REDIS_CMD_SINTER, &Comms::SInter, 2, -1, "r", 0, 0, 0 },
                { "sinterstore", REDIS_CMD_SINTERSTORE, &Comms::SInterStore, 3, -1, "r", 0, 0, 0 },
                { "sismember", REDIS_CMD_SISMEMBER, &Comms::SIsMember, 2, 2, "r", 0, 0, 0 },
                { "smembers", REDIS_CMD_SMEMBERS, &Comms::SMembers, 1, 1, "r", 0, 0, 0 },
                { "smove", REDIS_CMD_SMOVE, &Comms::SMove, 3, 3, "w", 0, 0, 0 },
                { "spop", REDIS_CMD_SPOP, &Comms::SPop, 1, 1, "w", 0, 0, 0 },
                { "srandmember", REDIS_CMD_SRANMEMEBER, &Comms::SRandMember, 1, 2, "r", 0, 0, 0 },
                { "srem", REDIS_CMD_SREM, &Comms::SRem, 2, -1, "w", 1, 0, 0 },
                { "sunion", REDIS_CMD_SUNION, &Comms::SUnion, 2, -1, "r", 0, 0, 0 },
                { "sunionstore", REDIS_CMD_SUNIONSTORE, &Comms::SUnionStore, 3, -1, "r", 0, 0, 0 },
                { "sscan", REDIS_CMD_SSCAN, &Comms::SScan, 2, 6, "r", 0, 0, 0 },
                { "zadd", REDIS_CMD_ZADD, &Comms::ZAdd, 3, -1, "w", 0, 0, 0 },
                { "zcard", REDIS_CMD_ZCARD, &Comms::ZCard, 1, 1, "r", 0, 0, 0 },
                { "zcount", REDIS_CMD_ZCOUNT, &Comms::ZCount, 3, 3, "r", 0, 0, 0 },
                { "zincrby", REDIS_CMD_ZINCRBY, &Comms::ZIncrby, 3, 3, "w", 0, 0, 0 },
                { "zrange", REDIS_CMD_ZRANGE, &Comms::ZRange, 3, 4, "r", 0, 0, 0 },
                { "zrangebyscore", REDIS_CMD_ZRANGEBYSCORE, &Comms::ZRangeByScore, 3, 7, "r", 0, 0, 0 },
                { "zrank", REDIS_CMD_ZRANK, &Comms::ZRank, 2, 2, "r", 0, 0, 0 },
                { "zrem", REDIS_CMD_ZREM, &Comms::ZRem, 2, -1, "w", 0, 0, 0 },
                { "zremrangebyrank", REDIS_CMD_ZREMRANGEBYRANK, &Comms::ZRemRangeByRank, 3, 3, "w", 0, 0, 0 },
                { "zremrangebyscore", REDIS_CMD_ZREMRANGEBYSCORE, &Comms::ZRemRangeByScore, 3, 3, "w", 0, 0, 0 },
                { "zrevrange", REDIS_CMD_ZREVRANGE, &Comms::ZRevRange, 3, 4, "r", 0, 0, 0 },
                { "zrevrangebyscore", REDIS_CMD_ZREVRANGEBYSCORE, &Comms::ZRevRangeByScore, 3, 7, "r", 0, 0, 0 },
                { "zinterstore", REDIS_CMD_ZINTERSTORE, &Comms::ZInterStore, 3, -1, "w", 0, 0, 0 },
                { "zunionstore", REDIS_CMD_ZUNIONSTORE, &Comms::ZUnionStore, 3, -1, "w", 0, 0, 0 },
                { "zrevrank", REDIS_CMD_ZREVRANK, &Comms::ZRevRank, 2, 2, "r", 0, 0, 0 },
                { "zscore", REDIS_CMD_ZSCORE, &Comms::ZScore, 2, 2, "r", 0, 0, 0 },
                { "zscan", REDIS_CMD_ZSCAN, &Comms::ZScan, 2, 6, "r", 0, 0, 0 },
                { "zlexcount", REDIS_CMD_ZLEXCOUNT, &Comms::ZLexCount, 3, 3, "r", 0, 0, 0 },
                { "zrangebylex", REDIS_CMD_ZRANGEBYLEX, &Comms::ZRangeByLex, 3, 6, "r", 0, 0, 0 },
                { "zrevrangebylex", REDIS_CMD_ZREVRANGEBYLEX, &Comms::ZRangeByLex, 3, 6, "r", 0, 0, 0 },
                { "zremrangebylex", REDIS_CMD_ZREMRANGEBYLEX, &Comms::ZRemRangeByLex, 3, 3, "w", 0, 0, 0 },
                { "lindex", REDIS_CMD_LINDEX, &Comms::LIndex, 2, 2, "r", 0, 0, 0 },
                { "linsert", REDIS_CMD_LINSERT, &Comms::LInsert, 4, 4, "w", 0, 0, 0 },
                { "llen", REDIS_CMD_LLEN, &Comms::LLen, 1, 1, "r", 0, 0, 0 },
                { "lpop", REDIS_CMD_LPOP, &Comms::LPop, 1, 1, "w", 0, 0, 0 },
                { "lpush", REDIS_CMD_LPUSH, &Comms::LPush, 2, -1, "w", 0, 0, 0 },
                { "lpushx", REDIS_CMD_LPUSHX, &Comms::LPushx, 2, 2, "w", 0, 0, 0 },
                { "lrange", REDIS_CMD_LRANGE, &Comms::LRange, 3, 3, "r", 0, 0, 0 },
                { "lrem", REDIS_CMD_LREM, &Comms::LRem, 3, 3, "w", 0, 0, 0 },
                { "lset", REDIS_CMD_LSET, &Comms::LSet, 3, 3, "w", 0, 0, 0 },
                { "ltrim", REDIS_CMD_LTRIM, &Comms::LTrim, 3, 3, "w", 0, 0, 0 },
                { "rpop", REDIS_CMD_RPOP, &Comms::RPop, 1, 1, "w", 0, 0, 0 },
                { "rpush", REDIS_CMD_RPUSH, &Comms::RPush, 2, -1, "w", 0, 0, 0 },
                { "rpushx", REDIS_CMD_RPUSHX, &Comms::RPushx, 2, 2, "w", 0, 0, 0 },
                { "rpoplpush", REDIS_CMD_RPOPLPUSH, &Comms::RPopLPush, 2, 2, "w", 0, 0, 0 },
                { "blpop", REDIS_CMD_BLPOP, &Comms::BLPop, 2, -1, "w", 0, 0, 0 },
                { "brpop", REDIS_CMD_BRPOP, &Comms::BRPop, 2, -1, "w", 0, 0, 0 },
                { "brpoplpush", REDIS_CMD_BRPOPLPUSH, &Comms::BRPopLPush, 3, 3, "w", 0, 0, 0 },
                { "rpoplpush", REDIS_CMD_RPOPLPUSH, &Comms::RPopLPush, 2, 2, "w", 0, 0, 0 },
                { "rpoplpush", REDIS_CMD_RPOPLPUSH, &Comms::RPopLPush, 2, 2, "w", 0, 0, 0 },
                { "move", REDIS_CMD_MOVE, &Comms::Move, 2, 2, "w", 0, 0, 0 },
                { "rename", REDIS_CMD_RENAME, &Comms::Rename, 2, 2, "w", 0, 0, 0 },
                { "renamenx", REDIS_CMD_RENAMENX, &Comms::RenameNX, 2, 2, "w", 0, 0, 0 },
                { "sort", REDIS_CMD_SORT, &Comms::Sort, 1, -1, "w", 0, 0, 0 },
                { "keys", REDIS_CMD_KEYS, &Comms::Keys, 1, 6, "r", 0, 0, 0 },
                { "eval", REDIS_CMD_EVAL, &Comms::Eval, 2, -1, "s", 0, 0, 0 },
                { "evalsha", REDIS_CMD_EVALSHA, &Comms::EvalSHA, 2, -1, "s", 0, 0, 0 },
                { "script", REDIS_CMD_SCRIPT, &Comms::Script, 1, -1, "s", 0, 0, 0 },
                { "randomkey", REDIS_CMD_RANDOMKEY, &Comms::Randomkey, 0, 0, "r", 0, 0, 0 },
                { "scan", REDIS_CMD_SCAN, &Comms::Scan, 1, 5, "r", 0, 0, 0 },
                { "geoadd", REDIS_CMD_GEO_ADD, &Comms::GeoAdd, 5, -1, "w", 0, 0, 0 },
                { "geosearch", REDIS_CMD_GEO_SEARCH, &Comms::GeoSearch, 5, -1, "r", 0, 0, 0 },
                { "auth", REDIS_CMD_AUTH, &Comms::Auth, 1, 1, "r", 0, 0, 0 },
                { "pfadd", REDIS_CMD_PFADD, &Comms::PFAdd, 2, -1, "w", 0, 0, 0 },
                { "pfcount", REDIS_CMD_PFCOUNT, &Comms::PFCount, 1, -1, "w", 0, 0, 0 },
                { "pfmerge", REDIS_CMD_PFMERGE, &Comms::PFMerge, 2, -1, "w", 0, 0, 0 }, };

        uint32 arraylen = arraysize(settingTable);
        for (uint32 i = 0; i < arraylen; i++)
        {
            settingTable[i].flags = 0;
            const char* f = settingTable[i].sflags;
            while (*f != '\0')
            {
                switch (*f)
                {
                    case 'w':
                        settingTable[i].flags |= COMMS_CMD_WRITE;
                        break;
                    case 'r':
                        settingTable[i].flags |= COMMS_CMD_READONLY;
                        break;
                    case 'a':
                        settingTable[i].flags |= COMMS_CMD_ADMIN;
                        break;
                    case 'p':
                        settingTable[i].flags |= COMMS_CMD_PUBSUB;
                        break;
                    case 's':
                        settingTable[i].flags |= COMMS_CMD_NOSCRIPT;
                        break;
                    case 'R':
                        settingTable[i].flags |= COMMS_CMD_RANDOM;
                        break;
                    default:
                        break;
                }
                f++;
            }
            m_settings[settingTable[i].name] = settingTable[i];
        }
    }

    Comms::~Comms()
    {
    }

    static void daemonize(void)
    {
        int fd;

        if (fork() != 0)
        {
            exit(0); /* parent exits */
        }
        setsid(); /* create a new session */

        if ((fd = open("/dev/null", O_RDWR, 0)) != -1)
        {
            dup2(fd, STDIN_FILENO);
            dup2(fd, STDOUT_FILENO);
            dup2(fd, STDERR_FILENO);
            if (fd > STDERR_FILENO)
            {
                close(fd);
            }
        }
    }
    static void ServerEventCallback(ChannelService* serv, uint32 ev, void* data)
    {
        switch (ev)
        {
            case SCRIPT_FLUSH_EVENT:
            case SCRIPT_KILL_EVENT:
            {
                //LUAInterpreter::ScriptEventCallback(serv, ev, data);
                break;
            }
            default:
            {
                break;
            }
        }
    }

    void Comms::RewriteClientCommand(Context& ctx, RedisCommandFrame& cmd)
    {
        if (NULL != ctx.current_cmd)
        {
            *(ctx.current_cmd) = cmd;
        }
    }

    void Comms::RenameCommand()
    {
        StringStringMap::iterator it = m_cfg.rename_commands.begin();
        while (it != m_cfg.rename_commands.end())
        {
            std::string cmd = string_tolower(it->first);
            RedisCommandHandlerSettingTable::iterator found = m_settings.find(cmd);
            if (found != m_settings.end())
            {
                RedisCommandHandlerSetting setting = found->second;
                m_settings.erase(found);
                m_settings[it->second] = setting;
            }
            it++;
        }
    }

    int Comms::Init(const CommsConfig& cfg)
    {
        m_cfg = cfg;
        if (m_cfg.daemonize)
        {
            daemonize();
        }
        if (!m_cfg.pidfile.empty())
        {
            char tmp[200];
            sprintf(tmp, "%d", getpid());
            std::string content = tmp;
            file_write_content(m_cfg.pidfile, content);
        }
        make_dir(m_cfg.home);
        if (0 != chdir(m_cfg.home.c_str()))
        {
            ERROR_LOG("Faild to change dir to home:%s", m_cfg.home.c_str());
            return -1;
        }

        INFO_LOG("Start init mmkv store.");
        m_cfg.mmkv_options.expire_cb = MMKVExpireCallback;
        int ret = mmkv::MMKV::Open(m_cfg.mmkv_options, m_kv_store);
        if (0 != ret)
        {
            return -1;
        }
        RenameCommand();
        m_stat.Init();
        return 0;
    }

    void Comms::Start()
    {
        CommsLogger::InitDefaultLogger(m_cfg.loglevel, m_cfg.logfile);
        uint32 worker_count = 0;
        for (uint32 i = 0; i < m_cfg.thread_pool_sizes.size(); i++)
        {
            if (m_cfg.thread_pool_sizes[i] == 0)
            {
                m_cfg.thread_pool_sizes[i] = 1;
            }
            else if (m_cfg.thread_pool_sizes[i] < 0)
            {
                m_cfg.thread_pool_sizes[i] = available_processors();
            }
            worker_count += m_cfg.thread_pool_sizes[i];
        }
        m_service = new ChannelService(m_cfg.max_clients + 32);
        m_service->SetThreadPoolSize(worker_count);
        m_service->RegisterUserEventCallback(LUAInterpreter::ScriptEventCallback, this);
        ChannelOptions ops;
        ops.tcp_nodelay = true;
        ops.reuse_address = true;
        if (m_cfg.tcp_keepalive > 0)
        {
            ops.keep_alive = m_cfg.tcp_keepalive;
        }
        for (uint32 i = 0; i < m_cfg.listen_addresses.size(); i++)
        {
            const std::string& address = m_cfg.listen_addresses[i];
            ServerSocketChannel* server = NULL;
            if (address.find(":") == std::string::npos)
            {
                SocketUnixAddress unix_address(address);
                server = m_service->NewServerSocketChannel();
                if (!server->Bind(&unix_address))
                {
                    ERROR_LOG("Failed to bind on %s", address.c_str());
                    goto sexit;
                }
                chmod(address.c_str(), m_cfg.unixsocketperm);
            }
            else
            {
                std::vector<std::string> ss = split_string(address, ":");
                uint32 port;
                if (ss.size() != 2 || !string_touint32(ss[1], port))
                {
                    ERROR_LOG("Invalid server socket address %s", address.c_str());
                    goto sexit;
                }
                SocketHostAddress socket_address(ss[0], port);
                server = m_service->NewServerSocketChannel();

                if (!server->Bind(&socket_address))
                {
                    ERROR_LOG("Failed to bind on %s", address.c_str());
                    goto sexit;
                }
                if (m_cfg.primary_port == 0)
                {
                    m_cfg.primary_port = port;
                }
            }
            server->Configure(ops);
            server->SetChannelPipelineInitializor(RedisRequestHandler::PipelineInit, this);
            server->SetChannelPipelineFinalizer(RedisRequestHandler::PipelineDestroy, NULL);
            uint32 min = 0;
            for (uint32 j = 0; j < i; j++)
            {
                min += min + m_cfg.thread_pool_sizes[j];
            }
            server->BindThreadPool(min, min + m_cfg.thread_pool_sizes[i]);
        }

        m_repl.Init();

        m_service->RegisterUserEventCallback(ServerEventCallback, this);
        m_cron.Start();

        INFO_LOG("Server started, Comms version %s", COMMS_VERSION);
        INFO_LOG("The server is now ready to accept connections on  %s",
                string_join_container(m_cfg.listen_addresses, ",").c_str());

        m_starttime = time(NULL);
        m_service->Start();
        sexit: m_cron.StopSelf();
        DELETE(m_service);
        CommsLogger::DestroyDefaultLogger();
    }

    bool Comms::GetInt64Value(Context& ctx, const std::string& str, int64& v)
    {
        if (!string_toint64(str, v))
        {
            fill_error_reply(ctx.reply, "value is not a integer or out of range");
            return false;
        }
        return true;
    }

    bool Comms::GetDoubleValue(Context& ctx, const std::string& str, long double& v)
    {
        if (!string_todouble(str, v))
        {
            fill_error_reply(ctx.reply, "value is not a float or out of range");
            return false;
        }
        return true;
    }


    int Comms::FireKeyChangedEvent(Context& ctx, const std::string& key)
    {
        return FireKeyChangedEvent(ctx, ctx.currentDB, key);
    }
    int Comms::FireKeyChangedEvent(Context& ctx, DBID db, const std::string& key)
    {
        ctx.data_change = true;
        if (!key.empty())
        {
            AbortWatchKey(db, key);
        }
        return 0;
    }

    void Comms::FreeClientContext(Context& ctx)
    {
        UnwatchKeys(ctx);
        UnsubscribeAll(ctx, false);
        PUnsubscribeAll(ctx, false);
        ClearBlockKeys(ctx);

        LockGuard<SpinMutexLock> guard(m_clients_lock);
        m_clients.erase(ctx.client->GetID());
    }

    void Comms::AddClientContext(Context& ctx)
    {
        LockGuard<SpinMutexLock> guard(m_clients_lock);
        m_clients[ctx.client->GetID()] = &ctx;
    }

    RedisReplyPool& Comms::GetRedisReplyPool()
    {
        return m_reply_pool.GetValue();
    }

    int Comms::DoCall(Context& ctx, RedisCommandHandlerSetting& setting, RedisCommandFrame& args)
    {
        uint64 start_time = get_current_epoch_micros();
        ctx.last_interaction_ustime = start_time;
        int ret = (this->*(setting.handler))(ctx, args);
        uint64 stop_time = get_current_epoch_micros();
        ctx.last_interaction_ustime = stop_time;
        uint64 latency = stop_time - start_time;
        atomic_add_uint64(&(setting.calls), 1);
        atomic_add_uint64(&(setting.microseconds), stop_time - start_time);
        if (latency > setting.max_latency)
        {
            setting.max_latency = latency;
        }
        TryPushSlowCommand(args, stop_time - start_time);
        DEBUG_LOG("Process recved cmd[%lld] %s cost %lluus", ctx.sequence, args.GetCommand().c_str(), stop_time - start_time);
        return ret;
    }

    Comms::RedisCommandHandlerSetting* Comms::FindRedisCommandHandlerSetting(RedisCommandFrame& args)
    {
        std::string& cmd = args.GetMutableCommand();
        RedisCommandHandlerSettingTable::iterator found = m_settings.find(cmd);
        if (found == m_settings.end())
        {
            return NULL;
        }
        args.SetType(found->second.type);
        return &(found->second);
    }

    struct ResumeOverloadConnection: public Runnable
    {
            ChannelService& chs;
            uint32 channle_id;
            ResumeOverloadConnection(ChannelService& serv, uint32 id) :
                    chs(serv), channle_id(id)
            {
            }
            void Run()
            {
                Channel* ch = chs.GetChannel(channle_id);
                if (NULL != ch)
                {
                    ch->AttachFD();
                }
            }
    };

    int Comms::Call(Context& ctx, RedisCommandFrame& args, CallFlags flags)
    {
        RedisCommandHandlerSetting* found = FindRedisCommandHandlerSetting(args);
        if (NULL == found)
        {
            ERROR_LOG("No handler found for:%s with size:%u", args.GetCommand().c_str(), args.GetCommand().size());
            ERROR_LOG("Invalid command's ascii codes:%s", ascii_codes(args.GetCommand()).c_str());
            fill_error_reply(ctx.reply, "unknown command '%s'", args.GetCommand().c_str());
            ctx.close_after_processed = true;
            return 0;
        }
        ctx.ClearState();
        int err = m_stat.IncRecvCommands(ctx.server_address, ctx.sequence);
        DEBUG_LOG("Process recved cmd[%lld]:%s", ctx.sequence, args.ToString().c_str());
        if (err == ERR_OVERLOAD)
        {
            /*
             * block overloaded connection
             */
            if (NULL != ctx.client && !ctx.client->IsDetached())
            {
                ctx.client->DetachFD();
                uint64 now = get_current_epoch_millis();
                uint64 next = 1000 - (now % 1000);
                ChannelService& serv = ctx.client->GetService();
                ctx.client->GetService().GetTimer().ScheduleHeapTask(
                        new ResumeOverloadConnection(serv, ctx.client->GetID()), next == 0 ? 1 : next, -1, MILLIS);
            }
        }
        ctx.current_cmd = &args;
        ctx.current_cmd_type = args.GetType();
        RedisCommandHandlerSetting& setting = *found;
        int ret = 0;

        //Check if the user is authenticated
        if (!ctx.authenticated && setting.type != REDIS_CMD_AUTH && setting.type != REDIS_CMD_QUIT)
        {
            fill_fix_error_reply(ctx.reply, "NOAUTH Authentication required");
            return ret;
        }

        bool valid_cmd = true;
        if (setting.min_arity > 0)
        {
            valid_cmd = args.GetArguments().size() >= (uint32) setting.min_arity;
        }
        if (setting.max_arity >= 0 && valid_cmd)
        {
            valid_cmd = args.GetArguments().size() <= (uint32) setting.max_arity;
        }

        if (!valid_cmd)
        {
            fill_error_reply(ctx.reply, "wrong number of arguments for '%s' command", args.GetCommand().c_str());
        }
        else
        {
            bool exec_cmd = true;
            if (ctx.InTransc())
            {
                switch (setting.type)
                {
                    case REDIS_CMD_MULTI:
                    case REDIS_CMD_EXEC:
                    case REDIS_CMD_DISCARD:
                    case REDIS_CMD_QUIT:
                    {
                        break;
                    }
                    default:
                    {
                        ctx.GetTransc().cached_cmds.push_back(args);
                        fill_status_reply(ctx.reply, "QUEUED");
                        exec_cmd = false;
                        break;
                    }
                }

            }
            else if (ctx.IsSubscribedConn())
            {
                switch (setting.type)
                {
                    case REDIS_CMD_SUBSCRIBE:
                    case REDIS_CMD_PSUBSCRIBE:
                    case REDIS_CMD_UNSUBSCRIBE:
                    case REDIS_CMD_PUNSUBSCRIBE:
                    case REDIS_CMD_QUIT:
                    {
                        break;
                    }
                    default:
                    {
                        fill_error_reply(ctx.reply,
                                "only (P)SUBSCRIBE / (P)UNSUBSCRIBE / QUIT allowed in this context");
                        exec_cmd = false;
                        break;
                    }
                }

            }
            if (exec_cmd)
            {
                ctx.flags = flags;
                ret = DoCall(ctx, setting, args);
            }
        }
        if (!flags.no_wal && ctx.data_change)
        {
            m_repl.WriteWAL(ctx.currentDB, *ctx.current_cmd);
        }
        return ret;
    }

    bool Comms::FillErrorReply(Context& ctx, int err)
    {
        switch (err)
        {
            case mmkv::ERR_INVALID_ARGS:
            case mmkv::ERR_INVALID_MIN_MAX:
            case mmkv::ERR_INVALID_COORD_TYPE:
            {
                fill_error_reply(ctx.reply, "Invalid arguments.");
                return true;
            }
            case mmkv::ERR_INVALID_TYPE:
            {
                fill_error_reply(ctx.reply, "Operation against a key holding the wrong kind of value.");
                return true;
            }
            case mmkv::ERR_CORRUPTED_HLL_VALUE:
            case mmkv::ERR_NOT_HYPERLOGLOG_STR:
            {
                fill_error_reply(ctx.reply, "INVALIDOBJ Corrupted HLL object detected.");
                return true;
            }
            case mmkv::ERR_SYNTAX_ERROR:
            {
                fill_error_reply(ctx.reply, "Syntax error");
                return true;
            }
            case mmkv::ERR_ENTRY_NOT_EXIST:
            case mmkv::ERR_DB_NOT_EXIST:
            {
                fill_error_reply(ctx.reply, "Entry not exist");
                return true;
            }
            case mmkv::ERR_PERMISSION_DENIED:
            {
                fill_error_reply(ctx.reply, "Permission denied");
                return true;
            }
            case mmkv::ERR_ENTRY_EXISTED:
            {
                fill_error_reply(ctx.reply, "Entry already exist");
                return true;
            }
            case mmkv::ERR_NOT_INTEGER:
            case mmkv::ERR_NOT_NUMBER:
            case mmkv::ERR_INVALID_NUMBER:
            case mmkv::ERR_BIT_OUTRANGE:
            case mmkv::ERR_ARGS_EXCEED_LIMIT:
            case mmkv::ERR_INVALID_COORD_VALUE:
            {
                fill_error_reply(ctx.reply, "value is not a valid number or out of range");
                return true;
            }
            case mmkv::ERR_NOT_IMPLEMENTED:
            {
                fill_error_reply(ctx.reply, "command not implemented");
                return true;
            }
            case mmkv::ERR_OFFSET_OUTRANGE:
            {
                fill_error_reply(ctx.reply, "index out of range");
                return true;
            }
            default:
            {
                ERROR_LOG("Not supported error:%u", err);
                break;
            }
        }
        return false;
    }

OP_NAMESPACE_END

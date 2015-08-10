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

#ifndef CONTEXT_HPP_
#define CONTEXT_HPP_
#include "common/common.hpp"
#include "thread/thread_local.hpp"
#include "channel/all_includes.hpp"

using namespace comms::codec;
OP_NAMESPACE_BEGIN

    struct WatchKey
    {
            DBID db;
            std::string key;
            WatchKey(const DBID& id = 0, const std::string& k = "") :
                    db(id), key(k)
            {
            }
            bool operator<(const WatchKey& other) const
            {
                if (db < other.db)
                {
                    return true;
                }
                if (db == other.db)
                {
                    return key < other.key;
                }
                return false;
            }
    };
    typedef TreeSet<WatchKey>::Type WatchKeySet;

    struct ListBlockContext
    {
            WatchKeySet keys;
            std::string push_key;
            int32 blocking_timer_task_id;
            bool lpop;
            ListBlockContext() :
                    blocking_timer_task_id(-1),lpop(false)
            {
            }
    };
    struct TranscContext
    {
            bool in_transc;
            RedisCommandFrameArray cached_cmds;
            WatchKeySet watched_keys;
            TranscContext() :
                    in_transc(true)
            {
            }
            void AddRedisCommand(const RedisCommandFrame& cmd)
            {
                cached_cmds.push_back(cmd);
            }
    };

    struct CallFlags
    {
            unsigned no_wal :1;
            CallFlags() :
                    no_wal(0)
            {
            }
    };

    struct PubSubContext
    {
            StringSet pubsub_channels;
            StringSet pubsub_patterns;
    };

    struct LUAContext
    {
            uint64 lua_time_start;
            bool lua_timeout;
            bool lua_kill;
            const char* lua_executing_func;

            LUAContext() :
                    lua_time_start(0), lua_timeout(false), lua_kill(false), lua_executing_func(NULL)
            {
            }
    };

    struct Context
    {
            TranscContext* transc;
            PubSubContext* pubsub;
            LUAContext* lua;
            ListBlockContext* block;

            Channel* client;
            DBID currentDB;
            RedisReply reply;

            std::string server_address;
            bool authenticated;

            bool data_change;
            bool write_success;
            RedisCommandFrame* current_cmd;
            RedisCommandType current_cmd_type;
            int64 born_time;
            int64 last_interaction_ustime;

            std::string name;

            bool processing;
            bool close_after_processed;
            CallFlags flags;
            WatchKeySet* watch_keys;
            int64 sequence;  //recv command sequence in the server, start from 1

            bool abort_exec;
            Context() :
                    transc(NULL), pubsub(NULL), lua(NULL), block(NULL), client(
                    NULL), currentDB(0), authenticated(true), data_change(false), write_success(true), current_cmd(
                    NULL), current_cmd_type(REDIS_CMD_INVALID), born_time(0), last_interaction_ustime(0), processing(
                            false), close_after_processed(false), watch_keys(NULL), sequence(0),abort_exec(false)
            {
            }
            TranscContext& GetTransc()
            {
                if (NULL == transc)
                {
                    transc = new TranscContext;
                }
                return *transc;
            }
            PubSubContext& GetPubsub()
            {
                if (NULL == pubsub)
                {
                    pubsub = new PubSubContext;
                }
                return *pubsub;
            }
            LUAContext& GetLua()
            {
                if (NULL == lua)
                {
                    lua = new LUAContext;
                }
                return *lua;
            }
            ListBlockContext& GetBlockContext()
            {
                if (NULL == block)
                {
                    block = new ListBlockContext;
                }
                return *block;
            }
            WatchKeySet& GetWatchKeySet()
            {
                if (NULL == watch_keys)
                {
                    watch_keys = new WatchKeySet;
                }
                return *watch_keys;
            }
            bool InTransc()
            {
                return NULL != transc && transc->in_transc;
            }
            bool IsSubscribedConn()
            {
                return NULL != pubsub;
            }

            void ClearPubsub()
            {
                DELETE(pubsub);
            }
            void ClearLua()
            {
                DELETE(lua);
            }
            void ClearBlockContext()
            {
                DELETE(block);
            }
            void ClearWatchKeySet()
            {
                DELETE(watch_keys);
                abort_exec = false;
            }
            void ClearTransc()
            {
                DELETE(transc);
                ClearWatchKeySet();
            }
            void ClearState()
            {
                reply.Clear();
                data_change = false;
                write_success = true;
                current_cmd = NULL;
            }
            void Clear()
            {
                ClearState();
                ClearTransc();
                ClearPubsub();
                ClearLua();
                ClearBlockContext();
                DELETE(watch_keys);
            }
            ~Context()
            {
                Clear();
            }
    };

    typedef TreeSet<Context*>::Type ContextSet;
    typedef std::deque<Context*> ContextDeque;
    typedef TreeMap<uint32, Context*>::Type ContextTable;

    struct RedisCursor
    {
            std::string element;
            time_t ts;
            uint64 cursor;
            RedisCursor() :
                    ts(0), cursor(0)
            {
            }
    };
    typedef TreeMap<uint64, RedisCursor>::Type RedisCursorTable;

OP_NAMESPACE_END

#endif /* CONTEXT_HPP_ */

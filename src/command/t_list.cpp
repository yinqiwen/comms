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
#include <float.h>
#include <cmath>

OP_NAMESPACE_BEGIN

    int Comms::LIndex(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 index;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], index))
        {
            return 0;
        }
        std::string v;
        int err = m_kv_store->LIndex(ctx.currentDB, cmd.GetArguments()[0], index, v);
        if (err >= 0)
        {
            fill_str_reply(ctx.reply, v);
        }
        else if (mmkv::ERR_OFFSET_OUTRANGE == err)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::LInsert(Context& ctx, RedisCommandFrame& cmd)
    {
        bool before_or_after = false;
        if (!strcasecmp(cmd.GetArguments()[1].c_str(), "before"))
        {
            before_or_after = true;
        }
        else if (!strcasecmp(cmd.GetArguments()[1].c_str(), "after"))
        {
            before_or_after = false;
        }
        else
        {
            fill_error_reply(ctx.reply, "Syntax error");
            return 0;
        }
        int err = m_kv_store->LInsert(ctx.currentDB, cmd.GetArguments()[0], before_or_after, cmd.GetArguments()[2],
                cmd.GetArguments()[3]);
        if (err >= 0 || -1 == err)
        {
            fill_int_reply(ctx.reply, err);
            if (err > 0)
            {
                FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            }
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::LLen(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->LLen(ctx.currentDB, cmd.GetArguments()[0]);
        if (err >= 0)
        {
            fill_int_reply(ctx.reply, err);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::LPop(Context& ctx, RedisCommandFrame& cmd)
    {
        std::string v;
        int err = m_kv_store->LPop(ctx.currentDB, cmd.GetArguments()[0], v);
        if (err >= 0)
        {
            fill_str_reply(ctx.reply, v);
            FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
        }
        else
        {
            if (err == mmkv::ERR_ENTRY_NOT_EXIST || err == mmkv::ERR_DB_NOT_EXIST)
            {
                ctx.reply.type = REDIS_REPLY_NIL;
            }
            else
            {
                FillErrorReply(ctx, err);
            }
        }
        return 0;
    }
    int Comms::LPush(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        int err = m_kv_store->LPush(ctx.currentDB, cmd.GetArguments()[0], fs);
        if (err >= 0)
        {
            fill_int_reply(ctx.reply, err);
            FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            WakeBlockingListsByKey(ctx, cmd.GetArguments()[0]);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::LPushx(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        int err = m_kv_store->LPush(ctx.currentDB, cmd.GetArguments()[0], fs, true);
        if (err >= 0)
        {
            fill_int_reply(ctx.reply, err);
            if (err > 0)
            {
                FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
                WakeBlockingListsByKey(ctx, cmd.GetArguments()[0]);
            }
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::LRange(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 start, end;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], start) || !GetInt64Value(ctx, cmd.GetArguments()[2], end))
        {
            return 0;
        }
        ctx.reply.type = REDIS_REPLY_ARRAY;
        mmkv::StringArrayResult vs(ReplyResultStringAlloc, &ctx.reply);
        int err = m_kv_store->LRange(ctx.currentDB, cmd.GetArguments()[0], start, end, vs);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::LRem(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 count;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], count))
        {
            return 0;
        }
        int err = m_kv_store->LRem(ctx.currentDB, cmd.GetArguments()[0], count, cmd.GetArguments()[2]);
        if (err >= 0)
        {
            fill_int_reply(ctx.reply, err);
            if (err > 0)
            {
                FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            }
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::LSet(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 index;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], index))
        {
            return 0;
        }
        int err = m_kv_store->LSet(ctx.currentDB, cmd.GetArguments()[0], index, cmd.GetArguments()[2]);
        if (err >= 0)
        {
            fill_ok_reply(ctx.reply);
            FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::LTrim(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 start, end;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], start) || !GetInt64Value(ctx, cmd.GetArguments()[2], end))
        {
            return 0;
        }
        int err = m_kv_store->LTrim(ctx.currentDB, cmd.GetArguments()[0], start, end);
        if (err >= 0)
        {
            fill_ok_reply(ctx.reply);
            FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::RPop(Context& ctx, RedisCommandFrame& cmd)
    {
        std::string v;
        int err = m_kv_store->RPop(ctx.currentDB, cmd.GetArguments()[0], v);
        if (err >= 0)
        {
            fill_str_reply(ctx.reply, v);
            FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
        }
        else
        {
            if (err == mmkv::ERR_ENTRY_NOT_EXIST || err == mmkv::ERR_DB_NOT_EXIST)
            {
                ctx.reply.type = REDIS_REPLY_NIL;
            }
            else
            {
                FillErrorReply(ctx, err);
            }
        }
        return 0;
    }
    int Comms::RPopLPush(Context& ctx, RedisCommandFrame& cmd)
    {
        std::string v;
        int err = m_kv_store->RPopLPush(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], v);
        if (err >= 0)
        {
            fill_str_reply(ctx.reply, v);
            FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            FireKeyChangedEvent(ctx, cmd.GetArguments()[1]);
            if (cmd.GetType() == REDIS_CMD_BRPOPLPUSH)
            {
                ctx.current_cmd->SetCommand("rpoplpush");
            }
        }
        else
        {
            if (err == mmkv::ERR_ENTRY_NOT_EXIST || err == mmkv::ERR_DB_NOT_EXIST)
            {
                ctx.reply.type = REDIS_REPLY_NIL;
            }
            else
            {
                FillErrorReply(ctx, err);
            }
        }
        return 0;
    }
    int Comms::RPush(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        int err = m_kv_store->RPush(ctx.currentDB, cmd.GetArguments()[0], fs);
        if (err >= 0)
        {
            fill_int_reply(ctx.reply, err);
            FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            WakeBlockingListsByKey(ctx, cmd.GetArguments()[0]);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::RPushx(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        int err = m_kv_store->RPush(ctx.currentDB, cmd.GetArguments()[0], fs, true);
        if (err >= 0)
        {
            fill_int_reply(ctx.reply, err);
            if (err > 0)
            {
                FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
                WakeBlockingListsByKey(ctx, cmd.GetArguments()[0]);
            }
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    struct BlockListTimeout: public Runnable
    {
            Context* ctx;
            BlockListTimeout(Context* cc) :
                    ctx(cc)
            {
            }
            void Run()
            {
                g_db->ClearBlockKeys(*ctx);
                ctx->reply.type = REDIS_REPLY_NIL;
                ctx->client->Write(ctx->reply);
                ctx->client->AttachFD();
            }
    };
    int Comms::BLPop(Context& ctx, RedisCommandFrame& cmd)
    {
        uint32 timeout;
        if (!string_touint32(cmd.GetArguments()[cmd.GetArguments().size() - 1], timeout))
        {
            fill_error_reply(ctx.reply, "timeout is not an integer or out of range");
            return 0;
        }
        bool lpop = cmd.GetType() == REDIS_CMD_BLPOP;
        for (uint32 i = 0; i < cmd.GetArguments().size() - 1; i++)
        {
            std::string v;
            int err =
                    lpop ? m_kv_store->LPop(ctx.currentDB, cmd.GetArguments()[i], v) : m_kv_store->RPop(ctx.currentDB,
                                   cmd.GetArguments()[i], v);
            if (0 == err && !v.empty())
            {
                RedisReply& r1 = ctx.reply.AddMember();
                RedisReply& r2 = ctx.reply.AddMember();
                fill_str_reply(r1, cmd.GetArguments()[i]);
                fill_str_reply(r2, v);

                FireKeyChangedEvent(ctx, ctx.currentDB, cmd.GetArguments()[i]);
                RedisCommandFrame list_pop(lpop ? "lpop" : "rpop");
                list_pop.AddArg(cmd.GetArguments()[i]);
                RewriteClientCommand(ctx, list_pop);
                return 0;
            }
            if (err != 0 && err != mmkv::ERR_ENTRY_NOT_EXIST && err != mmkv::ERR_DB_NOT_EXIST)
            {
                FillErrorReply(ctx, err);
                return 0;
            }
        }
        if (NULL != ctx.client)
        {
            ctx.client->DetachFD();
            ctx.GetBlockContext().lpop = cmd.GetType() == REDIS_CMD_BLPOP;
            if (timeout > 0)
            {
                ctx.block->blocking_timer_task_id = ctx.client->GetService().GetTimer().ScheduleHeapTask(
                        new BlockListTimeout(&ctx), timeout, -1, SECONDS);
            }
        }
        for (uint32 i = 0; i < cmd.GetArguments().size() - 1; i++)
        {
            AddBlockKey(ctx, cmd.GetArguments()[i]);
        }
        return 0;
    }

    void Comms::AddBlockKey(Context& ctx, const std::string& keystr)
    {
        WatchKey key(ctx.currentDB, keystr);
        WriteLockGuard<SpinRWLock> guard(m_block_ctx_lock);
        ctx.GetBlockContext().keys.insert(key);
        m_block_context_table[key].insert(&ctx);
    }

    struct WakeBlockListData
    {
            std::string key;
            Context* block_ctx;
            WakeBlockListData() :
                    block_ctx(NULL)
            {
            }
            static void WakeBlockedListCallback(Channel* ch, void* data)
            {
                WakeBlockListData* cbdata = (WakeBlockListData*) data;
                g_db->WakeBlockedList(*(cbdata->block_ctx), cbdata->key);
                DELETE(cbdata);
            }
    };

    void Comms::WakeBlockingListsByKey(Context& ctx, const std::string& key)
    {
        ReadLockGuard<SpinRWLock> guard(m_block_ctx_lock);
        if (!m_block_context_table.empty())
        {
            WatchKey k(ctx.currentDB, key);
            BlockContextTable::iterator fit = m_block_context_table.find(k);
            if (fit != m_block_context_table.end())
            {
                ContextSet::iterator sit = fit->second.begin();
                while (sit != fit->second.end())
                {
                    Context* block_ctx = *sit;
                    WakeBlockListData* cbdata = new WakeBlockListData;
                    cbdata->block_ctx = block_ctx;
                    cbdata->key = key;
                    block_ctx->client->GetService().AsyncIO(0, WakeBlockListData::WakeBlockedListCallback, cbdata);
                    sit++;
                }
            }
        }
    }
    void Comms::WakeBlockedList(Context& ctx, const std::string& key)
    {
        std::string v;
        int err =
                ctx.GetBlockContext().lpop ?
                        m_kv_store->LPop(ctx.currentDB, key, v) : m_kv_store->RPop(ctx.currentDB, key, v);
        if (0 == err && !v.empty())
        {
            if (ctx.GetBlockContext().push_key.empty())
            {
                RedisReply& r1 = ctx.reply.AddMember();
                RedisReply& r2 = ctx.reply.AddMember();
                fill_str_reply(r1, key);
                fill_str_reply(r2, v);
                if (!ctx.flags.no_wal)
                {
                    RedisCommandFrame list_pop(ctx.GetBlockContext().lpop ? "lpop" : "rpop");
                    list_pop.AddArg(key);
                    m_repl.WriteWAL(ctx.currentDB, list_pop);
                }
            }
            else
            {
                err = m_kv_store->RPush(ctx.currentDB, ctx.GetBlockContext().push_key, v);
                if (err < 0)
                {
                    ctx.GetBlockContext().lpop ?
                            m_kv_store->LPush(ctx.currentDB, key, v) : m_kv_store->RPush(ctx.currentDB, key, v);
                    FillErrorReply(ctx, err);
                }
                else
                {
                    fill_str_reply(ctx.reply, v);
                    if (!ctx.flags.no_wal)
                    {
                        RedisCommandFrame rpoplpush("rpoplpush");
                        rpoplpush.AddArg(key);
                        m_repl.WriteWAL(ctx.currentDB, rpoplpush);
                    }
                }
            }
            ctx.client->Write(ctx.reply);
            ctx.client->AttachFD();
            ClearBlockKeys(ctx);
        }
    }
    void Comms::ClearBlockKeys(Context& ctx)
    {
        if (NULL != ctx.block)
        {
            if (ctx.block->blocking_timer_task_id != -1)
            {
                ctx.client->GetService().GetTimer().Cancel(ctx.block->blocking_timer_task_id);
            }
            WriteLockGuard<SpinRWLock> guard(m_block_ctx_lock);
            WatchKeySet::iterator it = ctx.block->keys.begin();
            while (it != ctx.block->keys.end())
            {
                BlockContextTable::iterator fit = m_block_context_table.find(*it);
                if (fit != m_block_context_table.end())
                {
                    fit->second.erase(&ctx);
                    if (fit->second.empty())
                    {
                        m_block_context_table.erase(fit);
                    }
                }
                it++;
            }
            ctx.ClearBlockContext();
        }
    }

    int Comms::BRPop(Context& ctx, RedisCommandFrame& cmd)
    {
        return BLPop(ctx, cmd);
    }
    int Comms::BRPopLPush(Context& ctx, RedisCommandFrame& cmd)
    {
        uint32 timeout;
        if (!string_touint32(cmd.GetArguments()[cmd.GetArguments().size() - 1], timeout))
        {
            fill_error_reply(ctx.reply, "timeout is not an integer or out of range");
            return 0;
        }
        RPopLPush(ctx, cmd);
        if (ctx.reply.type == REDIS_REPLY_NIL)
        {
            //block;
            AddBlockKey(ctx, cmd.GetArguments()[0]);
            ctx.GetBlockContext().lpop = false;
            ctx.GetBlockContext().push_key = cmd.GetArguments()[1];
            if (NULL != ctx.client)
            {
                ctx.client->DetachFD();
                if (timeout > 0)
                {
                    ctx.block->blocking_timer_task_id = ctx.client->GetService().GetTimer().ScheduleHeapTask(
                            new BlockListTimeout(&ctx), timeout, -1, SECONDS);
                }
            }
            ctx.reply.type = 0;
        }
        return 0;
    }
OP_NAMESPACE_END


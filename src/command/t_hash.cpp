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

OP_NAMESPACE_BEGIN

    int Comms::HDel(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        int err = m_kv_store->HDel(ctx.currentDB, cmd.GetArguments()[0], fs);
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
    int Comms::HExists(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->HExists(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
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
    int Comms::HGet(Context& ctx, RedisCommandFrame& cmd)
    {
        std::string v;
        int err = m_kv_store->HGet(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], v);
        if (err == mmkv::ERR_ENTRY_NOT_EXIST || err == mmkv::ERR_DB_NOT_EXIST)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        else if(0 != err)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_str_reply(ctx.reply, v);
        }
        return 0;
    }
    int Comms::HGetAll(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::StringArray vs;
        int err = m_kv_store->HGetAll(ctx.currentDB, cmd.GetArguments()[0], vs);
        if (err >= 0)
        {
            fill_str_array_reply(ctx.reply, vs);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::HIncrby(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 increment, val = 0;
        if (!GetInt64Value(ctx, cmd.GetArguments()[2], increment))
        {
            return 0;
        }
        int err = m_kv_store->HIncrBy(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], increment, val);
        if (err == 0)
        {
            fill_int_reply(ctx.reply, val);
            FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::HIncrbyFloat(Context& ctx, RedisCommandFrame& cmd)
    {
        long double increment, val = 0;
        if (!GetDoubleValue(ctx, cmd.GetArguments()[2], increment))
        {
            return 0;
        }
        int err = m_kv_store->HIncrByFloat(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], increment, val);
        if (err == 0)
        {
            fill_double_reply(ctx.reply, val);
            FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            if (!ctx.flags.no_wal)
            {
                RedisCommandFrame rewrite("hset");
                rewrite.AddArg(cmd.GetArguments()[0]);
                std::string tmp;
                fast_dtoa(val, 10, tmp);
                rewrite.AddArg(tmp);
                RewriteClientCommand(ctx, rewrite);
            }
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::HKeys(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.reply.type = REDIS_REPLY_ARRAY;
        mmkv::StringArrayResult vs(ReplyResultStringAlloc, &ctx.reply);
        int err = m_kv_store->HKeys(ctx.currentDB, cmd.GetArguments()[0], vs);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::HLen(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->HLen(ctx.currentDB, cmd.GetArguments()[0]);
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
    int Comms::HMGet(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        ctx.reply.type = REDIS_REPLY_ARRAY;
        mmkv::StringArrayResult results(ReplyResultStringAlloc, &ctx.reply);
        mmkv::BooleanArray get_flags;
        int err = m_kv_store->HMGet(ctx.currentDB, cmd.GetArguments()[0], fs, results, &get_flags);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            for (uint32 i = 0; i < fs.size(); i++)
            {
                if (!get_flags[i])
                {
                    ctx.reply.MemberAt(i).type = REDIS_REPLY_NIL;
                }
            }
        }
        return 0;
    }
    int Comms::HMSet(Context& ctx, RedisCommandFrame& cmd)
    {
        if((cmd.GetArguments().size() - 1) % 2 != 0)
        {
            FillErrorReply(ctx, mmkv::ERR_INVALID_ARGS);
            return 0;
        }
        mmkv::DataPairArray kvs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i += 2)
        {
            mmkv::DataPair kv;
            kv.first = cmd.GetArguments()[i];
            kv.second = cmd.GetArguments()[i + 1];
            kvs.push_back(kv);
        }
        int err = m_kv_store->HMSet(ctx.currentDB, cmd.GetArguments()[0], kvs);
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
    int Comms::HSet(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->HSet(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], cmd.GetArguments()[2]);
        if (err >= 0)
        {
            fill_int_reply(ctx.reply, err);
            FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::HSetNX(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->HSet(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], cmd.GetArguments()[2],
                true);
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
    int Comms::HVals(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.reply.type = REDIS_REPLY_ARRAY;
        mmkv::StringArrayResult vs(ReplyResultStringAlloc, &ctx.reply);
        int err = m_kv_store->HVals(ctx.currentDB, cmd.GetArguments()[0], vs);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::HStrlen(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->HStrlen(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
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

    int Comms::HScan(Context& ctx, RedisCommandFrame& cmd)
    {
        return Scan(ctx, cmd);
    }
OP_NAMESPACE_END


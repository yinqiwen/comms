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

OP_NAMESPACE_BEGIN

    int Comms::SAdd(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        int err = m_kv_store->SAdd(ctx.currentDB, cmd.GetArguments()[0], fs);
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
    int Comms::SCard(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->SCard(ctx.currentDB, cmd.GetArguments()[0]);
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
    int Comms::SDiff(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        ctx.reply.type = REDIS_REPLY_ARRAY;
        mmkv::StringArrayResult vs(ReplyResultStringAlloc, &ctx.reply);
        int err = m_kv_store->SDiff(ctx.currentDB, fs, vs);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::SDiffStore(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        int err = m_kv_store->SDiffStore(ctx.currentDB, cmd.GetArguments()[0], fs);
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
    int Comms::SInter(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        ctx.reply.type = REDIS_REPLY_ARRAY;
        mmkv::StringArrayResult vs(ReplyResultStringAlloc, &ctx.reply);
        int err = m_kv_store->SInter(ctx.currentDB, fs, vs);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::SInterStore(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        int err = m_kv_store->SInterStore(ctx.currentDB, cmd.GetArguments()[0], fs);
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
    int Comms::SIsMember(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->SIsMember(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
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
    int Comms::SMembers(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.reply.type = REDIS_REPLY_ARRAY;
        mmkv::StringArrayResult vs(ReplyResultStringAlloc, &ctx.reply);
        int err = m_kv_store->SMembers(ctx.currentDB, cmd.GetArguments()[0], vs);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::SMove(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->SMove(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], cmd.GetArguments()[2]);
        if (err >= 0)
        {
            fill_int_reply(ctx.reply, err);
            if (err > 0)
            {
                FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
                FireKeyChangedEvent(ctx, cmd.GetArguments()[1]);
            }
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::SPop(Context& ctx, RedisCommandFrame& cmd)
    {
        int64_t count = 1;
        if (cmd.GetArguments().size() > 1 && !GetInt64Value(ctx, cmd.GetArguments()[1], count))
        {
            return 0;
        }
        mmkv::StringArray vs;
        int err = m_kv_store->SPop(ctx.currentDB, cmd.GetArguments()[0], vs, count);
        if (err >= 0)
        {
            if (cmd.GetArguments().size() == 1)
            {
                fill_str_reply(ctx.reply, vs.size() >= 1 ? vs[0] : "");
            }
            else
            {
                fill_str_array_reply(ctx.reply, vs);
            }
            if (err > 0)
            {
                if (!ctx.flags.no_wal)
                {
                    RedisCommandFrame srem("srem");
                    srem.AddArg(cmd.GetArguments()[0]);
                    for (size_t i = 0; i < vs.size(); i++)
                    {
                        srem.AddArg(vs[i]);
                    }
                    RewriteClientCommand(ctx, srem);
                }
                FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            }
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::SRandMember(Context& ctx, RedisCommandFrame& cmd)
    {
        int64_t count = 1;
        if (cmd.GetArguments().size() > 1 && !GetInt64Value(ctx, cmd.GetArguments()[1], count))
        {
            return 0;
        }
        mmkv::StringArray vs;
        int err = m_kv_store->SRandMember(ctx.currentDB, cmd.GetArguments()[0], vs, count);
        if (err >= 0)
        {
            if (cmd.GetArguments().size() == 1)
            {
                fill_str_reply(ctx.reply, vs.size() >= 1 ? vs[0] : "");
            }
            else
            {
                fill_str_array_reply(ctx.reply, vs);
            }
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::SRem(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        int err = m_kv_store->SRem(ctx.currentDB, cmd.GetArguments()[0], fs);
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
    int Comms::SUnion(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        ctx.reply.type = REDIS_REPLY_ARRAY;
        mmkv::StringArrayResult vs(ReplyResultStringAlloc, &ctx.reply);
        int err = m_kv_store->SUnion(ctx.currentDB, fs, vs);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::SUnionStore(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        int err = m_kv_store->SUnionStore(ctx.currentDB, cmd.GetArguments()[0], fs);
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
    int Comms::SScan(Context& ctx, RedisCommandFrame& cmd)
    {
        return Scan(ctx, cmd);
    }

OP_NAMESPACE_END


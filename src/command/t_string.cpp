/*
 *Copyright (c) 2015-2015, yinqiwen <yinqiwen@gmail.com>
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

    int Comms::Get(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        std::string value;
        ctx.reply.type = REDIS_REPLY_STRING;
        int err = m_kv_store->Get(ctx.currentDB, key, ctx.reply.str);
        if (err == mmkv::ERR_ENTRY_NOT_EXIST || err == mmkv::ERR_DB_NOT_EXIST)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        else if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::MGet(Context& ctx, RedisCommandFrame& cmd)
    {
        ctx.reply.type = REDIS_REPLY_ARRAY;
        mmkv::StringArrayResult results(ReplyResultStringAlloc, &ctx.reply);
        mmkv::DataArray keys;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
        }
        mmkv::BooleanArray get_flags;
        m_kv_store->MGet(ctx.currentDB, keys, results, &get_flags);
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
           if(!get_flags[i])
           {
               ctx.reply.MemberAt(i).type = REDIS_REPLY_NIL;
           }
        }
        return 0;
    }

    int Comms::Append(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        const std::string& append = cmd.GetArguments()[1];
        int err = m_kv_store->Append(ctx.currentDB, key, append);
        if (err >= 0)
        {
            fill_int_reply(ctx.reply, err);
            FireKeyChangedEvent(ctx, key);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }

    int Comms::PSetEX(Context& ctx, RedisCommandFrame& cmd)
    {
        uint32 mills;
        const std::string& key = cmd.GetArguments()[0];
        if (!string_touint32(cmd.GetArguments()[1], mills))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        int err = m_kv_store->Set(ctx.currentDB, key, cmd.GetArguments()[2], -1, mills, -1);
        if (err == 0)
        {
            fill_ok_reply(ctx.reply);
            FireKeyChangedEvent(ctx, key);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }

    int Comms::MSet(Context& ctx, RedisCommandFrame& cmd)
    {
        if (cmd.GetArguments().size() % 2 != 0)
        {
            fill_error_reply(ctx.reply, "wrong number of arguments for MSET");
            return 0;
        }
        mmkv::DataPairArray kvs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i += 2)
        {
            mmkv::DataPair kv;
            kv.first = cmd.GetArguments()[i];
            kv.second = cmd.GetArguments()[i + 1];
            kvs.push_back(kv);
        }
        bool data_changed = false;
        if (cmd.GetType() == REDIS_CMD_MSETNX)
        {
            int err = m_kv_store->MSetNX(ctx.currentDB, kvs);
            fill_int_reply(ctx.reply, 0 == err ? 1 : 0);
            if (0 == err)
            {
                data_changed = true;
            }
        }
        else
        {
            m_kv_store->MSet(ctx.currentDB, kvs);
            fill_ok_reply(ctx.reply);
            data_changed = true;
        }
        if (data_changed)
        {
            for (uint32 i = 0; i < cmd.GetArguments().size(); i += 2)
            {
                FireKeyChangedEvent(ctx, cmd.GetArguments()[i]);
            }
        }
        return 0;
    }

    int Comms::MSetNX(Context& ctx, RedisCommandFrame& cmd)
    {
        return MSet(ctx, cmd);
    }

    int Comms::IncrbyFloat(Context& ctx, RedisCommandFrame& cmd)
    {
        long double increment, val;
        if (!GetDoubleValue(ctx, cmd.GetArguments()[1], increment))
        {
            return 0;
        }
        const std::string& key = cmd.GetArguments()[0];
        int err = m_kv_store->IncrByFloat(ctx.currentDB, key, increment, val);
        if (err == 0)
        {
            fill_double_reply(ctx.reply, val);
            if (!ctx.flags.no_wal)
            {
                RedisCommandFrame rewrite("set");
                rewrite.AddArg(cmd.GetArguments()[1]);
                std::string tmp;
                fast_dtoa(val, 10, tmp);
                rewrite.AddArg(tmp);
                RewriteClientCommand(ctx, rewrite);
            }
            FireKeyChangedEvent(ctx, key);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }

    int Comms::IncrDecrCommand(Context& ctx, const std::string& key, int64 incr)
    {
        int64_t new_val;
        int err = m_kv_store->IncrBy(ctx.currentDB, key, incr, new_val);
        if (err == 0)
        {
            fill_int_reply(ctx.reply, new_val);
            FireKeyChangedEvent(ctx, key);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }

    int Comms::Incrby(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 increment;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], increment))
        {
            return 0;
        }
        return IncrDecrCommand(ctx, cmd.GetArguments()[0], increment);
    }

    int Comms::Incr(Context& ctx, RedisCommandFrame& cmd)
    {
        return IncrDecrCommand(ctx, cmd.GetArguments()[0], 1);
    }

    int Comms::Decrby(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 increment;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], increment))
        {
            return 0;
        }
        return IncrDecrCommand(ctx, cmd.GetArguments()[0], -increment);
    }

    int Comms::Decr(Context& ctx, RedisCommandFrame& cmd)
    {
        return IncrDecrCommand(ctx, cmd.GetArguments()[0], -1);
    }

    int Comms::GetSet(Context& ctx, RedisCommandFrame& cmd)
    {
        std::string old_value;
        const std::string& key = cmd.GetArguments()[0];
        int err = m_kv_store->GetSet(ctx.currentDB, key, cmd.GetArguments()[1], old_value);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            if(err == 1)
            {
                ctx.reply.type = REDIS_REPLY_NIL;
            }else
            {
                fill_str_reply(ctx.reply, old_value);
            }
            FireKeyChangedEvent(ctx, key);
        }
        return 0;
    }

    int Comms::GetRange(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 start, end;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], start) || !GetInt64Value(ctx, cmd.GetArguments()[2], end))
        {
            return 0;
        }
        std::string range;
        int err = m_kv_store->GetRange(ctx.currentDB, cmd.GetArguments()[0], start, end, range);
        if (0 != err)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_str_reply(ctx.reply, range);
        }
        return 0;
    }

    int Comms::Set(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        const std::string& value = cmd.GetArguments()[1];
        int32_t ex = -1;
        int64_t px = -1;
        int8_t nx_xx = -1;
        if (cmd.GetArguments().size() > 2)
        {
            uint32 i = 0;
            bool syntaxerror = false;
            for (i = 2; i < cmd.GetArguments().size(); i++)
            {
                const std::string& arg = cmd.GetArguments()[i];
                if (!strcasecmp(arg.c_str(), "px") || !strcasecmp(arg.c_str(), "ex"))
                {
                    int64 iv;
                    if (!raw_toint64(cmd.GetArguments()[i + 1].c_str(), cmd.GetArguments()[i + 1].size(), iv) || iv < 0)
                    {
                        fill_error_reply(ctx.reply, "value is not an integer or out of range");
                        return 0;
                    }
                    if (!strcasecmp(arg.c_str(), "px"))
                    {
                        px = iv;
                    }
                    else
                    {
                        ex = iv;
                    }
                    i++;
                }
                else if (!strcasecmp(arg.c_str(), "xx"))
                {
                    nx_xx = 1;
                }
                else if (!strcasecmp(arg.c_str(), "nx"))
                {
                    nx_xx = 0;
                }
                else
                {
                    syntaxerror = true;
                    break;
                }
            }
            if (syntaxerror)
            {
                fill_error_reply(ctx.reply, "syntax error");
                return 0;
            }
        }
        int err = m_kv_store->Set(ctx.currentDB, key, value, ex, px, nx_xx);
        if (err == mmkv::ERR_ENTRY_EXISTED || err == mmkv::ERR_ENTRY_NOT_EXIST)
        {
            ctx.reply.type = REDIS_REPLY_NIL;
        }
        else
        {
            if (0 != err)
            {
                FillErrorReply(ctx, err);
            }
            else
            {
                FireKeyChangedEvent(ctx, key);
                fill_ok_reply(ctx.reply);
            }
        }
        return 0;
    }

    int Comms::SetEX(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 secs;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], secs))
        {
            return 0;
        }
        const std::string& key = cmd.GetArguments()[0];
        int err = m_kv_store->SetEX(ctx.currentDB, key, secs, cmd.GetArguments()[2]);
        if (0 != err)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_ok_reply(ctx.reply);
            FireKeyChangedEvent(ctx, key);
        }
        return 0;
    }
    int Comms::SetNX(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        int inserted = 0;
        int err = m_kv_store->SetNX(ctx.currentDB, key, cmd.GetArguments()[1]);
        if (0 != err && err != mmkv::ERR_ENTRY_EXISTED)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            if (err == 0)
            {
                inserted = 1;
            }
            fill_int_reply(ctx.reply, inserted);
            if (inserted)
            {
                FireKeyChangedEvent(ctx, key);
            }
        }
        return 0;
    }
    int Comms::SetRange(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 offset;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], offset))
        {
            return 0;
        }
        const std::string& key = cmd.GetArguments()[0];
        int err = m_kv_store->SetRange(ctx.currentDB, key, offset, cmd.GetArguments()[2]);
        if (err == mmkv::ERR_ENTRY_NOT_EXIST || err == mmkv::ERR_DB_NOT_EXIST)
        {
            err = 0;
        }
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_int_reply(ctx.reply, err);
            if (err > 0)
            {
                FireKeyChangedEvent(ctx, key);
            }
        }
        return 0;
    }
    int Comms::Strlen(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->Strlen(ctx.currentDB, cmd.GetArguments()[0]);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_int_reply(ctx.reply, err);
        }
        return 0;
    }

    int Comms::Bitcount(Context& ctx, RedisCommandFrame& cmd)
    {
        if (cmd.GetArguments().size() == 2)
        {
            fill_error_reply(ctx.reply, "syntax error");
            return 0;
        }
        int64 start, end;
        if (cmd.GetArguments().size() == 1)
        {
            start = 0;
            end = -1;
        }
        else
        {
            if (!GetInt64Value(ctx, cmd.GetArguments()[1], start) || !GetInt64Value(ctx, cmd.GetArguments()[2], end))
            {
                return 0;
            }
        }
        int err = m_kv_store->BitCount(ctx.currentDB, cmd.GetArguments()[0], start, end);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_int_reply(ctx.reply, err);
        }
        return 0;
    }
    int Comms::Bitop(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray keys;
        for (uint32 i = 2; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
        }
        int err = m_kv_store->BitOP(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], keys);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_int_reply(ctx.reply, err);
            FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
        }
        return 0;
    }
    int Comms::SetBit(Context& ctx, RedisCommandFrame& cmd)
    {
        uint64 offset;
        if (!string_touint64(cmd.GetArguments()[1], offset))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        if (cmd.GetArguments()[2] != "1" && cmd.GetArguments()[2] != "0")
        {
            fill_error_reply(ctx.reply, "bit is not an integer or out of range");
            return 0;
        }
        uint8 value = cmd.GetArguments()[2] != "0";
        const std::string& key = cmd.GetArguments()[0];
        int err = m_kv_store->SetBit(ctx.currentDB, key, offset, value);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_int_reply(ctx.reply, err != 0 ? 1 : 0);
            FireKeyChangedEvent(ctx, key);
        }
        return 0;
    }
    int Comms::GetBit(Context& ctx, RedisCommandFrame& cmd)
    {
        uint64 offset;
        if (!string_touint64(cmd.GetArguments()[1], offset))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }

        uint8 value = cmd.GetArguments()[2] != "0";
        int err = m_kv_store->GetBit(ctx.currentDB, cmd.GetArguments()[0], offset);
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_int_reply(ctx.reply, err);
        }
        return 0;
    }
    int Comms::Bitpos(Context& ctx, RedisCommandFrame& cmd)
    {
        uint8_t bit = 0;
        int64_t start = 0, end = -1;
        if (cmd.GetArguments()[1] != "1" && cmd.GetArguments()[1] != "0")
        {
            fill_error_reply(ctx.reply, "bit is not an integer or out of range");
            return 0;
        }
        if (cmd.GetArguments().size() >= 3 && !GetInt64Value(ctx, cmd.GetArguments()[2], start))
        {
            return 0;
        }
        if (cmd.GetArguments().size() == 4 && !GetInt64Value(ctx, cmd.GetArguments()[3], end))
        {
            return 0;
        }
        bit = cmd.GetArguments()[1] != "0";
        int err = m_kv_store->BitPos(ctx.currentDB, cmd.GetArguments()[0], bit, start, end);
        if (err < -1)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_int_reply(ctx.reply, err);
        }
        return 0;
    }

OP_NAMESPACE_END


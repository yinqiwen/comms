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

    int Comms::Randomkey(Context& ctx, RedisCommandFrame& cmd)
    {
        std::string key;
        m_kv_store->RandomKey(ctx.currentDB, key);
        fill_str_reply(ctx.reply, key);
        return 0;
    }

    std::string& Comms::ReplyResultStringAlloc(void* data)
    {
        RedisReply* reply = (RedisReply*) data;
        RedisReply& r = reply->AddMember();
        r.type = REDIS_REPLY_STRING;
        return r.str;
    }

    int Comms::Scan(Context& ctx, RedisCommandFrame& cmd)
    {
        StringArray keys;
        std::string pattern;
        uint32 limit = 1000; //return max 1000 keys one time

        int arg_start = 0;
        if (cmd.GetType() != REDIS_CMD_SCAN)
        {
            arg_start = 1;
        }
        if (cmd.GetArguments().size() > arg_start + 1)
        {
            for (uint32 i = arg_start + 1; i < cmd.GetArguments().size(); i++)
            {
                if (!strcasecmp(cmd.GetArguments()[i].c_str(), "count"))
                {
                    if (i + 1 >= cmd.GetArguments().size() || !string_touint32(cmd.GetArguments()[i + 1], limit))
                    {
                        fill_error_reply(ctx.reply, "value is not an integer or out of range");
                        return 0;
                    }
                    i++;
                }
                else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "match"))
                {
                    if (i + 1 >= cmd.GetArguments().size())
                    {
                        fill_error_reply(ctx.reply, "'MATCH' need one args followed");
                        return 0;
                    }
                    pattern = cmd.GetArguments()[i + 1];
                    i++;
                }
                else
                {
                    fill_error_reply(ctx.reply, "Syntax error, try scan 0 ");
                    return 0;
                }
            }
        }
        uint32 scan_count_limit = limit * 10;
        uint32 scan_count = 0;
        uint32 scan_cursor = 0;
        if (!string_touint32(cmd.GetArguments()[arg_start], scan_cursor))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }

        RedisReply& r1 = ctx.reply.AddMember();
        RedisReply& r2 = ctx.reply.AddMember();
        r2.type = REDIS_REPLY_ARRAY;
        mmkv::StringArrayResult results(ReplyResultStringAlloc, &r2);
        int new_cursor = 0;
        switch (cmd.GetType())
        {
            case REDIS_CMD_SCAN:
            {
                new_cursor = m_kv_store->Scan(ctx.currentDB, scan_cursor, pattern, scan_count_limit, results);
                break;
            }
            case REDIS_CMD_SSCAN:
            {
                new_cursor = m_kv_store->SScan(ctx.currentDB, cmd.GetArguments()[0], scan_cursor, pattern,
                        scan_count_limit, results);
                break;
            }
            case REDIS_CMD_HSCAN:
            {
                new_cursor = m_kv_store->HScan(ctx.currentDB, cmd.GetArguments()[0], scan_cursor, pattern,
                        scan_count_limit, results);
                break;
            }
            case REDIS_CMD_ZSCAN:
            {
                new_cursor = m_kv_store->ZScan(ctx.currentDB, cmd.GetArguments()[0], scan_cursor, pattern,
                        scan_count_limit, results);
                break;
            }
            default:
            {
                new_cursor = 0;
                break;
            }
        }
        fill_int_reply(r1, new_cursor);
        return 0;
    }

    int Comms::Keys(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::StringArray keys;
        m_kv_store->Keys(ctx.currentDB, cmd.GetArguments()[0], keys);
        fill_str_array_reply(ctx.reply, keys);
        return 0;
    }

    int Comms::Rename(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->Rename(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
        if (err >= 0)
        {
            FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            FireKeyChangedEvent(ctx, cmd.GetArguments()[1]);
            fill_ok_reply(ctx.reply);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }

    int Comms::RenameNX(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->RenameNX(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
        if (err >= 0)
        {
            if (err > 0)
            {
                FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
                FireKeyChangedEvent(ctx, cmd.GetArguments()[1]);
            }
            fill_int_reply(ctx.reply, err);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }

    int Comms::Move(Context& ctx, RedisCommandFrame& cmd)
    {
        DBID dst = 0;
        if (!string_touint32(cmd.GetArguments()[1], dst))
        {
            fill_error_reply(ctx.reply, "value is not an integer or out of range");
            return 0;
        }
        int err = m_kv_store->Move(ctx.currentDB, cmd.GetArguments()[0], dst);
        if (err >= 0)
        {
            if (err > 0)
            {
                FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
                FireKeyChangedEvent(ctx, dst, cmd.GetArguments()[0]);
            }
            fill_int_reply(ctx.reply, err);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }

    int Comms::Type(Context& ctx, RedisCommandFrame& cmd)
    {
        int type = m_kv_store->Type(ctx.currentDB, cmd.GetArguments()[0]);
        switch (type)
        {
            case mmkv::V_TYPE_SET:
            {
                fill_status_reply(ctx.reply, "set");
                break;
            }
            case mmkv::V_TYPE_LIST:
            {
                fill_status_reply(ctx.reply, "list");
                break;
            }
            case mmkv::V_TYPE_ZSET:
            {
                fill_status_reply(ctx.reply, "zset");
                break;
            }
            case mmkv::V_TYPE_HASH:
            {
                fill_status_reply(ctx.reply, "hash");
                break;
            }
            case mmkv::V_TYPE_STRING:
            {
                fill_status_reply(ctx.reply, "string");
                break;
            }
            case mmkv::V_TYPE_POD:
            {
                fill_status_reply(ctx.reply, "pod");
                break;
            }
            default:
            {
                fill_status_reply(ctx.reply, "none");
                break;
            }
        }
        return 0;
    }

    int Comms::Persist(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->Persist(ctx.currentDB, cmd.GetArguments()[0]);
        if (err >= 0)
        {
            if (err > 0)
            {
                FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            }
            fill_int_reply(ctx.reply, err);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }

    int Comms::PExpire(Context& ctx, RedisCommandFrame& cmd)
    {
        uint64 v = 0;
        if (!check_uint64_arg(ctx.reply, cmd.GetArguments()[1], v))
        {
            return 0;
        }
        int err = m_kv_store->PExpire(ctx.currentDB, cmd.GetArguments()[0], v);
        if (err >= 0)
        {
            if (err > 0)
            {
                FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            }
            fill_int_reply(ctx.reply, err);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::PExpireat(Context& ctx, RedisCommandFrame& cmd)
    {
        uint64 v = 0;
        if (!check_uint64_arg(ctx.reply, cmd.GetArguments()[1], v))
        {
            return 0;
        }
        int err = m_kv_store->PExpireat(ctx.currentDB, cmd.GetArguments()[0], v);
        if (err >= 0)
        {
            if (err > 0)
            {
                FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            }
            fill_int_reply(ctx.reply, err);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }

    int Comms::PTTL(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 ttl = m_kv_store->PTTL(ctx.currentDB, cmd.GetArguments()[0]);
        fill_int_reply(ctx.reply, ttl);
        return 0;
    }
    int Comms::TTL(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 ttl = m_kv_store->TTL(ctx.currentDB, cmd.GetArguments()[0]);
        fill_int_reply(ctx.reply, ttl);
        return 0;
    }

    int Comms::Expire(Context& ctx, RedisCommandFrame& cmd)
    {
        uint64 v = 0;
        if (!check_uint64_arg(ctx.reply, cmd.GetArguments()[1], v))
        {
            return 0;
        }
        int err = m_kv_store->Expire(ctx.currentDB, cmd.GetArguments()[0], v);
        if (err >= 0)
        {
            if (err > 0)
            {
                FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            }
            fill_int_reply(ctx.reply, err);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }

    int Comms::Expireat(Context& ctx, RedisCommandFrame& cmd)
    {
        uint64 v = 0;
        if (!check_uint64_arg(ctx.reply, cmd.GetArguments()[1], v))
        {
            return 0;
        }
        int err = m_kv_store->PExpireat(ctx.currentDB, cmd.GetArguments()[0], v * 1000);
        if (err >= 0)
        {
            if (err > 0)
            {
                FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            }
            fill_int_reply(ctx.reply, err);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }

    int Comms::Exists(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->Exists(ctx.currentDB, cmd.GetArguments()[0]);
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

    int Comms::Del(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        int err = m_kv_store->Del(ctx.currentDB, fs);
        if (err >= 0)
        {
            if (err > 0)
            {
                for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
                {
                    FireKeyChangedEvent(ctx, cmd.GetArguments()[i]);
                }
            }
            fill_int_reply(ctx.reply, err);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }

OP_NAMESPACE_END


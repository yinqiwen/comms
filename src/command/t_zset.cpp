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

    int Comms::ZAdd(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::ScoreDataArray svs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i += 2)
        {
            mmkv::ScoreData sv;
            if (!GetDoubleValue(ctx, cmd.GetArguments()[i], sv.score))
            {
                return 0;
            }
            Data element;
            sv.value = cmd.GetArguments()[i + 1];
            svs.push_back(sv);
        }
        int err = m_kv_store->ZAdd(ctx.currentDB, cmd.GetArguments()[0], svs);
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
    int Comms::ZCard(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->ZCard(ctx.currentDB, cmd.GetArguments()[0]);
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
    int Comms::ZCount(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->ZCount(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1],
                cmd.GetArguments()[2]);
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
    int Comms::ZIncrby(Context& ctx, RedisCommandFrame& cmd)
    {
        long double incr, val;
        if (!GetDoubleValue(ctx, cmd.GetArguments()[1], incr))
        {
            return 0;
        }
        int err = m_kv_store->ZIncrBy(ctx.currentDB, cmd.GetArguments()[0], incr, cmd.GetArguments()[2], val);
        if (err >= 0)
        {
            fill_double_reply(ctx.reply, val);
            FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
        }
        else
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
    int Comms::ZRange(Context& ctx, RedisCommandFrame& cmd)
    {
        bool withscores = false;
        if (cmd.GetArguments().size() == 4)
        {
            if (strcasecmp(cmd.GetArguments()[3].c_str(), "withscores") != 0)
            {
                fill_error_reply(ctx.reply, "syntax error");
                return 0;
            }
            withscores = true;
        }
        int64 start, stop;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], start) || !GetInt64Value(ctx, cmd.GetArguments()[2], stop))
        {
            return 0;
        }
        mmkv::StringArray vs;
        int err = m_kv_store->ZRange(ctx.currentDB, cmd.GetArguments()[0], start, stop, withscores, vs);
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
    int Comms::ZRangeByScore(Context& ctx, RedisCommandFrame& cmd)
    {
        bool withscores = false;
        int64 limit_offset = 0;
        int64 limit_count = -1;
        for (size_t i = 3; i < cmd.GetArguments().size(); i++)
        {
            if (!strcasecmp(cmd.GetArguments()[i].c_str(), "withscores"))
            {
                withscores = true;
            }
            else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "limit"))
            {
                if (!GetInt64Value(ctx, cmd.GetArguments()[i + 1], limit_offset)
                        || !GetInt64Value(ctx, cmd.GetArguments()[i + 2], limit_count))
                {
                    return 0;
                }
                i += 2;
            }
            else
            {
                FillErrorReply(ctx, mmkv::ERR_SYNTAX_ERROR);
                return 0;
            }
        }
        mmkv::StringArray vs;
        int err = m_kv_store->ZRangeByScore(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1],
                cmd.GetArguments()[2], withscores, limit_offset, limit_count, vs);
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
    int Comms::ZRank(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->ZRank(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
        if (err >= 0)
        {
            fill_int_reply(ctx.reply, err);
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
    int Comms::ZRem(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray fs;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            fs.push_back(cmd.GetArguments()[i]);
        }
        int err = m_kv_store->ZRem(ctx.currentDB, cmd.GetArguments()[0], fs);
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
    int Comms::ZRemRangeByRank(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 start, stop;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], start) || !GetInt64Value(ctx, cmd.GetArguments()[2], stop))
        {
            return 0;
        }
        int err = m_kv_store->ZRemRangeByRank(ctx.currentDB, cmd.GetArguments()[0], start, stop);
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
    int Comms::ZRemRangeByScore(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->ZRemRangeByScore(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1],
                cmd.GetArguments()[2]);
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
    int Comms::ZRevRange(Context& ctx, RedisCommandFrame& cmd)
    {
        bool withscores = false;
        if (cmd.GetArguments().size() == 4)
        {
            if (strcasecmp(cmd.GetArguments()[3].c_str(), "withscores") != 0)
            {
                fill_error_reply(ctx.reply, "syntax error");
                return 0;
            }
            withscores = true;
        }
        int64 start, stop;
        if (!GetInt64Value(ctx, cmd.GetArguments()[1], start) || !GetInt64Value(ctx, cmd.GetArguments()[2], stop))
        {
            return 0;
        }
        mmkv::StringArray vs;
        int err = m_kv_store->ZRevRange(ctx.currentDB, cmd.GetArguments()[0], start, stop, withscores, vs);
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
    int Comms::ZRevRangeByScore(Context& ctx, RedisCommandFrame& cmd)
    {
        bool withscores = false;
        int64 limit_offset = 0;
        int64 limit_count = -1;
        for (size_t i = 3; i < cmd.GetArguments().size(); i++)
        {
            if (!strcasecmp(cmd.GetArguments()[i].c_str(), "withscores"))
            {
                withscores = true;
            }
            else if (!strcasecmp(cmd.GetArguments()[i].c_str(), "limit"))
            {
                if (!GetInt64Value(ctx, cmd.GetArguments()[i + 1], limit_offset)
                        || !GetInt64Value(ctx, cmd.GetArguments()[i + 2], limit_count))
                {
                    return 0;
                }
                i += 2;
            }
            else
            {
                FillErrorReply(ctx, mmkv::ERR_SYNTAX_ERROR);
                return 0;
            }
        }
        mmkv::StringArray vs;
        int err = m_kv_store->ZRevRangeByScore(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1],
                cmd.GetArguments()[2], withscores, limit_offset, limit_count, vs);
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
    int Comms::ZRevRank(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->ZRevRank(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1]);
        if (err >= 0)
        {
            fill_int_reply(ctx.reply, err);
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
    int Comms::ZInterStore(Context& ctx, RedisCommandFrame& cmd)
    {
        const ArgumentArray& args = cmd.GetArguments();
        const std::string& dest = args[0];
        mmkv::DataArray keys;
        mmkv::WeightArray weights;
        std::string aggregate;
        int numkeys;
        int err = 0;
        if (!string_toint32(args[1], numkeys) || numkeys <= 0)
        {
            err = mmkv::ERR_INVALID_ARGS;
            goto _end;
        }
        if (args.size() < (uint32) numkeys + 1)
        {
            err = mmkv::ERR_INVALID_ARGS;
            goto _end;
        }
        for (int i = 2; i < numkeys + 2; i++)
        {
            keys.push_back(args[i]);
        }
        for (uint32 i = numkeys + 2; i < args.size(); i++)
        {
            if (!strcasecmp(args[i].c_str(), "weights"))
            {
                if (args.size() < i + keys.size())
                {
                    err = mmkv::ERR_INVALID_ARGS;
                    goto _end;
                }

                i++;
                for (int j = 0; j < keys.size(); j++)
                {
                    uint32 weight;
                    if (!string_touint32(args[i + j], weight))
                    {
                        err = mmkv::ERR_INVALID_ARGS;
                        goto _end;
                    }
                    weights.push_back(weight);
                }
                i += weights.size() - 1;
            }
            else if (!strcasecmp(args[i].c_str(), "aggregate"))
            {
                if (args.size() < i + 1)
                {
                    err = mmkv::ERR_INVALID_ARGS;
                    goto _end;
                }
                i++;
                aggregate = args[i];
            }
            else
            {
                err = mmkv::ERR_INVALID_ARGS;
                goto _end;
            }
        }
        if (cmd.GetType() == REDIS_CMD_ZINTERSTORE)
        {
            err = m_kv_store->ZInterStore(ctx.currentDB, dest, keys, weights, aggregate);
        }
        else
        {
            err = m_kv_store->ZUnionStore(ctx.currentDB, dest, keys, weights, aggregate);
        }
        _end: if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        else
        {
            fill_int_reply(ctx.reply, err);
        }
        return 0;
    }
    int Comms::ZUnionStore(Context& ctx, RedisCommandFrame& cmd)
    {
        return ZInterStore(ctx, cmd);
    }
    int Comms::ZScore(Context& ctx, RedisCommandFrame& cmd)
    {
        long double score;
        int err = m_kv_store->ZScore(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1], score);
        if (err >= 0)
        {
            fill_double_reply(ctx.reply, score);
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
    int Comms::ZScan(Context& ctx, RedisCommandFrame& cmd)
    {
        return Scan(ctx, cmd);
    }
    int Comms::ZLexCount(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->ZLexCount(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1],
                cmd.GetArguments()[2]);
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
    int Comms::ZRangeByLex(Context& ctx, RedisCommandFrame& cmd)
    {
        int64 limit_offset = 0;
        int64 limit_count = -1;
        for (size_t i = 3; i < cmd.GetArguments().size(); i++)
        {
            if (!strcasecmp(cmd.GetArguments()[i].c_str(), "limit"))
            {
                if (!GetInt64Value(ctx, cmd.GetArguments()[i + 1], limit_offset)
                        || !GetInt64Value(ctx, cmd.GetArguments()[i + 2], limit_count))
                {
                    return 0;
                }
                i += 2;
            }
            else
            {
                FillErrorReply(ctx, mmkv::ERR_SYNTAX_ERROR);
                return 0;
            }
        }
        mmkv::StringArray vs;
        int err = m_kv_store->ZRangeByLex(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1],
                cmd.GetArguments()[2], limit_offset, limit_count, vs);
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
    int Comms::ZRemRangeByLex(Context& ctx, RedisCommandFrame& cmd)
    {
        int err = m_kv_store->ZRemRangeByLex(ctx.currentDB, cmd.GetArguments()[0], cmd.GetArguments()[1],
                cmd.GetArguments()[2]);
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

OP_NAMESPACE_END

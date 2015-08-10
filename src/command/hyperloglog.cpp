/* hyperloglog.c - Redis HyperLogLog probabilistic cardinality approximation.
 * This file implements the algorithm and the exported Redis commands.
 *
 * Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "comms.hpp"
#include <stdint.h>
#include <math.h>
OP_NAMESPACE_BEGIN
    int Comms::PFAdd(Context& ctx, RedisCommandFrame& cmd)
    {
        const std::string& key = cmd.GetArguments()[0];
        mmkv::DataArray members;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            members.push_back(cmd.GetArguments()[i]);
        }
        int ret = m_kv_store->PFAdd(ctx.currentDB, key, members);
        if (ret >= 0)
        {
            FireKeyChangedEvent(ctx, key);
            fill_int_reply(ctx.reply, ret);
            return 0;
        }
        else
        {
            FillErrorReply(ctx, ret);
        }
        return 0;

    }
    int Comms::PFCount(Context& ctx, RedisCommandFrame& cmd)
    {
        uint64 card = 0;
        mmkv::DataArray keys;
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            keys.push_back(cmd.GetArguments()[i]);
        }
        int ret = m_kv_store->PFCount(ctx.currentDB, keys);
        if (ret >= 0)
        {
            fill_int_reply(ctx.reply, ret);
            return 0;
        }
        else
        {
            FillErrorReply(ctx, ret);
        }
        return 0;
    }

    int Comms::PFMerge(Context& ctx, RedisCommandFrame& cmd)
    {
        mmkv::DataArray srckeys;
        for (uint32 i = 1; i < cmd.GetArguments().size(); i++)
        {
            srckeys.push_back(cmd.GetArguments()[i]);
        }
        int ret = m_kv_store->PFMerge(ctx.currentDB, cmd.GetArguments()[0], srckeys);
        if (0 == ret)
        {
            FireKeyChangedEvent(ctx, cmd.GetArguments()[0]);
            fill_ok_reply(ctx.reply);
        }
        else
        {
            FillErrorReply(ctx, ret);
        }
        return 0;
    }

OP_NAMESPACE_END

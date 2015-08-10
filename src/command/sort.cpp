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
#include <algorithm>
#include <vector>

namespace comms
{
    int Comms::Sort(Context& ctx, RedisCommandFrame& cmd)
    {
        bool is_desc = false, with_alpha = false, with_limit = false;
        const ArgumentArray& args = cmd.GetArguments();
        const std::string& key = args[0];
        std::string by, store_dst;
        mmkv::StringArray get_patterns;
        int limit_offset = 0, limit_count = -1;
        for (uint32 i = 1; i < args.size(); i++)
        {
            if (!strcasecmp(args[i].c_str(), "asc"))
            {
                is_desc = false;
            }
            else if (!strcasecmp(args[i].c_str(), "desc"))
            {
                is_desc = true;
            }
            else if (!strcasecmp(args[i].c_str(), "alpha"))
            {
                with_alpha = true;
            }
            else if (!strcasecmp(args[i].c_str(), "limit") && (i + 2) < args.size())
            {
                with_limit = true;
                if (!string_toint32(args[i + 1], limit_offset) || !string_toint32(args[i + 2], limit_count))
                {
                    return false;
                }
                i += 2;
            }
            else if (!strcasecmp(args[i].c_str(), "store") && i < args.size() - 1)
            {
                store_dst = args[i + 1];
                i++;
            }
            else if (!strcasecmp(args[i].c_str(), "by") && i < args.size() - 1)
            {
                by = args[i + 1];
                i++;
            }
            else if (!strcasecmp(args[i].c_str(), "get") && i < args.size() - 1)
            {
                get_patterns.push_back(args[i + 1].c_str());
                i++;
            }
            else
            {
                DEBUG_LOG("Invalid sort option:%s", args[i].c_str());
                return false;
            }
        }
        int err = 0;
        if (store_dst.empty())
        {
            ctx.reply.type = REDIS_REPLY_ARRAY;
            mmkv::StringArrayResult results(ReplyResultStringAlloc, &ctx.reply);
            err = m_kv_store->Sort(ctx.currentDB, key, by, limit_offset, limit_count, get_patterns, is_desc, with_alpha,
                    store_dst, results);
        }
        else
        {
            ctx.reply.type = REDIS_REPLY_INTEGER;
            mmkv::StringArrayResult results;
            err = m_kv_store->Sort(ctx.currentDB, key, by, limit_offset, limit_count, get_patterns, is_desc, with_alpha,
                    store_dst, results);
            ctx.reply.integer = err >= 0 ? err : 0;
            if(err > 0)
            {
                FireKeyChangedEvent(ctx, store_dst);
            }
        }
        if (err < 0)
        {
            FillErrorReply(ctx, err);
        }
        return 0;
    }
}


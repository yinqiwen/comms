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
#include "thread/thread_mutex.hpp"

namespace comms
{
    static ThreadMutex g_transc_mutex;

    int Comms::Multi(Context& ctx, RedisCommandFrame& cmd)
    {
        if (ctx.InTransc())
        {
            fill_error_reply(ctx.reply, "MULTI calls can not be nested");
            return 0;
        }
        ctx.GetTransc().in_transc = true;
        fill_ok_reply(ctx.reply);
        return 0;
    }

    int Comms::Discard(Context& ctx, RedisCommandFrame& cmd)
    {
        if (!ctx.InTransc())
        {
            fill_error_reply(ctx.reply, "DISCARD without MULTI");
            return 0;
        }
        UnwatchKeys(ctx);
        ctx.ClearTransc();
        fill_ok_reply(ctx.reply);
        return 0;
    }

    int Comms::Exec(Context& ctx, RedisCommandFrame& cmd)
    {
        if (!ctx.InTransc())
        {
            fill_error_reply(ctx.reply, "EXEC without MULTI");
            return 0;
        }
        if (ctx.abort_exec)
        {
            ctx.reply.Clear();
            ctx.reply.type = REDIS_REPLY_NIL;
            UnwatchKeys(ctx);
            ctx.ClearTransc();
            return 0;
        }
        LockGuard<ThreadMutex> guard(g_transc_mutex); //only one transc allowed exec at the same time in multi threads

        RedisCommandFrameArray::iterator it = ctx.GetTransc().cached_cmds.begin();
        Context transc_ctx;
        transc_ctx.currentDB = ctx.currentDB;
        transc_ctx.flags = ctx.flags;
        while (it != ctx.GetTransc().cached_cmds.end())
        {
            RedisReply& r = ctx.reply.AddMember();
            RedisCommandHandlerSetting* setting = FindRedisCommandHandlerSetting(*it);
            if (NULL != setting)
            {
                transc_ctx.reply.Clear();
                transc_ctx.data_change = false;
                transc_ctx.current_cmd = &(*it);
                DoCall(transc_ctx, *setting, *it);
                r.Clone(transc_ctx.reply);
                if (transc_ctx.data_change && !ctx.flags.no_wal)
                {
                    m_repl.WriteWAL(transc_ctx.currentDB, *transc_ctx.current_cmd);
                }
            }
            else
            {
                fill_error_reply(r, "unknown command '%s'", it->GetCommand().c_str());
            }
            it++;
        }
        ctx.currentDB = transc_ctx.currentDB;

        UnwatchKeys(ctx);
        ctx.ClearTransc();
        return 0;
    }

    int Comms::AbortWatchKey(DBID db, const std::string& key)
    {
        ReadLockGuard<SpinRWLock> guard(m_watched_keys_lock);
        if (!m_watched_ctx.empty())
        {
            WatchKey k(db, key);
            WatchedContextTable::iterator fit = m_watched_ctx.find(k);
            if (fit != m_watched_ctx.end())
            {
                ContextSet::iterator sit = fit->second.begin();
                while (sit != fit->second.end())
                {
                    (*sit)->abort_exec = true;
                    sit++;
                }
            }
        }
        return 0;
    }

    int Comms::UnwatchKeys(Context& ctx)
    {
        if (NULL != ctx.watch_keys)
        {
            WriteLockGuard<SpinRWLock> guard(m_watched_keys_lock);
            WatchKeySet::iterator it = ctx.watch_keys->begin();
            while (it != ctx.watch_keys->end())
            {
                WatchedContextTable::iterator fit = m_watched_ctx.find(*it);
                if (fit != m_watched_ctx.end())
                {
                    fit->second.erase(&ctx);
                    if (fit->second.empty())
                    {
                        m_watched_ctx.erase(fit);
                    }
                }
                it++;
            }
            ctx.ClearWatchKeySet();
        }
        return 0;
    }

    int Comms::Watch(Context& ctx, RedisCommandFrame& cmd)
    {
        WriteLockGuard<SpinRWLock> guard(m_watched_keys_lock);
        for (uint32 i = 0; i < cmd.GetArguments().size(); i++)
        {
            WatchKey key(ctx.currentDB, cmd.GetArguments()[i]);
            ctx.GetWatchKeySet().insert(key);
            m_watched_ctx[key].insert(&ctx);
        }
        fill_ok_reply(ctx.reply);
        return 0;
    }
    int Comms::UnWatch(Context& ctx, RedisCommandFrame& cmd)
    {
        fill_ok_reply(ctx.reply);
        UnwatchKeys(ctx);
        return 0;
    }
}


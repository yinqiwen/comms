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
#include "cron.hpp"
#include "comms.hpp"

OP_NAMESPACE_BEGIN

    static uint64_t expire_check_start_time = 0;
    static uint64_t expire_check_max_mills = 10;
    static uint64_t expire_check_period_delete_keynum = 0;
    int MMKVExpireCallback(DBID db, const std::string& key)
    {
        RedisCommandFrame del("del");
        del.AddArg(key);
        g_repl->WriteWAL(db, del);
        expire_check_period_delete_keynum++;
        g_db->GetStatistics().stat_expiredkeys++;
        uint64_t now = get_current_epoch_millis();
        if (now - expire_check_start_time >= expire_check_max_mills)
        {
            return -1;
        }
        return 0;
    }
    struct ExpireCheck: public Runnable
    {
            void Run()
            {
                if (!g_db->GetConfig().master_host.empty())
                {
                    /*
                     * no expire on slave, master would sync the expire/del operations to slaves.
                     */
                    return;
                }
                expire_check_period_delete_keynum = 0;
                do
                {
                    expire_check_start_time = get_current_epoch_millis();
                } while (g_db->m_kv_store->RemoveExpiredKeys() < 0);
                if (expire_check_period_delete_keynum > 0)
                {
                    DEBUG_LOG("%u expired keys deleted.", expire_check_period_delete_keynum);
                }
            }
    };

    static void ChannelCloseCallback(Channel* ch, void*)
    {
        if (NULL != ch)
        {
            ch->Close();
        }
    }

    struct ConnectionTimeout: public Runnable
    {
            void Run()
            {
                static uint32 last_check_id = 0;
                if (g_db->GetConfig().timeout == 0)
                {
                    return;
                }
                uint32 max_check = 100;
                uint32 cursor = 0;
                LockGuard<SpinMutexLock> guard(g_db->m_clients_lock);
                ContextTable::iterator it = g_db->m_clients.lower_bound(last_check_id);
                while (it != g_db->m_clients.end())
                {
                    if (!it->second->processing
                            && (get_current_epoch_micros() - it->second->last_interaction_ustime)
                                    > g_db->GetConfig().timeout * 1000000)
                    {
                        it->second->client->GetService().AsyncIO(it->first, ChannelCloseCallback, NULL);
                    }
                    last_check_id = it->first;
                    cursor++;
                    if (cursor >= max_check)
                    {
                        break;
                    }
                    it++;
                }
            }
    };

    struct TrackOpsTask: public Runnable
    {
            void Run()
            {
                g_db->GetStatistics().TrackOperationsPerSecond();
            }
    };

    CronManager::CronManager()
    {

    }
    void CronManager::Start()
    {
        m_db_cron.serv.GetTimer().ScheduleHeapTask(new ExpireCheck, 100, 100, MILLIS);
        m_misc_cron.serv.GetTimer().ScheduleHeapTask(new ConnectionTimeout, 100, 100, MILLIS);
        m_misc_cron.serv.GetTimer().ScheduleHeapTask(new TrackOpsTask, 1, 1, SECONDS);

        m_db_cron.Start();
        m_misc_cron.Start();
    }

    void CronManager::StopSelf()
    {
        m_db_cron.StopSelf();
        m_misc_cron.StopSelf();
    }

OP_NAMESPACE_END


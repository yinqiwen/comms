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
#ifndef STATISTICS_HPP_
#define STATISTICS_HPP_

#include "common/common.hpp"
#include "util/atomic.hpp"
#include "thread/spin_mutex_lock.hpp"
#include "thread/lock_guard.hpp"
#include "util/string_helper.hpp"
#define COMMS_OPS_SEC_SAMPLES 16
OP_NAMESPACE_BEGIN

    struct ServerStatistics
    {
            volatile uint64 stat_numcommands;
            volatile uint64 ops_sec_last_sample_ops;
            volatile uint32 ops_sec_idx;
            volatile uint64 connections_received;
            uint64 ops_sec_samples[COMMS_OPS_SEC_SAMPLES];
            volatile uint64 instantaneous_ops;
            volatile uint64 refused_connections;
            int64 ops_limit;
            ServerStatistics() :
                    stat_numcommands(0), ops_sec_last_sample_ops(0), ops_sec_idx(0), connections_received(0), instantaneous_ops(
                            0),refused_connections(0), ops_limit(0)
            {
                memset(ops_sec_samples, 0, sizeof(ops_sec_samples));
            }
            uint64 GetOperationsPerSecond()
            {
                int j;
                uint64 sum = 0;

                for (j = 0; j < COMMS_OPS_SEC_SAMPLES; j++)
                    sum += ops_sec_samples[j];
                return sum / COMMS_OPS_SEC_SAMPLES;
            }
            void Clear()
            {
                stat_numcommands = 0;
                ops_sec_last_sample_ops = 0;
                ops_sec_idx = 0;
                connections_received = 0;
                instantaneous_ops = 0;
                refused_connections = 0;
                memset(ops_sec_samples, 0, sizeof(ops_sec_samples));
            }
    };

    class Statistics
    {
        private:
            typedef TreeMap<std::string, ServerStatistics>::Type ServerStatisticsTable;
            ServerStatisticsTable m_server_stats;
            uint64 m_ops_sec_last_sample_time;

        public:
            uint64_t stat_expiredkeys;
            Statistics();
            void Init();
            void IncAcceptedClient(const std::string& server, int v);
            void IncRefusedConnection(const std::string& server);
            int IncRecvCommands(const std::string& server, int64& seq);
            void TrackOperationsPerSecond();
            const std::string& PrintStat(std::string& str);
            void Clear();

    };

OP_NAMESPACE_END

#endif /* STATISTICS_HPP_ */

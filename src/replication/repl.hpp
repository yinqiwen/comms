/*
 * repl.hpp
 *
 *  Created on: 2013-08-29     Author: wqy
 */

#ifndef REPL_HPP_
#define REPL_HPP_

#include "channel/all_includes.hpp"
#include "thread/thread.hpp"
#include "thread/thread_mutex.hpp"
#include "thread/lock_guard.hpp"
#include "util/concurrent_queue.hpp"
#include "master.hpp"
#include "slave.hpp"
#include "snapshot.hpp"
#include "swal.h"
#include <map>

using namespace comms::codec;

namespace comms
{
    class ReplicationService: public Thread
    {
        private:
            swal_t* m_wal;
            Master m_master;
            Slave m_slave;
            ChannelService m_io_serv;
            void Run();
            void ReCreateWAL();
            static void WriteWALCallback(Channel*, void* data);
            int WriteWAL(DBID db, const Buffer& cmd);
            int WriteWAL(const Buffer& cmd);
            void Routine();
            void FlushSyncWAL();
            friend class Master;
            friend class Slave;
        public:
            ReplicationService();
            int Init();
            swal_t* GetWAL();
            ChannelService& GetIOServ()
            {
                return m_io_serv;
            }
            Timer& GetTimer()
            {
                return m_io_serv.GetTimer();
            }
            Master& GetMaster()
            {
                return m_master;
            }
            Slave& GetSlave()
            {
                return m_slave;
            }
            const char* GetServerKey();
            void SetServerKey(const std::string& str);
            int WriteWAL(DBID db, RedisCommandFrame& cmd);
            bool IsValidOffsetCksm(int64_t offset, uint64_t cksm);
            uint64_t WALStartOffset();
            uint64_t WALEndOffset();
            uint64_t WALCksm();
            void ResetWALOffsetCksm(uint64_t offset, uint64_t cksm);

            void ResetDataOffsetCksm(uint64_t offset, uint64_t cksm);
            uint64_t DataOffset();
            uint64_t DataCksm();
            void UpdateDataOffsetCksm(const Buffer& data);
            ~ReplicationService();
    };
    extern ReplicationService* g_repl;
}

#endif /* REPL_HPP_ */

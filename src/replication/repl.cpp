/*
 * repl.cpp
 *
 *  Created on: 2015Äê7ÔÂ21ÈÕ
 *      Author: wangqiying
 */
#include "comms.hpp"
#include "redis/crc64.h"

#define SERVER_KEY_SIZE 40
#define RUN_PERIOD(name, ms) static uint64_t name##_exec_ms = 0;  \
    if(ms > 0 && (now - name##_exec_ms >= ms) && (name##_exec_ms = now))
namespace comms
{
    ReplicationService* g_repl = NULL;
    struct ReplMeta
    {
            uint64_t data_cksm;
            uint64_t data_offset;  //data offset saved in mmkv store
            DBID select_db;
            char serverkey[SERVER_KEY_SIZE + 1];
            ReplMeta() :
                    data_cksm(0), data_offset(0), select_db(0)
            {
            }
    };

    ReplicationService::ReplicationService() :
            m_wal(NULL)
    {
        g_repl = this;
    }
    void ReplicationService::Run()
    {
        m_master.Init();
        m_slave.Init();
        m_io_serv.Start();
    }
    void ReplicationService::Routine()
    {
        uint64_t now = get_current_epoch_millis();
        RUN_PERIOD(sync, g_db->GetConfig().repl_wal_sync_period * 1000)
        {
            FlushSyncWAL();
        }
    }
    int ReplicationService::Init()
    {
        swal_options_t* options = swal_options_create();
        options->create_ifnotexist = true;
        options->user_meta_size = 4096;
        options->max_file_size = g_db->GetConfig().repl_wal_size;
        options->ring_cache_size = g_db->GetConfig().repl_wal_cache_size;
        options->cksm_func = crc64;
        options->log_prefix = "comms";
        int err = swal_open(g_db->GetConfig().repl_data_dir.c_str(), options, &m_wal);
        swal_options_destroy(options);
        if (0 != err)
        {
            ERROR_LOG("Failed to init wal log with err code:%d", err);
            return err;
        }
        ReplMeta* meta = (ReplMeta*) swal_user_meta(m_wal);
        if (meta->serverkey[0] == 0)
        {
            std::string randomkey = random_hex_string(SERVER_KEY_SIZE);
            memcpy(meta->serverkey, randomkey.data(), SERVER_KEY_SIZE);
            meta->serverkey[SERVER_KEY_SIZE] = 0;
            meta->data_cksm = 0;
            meta->data_offset = 0;
            meta->select_db = 0;
        }
        struct RoutineTask: public Runnable
        {
                ReplicationService* serv;
                RoutineTask(ReplicationService* s) :
                        serv(s)
                {
                }
                void Run()
                {
                    serv->Routine();
                }
        };
        GetTimer().ScheduleHeapTask(new RoutineTask(this), 100, 100, MILLIS);
        Start();
        return 0;
    }
    void ReplicationService::FlushSyncWAL()
    {
        swal_sync(m_wal);
        swal_sync_meta(m_wal);
    }
    swal_t* ReplicationService::GetWAL()
    {
        return m_wal;
    }
    const char* ReplicationService::GetServerKey()
    {
        ReplMeta* meta = (ReplMeta*) swal_user_meta(m_wal);
        return meta->serverkey;
    }
    void ReplicationService::SetServerKey(const std::string& str)
    {
        ReplMeta* meta = (ReplMeta*) swal_user_meta(m_wal);
        memcpy(meta->serverkey, str.data(), str.size() < SERVER_KEY_SIZE ? str.size() : SERVER_KEY_SIZE);
    }
    int ReplicationService::WriteWAL(const Buffer& cmd)
    {
        swal_append(m_wal, cmd.GetRawReadBuffer(), cmd.ReadableBytes());
        return cmd.ReadableBytes();
    }
    int ReplicationService::WriteWAL(DBID db, const Buffer& cmd)
    {
        ReplMeta* meta = (ReplMeta*) swal_user_meta(m_wal);
        int len = 0;
        if (meta->select_db != db)
        {
            if (g_db->GetConfig().master_host.empty())
            {
                Buffer select;
                select.Printf("select %u\r\n", db);
                len += WriteWAL(select);
                meta->select_db = db;
            }
            else
            {
                //slave can NOT generate 'select' itself & never reach here
            }
        }
        len += WriteWAL(cmd);
        return len;
    }

    struct ReplCommand
    {
            DBID db;
            Buffer cmdbuf;
    };

    void ReplicationService::WriteWALCallback(Channel*, void* data)
    {
        ReplCommand* cmd = (ReplCommand*) data;
        g_repl->WriteWAL(cmd->db, cmd->cmdbuf);
        DELETE(cmd);
        g_repl->GetMaster().SyncWAL();
    }

    int ReplicationService::WriteWAL(DBID db, RedisCommandFrame& cmd)
    {
        ReplCommand* repl_cmd = new ReplCommand;
        repl_cmd->db = db;
        const Buffer& raw_protocol = cmd.GetRawProtocolData();
        if (raw_protocol.Readable())
        {
            repl_cmd->cmdbuf.Write(raw_protocol.GetRawReadBuffer(), raw_protocol.ReadableBytes());
        }
        else
        {
            RedisCommandEncoder::Encode(repl_cmd->cmdbuf, cmd);
        }
        m_io_serv.AsyncIO(0, WriteWALCallback, repl_cmd);
        return 0;
    }

    static int cksm_callback(const void* log, size_t loglen, void* data)
    {
        uint64_t* cksm = (uint64_t*) data;
        *cksm = crc64(*cksm, (const unsigned char *) log, loglen);
        return 0;
    }
    bool ReplicationService::IsValidOffsetCksm(int64_t offset, uint64_t cksm)
    {
        bool valid_offset = offset > 0 && offset <= swal_end_offset(m_wal) && offset >= swal_start_offset(m_wal);
        if (!valid_offset)
        {
            return false;
        }
        if (0 == cksm)
        {
            //DO not check cksm when it's 0
            return true;
        }
        uint64_t dest_cksm = swal_cksm(m_wal);
        uint64_t end_offset = swal_end_offset(m_wal);
        swal_replay(m_wal, offset, end_offset - offset, cksm_callback, &cksm);
        return cksm == dest_cksm;
    }
    uint64_t ReplicationService::WALCksm()
    {
        return swal_cksm(m_wal);
    }
    void ReplicationService::ResetWALOffsetCksm(uint64_t offset, uint64_t cksm)
    {
        swal_reset(m_wal, offset, cksm);
    }
    uint64_t ReplicationService::WALStartOffset()
    {
        return swal_start_offset(m_wal);
    }
    uint64_t ReplicationService::WALEndOffset()
    {
        return swal_end_offset(m_wal);
    }
    void ReplicationService::ResetDataOffsetCksm(uint64_t offset, uint64_t cksm)
    {
        ReplMeta* meta = (ReplMeta*) swal_user_meta(m_wal);
        meta->data_cksm = cksm;
        meta->data_offset = offset;
    }
    uint64_t ReplicationService::DataOffset()
    {
        ReplMeta* meta = (ReplMeta*) swal_user_meta(m_wal);
        return meta->data_offset;
    }
    uint64_t ReplicationService::DataCksm()
    {
        ReplMeta* meta = (ReplMeta*) swal_user_meta(m_wal);
        return meta->data_cksm;
    }
    void ReplicationService::UpdateDataOffsetCksm(const Buffer& data)
    {
        ReplMeta* meta = (ReplMeta*) swal_user_meta(m_wal);
        meta->data_offset += data.ReadableBytes();
        meta->data_cksm = crc64(meta->data_offset, (const unsigned char *) (data.GetRawReadBuffer()),
                data.ReadableBytes());
    }
    ReplicationService::~ReplicationService()
    {

    }
}


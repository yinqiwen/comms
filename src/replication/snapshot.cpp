/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
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

#include "snapshot.hpp"
#include "util/helpers.hpp"

extern "C"
{
#include "redis/lzf.h"
#include "redis/ziplist.h"
#include "redis/zipmap.h"
#include "redis/intset.h"
#include "redis/crc64.h"
#include "redis/endianconv.h"
}
#include <float.h>
#include <cmath>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include "comms.hpp"

#define RETURN_NEGATIVE_EXPR(x)  do\
    {                    \
	    int ret = (x);   \
        if(ret < 0) {   \
           return ret; \
        }             \
    }while(0)

#define REDIS_RDB_VERSION 6

#define COMMS_RDB_VERSION 1

/* Defines related to the dump file format. To store 32 bits lengths for short
 * keys requires a lot of space, so we check the most significant 2 bits of
 * the first byte to interpreter the length:
 *
 * 00|000000 => if the two MSB are 00 the len is the 6 bits of this byte
 * 01|000000 00000000 =>  01, the len is 14 byes, 6 bits + 8 bits of next byte
 * 10|000000 [32 bit integer] => if it's 01, a full 32 bit len will follow
 * 11|000000 this means: specially encoded object will follow. The six bits
 *           number specify the kind of object that follows.
 *           See the REDIS_RDB_ENC_* defines.
 *
 * Lengths up to 63 are stored using a single byte, most DB keys, and may
 * values, will fit inside. */
#define REDIS_RDB_6BITLEN 0
#define REDIS_RDB_14BITLEN 1
#define REDIS_RDB_32BITLEN 2
#define REDIS_RDB_ENCVAL 3
#define REDIS_RDB_LENERR UINT_MAX

/* When a length of a string object stored on disk has the first two bits
 * set, the remaining two bits specify a special encoding for the object
 * accordingly to the following defines: */
#define REDIS_RDB_ENC_INT8 0        /* 8 bit signed integer */
#define REDIS_RDB_ENC_INT16 1       /* 16 bit signed integer */
#define REDIS_RDB_ENC_INT32 2       /* 32 bit signed integer */
#define REDIS_RDB_ENC_LZF 3         /* string compressed with FASTLZ */

/* Special RDB opcodes (saved/loaded with rdbSaveType/rdbLoadType). */
#define REDIS_RDB_OPCODE_AUX        250
#define REDIS_RDB_OPCODE_RESIZEDB   251
#define REDIS_RDB_OPCODE_EXPIRETIME_MS 252
#define REDIS_RDB_OPCODE_EXPIRETIME 253
#define REDIS_RDB_OPCODE_SELECTDB   254
#define REDIS_RDB_OPCODE_EOF        255

/* Dup object types to RDB object types. Only reason is readability (are we
 * dealing with RDB types or with in-memory object types?). */
#define REDIS_RDB_TYPE_STRING 0
#define REDIS_RDB_TYPE_LIST   1
#define REDIS_RDB_TYPE_SET    2
#define REDIS_RDB_TYPE_ZSET   3
#define REDIS_RDB_TYPE_HASH   4

/* Object types for encoded objects. */
#define REDIS_RDB_TYPE_HASH_ZIPMAP    9
#define REDIS_RDB_TYPE_LIST_ZIPLIST  10
#define REDIS_RDB_TYPE_SET_INTSET    11
#define REDIS_RDB_TYPE_ZSET_ZIPLIST  12
#define REDIS_RDB_TYPE_HASH_ZIPLIST  13

#define COMMS_RDB_TYPE_CHUNK 1
#define COMMS_RDB_TYPE_SNAPPY_CHUNK 2
#define COMMS_RDB_TYPE_EOF 255

namespace comms
{
    static const uint32 kloading_process_events_interval_bytes = 10 * 1024 * 1024;

    struct SnapshotCacheState
    {
            uint64 last_save_mills;
            bool snapshot_saving[2];
            size_t snapshot_offset[2];
            uint64 snapshot_cksm[2];
            SnapshotCacheState()
            {
                memset(this, 0, sizeof(SnapshotCacheState));
                last_save_mills = get_current_epoch_millis(); /* At startup we consider the DB saved. */
            }
    };
    static SnapshotCacheState g_snapshot_state;

    Snapshot::Snapshot()
    {
        m_status.Clear();
    }

    void Snapshot::Flush()
    {
        if (NULL != m_status.fp)
        {
            fflush(m_status.fp);
            fsync(fileno(m_status.fp));
        }
    }

    int Snapshot::Rename(const std::string& file_name)
    {
        std::string file = g_db->GetConfig().repl_data_dir + "/" + file_name;
        if (file == GetPath())
        {
            return 0;
        }
        Close();
        int ret = rename(GetPath().c_str(), file.c_str());
        if (0 == ret)
        {
            m_status.file_path = file;
        }
        return ret;
    }

    void Snapshot::Remove()
    {
        Close();
        unlink(m_status.file_path.c_str());
    }

    int Snapshot::RenameDefault()
    {
        Close();
        SnapshotType type = IsRedisSnapshot(GetPath()) ? REDIS_SNAPSHOT : MMKV_SNAPSHOT;
        return Rename(g_db->GetConfig().snapshot_filename + "_" + stringfromll(type));
    }

    void Snapshot::Close()
    {
        if (NULL != m_status.fp)
        {
            fclose(m_status.fp);
            m_status.fp = NULL;
        }
    }
    int Snapshot::Open(const std::string& file, bool read_only)
    {
        std::string file_path = file;
        m_status.Clear();
        m_status.file_path = file_path;
        if ((m_status.fp = fopen(m_status.file_path.c_str(), read_only ? "r" : "w")) == NULL)
        {
            ERROR_LOG("Failed to open comms dump file:%s", m_status.file_path.c_str());
            return -1;
        }
        m_status.open_readonly = read_only;
        return 0;
    }

    int Snapshot::Reload(SnapshotRoutine* cb, void *data)
    {
        return Load(GetPath(), cb, data);
    }

    std::string Snapshot::DefaultPath(SnapshotType type)
    {
        return g_db->GetConfig().repl_data_dir + "/" + g_db->GetConfig().snapshot_filename + "_" + stringfromll(type);
    }

    int Snapshot::LoadDefault(SnapshotType type, SnapshotRoutine* cb, void *data)
    {
        return Load(DefaultPath(type), cb, data);
    }

    int Snapshot::Load(const std::string& file, SnapshotRoutine* cb, void *data)
    {
        m_status.routine_cb = cb;
        m_status.routine_cbdata = data;
        int ret = 0;
        uint64_t start_time = get_current_epoch_millis();
        bool is_redis_snapshot = IsRedisSnapshot(file);
        if (is_redis_snapshot)
        {
            ret = Open(file, true);
            if (0 != ret)
            {
                return ret;
            }
            ret = RedisLoad();
        }
        else
        {
            ret = MMKVLoad(file);
        }
        if (ret == 0)
        {
            uint64_t cost = get_current_epoch_millis() - start_time;
            INFO_LOG("Cost %.2fs to load snapshot file with type:%d.", cost / 1000.0,
                    is_redis_snapshot ? REDIS_SNAPSHOT : MMKV_SNAPSHOT);
        }
        return ret;
    }

    int Snapshot::Write(const void* buf, size_t buflen)
    {
        /*
         * routine callback every 100ms
         */
        uint64_t now = get_current_epoch_millis();
        if (NULL != m_status.routine_cb && now - m_status.routine_mills >= 100)
        {
            int cbret = m_status.routine_cb(m_status.routine_cbdata);
            if (0 != cbret)
            {
                return cbret;
            }
            m_status.routine_mills = now;
        }

        if (NULL == m_status.fp)
        {
            ERROR_LOG("Failed to open redis dump file:%s to write", m_status.file_path.c_str());
            return -1;
        }

        size_t max_write_bytes = 1024 * 1024 * 2;
        const char* data = (const char*) buf;
        while (buflen)
        {
            size_t bytes_to_write = (max_write_bytes < buflen) ? max_write_bytes : buflen;
            if (fwrite(data, bytes_to_write, 1, m_status.fp) == 0)
                return -1;
            //check sum here
            m_status.cksm = crc64(m_status.cksm, (unsigned char *) data, bytes_to_write);
            data += bytes_to_write;
            buflen -= bytes_to_write;
        }
        m_status.processed_bytes += buflen;
        return 0;
    }

    bool Snapshot::Read(void* buf, size_t buflen, bool cksm)
    {
        if ((m_status.processed_bytes + buflen) / kloading_process_events_interval_bytes
                > m_status.processed_bytes / kloading_process_events_interval_bytes)
        {
            INFO_LOG("%llu bytes loaded from dump file.", m_status.processed_bytes);
        }
        m_status.processed_bytes += buflen;
        /*
         * routine callback every 100ms
         */
        if (NULL != m_status.routine_cb && get_current_epoch_millis() - m_status.routine_mills >= 100)
        {
            int cbret = m_status.routine_cb(m_status.routine_cbdata);
            if (0 != cbret)
            {
                return cbret;
            }
            m_status.routine_mills = get_current_epoch_millis();
        }
        size_t max_read_bytes = 1024 * 1024 * 2;
        while (buflen)
        {
            size_t bytes_to_read = (max_read_bytes < buflen) ? max_read_bytes : buflen;
            if (fread(buf, bytes_to_read, 1, m_status.fp) == 0)
                return false;
            if (cksm)
            {
                //check sum here
                m_status.cksm = crc64(m_status.cksm, (unsigned char *) buf, bytes_to_read);
            }
            buf = (char*) buf + bytes_to_read;
            buflen -= bytes_to_read;
        }
        return true;
    }

    int Snapshot::Save(bool force, SnapshotType type, SnapshotRoutine* cb, void *data)
    {
        if (g_snapshot_state.snapshot_saving[type])
        {
            WARN_LOG("Snapshot:%d is in processing!", type);
            return ERR_SNAPSHOT_SAVING;
        }
        if (!force && g_repl->IsValidOffsetCksm(g_snapshot_state.snapshot_offset[type], 0)) //check offset only
        {
            uint64 total_from_last_snapshot = g_repl->WALEndOffset() - g_snapshot_state.snapshot_offset[type];
            if (total_from_last_snapshot <= g_db->GetConfig().repl_wal_size / 2) //only less than 1/2 wal size data in wal
            {
                if (g_repl->IsValidOffsetCksm(g_snapshot_state.snapshot_offset[type],
                        g_snapshot_state.snapshot_cksm[type])) //check offset & cksm again
                {
                    std::string snapshot_file = g_db->GetConfig().repl_data_dir + "/"
                            + g_db->GetConfig().snapshot_filename;
                    if (is_file_exist(snapshot_file))
                    {
                        //use last snapshot instead
                        return 0;
                    }
                }
            }
        }

        char tmpfile[1024];
        uint32 now = time(NULL);
        sprintf(tmpfile, "%s/comms-%u-%u-%d.snapshot", g_db->GetConfig().repl_data_dir.c_str(), getpid(), now, type);
        int ret = Open(tmpfile, false);
        if (0 != ret)
        {
            return ret;
        }
        g_snapshot_state.snapshot_saving[type] = true;
        m_status.routine_cb = cb;
        m_status.routine_cbdata = data;
        uint64 cache_offset = g_repl->WALEndOffset();
        uint64 cache_cksm = g_repl->WALCksm();
        uint64_t start_time = get_current_epoch_millis();
        if (REDIS_SNAPSHOT == type)
        {
            ret = RedisSave();
        }
        else
        {
            ret = MMKVSave(tmpfile);
        }
        g_snapshot_state.snapshot_offset[type] = cache_offset;
        g_snapshot_state.snapshot_cksm[type] = cache_cksm;
        g_snapshot_state.snapshot_saving[type] = false;
        g_snapshot_state.last_save_mills = get_current_epoch_millis();
        if (0 == ret)
        {
            Rename(g_db->GetConfig().snapshot_filename + "_" + stringfromll(type));
            g_repl->GetMaster().FullResyncSlaves(type == REDIS_SNAPSHOT);
            uint64_t cost = get_current_epoch_millis() - start_time;
            INFO_LOG("Cost %.2fs to save snapshot file with type:%d.", cost / 1000.0, type);
        }
        else
        {
            Close();
        }
        return ret;
    }
    int Snapshot::BGSave(SnapshotType format)
    {
        if (g_snapshot_state.snapshot_saving[format])
        {
            ERROR_LOG("There is already a background task.");
            return -1;
        }
        struct BGTask: public Thread
        {
                SnapshotType f;
                BGTask(SnapshotType fm) :
                        f(fm)
                {
                }
                void Run()
                {
                    Snapshot snapshot;
                    snapshot.Save(true, f, NULL, NULL);
                    delete this;
                }
        };
        BGTask* task = new BGTask(format);
        task->Start();
        return 0;
    }
    uint32 Snapshot::LastSave()
    {
        return g_snapshot_state.last_save_mills / 1000;
    }
    bool Snapshot::IsSaving(SnapshotType type)
    {
        return g_snapshot_state.snapshot_saving[type];
    }
    void Snapshot::UpdateSnapshotOffsetCksm(SnapshotType type, uint64 offset, uint64 cksm)
    {
        g_snapshot_state.snapshot_cksm[type] = cksm;
        g_snapshot_state.snapshot_offset[type] = offset;
    }
    uint64 Snapshot::SnapshotOffset(SnapshotType type)
    {
        return g_snapshot_state.snapshot_offset[type];
    }
    uint64 Snapshot::SnapshotCksm(SnapshotType type)
    {
        return g_snapshot_state.snapshot_cksm[type];
    }

    Snapshot::~Snapshot()
    {
        Close();
    }

    int Snapshot::RedisReadType()
    {
        unsigned char type;
        if (Read(&type, 1) == 0)
            return -1;
        return type;
    }

    time_t Snapshot::RedisReadTime()
    {
        int32_t t32;
        if (Read(&t32, 4) == 0)
            return -1;
        return (time_t) t32;
    }
    int64 Snapshot::RedisReadMillisecondTime()
    {
        int64_t t64;
        if (Read(&t64, 8) == 0)
            return -1;
        return (long long) t64;
    }

    uint32_t Snapshot::RedisReadLen(int *isencoded)
    {
        unsigned char buf[2];
        uint32_t len;
        int type;

        if (isencoded)
            *isencoded = 0;
        if (Read(buf, 1) == 0)
            return REDIS_RDB_LENERR;
        type = (buf[0] & 0xC0) >> 6;
        if (type == REDIS_RDB_ENCVAL)
        {
            /* Read a 6 bit encoding type. */
            if (isencoded)
                *isencoded = 1;
            return buf[0] & 0x3F;
        }
        else if (type == REDIS_RDB_6BITLEN)
        {
            /* Read a 6 bit len. */
            return buf[0] & 0x3F;
        }
        else if (type == REDIS_RDB_14BITLEN)
        {
            /* Read a 14 bit len. */
            if (Read(buf + 1, 1) == 0)
                return REDIS_RDB_LENERR;
            return ((buf[0] & 0x3F) << 8) | buf[1];
        }
        else
        {
            /* Read a 32 bit len. */
            if (Read(&len, 4) == 0)
                return REDIS_RDB_LENERR;
            return ntohl(len);
        }
    }

    bool Snapshot::RedisReadLzfStringObject(std::string& str)
    {
        unsigned int len, clen;
        unsigned char *c = NULL;

        if ((clen = RedisReadLen(NULL)) == REDIS_RDB_LENERR)
            return false;
        if ((len = RedisReadLen(NULL)) == REDIS_RDB_LENERR)
            return false;
        str.resize(len);
        NEW(c, unsigned char[clen]);
        if (NULL == c)
            return false;

        if (!Read(c, clen))
        {
            DELETE_A(c);
            return false;
        }
        str.resize(len);
        char* val = &str[0];
        if (lzf_decompress(c, clen, val, len) == 0)
        {
            DELETE_A(c);
            return false;
        }
        DELETE_A(c);
        return true;
    }

    bool Snapshot::RedisReadInteger(int enctype, int64& val)
    {
        unsigned char enc[4];

        if (enctype == REDIS_RDB_ENC_INT8)
        {
            if (Read(enc, 1) == 0)
                return false;
            val = (signed char) enc[0];
        }
        else if (enctype == REDIS_RDB_ENC_INT16)
        {
            uint16_t v;
            if (Read(enc, 2) == 0)
                return false;
            v = enc[0] | (enc[1] << 8);
            val = (int16_t) v;
        }
        else if (enctype == REDIS_RDB_ENC_INT32)
        {
            uint32_t v;
            if (Read(enc, 4) == 0)
                return false;
            v = enc[0] | (enc[1] << 8) | (enc[2] << 16) | (enc[3] << 24);
            val = (int32_t) v;
        }
        else
        {
            val = 0; /* anti-warning */
            FATAL_LOG("Unknown RDB integer encoding type");
            abort();
        }
        return true;
    }

    /* For information about double serialization check rdbSaveDoubleValue() */
    int Snapshot::RedisReadDoubleValue(double&val)
    {
        static double R_Zero = 0.0;
        static double R_PosInf = 1.0 / R_Zero;
        static double R_NegInf = -1.0 / R_Zero;
        static double R_Nan = R_Zero / R_Zero;
        char buf[128];
        unsigned char len;

        if (Read(&len, 1) == 0)
            return -1;
        switch (len)
        {
            case 255:
                val = R_NegInf;
                return 0;
            case 254:
                val = R_PosInf;
                return 0;
            case 253:
                val = R_Nan;
                return 0;
            default:
                if (Read(buf, len) == 0)
                    return -1;
                buf[len] = '\0';
                sscanf(buf, "%lg", &val);
                return 0;
        }
    }

    bool Snapshot::RedisReadString(std::string& str)
    {
        str.clear();
        int isencoded;
        uint32_t len;
        len = RedisReadLen(&isencoded);
        if (isencoded)
        {
            switch (len)
            {
                case REDIS_RDB_ENC_INT8:
                case REDIS_RDB_ENC_INT16:
                case REDIS_RDB_ENC_INT32:
                {
                    int64 v;
                    if (RedisReadInteger(len, v))
                    {
                        str = stringfromll(v);
                        return true;
                    }
                    else
                    {
                        return true;
                    }
                }
                case REDIS_RDB_ENC_LZF:
                {
                    return RedisReadLzfStringObject(str);
                }
                default:
                {
                    ERROR_LOG("Unknown RDB encoding type");
                    abort();
                    break;
                }
            }
        }

        if (len == REDIS_RDB_LENERR)
            return false;
        str.clear();
        str.resize(len);
        char* val = &str[0];
        if (len && Read(val, len) == 0)
        {
            return false;
        }
        return true;
    }

    void Snapshot::RedisLoadListZipList(unsigned char* data, const std::string& key)
    {
        unsigned char* iter = ziplistIndex(data, 0);
        while (iter != NULL)
        {
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;
            if (ziplistGet(iter, &vstr, &vlen, &vlong))
            {
                std::string value;
                if (vstr)
                {
                    value.assign((char*) vstr, vlen);
                }
                else
                {
                    value = stringfromll(vlong);
                }
                g_db->GetKVStore().RPush(m_status.current_db, key, value);
            }
            iter = ziplistNext(data, iter);
        }
    }

    static double zzlGetScore(unsigned char *sptr)
    {
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        char buf[128];
        double score;

        ASSERT(sptr != NULL);
        ASSERT(ziplistGet(sptr, &vstr, &vlen, &vlong));
        if (vstr)
        {
            memcpy(buf, vstr, vlen);
            buf[vlen] = '\0';
            score = strtod(buf, NULL);
        }
        else
        {
            score = vlong;
        }
        return score;
    }
    void Snapshot::RedisLoadZSetZipList(unsigned char* data, const std::string& key)
    {
        unsigned char* iter = ziplistIndex(data, 0);
        while (iter != NULL)
        {
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;
            std::string value;
            if (ziplistGet(iter, &vstr, &vlen, &vlong))
            {
                if (vstr)
                {
                    value.assign((char*) vstr, vlen);
                }
                else
                {
                    value = stringfromll(vlong);
                }
            }
            iter = ziplistNext(data, iter);
            if (NULL == iter)
            {
                break;
            }
            double score = zzlGetScore(iter);
            g_db->GetKVStore().ZAdd(m_status.current_db, key, score, value);
            iter = ziplistNext(data, iter);
        }
    }

    void Snapshot::RedisLoadHashZipList(unsigned char* data, const std::string& key)
    {
        unsigned char* iter = ziplistIndex(data, 0);
        while (iter != NULL)
        {
            unsigned char *vstr, *fstr;
            unsigned int vlen, flen;
            long long vlong, flong;
            std::string field, value;
            if (ziplistGet(iter, &fstr, &flen, &flong))
            {
                if (fstr)
                {
                    field.assign((char*) fstr, flen);
                }
                else
                {
                    field = stringfromll(flong);
                }
            }
            else
            {
                break;
            }
            iter = ziplistNext(data, iter);
            if (NULL == iter)
            {
                break;
            }
            if (ziplistGet(iter, &vstr, &vlen, &vlong))
            {
                if (vstr)
                {
                    value.assign((char*) vstr, vlen);
                }
                else
                {
                    value = stringfromll(vlong);
                }
                g_db->GetKVStore().HSet(m_status.current_db, key, field, value);

            }
            iter = ziplistNext(data, iter);
        }
    }

    void Snapshot::RedisLoadSetIntSet(unsigned char* data, const std::string& key)
    {
        int ii = 0;
        int64_t llele = 0;
        while (!intsetGet((intset*) data, ii++, &llele))
        {
            //value
            g_db->GetKVStore().SAdd(m_status.current_db, key, stringfromll(llele));
        }
    }

    bool Snapshot::RedisLoadObject(int rdbtype, const std::string& key)
    {
        switch (rdbtype)
        {
            case REDIS_RDB_TYPE_STRING:
            {
                std::string str;
                if (RedisReadString(str))
                {
                    //save key-value
                    g_db->GetKVStore().Set(m_status.current_db, key, str);
                }
                else
                {
                    return false;
                }
                break;
            }
            case REDIS_RDB_TYPE_LIST:
            case REDIS_RDB_TYPE_SET:
            {
                uint32 len;
                if ((len = RedisReadLen(NULL)) == REDIS_RDB_LENERR)
                    return false;
                while (len--)
                {
                    std::string str;
                    if (RedisReadString(str))
                    {
                        //push to list/set
                        if (REDIS_RDB_TYPE_SET == rdbtype)
                        {
                            g_db->GetKVStore().SAdd(m_status.current_db, key, str);
                        }
                        else
                        {
                            g_db->GetKVStore().RPush(m_status.current_db, key, str);
                        }
                    }
                    else
                    {
                        return false;
                    }
                }
                break;
            }
            case REDIS_RDB_TYPE_ZSET:
            {
                uint32 len;
                if ((len = RedisReadLen(NULL)) == REDIS_RDB_LENERR)
                    return false;
                while (len--)
                {
                    std::string str;
                    double score;
                    if (RedisReadString(str) && 0 == RedisReadDoubleValue(score))
                    {
                        //save value score
                        g_db->GetKVStore().ZAdd(m_status.current_db, key, score, str);
                    }
                    else
                    {
                        return false;
                    }
                }
                break;
            }
            case REDIS_RDB_TYPE_HASH:
            {
                uint32 len;
                if ((len = RedisReadLen(NULL)) == REDIS_RDB_LENERR)
                    return false;
                while (len--)
                {
                    std::string field, str;
                    if (RedisReadString(field) && RedisReadString(str))
                    {
                        //save hash value
                        g_db->GetKVStore().HSet(m_status.current_db, key, field, str);
                    }
                    else
                    {
                        return false;
                    }
                }
                break;
            }
            case REDIS_RDB_TYPE_HASH_ZIPMAP:
            case REDIS_RDB_TYPE_LIST_ZIPLIST:
            case REDIS_RDB_TYPE_SET_INTSET:
            case REDIS_RDB_TYPE_ZSET_ZIPLIST:
            case REDIS_RDB_TYPE_HASH_ZIPLIST:
            {
                std::string aux;
                if (!RedisReadString(aux))
                {
                    return false;
                }
                unsigned char* data = (unsigned char*) (&(aux[0]));
                switch (rdbtype)
                {
                    case REDIS_RDB_TYPE_HASH_ZIPMAP:
                    {
                        unsigned char *zi = zipmapRewind(data);
                        unsigned char *fstr, *vstr;
                        unsigned int flen, vlen;
                        unsigned int maxlen = 0;
                        while ((zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen)) != NULL)
                        {
                            if (flen > maxlen)
                                maxlen = flen;
                            if (vlen > maxlen)
                                maxlen = vlen;

                            //save hash value data
                            Data field((char*) fstr, flen);
                            Data value((char*) vstr, vlen);
                            g_db->GetKVStore().HSet(m_status.current_db, key, field, value);
                        }
                        break;
                    }
                    case REDIS_RDB_TYPE_LIST_ZIPLIST:
                    {
                        RedisLoadListZipList(data, key);
                        break;
                    }
                    case REDIS_RDB_TYPE_SET_INTSET:
                    {
                        RedisLoadSetIntSet(data, key);
                        break;
                    }
                    case REDIS_RDB_TYPE_ZSET_ZIPLIST:
                    {
                        RedisLoadZSetZipList(data, key);
                        break;
                    }
                    case REDIS_RDB_TYPE_HASH_ZIPLIST:
                    {
                        RedisLoadHashZipList(data, key);
                        break;
                    }
                    default:
                    {
                        ERROR_LOG("Unknown encoding");
                        abort();
                    }
                }
                break;
            }
            default:
            {
                ERROR_LOG("Unknown object type:%d", rdbtype);
                abort();
            }
        }
        return true;
    }

    int Snapshot::IsRedisSnapshot(const std::string& file)
    {
        FILE* fp = NULL;
        if ((fp = fopen(file.c_str(), "r")) == NULL)
        {
            ERROR_LOG("Failed to load redis dump file:%s", file.c_str());
            return -1;
        }
        char buf[10];
        if (fread(buf, 10, 1, fp) != 1)
        {
            fclose(fp);
            return -1;
        }
        buf[9] = '\0';
        fclose(fp);
        if (memcmp(buf, "REDIS", 5) != 0)
        {
            //WARN_LOG("Wrong signature trying to load DB from file:%s", file.c_str());
            return 0;
        }
        return 1;
    }

    int Snapshot::RedisLoad()
    {
        char buf[1024];
        int rdbver, type;
        int64 expiretime = -1;
        std::string key;

        m_status.current_db = 0;
        if (!Read(buf, 9, true))
            goto eoferr;
        buf[9] = '\0';
        if (memcmp(buf, "REDIS", 5) != 0)
        {
            Close();
            WARN_LOG("Wrong signature:%s trying to load DB from file:%s", buf, m_status.file_path.c_str());
            return -1;
        }
        rdbver = atoi(buf + 5);
        if (rdbver < 1 || rdbver > REDIS_RDB_VERSION)
        {
            WARN_LOG("Can't handle RDB format version %d", rdbver);
            return -1;
        }

        while (true)
        {
            expiretime = -1;
            /* Read type. */
            if ((type = RedisReadType()) == -1)
                goto eoferr;
            if (type == REDIS_RDB_OPCODE_EXPIRETIME)
            {
                if ((expiretime = RedisReadTime()) == -1)
                    goto eoferr;
                /* We read the time so we need to read the object type again. */
                if ((type = RedisReadType()) == -1)
                    goto eoferr;
                /* the EXPIRETIME opcode specifies time in seconds, so convert
                 * into milliseconds. */
                expiretime *= 1000;
            }
            else if (type == REDIS_RDB_OPCODE_EXPIRETIME_MS)
            {
                /* Milliseconds precision expire times introduced with RDB
                 * version 3. */
                if ((expiretime = RedisReadMillisecondTime()) == -1)
                    goto eoferr;
                /* We read the time so we need to read the object type again. */
                if ((type = RedisReadType()) == -1)
                    goto eoferr;
            }

            if (type == REDIS_RDB_OPCODE_EOF)
                break;

            /* Handle SELECT DB opcode as a special case */
            if (type == REDIS_RDB_OPCODE_SELECTDB)
            {
                if ((m_status.current_db = RedisReadLen(NULL)) == REDIS_RDB_LENERR)
                {
                    ERROR_LOG("Failed to read current DBID.");
                    goto eoferr;
                }
                continue;
            }
            //load key, object

            if (!RedisReadString(key))
            {
                ERROR_LOG("Failed to read current key.");
                goto eoferr;
            }
            g_db->GetKVStore().Del(m_status.current_db, key);
            if (!RedisLoadObject(type, key))
            {
                ERROR_LOG("Failed to load object:%d", type);
                goto eoferr;
            }
            if (-1 != expiretime)
            {
                g_db->GetKVStore().PExpire(m_status.current_db, key, expiretime);
                //m_db->Pexpireat(m_current_db, key, expiretime);
            }
        }

        /* Verify the checksum if RDB version is >= 5 */
        if (rdbver >= 5)
        {
            uint64_t cksum, expected = m_status.cksm;
            if (!Read(&cksum, 8))
            {
                goto eoferr;
            }memrev64ifbe(&cksum);
            if (cksum == 0)
            {
                WARN_LOG("RDB file was saved with checksum disabled: no check performed.");
            }
            else if (cksum != expected)
            {
                ERROR_LOG("Wrong RDB checksum. Aborting now(%llu-%llu)", cksum, expected);
                exit(1);
            }
        }
        Close();
        INFO_LOG("Redis dump file load finished.");
        return 0;
        eoferr: Close();
        WARN_LOG("Short read or OOM loading DB. Unrecoverable error, aborting now.");
        return -1;
    }

    void Snapshot::RedisWriteMagicHeader()
    {
        char magic[10];
        snprintf(magic, sizeof(magic), "REDIS%04d", REDIS_RDB_VERSION);
        Write(magic, 9);
    }

    int Snapshot::RedisWriteType(uint8 type)
    {
        return Write(&type, 1);
    }

    int Snapshot::RedisWriteKeyType(mmkv::ObjectType type)
    {
        switch (type)
        {
            case mmkv::V_TYPE_STRING:
            {
                return RedisWriteType(REDIS_RDB_TYPE_STRING);
            }
            case mmkv::V_TYPE_SET:
            {
                return RedisWriteType(REDIS_RDB_TYPE_SET);
            }
            case mmkv::V_TYPE_LIST:
            {
                return RedisWriteType(REDIS_RDB_TYPE_LIST);
            }
            case mmkv::V_TYPE_ZSET:
            {
                return RedisWriteType(REDIS_RDB_TYPE_ZSET);
            }
            case mmkv::V_TYPE_HASH:
            {
                return RedisWriteType(REDIS_RDB_TYPE_HASH);
            }
            default:
            {
                return -1;
            }
        }
    }

    int Snapshot::RedisWriteDouble(double val)
    {
        unsigned char buf[128];
        int len;

        if (std::isnan(val))
        {
            buf[0] = 253;
            len = 1;
        }
        else if (!std::isfinite(val))
        {
            len = 1;
            buf[0] = (val < 0) ? 255 : 254;
        }
        else
        {
#if (DBL_MANT_DIG >= 52) && (LLONG_MAX == 0x7fffffffffffffffLL)
            /* Check if the float is in a safe range to be casted into a
             * long long. We are assuming that long long is 64 bit here.
             * Also we are assuming that there are no implementations around where
             * double has precision < 52 bit.
             *
             * Under this assumptions we test if a double is inside an interval
             * where casting to long long is safe. Then using two castings we
             * make sure the decimal part is zero. If all this is true we use
             * integer printing function that is much faster. */
            double min = -4503599627370495LL; /* (2^52)-1 */
            double max = 4503599627370496LL; /* -(2^52) */
            if (val > min && val < max && val == ((double) ((long long) val)))
            {
                ll2string((char*) buf + 1, sizeof(buf), (long long) val);
            }
            else
#endif
                snprintf((char*) buf + 1, sizeof(buf) - 1, "%.17g", val);
            buf[0] = strlen((char*) buf + 1);
            len = buf[0] + 1;
        }
        return Write(buf, len);

    }

    int Snapshot::RedisWriteMillisecondTime(uint64 ts)
    {
        return Write(&ts, 8);
    }

    int Snapshot::RedisWriteTime(time_t t)
    {
        int32_t t32 = (int32_t) t;
        return Write(&t32, 4);
    }

    static int EncodeInteger(long long value, unsigned char *enc)
    {
        /* Finally check if it fits in our ranges */
        if (value >= -(1 << 7) && value <= (1 << 7) - 1)
        {
            enc[0] = (REDIS_RDB_ENCVAL << 6) | REDIS_RDB_ENC_INT8;
            enc[1] = value & 0xFF;
            return 2;
        }
        else if (value >= -(1 << 15) && value <= (1 << 15) - 1)
        {
            enc[0] = (REDIS_RDB_ENCVAL << 6) | REDIS_RDB_ENC_INT16;
            enc[1] = value & 0xFF;
            enc[2] = (value >> 8) & 0xFF;
            return 3;
        }
        else if (value >= -((long long) 1 << 31) && value <= ((long long) 1 << 31) - 1)
        {
            enc[0] = (REDIS_RDB_ENCVAL << 6) | REDIS_RDB_ENC_INT32;
            enc[1] = value & 0xFF;
            enc[2] = (value >> 8) & 0xFF;
            enc[3] = (value >> 16) & 0xFF;
            enc[4] = (value >> 24) & 0xFF;
            return 5;
        }
        else
        {
            return 0;
        }
    }

    /* String objects in the form "2391" "-100" without any space and with a
     * range of values that can fit in an 8, 16 or 32 bit signed value can be
     * encoded as integers to save space */
    static int TryIntegerEncoding(char *s, size_t len, unsigned char *enc)
    {
        long long value;
        char *endptr, buf[32];

        /* Check if it's possible to encode this value as a number */
        value = strtoll(s, &endptr, 10);
        if (endptr[0] != '\0')
            return 0;
        ll2string(buf, 32, value);

        /* If the number converted back into a string is not identical
         * then it's not possible to encode the string as integer */
        if (strlen(buf) != len || memcmp(buf, s, len))
            return 0;

        return EncodeInteger(value, enc);
    }

    /* Like rdbSaveStringObjectRaw() but handle encoded objects */
    int Snapshot::RedisWriteStringObject(const std::string& o)
    {
        /* Avoid to decode the object, then encode it again, if the
         * object is alrady integer encoded. */
//        if (o.encoding == STRING_ENCODING_INT64)
//        {
//            return WriteLongLongAsStringObject(o.value.iv);
//        }
//        else
//        {
//            o.ToString();
//            return WriteRawString(o.RawString(), sdslen(o.RawString()));
//        }
        return RedisWriteRawString(o.data(), o.size());
    }

    /* Save a string objet as [len][data] on disk. If the object is a string
     * representation of an integer value we try to save it in a special form */
    int Snapshot::RedisWriteRawString(const char *s, size_t len)
    {
        int enclen;
        int n, nwritten = 0;

        /* Try integer encoding */
        if (len > 0 && len <= 11)
        {
            unsigned char buf[5];
            if ((enclen = TryIntegerEncoding((char*) s, len, buf)) > 0)
            {
                if (Write(buf, enclen) == -1)
                    return -1;
                return enclen;
            }
        }

        /* Try LZF compression - under 20 bytes it's unable to compress even
         * aaaaaaaaaaaaaaaaaa so skip it */
        if (len > 20)
        {
            n = RedisWriteLzfStringObject(s, len);
            if (n == -1)
                return -1;
            if (n > 0)
                return n;
            /* Return value of 0 means data can't be compressed, save the old way */
        }

        /* Store verbatim */
        if ((n = RedisWriteLen(len)) == -1)
            return -1;
        nwritten += n;
        if (len > 0)
        {
            if (Write(s, len) == -1)
                return -1;
            nwritten += len;
        }
        return nwritten;
    }

    int Snapshot::RedisWriteLzfStringObject(const char *s, size_t len)
    {
        size_t comprlen, outlen;
        unsigned char byte;
        int n, nwritten = 0;
        void *out;

        /* We require at least four bytes compression for this to be worth it */
        if (len <= 4)
            return 0;
        outlen = len - 4;
        if ((out = malloc(outlen + 1)) == NULL)
            return 0;
        comprlen = lzf_compress(s, len, out, outlen);
        if (comprlen == 0)
        {
            free(out);
            return 0;
        }
        /* Data compressed! Let's save it on disk */
        byte = (REDIS_RDB_ENCVAL << 6) | REDIS_RDB_ENC_LZF;
        if ((n = Write(&byte, 1)) == -1)
            goto writeerr;
        nwritten += n;

        if ((n = RedisWriteLen(comprlen)) == -1)
            goto writeerr;
        nwritten += n;

        if ((n = RedisWriteLen(len)) == -1)
            goto writeerr;
        nwritten += n;

        if ((n = Write(out, comprlen)) == -1)
            goto writeerr;
        nwritten += n;

        free(out);
        return nwritten;

        writeerr: free(out);
        return -1;
    }

    /* Save a long long value as either an encoded string or a string. */
    int Snapshot::RedisWriteLongLongAsStringObject(long long value)
    {
        unsigned char buf[32];
        int n, nwritten = 0;
        int enclen = EncodeInteger(value, buf);
        if (enclen > 0)
        {
            return Write(buf, enclen);
        }
        else
        {
            /* Encode as string */
            enclen = ll2string((char*) buf, 32, value);
            if ((n = RedisWriteLen(enclen)) == -1)
                return -1;
            nwritten += n;
            if ((n = Write(buf, enclen)) == -1)
                return -1;
            nwritten += n;
        }
        return nwritten;
    }

    int Snapshot::RedisWriteLen(uint32 len)
    {
        unsigned char buf[2];
        size_t nwritten;

        if (len < (1 << 6))
        {
            /* Save a 6 bit len */
            buf[0] = (len & 0xFF) | (REDIS_RDB_6BITLEN << 6);
            if (Write(buf, 1) == -1)
                return -1;
            nwritten = 1;
        }
        else if (len < (1 << 14))
        {
            /* Save a 14 bit len */
            buf[0] = ((len >> 8) & 0xFF) | (REDIS_RDB_14BITLEN << 6);
            buf[1] = len & 0xFF;
            if (Write(buf, 2) == -1)
                return -1;
            nwritten = 2;
        }
        else
        {
            /* Save a 32 bit len */
            buf[0] = (REDIS_RDB_32BITLEN << 6);
            if (Write(buf, 1) == -1)
                return -1;
            len = htonl(len);
            if (Write(&len, 4) == -1)
                return -1;
            nwritten = 1 + 4;
        }
        return nwritten;
    }

    int Snapshot::RedisSaveAuxField(const std::string& key, const std::string& val)
    {
        if (RedisWriteType(REDIS_RDB_OPCODE_AUX) == -1)
            return -1;
        if (RedisWriteStringObject(key) == -1)
            return -1;
        if (RedisWriteStringObject(val) == -1)
            return -1;
        return 1;
    }

    int Snapshot::RedisSave()
    {
        RedisWriteMagicHeader();

#define DUMP_CHECK_WRITE(x)  if((err = (x)) < 0) break

        mmkv::Iterator* iter = g_db->GetKVStore().NewIterator();
        DBID currentdb = 0;
        bool first_key = true;
        int err = 0;
        if (NULL != iter)
        {
            std::string currentKey;
            std::string element;
            while (iter->Valid())
            {
                if (first_key || (iter->GetDBID() != currentdb))
                {
                    currentdb = iter->GetDBID();
                    currentKey.clear();
                    DUMP_CHECK_WRITE(RedisWriteType(REDIS_RDB_OPCODE_SELECTDB));
                    DUMP_CHECK_WRITE(RedisWriteLen(currentdb));
                    first_key = false;
                }
                iter->GetKey(currentKey);
                uint64 expiretime = iter->GetKeyTTL();
                if (expiretime > 0)
                {
                    DUMP_CHECK_WRITE(RedisWriteType(REDIS_RDB_OPCODE_EXPIRETIME_MS));
                    DUMP_CHECK_WRITE(RedisWriteMillisecondTime(expiretime));
                }
                DUMP_CHECK_WRITE(RedisWriteKeyType(iter->GetValueType()));
                DUMP_CHECK_WRITE(RedisWriteRawString(currentKey.data(), currentKey.size()));
                switch (iter->GetValueType())
                {
                    case mmkv::V_TYPE_STRING:
                    {
                        iter->GetStringValue(element);
                        DUMP_CHECK_WRITE(RedisWriteStringObject(element));
                        break;
                    }
                    case mmkv::V_TYPE_LIST:
                    case mmkv::V_TYPE_SET:
                    {
                        size_t elen = iter->ValueLength();
                        DUMP_CHECK_WRITE(RedisWriteLen(elen));
                        for (size_t i = 0; i < elen; i++)
                        {
                            iter->GetStringValue(element);
                            DUMP_CHECK_WRITE(RedisWriteStringObject(element));
                            iter->NextValueElement();
                        }
                        break;
                    }
                    case mmkv::V_TYPE_ZSET:
                    {
                        size_t elen = iter->ValueLength();
                        DUMP_CHECK_WRITE(RedisWriteLen(elen));
                        long double score;
                        for (size_t i = 0; i < elen; i++)
                        {
                            iter->GetZSetEntry(score, element);
                            DUMP_CHECK_WRITE(RedisWriteStringObject(element));
                            DUMP_CHECK_WRITE(RedisWriteDouble(score));
                            iter->NextValueElement();
                        }
                        break;
                    }
                    case mmkv::V_TYPE_HASH:
                    {
                        size_t elen = iter->ValueLength();
                        DUMP_CHECK_WRITE(RedisWriteLen(elen));
                        std::string field, value;
                        for (size_t i = 0; i < elen; i++)
                        {
                            iter->GetHashEntry(field, value);
                            DUMP_CHECK_WRITE(RedisWriteStringObject(field));
                            DUMP_CHECK_WRITE(RedisWriteStringObject(value));
                            iter->NextValueElement();
                        }
                        break;
                    }
                    default:
                    {
                        WARN_LOG("Invalid type to save in snapshot");
                        break;
                    }
                }
                iter->NextKey();
            }
            DELETE(iter);
        }
        if (err < 0)
        {
            ERROR_LOG("Failed to write dump file for reason code:%d", err);
            Close();
            return -1;
        }
        RedisWriteType(REDIS_RDB_OPCODE_EOF);
        uint64 cksm = m_status.cksm;
        memrev64ifbe(&cksm);
        Write(&cksm, sizeof(cksm));
        Flush();
        Close();
        return 0;
    }
    int Snapshot::MMKVSave(const std::string& file)
    {
        struct SaveTask: public Thread
        {
                std::string path;
                bool done;
                int err;
                SaveTask(const std::string& p) :
                        path(p), done(false), err(0)
                {
                }
                void Run()
                {
                    err = g_db->GetKVStore().Backup(path);
                    done = true;
                }
        };
        SaveTask task(file);
        task.Start();
        while (!task.done)
        {
            Thread::Sleep(100, MILLIS);
            if (NULL != m_status.routine_cb)
            {
                int cbret = m_status.routine_cb(m_status.routine_cbdata);
                if (0 != cbret)
                {
                    return cbret;
                }
                m_status.routine_mills = get_current_epoch_millis();
            }
        }
        return task.err;
    }
    int Snapshot::MMKVLoad(const std::string& file)
    {
        struct LoadTask: public Thread
        {
                std::string path;
                bool done;
                int err;
                LoadTask(const std::string& p) :
                        path(p), done(false), err(0)
                {
                }
                void Run()
                {
                    err = g_db->GetKVStore().Restore(path);
                    done = true;
                }
        };
        LoadTask task(file);
        task.Start();
        while (!task.done)
        {
            Thread::Sleep(100, MILLIS);
            if (NULL != m_status.routine_cb)
            {
                int cbret = m_status.routine_cb(m_status.routine_cbdata);
                if (0 != cbret)
                {
                    return cbret;
                }
                m_status.routine_mills = get_current_epoch_millis();
            }
        }
        return task.err;
    }
}


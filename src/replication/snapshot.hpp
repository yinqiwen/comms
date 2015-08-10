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

#ifndef SNAPSHOT_HPP_
#define SNAPSHOT_HPP_
#include <string>
#include "common.hpp"
#include "buffer/buffer_helper.hpp"

namespace comms
{
    typedef int SnapshotRoutine(void* cb);

    struct SnapshotStatus
    {
            FILE* fp;
            std::string file_path;
            bool open_readonly;
            DBID current_db;
            uint64 processed_bytes;
            uint64 routine_mills;
            uint64 cksm;
            SnapshotRoutine* routine_cb;
            void * routine_cbdata;
            void Clear()
            {
                fp = NULL;
                file_path.clear();
                open_readonly = false;
                current_db = 0;
                processed_bytes = 0;
                routine_mills = 0;
                routine_cb = NULL;
                routine_cbdata = NULL;
                cksm = 0;
            }
    };
    enum SnapshotType{
        REDIS_SNAPSHOT = 0,
        MMKV_SNAPSHOT
    };
    class Snapshot
    {
        protected:
            SnapshotStatus m_status;
            int RedisReadType();
            time_t RedisReadTime();
            int64 RedisReadMillisecondTime();
            uint32_t RedisReadLen(int *isencoded);
            bool RedisReadInteger(int enctype, int64& v);
            bool RedisReadLzfStringObject(std::string& str);
            bool RedisReadString(std::string& str);
            int RedisReadDoubleValue(double&val);
            bool RedisLoadObject(int type, const std::string& key);

            void RedisLoadListZipList(unsigned char* data, const std::string& key);
            void RedisLoadHashZipList(unsigned char* data, const std::string& key);
            void RedisLoadZSetZipList(unsigned char* data, const std::string& key);
            void RedisLoadSetIntSet(unsigned char* data, const std::string& key);

            void RedisWriteMagicHeader();
            int RedisWriteType(uint8 type);
            int RedisWriteKeyType(mmkv::ObjectType type);
            int RedisWriteLen(uint32 len);
            int RedisWriteMillisecondTime(uint64 ts);
            int RedisWriteDouble(double v);
            int RedisWriteLongLongAsStringObject(long long value);
            int RedisWriteRawString(const char *s, size_t len);
            int RedisWriteLzfStringObject(const char *s, size_t len);
            int RedisWriteTime(time_t t);
            int RedisWriteStringObject(const std::string& o);
            int RedisSaveAuxField(const std::string& key, const std::string& val);

            int RedisLoad();
            int RedisSave();
            int MMKVSave(const std::string& file);
            int MMKVLoad(const std::string& file);
            bool Read(void* buf, size_t buflen, bool cksm = true);
            int Rename(const std::string& file_name);
        public:
            Snapshot();
            const std::string& GetPath()
            {
                return m_status.file_path;
            }
            int Write(const void* buf, size_t buflen);
            int Open(const std::string& file, bool read_only);
            int OpenReadFile(const std::string& file);
            int Load(const std::string& file, SnapshotRoutine* cb, void *data);
            int LoadDefault(SnapshotType type, SnapshotRoutine* cb, void *data);
            int Reload(SnapshotRoutine* cb, void *data);
            int Save(bool force, SnapshotType type, SnapshotRoutine* cb, void *data);

            void Flush();
            void Remove();
            int RenameDefault();
            void Close();
            /*
             * return: -1:error 0:not redis dump 1:redis dump file
             */
            static int IsRedisSnapshot(const std::string& file);
            static int BGSave(SnapshotType type);
            static bool IsSaving(SnapshotType type);
            static uint32 LastSave();
            static std::string DefaultPath(SnapshotType type);
            static void UpdateSnapshotOffsetCksm(SnapshotType type, uint64 offset, uint64 cksm);
            static uint64 SnapshotOffset(SnapshotType type);
            static uint64 SnapshotCksm(SnapshotType type);
            ~Snapshot();
    };
}

#endif /* RDB_HPP_ */

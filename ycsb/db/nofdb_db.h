#ifndef YCSB_C_NOFDB_DB_H_
#define YCSB_C_NOFDB_DB_H_

#include "core/db.h"
#include "core/properties.h"
#include <iostream>
#include <string>
#include <cstdint>
#include <memory>

#include "../../rocksdb/db/nofdb/nofdb_config_default.h"
#include "../../rpc/include/nofdb_client.h"

namespace ycsbc
{
    class NoFDB : public DB
    {
    public:
        // 从配置文件获取心跳端口地址
        NoFDB();

        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields, std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key, int len,
                 const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result);

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Delete(const std::string &table, const std::string &key);

        // @CS0522
        int Special(const std::string &command);

        ~NoFDB();

    private:
        NoFDBClient *db_;
    };

}

#endif
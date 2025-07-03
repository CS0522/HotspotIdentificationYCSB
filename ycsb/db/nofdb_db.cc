#include "nofdb_db.h"
#include "../../rpc/include/rpc_utils.h"
#include <vector>
#include <sstream>
#include <regex>

using namespace std;

namespace ycsbc
{
    // 这里传入的是心跳端口地址。从配置文件读取
    NoFDB::NoFDB()
    {
        std::vector<std::string> heartbeat_address_list;

        // 获取心跳端口，以及所有服务器的 IP 地址
        uint16_t heartbeat_port = ROCKSDB_NAMESPACE::NoFDBConfig::heartbeat_port;
        std::string server_addrs(ROCKSDB_NAMESPACE::NoFDBConfig::server_addrs);
        auto node_id_tr_pairs = split_server_addrs(server_addrs);
        std::regex pattern(R"(traddr:([\d.]+))");
        for (size_t i = 0; i < node_id_tr_pairs.size(); i++)
        {
            std::smatch matches;
            if (std::regex_search(node_id_tr_pairs[i].second, matches, pattern) && matches.size() > 1)
            {
                heartbeat_address_list.push_back(matches[1].str() + ":" + std::to_string(heartbeat_port));
            }
            else
            {
                NOFDB_ERROR("Failed to get IP from trinfo\n");
                exit(EXIT_FAILURE);
            }
        }

        // output
        std::cout << "Heartbeat port addresses read from NoFDBConfig: " << std::endl;
        for (int i = 0; i < heartbeat_address_list.size(); i++)
        {
            std::cout << "    Node-" << i << ": " << heartbeat_address_list[i] << std::endl;
        }

        db_ = new NoFDBClient(heartbeat_address_list);
    }

    int NoFDB::Read(const std::string &table, const std::string &key,
                      const std::vector<std::string> *fields,
                      std::vector<KVPair> &result)
    {
        string value;
        if (db_->Get(key, value))
        {
            cerr << "read error" << endl;
            exit(0);
        }
        result.emplace_back(key, value);
        return DB::kOK;
    }

    int NoFDB::Scan(const std::string &table, const std::string &key, int len,
                      const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result)
    {
        db_->Scan(key, len);
        return DB::kOK;
    }

    int NoFDB::Update(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values)
    {
        return Insert(table, key, values);
    }

    int NoFDB::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values)
    {
        for (KVPair &p : values)
        {
            // TODO: 这里 Batch 的逻辑最好直接实现在客户端，防止一次大量修改多次调用 rpc
            if (db_->Put(key, p.second))
            {
                cerr << "insert error" << endl;
                exit(0);
            }
        }
        return DB::kOK;
    }

    int NoFDB::Delete(const std::string &table, const std::string &key)
    {
        if (db_->Delete(key))
        {
            cerr << "Delete error" << endl;
            exit(0);
        }
        return DB::kOK;
    }

    int NoFDB::Special(const std::string &command)
    {
        if (db_->Special(command))
        {
            std::cerr << "Send special command error" << std::endl;
            exit(0);
        }
        return DB::kOK;
    }

    NoFDB::~NoFDB()
    {
        delete db_;
    }

}
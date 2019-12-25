/*
 * @Company: TWL
 * @Author: xue jian
 * @Email: xuejian@kanzhun.com
 * @Date: 2019-12-24 18:28:27
 */
#include <sw/redis++/redis++.h>
#include <fstream>
#include <unordered_map>
#include <sstream>
#include <string>
#include <vector>
#include <memory>
#include <iostream>
using namespace sw::redis;

int main() {
    std::string redis_address("172.18.40.3:6479,172.18.40.6:6479,172.18.40.5:6479,172.18.40.4:6479,172.18.40.2:6479,172.18.4.152:6479,172.18.4.149:6479,172.18.4.151:6479,172.18.4.153:6479,172.18.4.150:6479,172.18.40.11:6479,172.18.40.8:6479,172.18.40.9:6479,172.18.40.10:6479,172.18.40.7:6479,172.18.4.157:6479,172.18.4.155:6479,172.18.4.154:6479,172.18.4.156:6479,172.18.4.158:6479,172.18.40.13:6479,172.18.40.16:6479,172.18.40.14:6479,172.18.40.12:6479,172.18.40.15:6479,172.18.4.159:6479,172.18.4.163:6479,172.18.4.160:6479,172.18.4.161:6479,172.18.4.162:6479,172.18.40.20:6479,172.18.40.19:6479,172.18.40.17:6479,172.18.40.18:6479,172.18.40.21:6479,172.18.4.167:6479,172.18.4.168:6479,172.18.4.165:6479,172.18.4.166:6479,172.18.4.164:6479,172.18.40.22:6479,172.18.40.25:6479,172.18.40.24:6479,172.18.40.23:6479,172.18.40.26:6479,172.18.4.169:6479,172.18.4.172:6479,172.18.4.170:6479,172.18.4.173:6479,172.18.4.171:6479,172.18.40.29:6479,172.18.40.31:6479,172.18.40.28:6479,172.18.40.27:6479,172.18.40.30:6479,172.18.4.176:6479,172.18.4.174:6479,172.18.4.175:6479,172.18.4.177:6479,172.18.4.178:6479,172.18.40.33:6479,172.18.40.34:6479,172.18.40.32:6479,172.18.40.35:6479,172.18.40.36:6479,172.18.4.180:6479,172.18.4.183:6479,172.18.4.179:6479,172.18.4.181:6479,172.18.4.182:6479,172.18.40.37:6479,172.18.40.38:6479,172.18.40.41:6479,172.18.40.40:6479,172.18.40.39:6479,172.18.4.186:6479,172.18.4.187:6479,172.18.4.188:6479,172.18.4.184:6479,172.18.4.185:6479,172.18.40.43:6479,172.18.40.42:6479,172.18.40.44:6479,172.18.40.45:6479");
    std::shared_ptr<RedisCluster> redis_cluster;
    ConnectionPoolOptions pool_options;
    pool_options.size = 15;
    
        std::stringstream ss(redis_address);
        std::vector<std::string> ip_ports;
        std::string tmp;
        while(getline(ss, tmp, ',')) {
            ip_ports.push_back(tmp);
        }
        for (auto& ip_port:ip_ports) {
            std::stringstream sip_port(ip_port);
            getline(sip_port, tmp, ':');
            ConnectionOptions connection_options;
            connection_options.host = tmp;//"172.21.32.129";  // Required.
            getline(sip_port, tmp, ':');
            connection_options.port = std::stoi(tmp); // Optional. The default port is 6379.
            std::shared_ptr<RedisCluster> tmp_cluster;
            try {
                tmp_cluster = std::make_shared<RedisCluster>(connection_options, pool_options);
            } catch (...) {
                // std::cout<<ip_port<<std::endl;
                // continue;
            }
            redis_cluster=tmp_cluster;
            break;
        }
    
    
    // auto redis_cluster = RedisCluster("tcp://172.21.32.109:6479");
    std::vector<OptionalString> re;
    std::vector<std::string> codes({"690","691","693","696","692","689","687","695","664"});

    auto guarded_connection = redis_cluster->pool().fetch("baf:20168580");
    Connection* haha = &guarded_connection.connection();
    std::shared_ptr<Connection> tmp_c(haha);
    auto pipe = redis_cluster->pipeline(tmp_c);
    pipe.hmget("baf:20168580", codes.begin(), codes.begin()+2);
    pipe.hmget("baf:20168580", codes.begin()+2, codes.begin()+4);
    pipe.hmget("baf:20168580", codes.begin()+4, codes.begin()+6);
    pipe.hmget("baf:20168580", codes.begin()+6, codes.begin()+8);

    auto reply = pipe.exec();
    for (size_t i(0); i<4; ++i) {
        auto tmp_re = reply.get<std::vector<OptionalString>>(i);
        for (auto it = tmp_re.begin(); it != tmp_re.end(); ++it) {
            re.push_back(*it);
        }
        // re.insert(re.begin(), tmp_re.begin(), tmp_re.end());
    }
    for (auto& r:re) {
        std::cout<<r.value().size()<<std::endl;
    }
}
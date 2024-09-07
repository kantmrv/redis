#include "Server.h"

static Server MainServer;

void handle_connection(int client_fd) {
    Database db = Database();
    BufReader buf = BufReader();
    int recvbytes = 0;

    do {
        recvbytes = recv(client_fd, buf.write_into(), buf.size(), 0);
        RESP* token = resp_deserialization(buf);

        if(token->type == Type::Arrays) {
            Arrays* arr = static_cast<Arrays*>(token);

            std::vector<std::string> args = to_cmd_args(arr->value);
            
            int i;
            for(i=0; i<args.size(); i++) {
                if(args[i]=="ping") {
                    const char* response = "+PONG\r\n";
                    send(client_fd, response, strlen(response), MSG_CONFIRM);
                } else if(args[i]=="echo") {
                    std::string response = resp_serialization(arr->value[++i]);
                    send(client_fd, response.c_str(), response.size(), MSG_CONFIRM);
                } else if(args[i]=="set" && args.size()-i>2) {
                    RESP* k = arr->value[++i];
                    RESP* v = arr->value[++i];
                    db.set(k, v);
                    const char* response = "+OK\r\n";
                    send(client_fd, response, strlen(response), MSG_CONFIRM);
                } else if(args[i]=="get" && args.size()-i>1) {
                    RESP* p_response = db.get(arr->value[++i]);
                    std::string response = resp_serialization(p_response);
                    send(client_fd, response.c_str(), response.size(), MSG_CONFIRM);
                } else if(args[i]=="px" && args.size()-i>1) {
                    RESP* v = arr->value[++i];
                    db.set_expiry(v);
                } else if(args[i]=="info") {
                    std::string response = MainServer.info();
                    send(client_fd, response.c_str(), response.size(), MSG_CONFIRM);
                }
            }
        }
        
        delete token;
    } while(recvbytes > 0);

    close(client_fd);
}

int main(int argc, char** argv) {
    MainServer = Server();

    int i;
    for (i = 0; i < argc; i++) {
        if (strcmp(argv[i], "--port")==0) {
            MainServer.port = std::stoi(argv[++i]);
        } else if (strcmp(argv[i], "--replicaof")==0) {
            MainServer.role = Role::Slave;
        }
    }


    sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(MainServer.port);

    int reuse = 1;
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
        return 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) != 0)
        return 1;
    if (bind(server_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) != 0)
        return 1;
    if (listen(server_fd, 5) != 0)
        return 1;


    std::vector<std::thread> threads;
    while(true) {
        sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        int client_fd = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_addr_len);
        threads.emplace_back(handle_connection, client_fd);
    }


    close(server_fd);
    return 0;
}

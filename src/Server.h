#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <thread>
#include <pthread.h>
#include <cstring>
#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include <array>
#include <iterator>
#include <numeric>
#include <sstream>
#include <memory>
#include <cassert>
#include <chrono>
#include <functional>



enum class Type {
    SimpleStrings,
    SimpleErrors,
    Integers,
    BulkStrings,
    Arrays,
    Nulls,
    Booleans,
    Doubles,
    BigNumbers,
    BulkErrors,
    VerbatimStrings,
    Maps,
    Sets,
    Pushes
};

class RESP {
public:
    Type type;
};

class SimpleStrings : public RESP {
public:
    std::string value;
    
    SimpleStrings(std::string value) {
        RESP::type = Type::SimpleStrings;
        SimpleStrings::value = value;
    };
};
class SimpleErrors : public RESP {
public:
    std::string value;

    SimpleErrors(std::string value) {
        RESP::type = Type::SimpleErrors;
        SimpleErrors::value = value;
    };
};
class Integers : public RESP {
public:
    int value;

    Integers(int value) {
        RESP::type = Type::Integers;
        Integers::value = value;
    };
};
class BulkStrings : public RESP {
public:
    int length;
    std::string value;

    BulkStrings(int length, std::string value) {
        assert(length==value.size());
        RESP::type = Type::BulkStrings;
        BulkStrings::length = length;
        BulkStrings::value = value;
    };
};
class Arrays : public RESP {
public:
    int length;
    std::vector<RESP*> value;

    Arrays(int length, std::vector<RESP*> value) {
        assert(length==value.size());
        RESP::type = Type::Arrays;
        Arrays::length = length;
        Arrays::value = value;
    };
};
enum class NullsEncoding {
    Nulls,
    NullBulkstrings,
    NullArrays
};
class Nulls : public RESP {
public:
    NullsEncoding encoding;

    Nulls(NullsEncoding encoding) {
        RESP::type = Type::Nulls;
        Nulls::encoding = encoding;
    }
};
class Booleans : public RESP {
public:
    bool value;

    Booleans(bool value) {
        RESP::type = Type::Booleans;
        Booleans::value = value;
    };
};
class Doubles : public RESP {
public:
    double value;

    Doubles(double value) {
        RESP::type = Type::Doubles;
        Doubles::value = value;
    };
};
class BigNumbers : public RESP {
public:
    int64_t value;

    BigNumbers(int64_t value) {
        RESP::type = Type::BigNumbers;
        BigNumbers::value = value;
    }
};
class BulkErrors : public RESP {
public:
    int length;
    std::string value;

    BulkErrors(int length, std::string value) {
        assert(length==value.size());
        RESP::type = Type::BulkErrors;
        BulkErrors::length = length;
        BulkErrors::value = value;
    };
};
class VerbatimStrings : public RESP {
public:
    int length;
    char encoding[3];
    std::string value;

    VerbatimStrings(int length, const char* encoding, std::string value) {
        assert(length-4==value.size() && strlen(encoding)==3);
        RESP::type = Type::VerbatimStrings;
        VerbatimStrings::length = length;
        VerbatimStrings::encoding[0] = encoding[0];
        VerbatimStrings::encoding[1] = encoding[1];
        VerbatimStrings::encoding[2] = encoding[2];
        VerbatimStrings::value = value;
    };
};
class Maps : public RESP {
public:
    int length;
    std::unordered_map<std::string, std::string> value;

    Maps(std::unordered_map<std::string, std::string> value) {
        RESP::type = Type::Maps;
        Maps::value = value;
    };
};
class Sets : public RESP {
public:
    int length;
    std::unordered_set<std::string> value;

    Sets(std::unordered_set<std::string> value) {
        RESP::type = Type::Sets;
        Sets::value = value;
    };
};
class Pushes : public RESP {
public:
    int length;
    std::vector<RESP*> value;

    Pushes(int length, std::vector<RESP*> value) {
        assert(length==value.size());
        RESP::type = Type::Pushes;
        Pushes::length = length;
        Pushes::value = value;
    };
};



// Custom 1024 bytes Buffer Reader
class BufReader {
private:
    std::array<char, 1024UL> _buf;
    ptrdiff_t _cursor;
public:
    explicit BufReader() {
        _buf.fill('\0');
        _cursor = 0;
    }

    constexpr size_t size() const noexcept { return 1024UL; };

    constexpr std::array<char, 1024>* write_into() noexcept {
        _cursor = 0;
        return &_buf;
    };

    void skip(size_t n = 1) {
        _cursor += n;
    }

    char read_char() {
        if(_cursor<1024) {
            return _buf[_cursor++];
        } else {
            return '\0';
        }
    }

    std::string read_line() {
        std::string line;
        bool flag = true;

        for(; _cursor<1024 && flag; _cursor++) {
            switch(_buf[_cursor]) {
                case '\0':
                case '\n':
                    flag = false;
                    break;
                case '\r':
                    break;
                default:
                    line.push_back(_buf[_cursor]);
            }
        }
        
        return line;
    }

    std::string read_exact(ptrdiff_t n) {
        ptrdiff_t length = _cursor+n;
        std::string line;

        for(; _cursor<length && length<=1024; _cursor++)
            line.push_back(_buf[_cursor]);

        return line;
    }
};


class Node {
typedef std::chrono::high_resolution_clock clock;
typedef std::chrono::system_clock::duration time_t;
private:
    time_t _creation_time; 
    time_t _expiry_time;
    RESP* key;
    RESP* value;
public:

    Node(RESP* key, RESP* value) {
        Node::key = key;
        Node::value = value;
        Node::_creation_time = clock::now().time_since_epoch();
        Node::_expiry_time = clock::now().time_since_epoch();
    }

    RESP* get_key() {
        return key;
    }
    RESP* get_value() {
        if( (clock::now().time_since_epoch()-_creation_time) < _expiry_time ) {
            return value;
        } else {
            return new Nulls( NullsEncoding::NullBulkstrings );
        }
    }

    void set_expiry(time_t duration) {
        _expiry_time = duration;
    }
};

// Custom Map for RESP Types
class Database {
private:
    std::vector<Node> _db;
public:
    explicit Database() {
        Database::_db = {};
    }

    void set(RESP* k, RESP* v) {
        _db.push_back(Node(k, v));
    }

    RESP* get(RESP* other_key) {
        for(auto node : _db) {
            if(node.get_key()->type==other_key->type) {
                switch (node.get_key()->type) {
                    case Type::SimpleStrings:
                    {
                        SimpleStrings* p_k = static_cast<SimpleStrings*>(node.get_key());
                        SimpleStrings* p_other_k = static_cast<SimpleStrings*>(other_key);

                        if(p_k->value==p_other_k->value) {
                            return node.get_value();
                        }
                    }
                    case Type::SimpleErrors:
                    {
                        SimpleErrors* p_k = static_cast<SimpleErrors*>(node.get_key());
                        SimpleErrors* p_other_k = static_cast<SimpleErrors*>(other_key);

                        if(p_k->value==p_other_k->value) {
                            return node.get_value();
                        }
                    }
                    case Type::Integers:
                    {
                        Integers* p_k = static_cast<Integers*>(node.get_key());
                        Integers* p_other_k = static_cast<Integers*>(other_key);

                        if(p_k->value==p_other_k->value) {
                            return node.get_value();
                        }
                    }
                    case Type::BulkStrings:
                    {
                        BulkStrings* p_k = static_cast<BulkStrings*>(node.get_key());
                        BulkStrings* p_other_k = static_cast<BulkStrings*>(other_key);

                        if(p_k->value==p_other_k->value) {
                            return node.get_value();
                        }
                    }
                    case Type::Arrays:
                    {
                        SimpleStrings* p_k = static_cast<SimpleStrings*>(node.get_key());
                        SimpleStrings* p_other_k = static_cast<SimpleStrings*>(other_key);

                        if(p_k->value==p_other_k->value) {
                            return node.get_value();
                        }
                    }
                    case Type::Nulls:
                    {
                        return node.get_value();
                    }
                    case Type::Booleans:
                    {
                        Booleans* p_k = static_cast<Booleans*>(node.get_key());
                        Booleans* p_other_k = static_cast<Booleans*>(other_key);

                        if(p_k->value==p_other_k->value) {
                            return node.get_value();
                        }
                    }
                    case Type::Doubles:
                    {
                        Doubles* p_k = static_cast<Doubles*>(node.get_key());
                        Doubles* p_other_k = static_cast<Doubles*>(other_key);

                        if(p_k->value==p_other_k->value) {
                            return node.get_value();
                        }
                    }
                    case Type::BigNumbers:
                    {
                        BigNumbers* p_k = static_cast<BigNumbers*>(node.get_key());
                        BigNumbers* p_other_k = static_cast<BigNumbers*>(other_key);

                        if(p_k->value==p_other_k->value) {
                            return node.get_value();
                        }
                    }
                    case Type::BulkErrors:
                    {
                        BulkErrors* p_k = static_cast<BulkErrors*>(node.get_key());
                        BulkErrors* p_other_k = static_cast<BulkErrors*>(other_key);

                        if(p_k->value==p_other_k->value) {
                            return node.get_value();
                        }
                    }
                    case Type::VerbatimStrings:
                    {
                        VerbatimStrings* p_k = static_cast<VerbatimStrings*>(node.get_key());
                        VerbatimStrings* p_other_k = static_cast<VerbatimStrings*>(other_key);

                        if(p_k->value==p_other_k->value 
                            && p_k->encoding[0]==p_other_k->encoding[0] 
                                && p_k->encoding[1]==p_other_k->encoding[1] 
                                    && p_k->encoding[2]==p_other_k->encoding[2])
                        {
                            return node.get_value();
                        }
                    }
                    case Type::Maps:
                    {
                        Maps* p_k = static_cast<Maps*>(node.get_key());
                        Maps* p_other_k = static_cast<Maps*>(other_key);

                        if(p_k->value==p_other_k->value) {
                            return node.get_value();
                        }
                    }
                    case Type::Sets:
                    {
                        Sets* p_k = static_cast<Sets*>(node.get_key());
                        Sets* p_other_k = static_cast<Sets*>(other_key);

                        if(p_k->value==p_other_k->value) {
                            return node.get_value();
                        }
                    }
                    case Type::Pushes:
                    {
                        Pushes* p_k = static_cast<Pushes*>(node.get_key());
                        Pushes* p_other_k = static_cast<Pushes*>(other_key);

                        if(p_k->value==p_other_k->value) {
                            return node.get_value();
                        }
                    }
                }
            }
        }
        return new Nulls( NullsEncoding::NullBulkstrings );
    }

    void set_expiry(RESP* value) {
        if(value->type==Type::BulkStrings) {
            BulkStrings* p_value = static_cast<BulkStrings*>(value);
            _db.back().set_expiry( std::chrono::system_clock::duration(std::stol(p_value->value)*1000000));
        }
    }
};



std::string resp_serialization(RESP* token) {
    switch (token->type) {
        case Type::SimpleStrings:
        {
            SimpleStrings* p_token = static_cast<SimpleStrings*>(token);
            std::stringstream ss;
            ss << "+" << p_token->value << "\r\n";

            return ss.str();
        }
        case Type::SimpleErrors:
        {
            SimpleErrors* p_token = static_cast<SimpleErrors*>(token);
            std::stringstream ss;
            ss << "-" << p_token->value << "\r\n";

            return ss.str();
        }
        case Type::Integers:
        {
            Integers* p_token = static_cast<Integers*>(token);
            std::stringstream ss;
            ss << ":" << p_token->value << "\r\n";

            return ss.str();
        }
        case Type::BulkStrings:
        {
            BulkStrings* p_token = static_cast<BulkStrings*>(token);
            std::stringstream ss;
            ss << "$" << p_token->length << "\r\n" << p_token->value << "\r\n";

            return ss.str();
        }
        case Type::Arrays:
        {
            Arrays* p_token = static_cast<Arrays*>(token);
            std::stringstream ss;
            ss << "*" << p_token->length << "\r\n";
            for(RESP* t : p_token->value)
                ss << resp_serialization(t);

            return ss.str();
        }
        case Type::Nulls:
        {
            Nulls* p_token = static_cast<Nulls*>(token);
            std::stringstream ss;
            switch(p_token->encoding) {
                case NullsEncoding::Nulls:
                    ss << "_";
                    break;
                case NullsEncoding::NullBulkstrings:
                    ss << "$-1";
                    break;
                case NullsEncoding::NullArrays:
                    ss << "*-1";
                    break;
            }
            ss<< "\r\n";

            return ss.str();
        }
        case Type::Booleans:
        {
            Booleans* p_token = static_cast<Booleans*>(token);
            std::stringstream ss;
            ss << "#" << (p_token->value ? "t" : "f") << "\r\n";

            return ss.str();
        }
        case Type::Doubles:
        {
            Doubles* p_token = static_cast<Doubles*>(token);
            std::stringstream ss;
            ss << "," << p_token->value << "\r\n";

            return ss.str();
        }
        case Type::BigNumbers:
        {
            BigNumbers* p_token = static_cast<BigNumbers*>(token);
            std::stringstream ss;
            ss << "(" << p_token->value << "\r\n";

            return ss.str();
        }
        case Type::BulkErrors:
        {
            BulkErrors* p_token = static_cast<BulkErrors*>(token);
            std::stringstream ss;
            ss << "!" << p_token->length << "\r\n" << p_token->value << "\r\n";

            return ss.str();
        }
        case Type::VerbatimStrings:
        {
            VerbatimStrings* p_token = static_cast<VerbatimStrings*>(token);
            std::stringstream ss;
            ss << "=" << p_token->length << "\r\n" << p_token->encoding << ":" << p_token->value << "\r\n";

            return ss.str();
        }
        case Type::Maps:
        {
            Maps* p_token = static_cast<Maps*>(token);
            std::stringstream ss;
            ss << "%" << p_token->length << "\r\n";
            for(const auto& [k, v] : p_token->value)
                ss << k << v;

            return ss.str();
        }
        case Type::Sets:
        {
            Sets* p_token = static_cast<Sets*>(token);
            std::stringstream ss;
            ss << "~" << p_token->length << "\r\n";
            for(const auto& t : p_token->value)
                ss << t;

            return ss.str();
        }
        case Type::Pushes:
        {
            Pushes* p_token = static_cast<Pushes*>(token);
            std::stringstream ss;
            ss << ">" << p_token->length << "\r\n";
            for(RESP* t : p_token->value)
                ss << resp_serialization(t);

            return ss.str();
        }
        default:
        {
            return std::string();
        }
    }
}

RESP* resp_deserialization(BufReader& buf) {
    switch(buf.read_char()) {
        case '+':
        {    
            return new SimpleStrings( buf.read_line() );
        }
        case '-':
        {
            return new SimpleErrors( buf.read_line() );
        }
        case ':':
        {
            return new Integers( std::stoi(buf.read_line()) );
        }
        case '$':
        {
            int length = std::stoi(buf.read_line());
            if(length >= 0) {
                return new BulkStrings( length, buf.read_line() );
            } else {
                return new Nulls( NullsEncoding::NullBulkstrings );
            }
        }
        case '*':
        {
            int length = std::stoi(buf.read_line());
            if(length >= 0) {
                std::vector<RESP*> value(length);
                for(int i=0; i<length; i++)
                    value[i] = resp_deserialization(buf);

                return new Arrays( length, value );
            } else {
                return new Nulls( NullsEncoding::NullArrays );
            }
        }
        case '_':
        {
            buf.skip(2);
            
            return new Nulls( NullsEncoding::Nulls );
        }
        case '#':
        {
            bool value = (buf.read_char()=='t');
            buf.skip(2);

            return new Booleans( value );
        }
        case ',':
        {
            return new Doubles( std::stod(buf.read_line()) );
        }
        case '(':
        {
            return new BigNumbers( std::stol(buf.read_line()) );
        }
        case '!':
        {
            int length = std::stoi(buf.read_line());
            if(length >= 0) {
                return new BulkErrors( length, buf.read_line() );
            }
        }
        case '=':
        {
            int length = std::stoi(buf.read_line());
            if(length >= 3) {
                std::string encoding = buf.read_exact(3);
                buf.skip(1);

                return new VerbatimStrings( length, encoding.c_str(), buf.read_line() );
            }
        }
        case '%':
        {
            int length = std::stoi(buf.read_line());
            if(length >= 0) {
                std::unordered_map<std::string, std::string> value;
                for(int i=0; i<length; i++) {
                    RESP* k = resp_deserialization(buf);
                    RESP* v = resp_deserialization(buf);
                    value[resp_serialization(k)] = resp_serialization(v);
                    delete k;
                    delete v;
                }

                return new Maps( value );
            }
        }
        case '~':
        {
            int length = std::stoi(buf.read_line());
            if(length >= 0) {
                std::unordered_set<std::string> value;
                for(int i=0; i<length; i++) {
                    RESP* token = resp_deserialization(buf);
                    value.insert(resp_serialization(token));
                    delete token;
                }

                return new Sets( value );
            }
        }
        case '>':
        {
            int length = std::stoi(buf.read_line());
            if(length >= 0) {
                std::vector<RESP*> value(length);
                for(int i=0; i<length; i++)
                    value[i] = resp_deserialization(buf);

                return new Pushes( length, value );
            }
        }
        default:
        {
            return new Nulls( NullsEncoding::Nulls );
        }
    }
}

std::vector<std::string> to_cmd_args(std::vector<RESP*> token) {
    std::vector<std::string> args;
    for(auto t : token) {
        if(t->type==Type::BulkStrings) {
            BulkStrings* p_cmd = static_cast<BulkStrings*>(t);
            std::string cmd = std::accumulate(p_cmd->value.begin(), p_cmd->value.end(), std::string(), 
                                                    [](std::string s, char c) { return s + (char)std::tolower(c); } );
            args.push_back(cmd);
        }
    }

    return args;
}

enum class Role {
    Master,
    Slave
};
class Server {
public:
    uint16_t port;
    Role role;
    char master_replid[40];
    int master_repl_offset;

    Server(uint16_t port = 6379, Role role = Role::Master) {
        Server::port = port;
        Server::role = role;
        char id[40] = {'8', '3', '7', '1', 'b', '4', 'f', 'b', '1', '1', '5', '5', 'b', '7', '1', 'f', '4', 'a', '0', '4', 'd', '3', 'e', '1', 'b', 'c', '3', 'e', '1', '8', 'c', '4', 'a', '9', '9', '0', 'a', 'e', 'e', 'b'};
        for(int i=0; i<40; i++) {
            Server::master_replid[i] = id[i];
        }
        Server::master_repl_offset = 0;
    }

    std::string info() {
        std::stringstream ss;
        if(role==Role::Master) {
            ss << "$" << "89" << "\r\n" << "role" << ":" << "master" << "\r\n" << "master_replid" << ":" << master_replid << "\r\n" << "master_repl_offset" << ":" << master_repl_offset << "\r\n"; 
        } else {
            ss << "$" << "10" << "\r\n" << "role" << ":" << "slave" << "\r\n";
        }

        return ss.str();
    }
};

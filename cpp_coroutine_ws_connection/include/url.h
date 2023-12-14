#pragma once

#include <string>
#include <regex>
#include <limits>

#include <boost/asio/ip/tcp.hpp>
#include <boost/algorithm/string/predicate.hpp>

namespace ws_toolkit {

struct SimpleUrl: public boost::less_than_comparable<SimpleUrl>  {
    const std::string protocol;
    // host: host name + domain
    const std::string host;
    const uint16_t port = 0;
    const std::string path;
    const std::string query;
    const std::string fragment;
    mutable std::optional<boost::asio::ip::tcp::resolver::results_type> opt_cached_endpoints;

 public:

    SimpleUrl(const std::string& a_protocol
              , const std::string& a_host, const uint16_t a_port
              , const std::string& a_path
              , const std::string& a_query, const std::string& a_fragment)
              : protocol(a_protocol)
              , host(a_host)
              , port(a_port)
              , path(a_path)
              , query(a_query)
              , fragment(a_fragment) {}

    std::string websocketTarget() const {
        std::string result;
        if (path.empty())
            result = "/";
        else
            result = path;
        if (!query.empty())
            result += "?" + query;
        if (!fragment.empty())
            result += "#" + fragment;
        return result;
    }

    bool operator==(const SimpleUrl &other) const {
        if (this == &other) {
            return true;
        } else {
            return this->protocol == other.protocol
               && this->host == other.host
               && this->port == other.port
               && this->path == other.path
               && this->query == other.query
               && this->fragment == other.fragment;
        }
    }
    bool operator!=(const SimpleUrl& other) const {
        return !(*this == other);
    }
    bool operator<(const SimpleUrl& other) const {
        return this->protocol != other.protocol ? this->protocol < other.protocol
                    : this->host != other.host ? this->host < other.host
                    : this->port != other.port ? this->port < other.port
                    : this->path != other.path ? this->path < other.path
                    : this->query != other.query ? this->query < other.query
                    : this->fragment != other.fragment ? this->fragment < other.fragment
                    : false;
    }
    friend std::ostream& operator<<(std::ostream& os, const SimpleUrl& e) {
        os << e.protocol << "://" << e.host << ':' << e.port << e.path;
        if (!e.query.empty()) {
            os << '?' << e.query;
        }
        if (!e.fragment.empty()) {
            os << '#' << e.fragment;
        }
        return os;
    }

    std::string toString() const {
        std::stringstream ss;
        ss << *this;
        return ss.str();
    }

    static SimpleUrl parse(std::string url) noexcept(false) {
        using boost::algorithm::iequals;
        static auto url_regex = std::regex(
                R"regex((ftp|ws|wss|http|https)://([^/ :]+):?([^/ ]*)(/?[^ #?]*)\x3f?([^ #]*)#?([^ ]*))regex",
                std::regex_constants::icase);
        auto match = std::smatch();
        if (!std::regex_match(url, match, url_regex))
            throw std::invalid_argument("invalid url[" + url + "]");
        std::string protocol = match[1];
        std::string host     = match[2];
        std::string port_str = match[3];
        std::string path     = match[4];
        std::string query    = match[5];
        std::string fragment = match[6];
        uint16_t port;
        if (port_str.empty()) {
            if (iequals(protocol, "ws") || iequals(protocol, "http"))
                port = 80;
            else if (iequals(protocol, "wss") || iequals(protocol, "https"))
                port = 443;
            else
                throw std::invalid_argument("cannot deduce port [" + url + "]");
        } else {
            int _p = std::stoi(port_str);
            if (_p <= 0 || _p >= std::numeric_limits<uint16_t>::max()) {
                throw std::invalid_argument("bad port number [" + url + "]");
            } else {
                port = static_cast<uint16_t>(_p);
            }
        }
        return SimpleUrl(protocol, host, port, path, query, fragment);
    }
    
};

} // namespace 


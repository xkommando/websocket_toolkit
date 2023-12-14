
#pragma once

#include <list>

#include <boost/exception/all.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/error.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/buffer.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/core/buffer_traits.hpp>
#include <boost/beast/core/buffers_range.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/use_awaitable.hpp>

#include "spdlog/spdlog.h"

#include "url.h"


namespace ws_toolkit {

namespace asio = boost::asio;
namespace websocket = boost::beast::websocket;

using error_code = boost::system::error_code;

using TcpLayer = boost::beast::tcp_stream;
using TlsLayer = boost::beast::ssl_stream<TcpLayer>;

using BeastSecureWebSocket = websocket::stream<TlsLayer>;
using BeastInsecureWebSocket = websocket::stream<TcpLayer>;

using LoggerPtr = std::shared_ptr<spdlog::logger>;

template <typename DurationT>
asio::awaitable<error_code> asyncSleep(const DurationT& d, boost::asio::io_context& io_ctx) {
    asio::deadline_timer timer(io_ctx);
    timer.expires_from_now(d);
    error_code ec;
    co_await timer.async_wait(boost::asio::redirect_error(asio::use_awaitable, ec));
    co_return ec;
}

struct WebsocketConfiguration : private boost::noncopyable {
    // socket read buffer
    uint32_t webSocketReadBufferSize = 2 * 1024 * 1024;
    // socket write buffer
    uint32_t webSocketWriteBufferSize = 1 * 1024 * 1024;

    int connection_timeout_sec = 30;
    int idle_timeout_sec = 90;
    
    // automatically pings the server if no data is received > `idle_timeout_sec`
    bool keep_alive_pings = true;
    
    // if negative, do not try to reconnect on error
    int reconnect_delay_sec = 10;

    // server may not implement RFC6455 ping/pong protocol
    // set this value to a positive integer (10~60) and the WebSocketConnection will explicitly ping the server
    int additional_heartbeat_ping_sec = 0;

    std::unique_ptr<SimpleUrl> ptr_url;
    boost::asio::ssl::context tls_ctx;
    boost::beast::http::fields http_headers;

    WebsocketConfiguration(): ptr_url(), tls_ctx(boost::asio::ssl::context::tlsv12_client) {}
};

enum class WSConnectionEventType: char {
    Open = 'O',
    Message = 'M',
    Error = 'E',
    Close = 'C'
};


template<typename WebsocketType>
class WebSocketConnection : private boost::noncopyable {
 public:
 
    using WriteCallBack = std::function<void(const error_code, std::string&, const std::size_t length)>;
    using ReadCallBack = std::function<void(const error_code, boost::beast::flat_buffer&, const std::size_t)>;
    using WsEventCallBack = std::function<void()>;

    WebSocketConnection(uint32_t connection_id
                        , WebsocketConfiguration &conf
                        , asio::io_context &io_ctx
                        , LoggerPtr logger)
            : logger_(logger),
              connection_id_(connection_id),
              io_ctx_(io_ctx),
              config_(conf),
              read_buffer_(conf.webSocketReadBufferSize),
              ptr_ws_(nullptr),
              is_connected_(false),
              is_waiting_for_reconnect_(false),
              set_running_(true),
              is_sending_(false),
              send_delay_timer_(io_ctx),
              // by default callbacks are bind to empty functions (no-operation)
              callback_read_(std::bind(&WebSocketConnection::readCallbackNOP, this,
                                       std::placeholders::_1, std::placeholders::_2, std::placeholders::_3)),
              call_back_open_(std::bind(&WebSocketConnection::wsCallbackNOP, this)),
              call_back_error_(std::bind(&WebSocketConnection::wsCallbackNOP, this)),
              call_back_close_(std::bind(&WebSocketConnection::wsCallbackNOP, this)) {}

    void subscribeRead(const ReadCallBack cb) {
        callback_read_ = cb;
        logger_->trace("conn {}: {} subscribed read", connectionID(), static_cast<const void *>(std::addressof(cb)));
    }

    void subscribeEvent(WSConnectionEventType e, WsEventCallBack cb) {
        if (e == WSConnectionEventType::Open) {
            call_back_open_ = cb;
        } else if (e == WSConnectionEventType::Error) {
            call_back_error_ = cb;
        } else if (e == WSConnectionEventType::Close) {
            call_back_close_ = cb;
        } else {
            throw std::invalid_argument(std::to_string(connectionID()) + " register read callback with subscribeRead");
        }
        logger_->trace("conn {}: {} subscribed event '{}'", connectionID(),
                       static_cast<void *>(std::addressof(cb)), static_cast<char>(e));
    }

    /**
     * @param opt_defer_sec optionally defer connecting to the server. 
     * useful when initiating multiple connections to the same server 
    */
    asio::awaitable<void> start(std::optional<int> opt_defer_sec) {
        if (!config_.ptr_url->opt_cached_endpoints) {
            throw std::runtime_error(" endpoint is not resolved: " + config_.ptr_url->toString());
        }
        if (opt_defer_sec) {
            co_await asyncSleep(boost::posix_time::seconds(*opt_defer_sec), io_ctx_);
        }
        set_running_ = true;
        ptr_ws_ = createConnection();
        error_code connection_error = co_await doConnect();
        if (connection_error)
            co_return;
        is_connected_ = true;
        call_back_open_();
        co_await startReadLoop();
        co_await stop();
        logger_->info("conn {}: exit", connectionID());
    }

    asio::awaitable<void> stop() {
        if (!is_connected_)
            co_return;
        set_running_ = false;
        call_back_close_();
        is_sending_ = false;
        size_t write_queue_length = clearSendQueue();
        logger_->debug("conn {}: write queue cleaned, {} data dropped. disconnecting",
                            connectionID(), write_queue_length);
        error_code ec;
        co_await ptr_ws_->async_close(websocket::close_code::normal, asio::redirect_error(asio::use_awaitable, ec));
        if (ec) [[unlikely]] {
            logger_->error("conn {}: close error: {}", connectionID(), ec.message());
        }
        is_connected_ = false;
        read_buffer_.consume(read_buffer_.size());
        logger_->info("conn {}: disconnected {}", connectionID(), config_.ptr_url->toString());
        ptr_ws_ = nullptr;
    }

    /**
     * data will be stored in an internal FIFO queue and send by the underlying ws stream one by one
     * @param args to construct a std::string object. the ws_connection will take ownership of the data being sent (safe to move)
     * @param delay if not empty, after the previous message is sent, wait for this period of time before sending the message
     */
    template<typename Str>
    void sendNonBlocking(Str &&args, std::optional<boost::posix_time::time_duration> delay, WriteCallBack a_cb) {
        write_queue_.emplace_back(std::forward<Str>(args), delay, a_cb);
        if (!is_sending_) {
            error_code ec;
            startWriteLoop(ec, 0, &writeCallbackNOP);
        }
    }

    static void writeCallbackNOP(const error_code, std::string &data, const std::size_t length) {}

    void readCallbackNOP(const error_code ec, boost::beast::flat_buffer &buf, const std::size_t length) {
        logger_->warn("conn {}: read callback. ec: {}. read {}B", connectionID(), ec.message(), length);
    }
    void wsCallbackNOP() {
        logger_->warn("conn {}: NOP event callback", connectionID());
    }

 protected:

    auto createConnection() requires(std::is_same_v<BeastSecureWebSocket, WebsocketType>) {
        return std::make_unique<BeastSecureWebSocket>(io_ctx_, config_.tls_ctx);
    }
    auto createConnection() requires(std::is_same_v<BeastInsecureWebSocket, WebsocketType>) {
        return std::make_unique<BeastInsecureWebSocket>(io_ctx_);
    }

    asio::awaitable<error_code> doConnect() {
        auto timeout = websocket::stream_base::timeout{
                .handshake_timeout = std::chrono::seconds(config_.connection_timeout_sec),
                .idle_timeout      = std::chrono::seconds(config_.idle_timeout_sec),
                .keep_alive_pings  = config_.keep_alive_pings
        };
        ptr_ws_->set_option(timeout);
        auto &tcp_layer = getTcp();
        logger_->debug("conn {}: start to connect [{}]", connectionID(), config_.ptr_url->toString());
        error_code ec;
        tcp_layer.expires_after(std::chrono::seconds(config_.connection_timeout_sec));
        auto endpoint = co_await tcp_layer.async_connect(*config_.ptr_url->opt_cached_endpoints,
                                                         asio::redirect_error(asio::use_awaitable, ec));
        boost::ignore_unused(endpoint);
        if (ec) {
            logger_->error("conn {}: failed to connect [{}]", connectionID(), config_.ptr_url->toString());
            co_return ec;
        } else {
            logger_->trace("conn {}: tcp connected to [{}]", connectionID(), config_.ptr_url->toString());
        }

        tcp_layer.socket().set_option(asio::socket_base::keep_alive(true));
        tcp_layer.socket().set_option(asio::ip::tcp::no_delay(true));
        tcp_layer.socket().set_option(
                asio::socket_base::receive_buffer_size(config_.webSocketReadBufferSize));
        tcp_layer.socket().set_option(asio::socket_base::send_buffer_size(config_.webSocketWriteBufferSize));

        if constexpr(std::is_same<BeastSecureWebSocket, WebsocketType>::value) {
            TlsLayer &tls_layer = ptr_ws_->next_layer();
            if (!SSL_set_tlsext_host_name(tls_layer.native_handle(), config_.ptr_url->host.c_str())) {
                throw boost::system::system_error(error_code(static_cast<int>(::ERR_get_error()),
                                                                            asio::error::get_ssl_category()));
            }
            tcp_layer.expires_after(std::chrono::seconds(config_.connection_timeout_sec));
            co_await tls_layer.async_handshake(asio::ssl::stream_base::client, asio::redirect_error(asio::use_awaitable, ec));
            if (ec) {
                logger_->error("conn:{}. tls handshake failed: {}", connectionID(), ec.message());
                co_return ec;
            } else {
                logger_->trace("conn:{}. tls handshake completed", connectionID());
            }
        }

        ptr_ws_->set_option(websocket::stream_base::decorator(
                [&](websocket::request_type &req) {
                    for (auto &&field : config_.http_headers)
                        req.insert(field.name(), field.value());
                }));
        websocket::response_type resp;
        co_await ptr_ws_->async_handshake(resp, config_.ptr_url->host, config_.ptr_url->websocketTarget()
                                            , asio::redirect_error(asio::use_awaitable, ec));
        if (ec) {
            logger_->error("conn {}: websocket handshake failed {}: {}", connectionID()
                           , config_.ptr_url->toString(), ec.message());
        } else {
            logger_->trace("conn {}: websocket handshake completed [{}], response: {} [{}] "
                          , connectionID(), config_.ptr_url->toString(), resp.result_int(), resp.body());
            tcp_layer.expires_never();
            is_connected_ = true;
            if (config_.additional_heartbeat_ping_sec > 0) {
                error_code _ec;
                // scheduleNextActivePing(_ec);
                boost::asio::co_spawn(io_ctx_.get_executor()
                                    , std::bind(&WebSocketConnection::scheduleNextActivePing, this)
                                    , boost::asio::detached);
                logger_->info("conn {}: application heartbeat frequency: {} sec",
                              connectionID(), config_.additional_heartbeat_ping_sec);
            }
        }
        co_return ec;
    }

    asio::awaitable <error_code> awaitReconnect() {
        if (is_waiting_for_reconnect_ || !set_running_) {
            error_code ec;
            co_return ec;
        }
        is_waiting_for_reconnect_ = true;
        co_await stop();
        logger_->info("conn {}: schedule to reconnect {} in {} seconds"
                      , connectionID(), config_.ptr_url->toString(), config_.reconnect_delay_sec);
        error_code ec = co_await asyncSleep(boost::posix_time::seconds(config_.reconnect_delay_sec), io_ctx_);
        if (ec) {
            logger_->info("conn {}: unexpected error while waiting for reconnect: {}",
                          connectionID(), ec.message());
            co_return ec;
        }
        if (!set_running_) {
            error_code ok;
            co_return ok;
        }
        ptr_ws_ = createConnection();
        ec = co_await doConnect();
        is_waiting_for_reconnect_ = false;
        co_return ec;
    }

    inline TcpLayer &getTcp() requires(std::is_same_v<BeastSecureWebSocket, WebsocketType>) {
        return ptr_ws_->next_layer().next_layer();
    }
    inline TcpLayer &getTcp() requires(std::is_same_v<BeastInsecureWebSocket, WebsocketType>) {
        return ptr_ws_->next_layer();
    }

    asio::awaitable<void> scheduleNextActivePing() {
        while (set_running_) {
            co_await asyncSleep(boost::posix_time::seconds(config_.additional_heartbeat_ping_sec), io_ctx_);
            if (is_waiting_for_reconnect_ || !is_connected_) [[unlikely]] {
                co_return;
            }
            if (is_sending_) {
                continue;
            }
            error_code ec;
            co_await ptr_ws_->async_ping("", asio::redirect_error(asio::use_awaitable, ec));
            if (ec)  [[unlikely]] {
                logger_->error("conn {}: error when pinging server: {}", connectionID(), ec.message());
            }
        }
    }
    
    size_t clearSendQueue() {
        const size_t sz = write_queue_.size();
        write_queue_.clear();
        send_delay_timer_.cancel();
        return sz;
    }

    void startWriteLoop(const error_code ec, const size_t bytes_transferred, WriteCallBack last_cb) {
        try {
            last_cb(ec, send_buffer_, bytes_transferred);
        } catch (const std::exception& ex1) {
            logger_->error("conn{}: send callback std exception {}"
                           , connectionID(), ex1.what());
        } catch (const boost::exception& ex2) {
            logger_->error("conn{}: send callback boost exception {}"
                           , connectionID(), boost::diagnostic_information(ex2));
        }
        if (ec) {
            size_t w_size = clearSendQueue();
            if (set_running_) {
                logger_->error("conn {}: send error; bytes_transferred {}; error: {}; "
                        "write queue cleared, {}  message dropped; data:{}",
                        connectionID(), bytes_transferred, ec.message(), w_size, send_buffer_);
                call_back_error_();
            }
            is_sending_ = false;
            return;
        }
        if (write_queue_.empty()) {
            send_delay_timer_.cancel();
            is_sending_ = false;
            return;
        }
        is_sending_ = true;
        WriteCallBack callback_with_dat;
        std::optional<boost::posix_time::time_duration> opt_delay;
        std::tie(send_buffer_, opt_delay, callback_with_dat) = std::move(write_queue_.front());
        write_queue_.pop_front();
        if (opt_delay) {
            send_delay_timer_.expires_from_now(*opt_delay);
            send_delay_timer_.async_wait(std::bind(&WebSocketConnection::doSendNow, this
                                                   , callback_with_dat
                                                   , std::placeholders::_1));
        } else {
            doSendNow(callback_with_dat, ec);
        }
    }

    void doSendNow(WriteCallBack current_cb, error_code ec) {
        boost::ignore_unused(ec);
        if (!is_connected_ || !set_running_) [[unlikely]] {
            size_t w_size = clearSendQueue();
            if (w_size > 0) {
                logger_->warn("conn {}: cancel send. write queue cleared, {} message dropped",
                              connectionID(), w_size);
            }
            is_sending_ = false;
            return;
        }
        auto asio_cb = std::bind(&WebSocketConnection::startWriteLoop, this,
                                 std::placeholders::_1,
                                 std::placeholders::_2,
                                 current_cb);
        ptr_ws_->async_write(asio::buffer(send_buffer_), asio_cb);
        logger_->trace("conn {}: sending {}", connectionID(), send_buffer_);
    }

    asio::awaitable <error_code> startReadLoop() {
        while (is_connected_ && set_running_) [[likely]] {
            error_code ec;
            size_t length = co_await ptr_ws_->async_read(read_buffer_, asio::redirect_error(asio::use_awaitable, ec));
            if (ec) [[unlikely]] {
                if (set_running_) {
                    logger_->error("conn {}: read error: {}", connectionID(), ec.message());
                    call_back_error_();
                }
                read_buffer_.consume(read_buffer_.size());
                if (set_running_ && config_.reconnect_delay_sec > 0) {
                    do {
                        ec = co_await awaitReconnect();
                    } while (ec && set_running_);
                    is_connected_ = true;
                    call_back_open_();
                } else {
                    co_return ec;
                }
            }
            callback_read_(ec, read_buffer_, length);
            read_buffer_.consume(read_buffer_.size());
        }
    }
    
    uint32_t connectionID() const noexcept {
        return connection_id_;
    }

 protected:
    
    LoggerPtr logger_;
    uint32_t connection_id_;    
    asio::io_context &io_ctx_;
    WebsocketConfiguration &config_;

    boost::beast::flat_buffer read_buffer_;
    std::unique_ptr<WebsocketType> ptr_ws_;

    bool is_connected_;
    bool is_waiting_for_reconnect_;
    bool set_running_;
    bool is_sending_;
    std::string send_buffer_;
    boost::asio::deadline_timer send_delay_timer_;

    using MessageHolder = std::tuple<std::string, std::optional<boost::posix_time::time_duration>, WriteCallBack>;
    std::list<MessageHolder> write_queue_;

    ReadCallBack callback_read_;
    WsEventCallBack call_back_open_;
    WsEventCallBack call_back_error_;
    WsEventCallBack call_back_close_;
};


} // namespace



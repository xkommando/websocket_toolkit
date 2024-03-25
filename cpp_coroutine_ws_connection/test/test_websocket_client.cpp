

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

#include "url.h"
#include "ws_connection.h"

#include <boost/test/unit_test.hpp>

namespace ws_toolkit::test {

// copy ascii message from the asio buffers to a std::string
void buffers_to_string(const boost::beast::flat_buffer& buffers, std::string& out) {
    out.reserve(boost::asio::buffer_size(buffers.data()));
    for(auto const buffer : boost::beast::buffers_range_ref(buffers.data()))
        out.append(static_cast<char const*>(buffer.data()), buffer.size());
}


BOOST_AUTO_TEST_CASE(test_subscribe) {
    using SecureWebsocket = WebSocketConnection<BeastSecureWebSocket>;
    spdlog::set_level(spdlog::level::trace);

    // shared by all ws connections
    std::string msg_buffer;
    auto write_callback_nop = [](const boost::system::error_code, std::string &data, const std::size_t length) {};
    // on exchange messages, print to console
    auto read_callback = [&msg_buffer](const boost::system::error_code ec, boost::beast::flat_buffer& buf, const std::size_t len) {
            msg_buffer.clear();
            buffers_to_string(buf, msg_buffer);
            std::cout << msg_buffer << std::endl;
    };

    asio::io_context io_context;
    asio::ip::tcp::resolver resolver(io_context);

    WebsocketConfiguration conf_coinbase_spot;
    conf_coinbase_spot.ptr_url = std::make_unique<SimpleUrl>(SimpleUrl::parse("wss://ws-feed.exchange.coinbase.com"));
    conf_coinbase_spot.ptr_url->opt_cached_endpoints = resolver.resolve(conf_coinbase_spot.ptr_url->host
                                                                , std::to_string(conf_coinbase_spot.ptr_url->port));

    // connection ID 0
    SecureWebsocket ws_coinbase(0, conf_coinbase_spot, io_context, spdlog::stdout_color_mt("ws_coinbase"));
    // once connected, send subscription request
    auto subscribe_coinbase = [&ws_coinbase, &write_callback_nop]() {
        auto symbols = {"BTC-USD", "ETH-USD"};
        for(auto& sym : symbols) {
            auto msg = fmt::format(R"(
            {{
            "type": "subscribe",
            "channels": ["ticker_batch"],
            "product_ids": ["{}"]
            }} )", sym);
            ws_coinbase.sendNonBlocking(std::move(msg), boost::posix_time::milliseconds(500), write_callback_nop);
        }
    };
    ws_coinbase.subscribeEvent(WSConnectionEventType::Open, subscribe_coinbase);
    ws_coinbase.subscribeRead(read_callback);

    WebsocketConfiguration conf_kraken_spot;
    conf_kraken_spot.ptr_url = std::make_unique<SimpleUrl>(SimpleUrl::parse("wss://ws.kraken.com"));
    conf_kraken_spot.ptr_url->opt_cached_endpoints = resolver.resolve(conf_kraken_spot.ptr_url->host
                                                                , std::to_string(conf_kraken_spot.ptr_url->port));
    // explicitly setting heartbeat will force the ws connection to ping the server
    conf_kraken_spot.additional_heartbeat_ping_sec = 20;
    // connection ID 1
    SecureWebsocket ws_kraken(1, conf_kraken_spot, io_context, spdlog::stdout_color_mt("ws_kraken"));
    ws_kraken.subscribeEvent(WSConnectionEventType::Open, [&ws_kraken, &write_callback_nop]() {
        auto symbols = {"ETH/USD"};
        for(auto& sym : symbols) {
            auto msg = fmt::format(R"(
            {{
            "event": "subscribe",
            "subscription": {{"name": "trade"}},
            "pair": ["{}"]
            }} )", sym);
            ws_kraken.sendNonBlocking(std::move(msg), boost::posix_time::milliseconds(500), write_callback_nop);
        }
    });

    // two coroutines runs in the same io_conext (which runs in a single thread)
    boost::asio::co_spawn(io_context.get_executor(), ws_coinbase.start(std::nullopt), boost::asio::detached);
    boost::asio::co_spawn(io_context.get_executor(), ws_kraken.start(std::nullopt), boost::asio::detached);

    // the Websocket object uses both callbacks and coroutines
    // callback is used in defering send (line 334 startWriteLoop())
    // coroutines are used for everything else, e.g. line 208 doConnect(), 394 startReadLoop(), 310 schedulePing() 
    
    io_context.run();
}

}

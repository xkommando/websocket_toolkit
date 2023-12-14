

### C++ 20 Async Websocket Client Connection

`ws_connection.h` wraps the [Boost beast websocket](https://www.boost.org/doc/libs/1_83_0/libs/beast/doc/html/beast/using_websocket.html) and provide higher level send/receive APIs to write websocket client applications.
The proactor IO model and c++ coroutine makes it easy to manage multiple network connections in a single thread with minimal callback functions.


#### features:
1. built using C++ 20 stackless coroutine and Boost ASIO.
2. compile time polymorphism using C++ 20 concept.
3. connection and message sending operations can be defered, ie the `WebSocketConnection` automatically queues the messages and send only after a certain `delay`, this is useful when paired with a message throttler.
4. error code rather than exception.

#### build the example
1. g++ >= 11.3, may work on lower versions but not tested
2. Boost >= 1.76, may work on lower versions but not tested.

edit `CMakeLists.txt` and point `BOOST_ROOT` (line 14) to your local lib: `set(BOOST_ROOT "/my_libraries/boost_178/")`

```
mkdir build
cd build
cmake -DBUILD_TESTS=1 ..
make
./test/ws_toolkit_test
```
(note the test code connects to `ws.kraken.com` and `ws-feed.exchange.coinbase.com`, make sure they are not blacklisted in your environment)

#### todo
* read/send functions return completion tokens to the caller, so that the subsequent async operations can be canceled.
* add decompression support

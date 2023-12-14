
### Python STOMP Server

[Simple Text Oriented Messaging Protocol](https://en.wikipedia.org/wiki/Streaming_Text_Oriented_Messaging_Protocol) is a popular application messaging protocol, there are some server/client [implementations](https://stomp.github.io/implementations.html), but it lacks a python STOMP server that is up to date and can be easily integrated/embeded to other python applications.
this code implemented the STOMP 1.2 on top of the python [websockets](https://github.com/python-websockets) package. features:

1. STOMP protocol version 1.2
2. does not support transaction (BEGIN, COMMIT, ABORT)
3. server re-sends messages to the subscriber that requires ACK and when : 1. message is not ACKed in time 2. subscriber sends NACK
4. the server can be embedded to your asio loop
5. the server only handles immutable payload data, ie, for binary message the sender must serialize data to bytes, not memview or bytearray


quick start:
```python
import stomp_broker
import websockets

broker = stomp_broker.Stomp12Broker(stomp_broker.StompConfig(), logging.getLogger('stomp'), asyncio.get_running_loop())

broker.message_handlers['cmd_update_config'].append(handle_config_update)
broker.message_handlers['cmd_query'].append(handle_user_query)

ws_server = await websockets.serve(broker.accept_websocket_connection
                                        , host='0.0.0.0'
                                        , port=self.port
                                        , logging=logging.getLogger('stomp')
                                        , origins=None)

...

await broker.publish_message(destination='status_update', dat='exceesive latency', content_type=stomp_broker.CONTENT_PLAIN_TEXT)

broker.publish_message_async(destination='trade_update', dat='trade shutdown', content_type=stomp_broker.CONTENT_PLAIN_TEXT)

```

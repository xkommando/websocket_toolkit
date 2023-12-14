
from typing import Dict, List, Tuple, Optional, Set, Any, Generator, Callable, cast

import weakref
import dataclasses
import asyncio
import logging
import threading
import uuid
from collections import defaultdict
import time
import io

import websockets
import websockets.frames
import websockets.connection
from websockets.connection import State as WebsocketState
import websockets.exceptions


LibConnection = websockets.WebSocketServerProtocol # type: ignore

HEADER_CONTENT_TYPE = 'content-type'
HEADER_CONTENT_LEN = 'content-length'
HEADER_DESTINATION = 'destination'
HEADER_MSG_ID = 'message-id'
HEADER_SUB = 'subscription'
HEADER_RECEIPT_ID = 'receipt-id'


CONTENT_JSON = 'application/json'
CONTENT_XML = 'application/xml'
CONTENT_JAVASCRIPT = 'application/javascript'
CONTENT_HTML = 'text/html'
CONTENT_PLAIN_TEXT = 'text/plain'
CONTENT_BINARY = 'application/octet-stream'


@dataclasses.dataclass(slots=True, kw_only=True, frozen=True)
class Frame:
    cmd: str
    headers: Dict[str, str]
    body: Optional[bytes | str]
    ack_index: int = 0


def _decode_binary_frame(content: bytes, time_ns: int) -> Optional[Frame]:
    headers = {}
    i = content.find(b'\n')
    if i <= 0:
        return None
    command = content[:i].decode('utf-8')
    i_left = i + 1
    while True:
        i_right = content.find(b'\n', i_left)
        if i_right <= i_left:
            break
        cm = content.find(b':', i_left, i_right)
        if cm <= i_left:
            continue
        header_name = content[i_left:cm].decode('utf-8')
        if header_name not in headers:
            header_val = content[cm + 1:i_right].decode('utf-8')
            headers[header_name] = header_val
        i_left = i_right + 1
    last_char = content[-1]
    if last_char != 0:
        return None
    else:
        if i_left + 1 == len(content) - 1:
            body = None
        else:
            body = content[i_left + 1: len(content) - 1]
        return Frame(cmd=command, headers=headers, body=body, ack_index=time_ns)


def _decode_text_frame(content: str, time_ns: int) -> Optional[Frame]:
    headers = {}
    i = content.find('\n')
    if i <= 0:
        return None
    command = content[:i]
    i_left = i + 1
    while True:
        i_right = content.find('\n', i_left)
        if i_right <= i_left:
            break
        cm = content.find(':', i_left, i_right)
        if cm <= i_left:
            continue
        header_name = content[i_left:cm]
        if header_name not in headers:
            headers[header_name] = content[cm + 1:i_right]
        i_left = i_right + 1
    last_char = content[-1]
    if last_char != '\x00':
        return None
    else:
        if i_left + 1 == len(content) - 1:
            body = None
        else:
            body = content[i_left + 1: len(content) - 1]
        return Frame(cmd=command, headers=headers, body=body, ack_index=time_ns)


def _encode_frame(byte_buf: io.BytesIO, frame: Frame):
    byte_buf.write(frame.cmd.encode())
    byte_buf.write(b'\n')
    for k, v in frame.headers.items():
        byte_buf.write(k.encode())
        byte_buf.write(b':')
        byte_buf.write(v.encode())
        byte_buf.write(b'\n')
    if frame.body and HEADER_CONTENT_LEN not in frame.headers:
        byte_buf.write(b'content-length:')
        byte_buf.write(str(len(frame.body)).encode())
        byte_buf.write(b'\n')

    byte_buf.write(b'\n')
    if frame.body:
        if isinstance(frame.body, str):
            byte_buf.write(frame.body.encode())
        else:
            byte_buf.write(frame.body)
    byte_buf.write(b'\0')


@dataclasses.dataclass(frozen=False, slots=True, kw_only=True)
class _ConnectionInfo:
    """
    broker does not check client heartbeat
    """
    ws_conn: LibConnection
    last_hb_sent_ms: int = 0
    server_hb_send_interval_ms: int = 0


_ACK_AUTO = 0
_ACK_CLIENT = 1
_ACK_INDIVIDUAL = 2


class _SubscriptionSession:
    __slots__ = ('destination', 'ws_conn', 'sub_id', 'ack', '__weakref__')

    def __init__(self, destination: str, sub_id: str, ack: int, conn: LibConnection):
        self.destination = destination
        self.ws_conn = conn
        self.sub_id = sub_id
        self.ack = ack

    def __hash__(self):
        return hash(self.sub_id) ^ hash(self.ws_conn.id)

    def __eq__(self, other):
        if not isinstance(other, _SubscriptionSession):
            return False
        return self.sub_id == other.sub_id and self.ws_conn.id == other.ws_conn.id


@dataclasses.dataclass(frozen=False, kw_only=True, slots=True, eq=False)
class _MessageAckCheckList:
    msg: Frame
    subscribers: weakref.WeakValueDictionary[str, _SubscriptionSession]


class _MessageAckTracker:

    def __init__(self, msg_ttl_ms: int, ack_timeout_ms: int):
        self.check_tasks: List[_MessageAckCheckList] = []
        self.ack_timeout_ms = ack_timeout_ms
        self.msg_ttl_ms = msg_ttl_ms

    def add_check_list(self, msg: Frame, sub_tab: weakref.WeakValueDictionary[str, _SubscriptionSession]):
        if not msg.ack_index:
            return
        self.check_tasks.append(_MessageAckCheckList(msg=msg, subscribers=sub_tab))

    def remove_connection(self, ws_id: int):
        live_tasks = []
        for check_list in self.check_tasks:
            for sub_id in list(check_list.subscribers.keys()):
                if check_list.subscribers[sub_id].ws_conn.id == ws_id:
                    del check_list.subscribers[sub_id]
            if check_list.subscribers:
                live_tasks.append(check_list)
        self.check_tasks = live_tasks

    def _ack_client(self, topic: str, sub_id: str, ack_index: int):
        for check_list in self.check_tasks:
            if check_list.msg.headers[HEADER_DESTINATION] == topic and check_list.msg.ack_index <= ack_index:
                if sub_id in check_list.subscribers:
                    del check_list.subscribers[sub_id]

    def handle_ack(self, ack_id: Optional[str], sub_id: Optional[str], ws_id: str):
        """
        subscriber must provide at least the ack id (in this case also the message id), or the subscriber id
        :param ack_id: message id is also the ACK id used when publishing that message.
            when a subscriber sends the ack id, it will ack one or all previous messages in this topic for this subscriber, this is the standard protocol
        :param sub_id: client provided subscriber id when sending 'SUBSCRIBE', this will ACK all the messages sent to this subscriber, this is not in the standard protocol
        :param ws_id: uuid4 of the underlying ws connection
        """
        if ack_id:
            for check_list in self.check_tasks:
                if check_list.msg.headers[HEADER_MSG_ID] == ack_id:
                    topic = check_list.msg.headers[HEADER_DESTINATION]
                    if sub_id:
                        if sub_id in check_list.subscribers:
                            sub_sess = check_list.subscribers[sub_id]
                            del check_list.subscribers[sub_id]
                            if sub_sess.ack != _ACK_INDIVIDUAL:
                                self._ack_client(topic, sub_id, check_list.msg.ack_index)
                            return
                    else:
                        for sub_sess in check_list.subscribers.values():
                            if sub_sess.ws_conn.id == ws_id:
                                sub_id = sub_sess.sub_id
                                del check_list.subscribers[sub_id]
                                if sub_sess.ack != _ACK_INDIVIDUAL:
                                    self._ack_client(topic, sub_id, check_list.msg.ack_index)
                                return
        elif sub_id:
            for check_list in self.check_tasks:
                if sub_id in check_list.subscribers:
                    del check_list.subscribers[sub_id]

    def find_nack_messages(self, ack_id, sub_id: str) -> List[Frame]:
        """
        ack_id is optional, if provided, only return the message that matches both ack_id and sub_id
        other wise return all the pending ack messages for that sub_id
        :return:
        """
        ret = []
        if ack_id:
            for check_list in self.check_tasks:
                if check_list.msg.headers[HEADER_MSG_ID] == ack_id:
                    if sub_id:
                        if sub_id in check_list.subscribers:
                            ret.append(check_list.msg)
                    else:
                        ret.append(check_list.msg)
        elif sub_id:
            for check_list in self.check_tasks:
                if sub_id in check_list.subscribers:
                    ret.append(check_list.msg)
        return ret

    def gen_ack_timeout_messages(self) -> Generator[_MessageAckCheckList, None, None]:
        current_idx = time.monotonic_ns()
        msg_expired_idx = current_idx - self.msg_ttl_ms * 1000_000
        while self.check_tasks:
            if self.check_tasks[0].msg.ack_index < msg_expired_idx:
                self.check_tasks.pop(0)
            else:
                break
        live_tasks = []
        for check_list in self.check_tasks:
            if check_list.subscribers:
                live_tasks.append(check_list)
        self.check_tasks = live_tasks
        if live_tasks:
            ack_expire_idx = current_idx - self.ack_timeout_ms * 1000_000
            for check_list in self.check_tasks:
                if check_list.msg.ack_index <= ack_expire_idx:
                    yield check_list

    def count_pending_ack(self, sub_id: Optional[str] = None) -> int:
        count = 0
        for check_list in self.check_tasks:
            if sub_id:
                if sub_id in check_list.subscribers:
                    count += 1
            else:
                count += len(check_list.subscribers)
        return count


@dataclasses.dataclass(frozen=True, slots=True, kw_only=True)
class StompConfig:
    # if a client requires the server to send heart beat, and suggests value of hb_c,
    # server heart beat time = max(server_heartbeat_minimal_ms, min(hb_c, server_heartbeat_preference_ms) )
    server_heartbeat_preference_ms: int = 40000
    server_heartbeat_minimal_ms: int = 5000

    # if a message is not ACKed for more than "client_ack_timeout_sec" seconds, server will resent the message
    client_ack_timeout_sec: int = 10
    # if any of the subscribers requires ACK, the published messages will be cached for "message_ttl_sec" seconds and then get deleted.
    # once the message is deleted, server will stop resending the un-ACKed messages
    message_ttl_sec: int = 60

    # when server disconnects with a client, the underlying websocket is not closed immediately
    # instead the server waits for 'connection_lingering_ms' mill-seconds before disconnecting the websocket
    connection_lingering_ms: int = 10


class Stomp12Broker:
    """
    A quick/partial implementation of STOMP 1.2 server (https://stomp.github.io/stomp-specification-1.2.html),
     built on top of the python websockets lib, and can be embedded to your python asyncio application

    features:
    1. only support protocol version 1.2
    2. does not support transaction (BEGIN, COMMIT, ABORT)
    3. server re-sends messages to the subscriber that requires ACK and when : 1. message is not ACKed in time 2. subscriber sends NACK
    4. the server can be embedded to your asio loop, however it is not optimized and not suitable for massive subscriber * intensive messaging
    5. the server only handles immutable payload data, ie, for binary message the sender must serialize data to bytes, not memview or bytearray
    """
    STOMP_VERSION = '1.2'

    def __init__(self, conf: StompConfig
                 , logger: logging.Logger
                 , io_context: asyncio.AbstractEventLoop
                 , authenticate: Optional[Callable[[str, str], bool]] = None):
        """
        :param authenticate: if not provided, all connection requests are granted immediately
        """
        self.config = conf
        self.io_context = io_context
        self.logger = logger
        self.authenticate: Optional[Callable[[str, str], bool]] = authenticate
        self.message_handlers: Dict[str, List[Callable[[Frame, LibConnection], None]]] = defaultdict(list)

        # client connections that need server HB, ws.id -> connectionInfo
        self._hb_connections: Dict[str, _ConnectionInfo] = {}
        # destination -> [id -> subscribers]
        self._subscribers: Dict[str, Dict[str, _SubscriptionSession]] = {}
        self._ack_tracker = _MessageAckTracker(self.config.message_ttl_sec * 1000, self.config.client_ack_timeout_sec * 1000)
        self._write_buf = io.BytesIO()
        self._minimal_hb_time_ms: int = self.config.server_heartbeat_minimal_ms
        self._heart_beat_task: asyncio.Task = self.io_context.create_task(self._send_heartbeats())
        self._ack_tracking_task: asyncio.Task = self.io_context.create_task(self._resend_nack_messages())

    async def accept_websocket_connection(self, ws_conn: LibConnection):
        while True:
            try:
                content = await ws_conn.recv()
            except websockets.exceptions.ConnectionClosed:
                await self.remove_connection(ws_conn, 0)
                return
            except asyncio.CancelledError:
                await self.remove_connection(ws_conn, 0)
                return
            time_ns = time.monotonic_ns()
            if isinstance(content, str):
                f = _decode_text_frame(content, time_ns)
            else:
                f = _decode_binary_frame(content, time_ns)
            if f is None:
                self.logger.warning('bad frame from connection %s: %s', ws_conn.id, str(content))
                await self._do_send_frame(ws_conn, Frame(cmd='ERROR'
                                                         , headers={'message': 'cannot parse stomp frame'}, body=None))
                await self.remove_connection(ws_conn, self.config.connection_lingering_ms)
            else:
                await self._handle_frame(f, ws_conn)

    def close(self):
        self.logger.info('closing stomp broker')
        if self._heart_beat_task:
            self._heart_beat_task.cancel()
            self._heart_beat_task = cast(asyncio.Task, None)
        if self._ack_tracking_task:
            self._ack_tracking_task.cancel()
            self._ack_tracking_task = cast(asyncio.Task, None)
            self._ack_tracker.check_tasks.clear()
        live_conn = {}
        for sub_tab in self._subscribers.values():
            for sub in sub_tab.values():
                live_conn[sub.ws_conn.id] = sub.ws_conn
        for ci in self._hb_connections.values():
            live_conn[ci.ws_conn.id] = ci.ws_conn
        self._subscribers.clear()
        self._hb_connections.clear()

        async def _close_all():
            for ws_conn in live_conn.values():
                if ws_conn.state == WebsocketState.OPEN:
                    await self.remove_connection(ws_conn, 0)
        threading.Thread(target=asyncio.run, args=(_close_all(),)).start()

    async def _handle_frame(self, frame: Frame, ws_conn: LibConnection):
        """
        does not support transaction
        """
        error_msg = None
        try:
            match frame.cmd:
                case 'SEND':
                    error_msg = await self._handle_client_msg(frame, ws_conn)
                case 'ACK':
                    error_msg = await self._handle_client_ack(frame, ws_conn)
                case 'SUBSCRIBE':
                    error_msg = await self._handle_subscribe(frame, ws_conn)
                case 'UNSUBSCRIBE':
                    error_msg = await self._handle_unsubscribe(frame, ws_conn)
                case 'NACK':
                    error_msg = await self._handle_client_nack(frame, ws_conn)
                case 'CONNECT':
                    error_msg = await self._handle_connect(frame, ws_conn)
                case 'DISCONNECT':
                    error_msg = await self._handle_disconnect(frame, ws_conn)
                case _:
                    error_msg = 'unsupported command:' + frame.cmd
        except Exception as e:
            error_msg = str(e)

        if error_msg:
            self.logger.debug('failed to handle %s from %s (ws id:): %s', frame.cmd, ws_conn.remote_address, ws_conn.id, error_msg)
            err_frame = Frame(cmd='ERROR', headers={HEADER_CONTENT_TYPE: CONTENT_PLAIN_TEXT}, body=error_msg)
            try:
                await self._do_send_frame(ws_conn, err_frame)
                await self.remove_connection(ws_conn, 0)
            except websockets.exceptions.WebSocketException:
                pass

    async def _handle_connect(self, frame: Frame, ws_conn: LibConnection) -> Optional[str]:
        """
        because the underlying transport is websocket which comes with heartbeat
        server will send heartbeats if requested
        server will not check client sent heartbeats
        """
        if self.authenticate:
            if 'login' in frame.headers:
                login = frame.headers['login']
                passcode = frame.headers['passcode']
                if not self.authenticate(login, passcode):
                    return 'login failed'
            else:
                return 'need login and passcode'
        client_versions = []
        if 'accept-version' in frame.headers:
            acc_ver = frame.headers['accept-version']
            client_versions.extend([s.strip() for s in acc_ver.split(',')])
        if Stomp12Broker.STOMP_VERSION not in client_versions:
            return 'server only support stomp version ' + Stomp12Broker.STOMP_VERSION

        #  note: reuse websocket connection id (which is uuid4) as the stomp session id
        headers = {'session': str(ws_conn.id), 'version': Stomp12Broker.STOMP_VERSION}
        hb_client_send_ms = 0
        hb_server_send_ms = 0
        if 'heart-beat' in frame.headers:
            beats = frame.headers['heart-beat'].split(',')
            if len(beats) != 2:
                return 'bad headers: unrecognized heartbeat values'
            beat_from_client = int(beats[0])
            beat_from_server = int(beats[1])
            if beat_from_client > 0:
                hb_client_send_ms = beat_from_client
            if beat_from_server > 0:
                hb_server_send_ms = max(self.config.server_heartbeat_minimal_ms, min(self.config.server_heartbeat_preference_ms, beat_from_server))
            if hb_server_send_ms > 0:
                self._hb_connections[ws_conn.id] = _ConnectionInfo(ws_conn=ws_conn
                                                                   , last_hb_sent_ms=0
                                                                   , server_hb_send_interval_ms=hb_server_send_ms)
                self._minimal_hb_time_ms = min(self._minimal_hb_time_ms, hb_server_send_ms)
            if hb_server_send_ms + hb_client_send_ms > 0:
                hb_server_send_ms += 100
                headers['heart-beat'] = str(hb_server_send_ms) + ',' + str(hb_client_send_ms)
        f = Frame(cmd='CONNECTED', headers=headers, body=None)
        await self._do_send_frame(ws_conn, f)
        self.logger.info('client from %s is connected (ws id: %s), client heartbeat:%d, server heartbeat:%d'
                          , ws_conn.remote_address, ws_conn.id, hb_client_send_ms, hb_server_send_ms)
        return None

    async def _handle_subscribe(self, frame: Frame, ws_conn: LibConnection) -> Optional[str]:
        sub_id = frame.headers['id']
        destination = frame.headers[HEADER_DESTINATION]
        ack = 'auto'
        if 'ack' in frame.headers:
            ack = frame.headers['ack']
        if ack == 'auto':
            ack_mode = _ACK_AUTO
        elif ack == 'client':
            ack_mode = _ACK_CLIENT
        elif ack == 'client-individual':
            ack_mode = _ACK_INDIVIDUAL
        else:
            return 'unrecognized ack: ' + ack
        sub = _SubscriptionSession(destination, sub_id, ack_mode, ws_conn)
        if destination not in self._subscribers:
            self._subscribers[destination] = {}
        sub_tab = self._subscribers[destination]
        if sub_id in sub_tab:
            return '[' + sub_id + '] already subscribed to [' + destination + ']'
        sub_tab[sub_id] = sub
        self.logger.debug('[%s] (client from %s, ws id: %s) subscribed to [%s], ack: %s'
                          , sub_id, ws_conn.remote_address, ws_conn.id, destination, ack)
        await self._try_ack_client(ws_conn, frame)
        return None

    async def _handle_unsubscribe(self, frame: Frame, ws_conn: LibConnection) -> Optional[str]:
        sub_topic = None
        sub_id = frame.headers['id']
        for topic, sub_tab in self._subscribers.items():
            if sub_id in sub_tab:
                del sub_tab[sub_id]
                sub_topic = topic
                break
        if sub_topic:
            self.logger.debug('[%s] unsubscribed [%s]', sub_id, sub_topic)
            await self._try_ack_client(ws_conn, frame)
            return None
        else:
            return 'failed to unsubscribe: could not find subscription [' + sub_id + ']'

    async def _handle_client_ack(self, frame: Frame, ws_conn: LibConnection) -> Optional[str]:
        ack_id = ''
        sub_id = ''
        if 'id' in frame.headers:
            ack_id = frame.headers['id']
        elif HEADER_SUB in frame.headers:
            sub_id = frame.headers[HEADER_SUB]
        if len(ack_id) + len(sub_id) <= 0:
            return 'could not find id or subscription'
        self._ack_tracker.handle_ack(ack_id, sub_id, ws_conn.id)

    async def _handle_client_nack(self, frame: Frame, ws_conn: LibConnection) -> Optional[str]:
        """
        if id is provided: will re-send the message with matching id
        if no id is provided, will re-send all cached messages matching the sub_id
        """
        ack_id = ''
        sub_id = ''
        if 'id' in frame.headers:
            ack_id = frame.headers['id']
        elif HEADER_SUB in frame.headers:
            sub_id = frame.headers[HEADER_SUB]
        if len(ack_id) + len(sub_id) <= 0:
            return 'could not find id or subscription'
        self.logger.debug('subscriber[%s] from %s (ws id: %s) sent NACK with id:%s', sub_id, ws_conn.remote_address, ws_conn.id, ack_id)
        frame_ls = self._ack_tracker.find_nack_messages(ack_id, sub_id)
        for f in frame_ls:
            if sub_id:
                f.headers[HEADER_SUB] = sub_id
                topic = f.headers[HEADER_DESTINATION]
                if topic in self._subscribers and sub_id in self._subscribers[topic]:
                    sub_sess = self._subscribers[topic][sub_id]
                    if sub_sess.ack != _ACK_AUTO:
                        f.headers['ack'] = f.headers[HEADER_MSG_ID]
            await self._do_send_frame(ws_conn, f)
            if HEADER_SUB in f.headers:
                del f.headers[HEADER_SUB]

    async def _handle_disconnect(self, frame: Frame, ws_conn: LibConnection) -> Optional[str]:
        """ does not mandate `receipt` header, ie if 'receipt' is not in the DISCONNECT header,
                server will disconnect without sending a RECEIPT frame
        """
        await self._try_ack_client(ws_conn, frame)
        await self.remove_connection(ws_conn, self.config.connection_lingering_ms)
        self.logger.info('client from %s (ws id: ) requested disconnect', ws_conn.remote_address, ws_conn.id)
        return None

    async def _handle_client_msg(self, frame: Frame, ws_conn: LibConnection) -> Optional[str]:
        topic = frame.headers[HEADER_DESTINATION]
        if topic not in self.message_handlers:
            return 'could not find handlers for [' + topic + ']'
        handlers = self.message_handlers[topic]
        handled = False
        for hdl in handlers:
            try:
                hdl(frame, ws_conn)
                handled = True
            except Exception as e:
                self.logger.exception('failed to handle %s', frame, exc_info=e)
        if handled:
            await self._try_ack_client(ws_conn, frame)

    def publish_message_async(self, destination: str, dat: str | bytes, content_type: str):
        self.io_context.create_task(self.publish_message(destination, dat, content_type))

    async def publish_message(self, destination: str, dat: str | bytes, content_type: str) -> int:
        """
        :return: the number of subscribers that the message is sent to
        """
        partial_frame = Frame(cmd='MESSAGE'
                              , headers={HEADER_CONTENT_TYPE: content_type
                                        , HEADER_DESTINATION: destination
                                        , HEADER_MSG_ID: str(uuid.uuid4())}
                                          # to be replaced: subscription
                              , body=dat
                              , ack_index=time.monotonic_ns())
        return await self.publish_frame(partial_frame)

    async def broadcast_message(self, dat: str | bytes, content_type: str):
        """
        send the message to ALL topics synchronously,
        ie one topic by one topic, for each topic send the message to the subscribers one by one,
            the more subscriber the longer it takes
        :return: the number of subscribers that the message is sent to
        """
        sent_count = 0
        for destination in list(self._subscribers.keys()):
            if not self._subscribers[destination]:
                del self._subscribers[destination]
                continue
            partial_frame = Frame(cmd='MESSAGE'
                              , headers={HEADER_CONTENT_TYPE: content_type
                                        , HEADER_DESTINATION: destination
                                        , HEADER_MSG_ID: str(uuid.uuid4())}
                                          # to be replaced: subscription
                              , body=dat
                              , ack_index=time.monotonic_ns())
            sent_count += await self.publish_frame(partial_frame)
        return sent_count

    async def quick_broadcast(self, dat: str | bytes, content_type: str):
        pass

    async def publish_frame(self, f: Frame) -> int:
        """
        publish to all subscribers synchronously, ie send one by one, the more subscriber the longer it takes
        :param f: the same frame will be sent to all its subscribers,
            however the 'subscription' header will be assigned to the sub_id of each subscriber
        :return: the number of subscribers that the message is sent to
        """
        assert f.ack_index > 0
        destination = f.headers[HEADER_DESTINATION]
        if HEADER_MSG_ID not in f.headers:
            f.headers[HEADER_MSG_ID] = str(uuid.uuid4())
        if HEADER_CONTENT_LEN not in f.headers and f.body:
            f.headers[HEADER_CONTENT_LEN] = str(len(f.body))
        send_count = 0
        if destination in self._subscribers:
            sub_tab = self._subscribers[destination]
            if sub_tab.keys() == 0:
                del self._subscribers[destination]
                return 0
            ack_subscribers = weakref.WeakValueDictionary[str, _SubscriptionSession]()
            for sub_id in list(sub_tab.keys()):
                sub = sub_tab[sub_id]
                try:
                    f.headers[HEADER_SUB] = sub_id
                    if sub.ack == _ACK_AUTO:
                        await self._do_send_frame(sub.ws_conn, f)
                    else:
                        f.headers['ack'] = f.headers[HEADER_MSG_ID]
                        await self._do_send_frame(sub.ws_conn, f)
                        del f.headers['ack']
                        ack_subscribers[sub_id] = sub
                except websockets.exceptions.ConnectionClosed:
                    del sub_tab[sub_id]
                    continue
                send_count += 1
            if ack_subscribers:
                self._ack_tracker.add_check_list(f, ack_subscribers)
        return send_count

    async def _try_ack_client(self, ws_conn: LibConnection, frame: Frame):
        if 'receipt' in frame.headers:
            f = Frame(cmd='RECEIPT', headers={HEADER_RECEIPT_ID: frame.headers['receipt']}, body=None)
            await self._do_send_frame(ws_conn, f)

    async def remove_connection(self, ws_conn: LibConnection, delay_ms: int):
        """
        clean up subscriptions and pending ack checks
        and close the ws connection
        """
        if ws_conn.id in self._hb_connections:
            del self._hb_connections[ws_conn.id]
        for sub_tab in self._subscribers.values():
            for sub_id in list(sub_tab.keys()):
                if sub_tab[sub_id].ws_conn.id == ws_conn.id:
                    del sub_tab[sub_id]
        self._ack_tracker.remove_connection(ws_conn.id)
        if ws_conn.state in [WebsocketState.OPEN, WebsocketState.CONNECTING]:
            if delay_ms > 0:
                async def _defer():
                    await asyncio.sleep(delay_ms / 1e3)
                    if ws_conn.state in [WebsocketState.OPEN, WebsocketState.CONNECTING]:
                        await ws_conn.close()
                self.io_context.create_task(_defer())
            else:
                await ws_conn.close()

    async def _do_send_frame(self, ws_conn: LibConnection, f: Frame):
        if ws_conn.state != WebsocketState.OPEN:
            return
        op_code = websockets.frames.OP_TEXT if isinstance(f.body, str) else websockets.frames.OP_BINARY
        self._write_buf.seek(0)
        self._write_buf.truncate()
        _encode_frame(self._write_buf, f)
        await ws_conn.write_frame(fin=True, opcode=op_code, data=self._write_buf.getvalue())

    async def _send_heartbeats(self):
        try:
            while True:
                time_now_ms = int(time.time() * 1e3)
                for ws_id in list(self._hb_connections.keys()):
                    ci = self._hb_connections[ws_id]
                    if time_now_ms - ci.last_hb_sent_ms >= ci.server_hb_send_interval_ms:
                        if ci.ws_conn.state == WebsocketState.OPEN:
                            self.io_context.create_task(ci.ws_conn.ping(b''))
                            ci.last_hb_sent_ms = time_now_ms
                        else:
                            del self._hb_connections[ws_id]
                            await self.remove_connection(ci.ws_conn, 0)
                await asyncio.sleep(self._minimal_hb_time_ms / 1000)
        except asyncio.CancelledError:
            return

    async def _resend_nack_messages(self):
        """
        the protocol does not specify if the server should re-send non-ack messages after the client unsubscribed the topic
        in this implementation, the server won't
        """
        try:
            while True:
                for msg_subs in self._ack_tracker.gen_ack_timeout_messages():
                    f = msg_subs.msg
                    destination = f.headers[HEADER_DESTINATION]
                    # ignore client unsubscribed
                    if destination not in self._subscribers:
                        continue
                    for sub_id, sub_sess in msg_subs.subscribers.items():
                        if sub_sess.sub_id not in self._subscribers[destination]:
                            continue
                        if sub_sess.ws_conn.state == WebsocketState.OPEN:
                            f.headers[HEADER_SUB] = sub_id
                            if sub_sess.ack != _ACK_AUTO:
                                f.headers['ack'] = f.headers[HEADER_MSG_ID]
                                await self._do_send_frame(sub_sess.ws_conn, f)
                                del f.headers['ack']
                            else:
                                await self._do_send_frame(sub_sess.ws_conn, f)
                await asyncio.sleep(self.config.client_ack_timeout_sec)
        except asyncio.CancelledError:
            return


async def quick_bootstrap(port: int) -> Tuple[Stomp12Broker, websockets.WebSocketServer]:  # type: ignore
    """
    :return: broker and the underlying websocket server
    to close:
    broker.close()
    server.close()
    await server.wait_closed()
    """
    broker = Stomp12Broker(StompConfig(), logging.getLogger('stomp'), asyncio.get_running_loop())
    ws_server = await websockets.serve(broker.accept_websocket_connection, '0.0.0.0', port, compression=None, origins=None)      # type: ignore
    return broker, ws_server


"""
    Amqp Protocol
"""

import trio
import logging
from math import inf
import socket
import ssl
from async_generator import asynccontextmanager
from async_generator import async_generator,yield_

from . import channel as amqp_channel
from . import constants as amqp_constants
from . import frame as amqp_frame
from . import exceptions
from . import _version

logger = logging.getLogger(__name__)

CONNECTING, OPEN, CLOSING, CLOSED = range(4)


class ChannelContext:
    """This class is returned by :meth:`AmqpProtocol:new_channel`.
    It is responsible for creating a new channel when used as a context
    manager.
    """

    def __init__(self, conn, kwargs):
        self.conn = conn
        self.kwargs = kwargs

    async def __aenter__(self):
        self.channel = await self.conn.channel(**self.kwargs)
        return self.channel

    async def __aexit__(self, *tb):
        if not self.channel.is_open:
            return
        with trio.open_cancel_scope(shield=True):
            await self.channel.close()

    def __enter__(self):
        raise RuntimeError("This is an async-only context manager.")

    def __exit__(self, *tb):
        raise RuntimeError("This is an async-only context manager.")


class AmqpProtocol(trio.abc.AsyncResource):
    """The AMQP protocol for trio.
    """

    CHANNEL_FACTORY = amqp_channel.Channel
    CHANNEL_CONTEXT = ChannelContext

    _close_reason = None

    def __init__(
        self,
        nursery,
        host='localhost',
        port=None,
        ssl=False,
        verify_ssl=True,
        login='guest',
        password='guest',
        virtualhost='/',
        channel_max=None,
        frame_max=None,
        heartbeat=None,
        client_properties=None,
        login_method='AMQPLAIN',
        insist=False
    ):
        """Defines our new protocol instance

        Args:
            host:
                the host to connect to
            port:
                broker port
            login:
                login
            password:
                password
            virtualhost:
                AMQP virtualhost to use for this connection
            ssl:
                the SSL context to use
            login_method:
                AMQP auth method
            insist:
                Insist on connecting to a server

            kwargs:
                Arguments to be given to the protocol_factory instance

            channel_max:
                specifies highest channel number that the server permits.
                Usable channel numbers are in the range 1..channel-max.
                Zero indicates no specified limit.
            frame_max:
                the largest frame size that the server proposes for the
                connection, including frame header and end-byte. The client
                can negotiate a lower value.
                Zero means that the server does not impose any specific
                limit but may reject very large frames if it cannot
                allocate resources for them.
            heartbeat:
                the delay, in seconds, of the connection heartbeat that the
                server wants. Zero means the server does not want a
                heartbeat.
            client_properties:
                dict, client-props to tune the client identification
        """

        self._reader_scope = None
        self._writer_scope = None

        self._nursery = nursery
        self.client_properties = client_properties or {}
        self.connection_tunning = {}
        if channel_max is not None:
            self.connection_tunning['channel_max'] = channel_max
        if frame_max is not None:
            self.connection_tunning['frame_max'] = frame_max
        if heartbeat is not None:
            self.connection_tunning['heartbeat'] = heartbeat

        if login_method != 'AMQPLAIN':
            # TODO
            logger.warning('only AMQPLAIN login_method is supported, ' 'falling back to AMQPLAIN')

        self._host = host
        self._port = port
        self._ssl = ssl
        self._virtualhost = virtualhost
        self._login_method = login_method
        self._insist = insist
        self._auth = {
            'LOGIN': login,
            'PASSWORD': password,
        }

    @property
    def nursery(self):
        return self._nursery

    async def ensure_open(self):
        # Raise a suitable exception if the connection isn't open.
        # Handle cases from the most common to the least common.

        if self.state == OPEN:
            return

        if self.state == CLOSED:
            raise exceptions.AmqpClosedConnection()

        # If the closing handshake is in progress, let it complete.
        if self.state == CLOSING:
            await self.wait_closed()
            raise exceptions.AmqpClosedConnection()

        # Control may only reach this point in buggy third-party subclasses.
        assert self.state == CONNECTING
        raise exceptions.TrioAmqpException("connection isn't established yet.")

    async def _drain(self):
        return

        # with (await self._drain_lock):
        #    # drain() cannot be called concurrently by multiple coroutines:
        #    # http://bugs.python.org/issue29930. Remove this lock when no
        #    # version of Python where this bugs exists is supported anymore.
        #    await self._stream_writer.drain()

    async def _write_frame(self, frame, encoder, drain=True):
        # Doesn't actually write frame, pushes it for _writer_loop task to
        # pick it up.
        await self._send_queue.put((frame, encoder))

    @trio.hazmat.enable_ki_protection
    async def _writer_loop(self, task_status=trio.TASK_STATUS_IGNORED):
        with trio.open_cancel_scope(shield=True) as scope:
            self._writer_scope = scope
            task_status.started()
            while self.state != CLOSED:
                if self.server_heartbeat:
                    timeout = self.server_heartbeat / 2
                else:
                    timeout = inf

                with trio.move_on_after(timeout) as timeout_scope:
                    frame, encoder = await self._send_queue.get()
                if timeout_scope.cancelled_caught:
                    await self.send_heartbeat()
                    continue

                f = frame.get_frame(encoder)
                try:
                    await self._stream.send_all(f)
                except (trio.BrokenStreamError,trio.ClosedStreamError):
                    # raise exceptions.AmqpClosedConnection(self) from None
                    # the reader will raise the error also
                    return

    async def aclose(self, no_wait=False):
        """Close connection (and all channels)"""
        if self.state == CLOSED:
            return
        if self.state == CLOSING:
            if not no_wait:
                await self.wait_closed()
            return

        try:
            self.state = CLOSING
            got_close = self.connection_closed.is_set()
            self.connection_closed.set()
            if not got_close:
                self._close_channels()

                # If the closing handshake is in progress, let it complete.
                frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, 0)
                frame.declare_method(
                    amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_CLOSE
                )
                encoder = amqp_frame.AmqpEncoder()
                # we request a clean connection close
                encoder.write_short(0)
                encoder.write_shortstr('')
                encoder.write_short(0)
                encoder.write_short(0)
                try:
                    await self._write_frame(frame, encoder)
                except trio.ClosedStreamError:
                    pass
                except Exception:
                    logger.exception("Error while closing")
                else:
                    if not no_wait and self.server_heartbeat:
                        with trio.move_on_after(self.server_heartbeat / 2):
                            await self.wait_closed()

        except BaseException as exc:
            self._close_channels(exception=exc)
            raise

        finally:
            with trio.open_cancel_scope(shield=True):
                self._cancel_all()
                await self._stream.aclose()
                self._nursery = None
                self.state = CLOSED

    def close(self):
        """Close connection (and all channels) destructively"""
        if self.state == CLOSED:
            return
        self.state = CLOSED
        self.connection_closed.set()
        self._close_channels()
        self._stream.socket.close()

    async def wait_closed(self):
        await self.connection_closed.wait()

    async def close_ok(self, frame):
        logger.debug("Recv close ok")
        await self._stream.aclose()

    def __enter__(self):
        raise TypeError("You need to use an async context")

    def __exit__(self, a, b, c):
        raise TypeError("You need to use an async context")

    async def __aenter__(self):
        self.connecting = trio.Event()
        self.connection_closed = trio.Event()
        self.state = CONNECTING
        self.version_major = None
        self.version_minor = None
        self.server_properties = None
        self.server_mechanisms = None
        self.server_locales = None
        self.server_heartbeat = None
        self.channels = {}
        self.server_frame_max = None
        self.server_channel_max = None
        self.channels_ids_ceil = 0
        self.channels_ids_free = set()
        self._send_queue = trio.Queue(1)

        if self._ssl:
            if self._ssl is True:
                ssl_context = ssl.create_default_context()
            else:
                ssl_context = self._ssl

        port = self._port
        if port is None:
            if self._ssl:
                port = 5671
            else:
                port = 5672

        if self._ssl:
            stream = await trio.open_ssl_over_tcp_stream(self._host, port, ssl_context=ssl_context)
            sock = stream.transport_stream
        else:
            sock = stream = await trio.open_tcp_stream(self._host, port)

        sock.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        self._stream = stream

        # the writer loop needs to run since the beginning
        await self._nursery.start(self._writer_loop)

        try:
            await self._stream.send_all(amqp_constants.PROTOCOL_HEADER)

            # Wait 'start' method from the server
            await self.dispatch_frame()
            if self.version_major is None:
                raise RuntimeError("Server didn't start with a START packet")

            client_properties = {
                'capabilities': {
                    'consumer_cancel_notify': True,
                    'connection.blocked': False,
                },
                'copyright': 'BSD',
                'product': _version.__package__,
                'product_version': _version.__version__,
            }
            client_properties.update(self.client_properties)

            # waiting reply start with credentions and co
            await self.start_ok(client_properties, 'AMQPLAIN', self._auth, self.server_locales[0])

            # wait for a "tune" reponse
            await self.dispatch_frame()
            if self.server_channel_max is None:
                raise RuntimeError("Server didn't send a TUNE packet")

            tune_ok = {
                'channel_max':
                    self.connection_tunning.get('channel_max', self.server_channel_max),
                'frame_max':
                    self.connection_tunning.get('frame_max', self.server_frame_max),
                'heartbeat':
                    self.connection_tunning.get('heartbeat', self.server_heartbeat),
            }
            # "tune" the connexion with max channel, max frame, heartbeat
            await self.tune_ok(**tune_ok)

            # update connection tunning values
            self.server_frame_max = tune_ok['frame_max']
            self.server_channel_max = tune_ok['channel_max']
            self.server_heartbeat = tune_ok['heartbeat']

            # open a virtualhost
            await self.open(self._virtualhost, capabilities='', insist=self._insist)

            # wait for open-ok
            await self.dispatch_frame()
            if self.state != OPEN:
                if self._close_reason is not None:
                    raise exceptions.AmqpClosedConnection(self._close_reason['text'])
                else:
                    raise exceptions.AmqpClosedConnection()

            # read the other server's responses asynchronously
            await self._nursery.start(self._reader_loop)

        except BaseException as exc:
            await self.aclose(no_wait=True)
            raise

        return self

    async def __aexit__(self, typ, exc, tb):
        with trio.open_cancel_scope(shield=True):
            await self.aclose()

    async def get_frame(self):
        """Read the frame, and only decode its header

        """
        frame = amqp_frame.AmqpResponse(self._stream)
        try:
            await frame.read_frame()
        except trio.BrokenStreamError:
            raise exceptions.AmqpClosedConnection(self) from None

        return frame

    async def dispatch_frame(self, frame=None):
        """Dispatch the received frame to the corresponding handler"""

        method_dispatch = {
            (amqp_constants.CLASS_CONNECTION,
             amqp_constants.CONNECTION_CLOSE):  # noqa: E131
                self.server_close,
            (amqp_constants.CLASS_CONNECTION,
             amqp_constants.CONNECTION_CLOSE_OK):  # noqa: E131
                self.close_ok,
            (amqp_constants.CLASS_CONNECTION,
             amqp_constants.CONNECTION_TUNE):  # noqa: E131
                self.tune,
            (amqp_constants.CLASS_CONNECTION,
             amqp_constants.CONNECTION_START):  # noqa: E131
                self.start,
            (amqp_constants.CLASS_CONNECTION,
             amqp_constants.CONNECTION_OPEN_OK):  # noqa: E131
                self.open_ok,
        }
        if not frame:
            frame = await self.get_frame()

        if frame.frame_type == amqp_constants.TYPE_HEARTBEAT:
            return

        if frame.channel is not 0:
            channel = self.channels.get(frame.channel)
            if channel is not None:
                await channel.dispatch_frame(frame)
            else:
                logger.info("Unknown channel %s", frame.channel)
            return

        if (frame.class_id, frame.method_id) not in method_dispatch:
            logger.info("frame %s %s is not handled", frame.class_id, frame.method_id)
            return
        await method_dispatch[(frame.class_id, frame.method_id)](frame)

    def release_channel_id(self, channel_id):
        """Called from the channel instance, it relase a previously used
        channel_id
        """
        self.channels_ids_free.add(channel_id)

    @property
    def channels_ids_count(self):
        return self.channels_ids_ceil - len(self.channels_ids_free)

    def _close_channels(self, reply_code=None, reply_text=None, exception=None):
        """Cleanly close channels

            Args:
                reply_code:     int, the amqp error code
                reply_text:     str, the text associated with the error_code
                exception:      the exception responsible of this error

        """
        if exception is None:
            exception = exceptions.ChannelClosed(reply_code, reply_text)

        for channel in self.channels.values():
            channel.connection_closed(reply_code, reply_text, exception)

    @trio.hazmat.enable_ki_protection
    async def _reader_loop(self, task_status=trio.TASK_STATUS_IGNORED):
        with trio.open_cancel_scope(shield=True) as scope:
            self._reader_scope = scope
            try:
                task_status.started()
                while True:
                    try:
                        if self._stream is None:
                            raise exceptions.AmqpClosedConnection

                        if self.server_heartbeat:
                            timeout = self.server_heartbeat * 2
                        else:
                            timeout = inf

                        with trio.fail_after(timeout):
                            try:
                                frame = await self.get_frame()
                            except trio.ClosedStreamError:
                                # the stream is now *really* closed …
                                return
                        try:
                            await self.dispatch_frame(frame)
                        except Exception as exc:
                            # We want to raise this exception so that the
                            # nursery ends the protocol, but we need keep
                            # going for now (need to process the close-OK
                            # message). Thus we start a new task that
                            # raises the actual error, somewhat later.
                            async def owch(exc):
                                await trio.sleep(0)
                                raise exc

                            self._nursery.start_soon(owch, exc)

                    except trio.TooSlowError:
                        self.connection_closed.set()
                        raise exceptions.HeartbeatTimeoutError(self) from None
                    except exceptions.AmqpClosedConnection as exc:
                        logger.debug("Remote closed connection")
                        raise
            finally:
                self._reader_scope = None
                self.connection_closed.set()

    async def send_heartbeat(self):
        """Sends an heartbeat message.
        It can be an ack for the server or the client willing to check for the
        connexion timeout
        """
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_HEARTBEAT, 0)
        request = amqp_frame.AmqpEncoder()
        await self._write_frame(frame, request)

    # Amqp specific methods
    async def start(self, frame):
        """Method sent from the server to begin a new connection"""
        response = amqp_frame.AmqpDecoder(frame.payload)

        self.version_major = response.read_octet()
        self.version_minor = response.read_octet()
        self.server_properties = response.read_table()
        self.server_mechanisms = response.read_longstr().split(' ')
        self.server_locales = response.read_longstr().split(' ')

    async def start_ok(self, client_properties, mechanism, auth, locale):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_START_OK)
        request = amqp_frame.AmqpEncoder()
        request.write_table(client_properties)
        request.write_shortstr(mechanism)
        request.write_table(auth)
        request.write_shortstr(locale.encode())
        await self._write_frame(frame, request)

    async def server_close(self, frame):
        """The server is closing the connection"""
        self.state = CLOSING
        response = amqp_frame.AmqpDecoder(frame.payload)
        reply_code = response.read_short()
        reply_text = response.read_shortstr()
        class_id = response.read_short()
        method_id = response.read_short()
        self._close_reason = dict(text=reply_text, code=reply_code, class_id=class_id, method_id=method_id)
        logger.warning(
            "Server closed connection: %s, code=%s, class_id=%s, method_id=%s", reply_text,
            reply_code, class_id, method_id
        )
        self._close_channels(reply_code, reply_text)
        await self._close_ok()

    async def _close_ok(self):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_CLOSE_OK)
        request = amqp_frame.AmqpEncoder()
        await self._write_frame(frame, request)
        await trio.sleep(0)  # give the write task one shot to send the frame
        if self._nursery is not None:
            self._cancel_all()

    def _cancel_all(self):
        if self._reader_scope is not None:
            self._reader_scope.cancel()
        if self._writer_scope is not None:
            self._writer_scope.cancel()
        if self._nursery is not None:
            self._nursery.cancel_scope.cancel()

    async def tune(self, frame):
        decoder = amqp_frame.AmqpDecoder(frame.payload)
        self.server_channel_max = decoder.read_short()
        self.server_frame_max = decoder.read_long()
        self.server_heartbeat = decoder.read_short()

    async def tune_ok(self, channel_max, frame_max, heartbeat):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_TUNE_OK)
        encoder = amqp_frame.AmqpEncoder()
        encoder.write_short(channel_max)
        encoder.write_long(frame_max)
        encoder.write_short(heartbeat)

        await self._write_frame(frame, encoder)

    async def secure_ok(self, login_response):
        pass

    async def open(self, virtual_host, capabilities='', insist=False):
        """Open connection to virtual host."""
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_OPEN)
        encoder = amqp_frame.AmqpEncoder()
        encoder.write_shortstr(virtual_host)
        encoder.write_shortstr(capabilities)
        encoder.write_bool(insist)

        await self._write_frame(frame, encoder)

    async def open_ok(self, frame):
        self.state = OPEN
        logger.debug("Recv open ok")

    #
    # trio_amqp public methods
    #

    def new_channel(self, **kwargs):
        return self.CHANNEL_CONTEXT(self, kwargs)

    async def channel(self, **kwargs):
        """Factory to create a new channel

        """
        await self.ensure_open()
        try:
            channel_id = self.channels_ids_free.pop()
        except KeyError:
            assert self.server_channel_max is not None, \
                'connection channel-max tuning not performed'
            # channel-max = 0 means no limit
            if self.server_channel_max and \
                    self.channels_ids_ceil > self.server_channel_max:
                raise exceptions.NoChannelAvailable()
            self.channels_ids_ceil += 1
            channel_id = self.channels_ids_ceil
        channel = self.CHANNEL_FACTORY(self, channel_id, **kwargs)
        self.channels[channel_id] = channel
        await channel.open()
        return channel


@asynccontextmanager
@async_generator
async def connect_amqp(*args, protocol=AmqpProtocol, **kwargs):
    async with trio.open_nursery() as nursery:
        amqp = protocol(nursery, *args, **kwargs)
        try:
            async with amqp:
                await yield_(amqp)
        finally:
            amqp._cancel_all()


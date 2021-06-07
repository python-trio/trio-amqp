"""
    Amqp Protocol
"""

import anyio
import errno
import logging
from math import inf
import socket
import ssl
try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager
import pamqp
import pamqp.commands
from anyio.abc import SocketAttribute

from . import channel as amqp_channel
from . import constants as amqp_constants
from . import frame as amqp_frame
from . import exceptions

logger = logging.getLogger(__name__)

CONNECTING, OPEN, CLOSING, CLOSED = range(4)
READ_BUF_SIZE = 1024


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
        with anyio.move_on_after(2,shield=True):
            try:
                await self.channel.close()
            except exceptions.AmqpClosedConnection:
                pass

    def __enter__(self):
        raise RuntimeError("This is an async-only context manager.")

    def __exit__(self, *tb):
        raise RuntimeError("This is an async-only context manager.")


class AmqpProtocol:
    """The AMQP protocol for anyio.
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
        login_method='PLAIN',
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

        if login_method != 'PLAIN':
            logger.warning('login_method %s is not supported, falling back to PLAIN', login_method)

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
        raise exceptions.AsyncAmqpException("connection isn't established yet.")

    async def _drain(self):
        return

        # with (await self._drain_lock):
        #    # drain() cannot be called concurrently by multiple coroutines:
        #    # http://bugs.python.org/issue29930. Remove this lock when no
        #    # version of Python where this bugs exists is supported anymore.
        #    await self._stream_writer.drain()

    async def _write_frame(self, channel_id, request, drain=True):
        # Doesn't actually write frame, pushes it for _writer_loop task to
        # pick it up.
        data = pamqp.frame.marshal(request, channel_id)
        await self._send_queue_w.send(data)

    async def _writer_loop(self, *, task_status):
        with anyio.CancelScope(shield=True) as scope:
            self._writer_scope = scope
            task_status.started()
            while self.state != CLOSED:
                if self.server_heartbeat:
                    timeout = self.server_heartbeat / 2
                else:
                    timeout = inf

                with anyio.move_on_after(timeout) as timeout_scope:
                    data = await self._send_queue_r.receive()
                if timeout_scope.cancel_called:
                    await self.send_heartbeat()
                    continue

                try:
                    await self._stream.send(data)
                except (anyio.ClosedResourceError, BrokenPipeError):
                    # raise exceptions.AmqpClosedConnection(self) from None
                    # the reader will raise the error also
                    return

    async def close(self, no_wait=False):
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
                await self._close_channels()

                # If the closing handshake is in progress, let it complete.
                request = pamqp.commands.Connection.Close(
                    reply_code=0,
                    reply_text='',
                    class_id=0,
                    method_id=0
                )
                try:
                    await self._write_frame(0, request)
                except anyio.ClosedResourceError:
                    pass
                except Exception:
                    logger.exception("Error while closing")
                else:
                    if not no_wait and self.server_heartbeat:
                        with anyio.move_on_after(self.server_heartbeat / 2):
                            await self.wait_closed()

        except BaseException as exc:
            with anyio.fail_after(2, shield=True):
                await self._close_channels(exception=exc)
            raise

        finally:
            with anyio.fail_after(2, shield=True):
                try:
                    self._cancel_all()
                    await self._stream.aclose()
                finally:
                    self._nursery = None
                    self.state = CLOSED

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
        self.connection_closed = anyio.Event()
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
        self._send_queue_w,self._send_queue_r = anyio.create_memory_object_stream(1)

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
            stream = await anyio.connect_tcp(self._host, port, ssl_context=ssl_context, autostart_tls=True)
        else:
            stream = await anyio.connect_tcp(self._host, port)

        stream.extra(SocketAttribute.raw_socket).setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        from anyio.streams.buffered import BufferedByteReceiveStream
        self._stream = stream
        self._rstream = BufferedByteReceiveStream(stream)

        # the writer loop needs to run from the beginning
        await self._nursery.start(self._writer_loop)

        try:
            await self._stream.send(amqp_constants.PROTOCOL_HEADER)

            # Wait 'start' method from the server
            await self.dispatch_frame()
            if self.version_major is None:
                raise RuntimeError("Server didn't start with a START packet")

            client_properties = {
                'capabilities': {
                    'consumer_cancel_notify': True,
                    'connection.blocked': False,
                },
            }
            client_properties.update(self.client_properties)

            # waiting reply start with credentions and co
            await self.start_ok(client_properties, 'AMQPLAIN', self._auth, self.server_locales)

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
            with anyio.fail_after(2, shield=True):
                await self.close(no_wait=True)
            raise

        return self

    async def __aexit__(self, typ, exc, tb):
        with anyio.move_on_after(2, shield=True):
            await self.close()

    async def get_frame(self):
        """Read the frame, and only decode its header

        """
        try:
            channel, frame = await amqp_frame.read(self._rstream)
        except ConnectionResetError:
            raise exceptions.AmqpClosedConnection(self) from None
        except EnvironmentError as err:
            if err.errno == errno.EBADF:
                raise exceptions.AmqpClosedConnection(self) from None
            raise
        except anyio.ClosedResourceError:
            raise exceptions.AmqpClosedConnection(self) from None

        return channel, frame

    async def dispatch_frame(self, frame_channel=None, frame=None):
        """Dispatch the received frame to the corresponding handler"""

        method_dispatch = {
            pamqp.commands.Connection.Close.name: self.server_close,
            pamqp.commands.Connection.CloseOk.name: self.close_ok,
            pamqp.commands.Connection.Tune.name: self.tune,
            pamqp.commands.Connection.Start.name: self.start,
            pamqp.commands.Connection.OpenOk.name: self.open_ok,
        }
        if frame is None:
            frame_channel, frame = await self.get_frame()

        if isinstance(frame, pamqp.heartbeat.Heartbeat):
            return

        if frame_channel:
            channel = self.channels.get(frame_channel)
            if channel is not None:
                await channel.dispatch_frame(frame)
            else:
                logger.info("Unknown channel %s", frame_channel)
            return

        if frame.name not in method_dispatch:
            logger.info("frame %s is not handled", frame.name)
            return
        await method_dispatch[frame.name](frame)

    def release_channel_id(self, channel_id):
        """Called from the channel instance, it relase a previously used
        channel_id
        """
        self.channels_ids_free.add(channel_id)

    @property
    def channels_ids_count(self):
        return self.channels_ids_ceil - len(self.channels_ids_free)

    async def _close_channels(self, reply_code=None, reply_text=None, exception=None):
        """Cleanly close channels

            Args:
                reply_code:     int, the amqp error code
                reply_text:     str, the text associated with the error_code
                exception:      the exception responsible of this error

        """
        if exception is None:
            exception = exceptions.ChannelClosed(reply_code, reply_text)

        for channel in self.channels.values():
            await channel.connection_closed(reply_code, reply_text, exception)

    async def _reader_loop(self, *, task_status):
        with anyio.CancelScope(shield=True) as scope:
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

                        with anyio.fail_after(timeout):
                            try:
                                channel, frame = await self.get_frame()
                            except anyio.ClosedResourceError:
                                # the stream is now *really* closed …
                                return
                        try:
                            await self.dispatch_frame(channel, frame)
                        except Exception as exc:
                            # We want to raise this exception so that the
                            # nursery ends the protocol, but we need keep
                            # going for now (need to process the close-OK
                            # message). Thus we start a new task that
                            # raises the actual error, somewhat later.
                            if self._nursery is None:
                                raise

                            async def owch(exc):
                                await anyio.sleep(0.01)
                                raise exc

                            logger.error("Queue %r", exc)
                            self._nursery.start_soon(owch, exc)

                    except TimeoutError:
                        self.connection_closed.set()
                        raise exceptions.HeartbeatTimeoutError(self) from None
                    except exceptions.AmqpClosedConnection as exc:
                        logger.debug("Remote closed connection")
                        if self.state in (CLOSING, CLOSED):
                            return
                        raise
            finally:
                self._reader_scope = None
                self.connection_closed.set()

    async def send_heartbeat(self):
        """Sends an heartbeat message.
        It can be an ack for the server or the client willing to check for the
        connexion timeout
        """
        request = pamqp.heartbeat.Heartbeat()
        await self._write_frame(0, request)

    # Amqp specific methods
    async def start(self, frame):
        """Method sent from the server to begin a new connection"""
        self.version_major = frame.version_major
        self.version_minor = frame.version_minor
        self.server_properties = frame.server_properties
        self.server_mechanisms = frame.mechanisms
        self.server_locales = frame.locales

    async def start_ok(self, client_properties, mechanism, auth, locale):
        class StartOk(pamqp.commands.Connection.StartOk):
            _response = 'table'

        request = StartOk(
            client_properties=client_properties,
            mechanism=mechanism,
            locale=locale,
            response=auth,
        )
        await self._write_frame(0, request)

    async def server_close(self, frame):
        """The server is closing the connection"""
        self.state = CLOSING
        reply_code = frame.reply_code
        reply_text = frame.reply_text
        class_id = frame.class_id
        method_id = frame.method_id
        self._close_reason = dict(text=reply_text, code=reply_code, class_id=class_id, method_id=method_id)
        logger.warning(
            "Server closed connection: %s, code=%s, class_id=%s, method_id=%s", reply_text,
            reply_code, class_id, method_id
        )
        await self._close_channels(reply_code, reply_text)
        await self._close_ok()

    async def _close_ok(self):
        request = pamqp.commands.Connection.CloseOk()
        await self._write_frame(0, request)
        await anyio.sleep(0)  # give the write task one shot to send the frame
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
        self.server_channel_max = frame.channel_max
        self.server_frame_max = frame.frame_max
        self.server_heartbeat = frame.heartbeat

    async def tune_ok(self, channel_max, frame_max, heartbeat):
        request = pamqp.commands.Connection.TuneOk(
            channel_max, frame_max, heartbeat
        )
        await self._write_frame(0, request)

    async def secure_ok(self, login_response):
        pass

    async def open(self, virtual_host, capabilities='', insist=False):
        """Open connection to virtual host."""
        request = pamqp.commands.Connection.Open(
            virtual_host, capabilities, insist
        )
        await self._write_frame(0, request)

    async def open_ok(self, frame):
        self.state = OPEN
        logger.debug("Recv open ok")

    #
    # async_amqp public methods
    #

    def new_channel(self, **kwargs):
        return self.CHANNEL_CONTEXT(self, kwargs)

    async def channel(self, **kwargs):
        """Factory to create a new channel

        """
        await self.ensure_open()
        try:
            channel_id = self.channels_ids_free.pop()
        except KeyError as ex:
            assert self.server_channel_max is not None, \
                'connection channel-max tuning not performed'
            # channel-max = 0 means no limit
            if self.server_channel_max and \
                    self.channels_ids_ceil > self.server_channel_max:
                raise exceptions.NoChannelAvailable() from ex
            self.channels_ids_ceil += 1
            channel_id = self.channels_ids_ceil
        channel = self.CHANNEL_FACTORY(self, channel_id, **kwargs)
        self.channels[channel_id] = channel
        await channel.open()
        return channel


@asynccontextmanager
async def connect_amqp(*args, protocol=AmqpProtocol, **kwargs):
    async with anyio.create_task_group() as nursery:
        amqp = protocol(nursery, *args, **kwargs)
        try:
            async with amqp:
                yield amqp
        except anyio.BrokenResourceError as ex:
            raise exceptions.AmqpClosedConnection from ex
        finally:
            with anyio.fail_after(2, shield=True):
                amqp._cancel_all()


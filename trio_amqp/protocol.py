"""
    Amqp Protocol
"""

import trio
import asyncio
import logging

from . import channel as amqp_channel
from . import constants as amqp_constants
from . import frame as amqp_frame
from . import exceptions
from . import version
from .compat import ensure_future


logger = logging.getLogger(__name__)


CONNECTING, OPEN, CLOSING, CLOSED = range(4)


class _StreamWriter(asyncio.StreamWriter):

    def write(self, data):
        ret = super().write(data)
        self._protocol._heartbeat_timer_send_reset()
        return ret

    def writelines(self, data):
        ret = super().writelines(data)
        self._protocol._heartbeat_timer_send_reset()
        return ret

    def write_eof(self):
        ret = super().write_eof()
        self._protocol._heartbeat_timer_send_reset()
        return ret


class AmqpProtocol:
    """The AMQP protocol for asyncio.

    See http://docs.python.org/3.4/library/asyncio-protocol.html#protocols for more information
    on asyncio's protocol API.

    """

    CHANNEL_FACTORY = amqp_channel.Channel

    def __init__(self, stream, *args, **kwargs):
        """Defines our new protocol instance

        Args:
            stream: trio.Stream, raw connection to the server (optionally w/ SSL)
            channel_max: int, specifies highest channel number that the server permits.
                              Usable channel numbers are in the range 1..channel-max.
                              Zero indicates no specified limit.
            frame_max: int, the largest frame size that the server proposes for the connection,
                            including frame header and end-byte. The client can negotiate a lower value.
                            Zero means that the server does not impose any specific limit
                            but may reject very large frames if it cannot allocate resources for them.
            heartbeat: int, the delay, in seconds, of the connection heartbeat that the server wants.
                            Zero means the server does not want a heartbeat.
            client_properties: dict, client-props to tune the client identification
        """
        self.stream = stream
        self._on_error_callback = kwargs.get('on_error')

        self.client_properties = kwargs.get('client_properties', {})
        self.connection_tunning = {}
        if 'channel_max' in kwargs:
            self.connection_tunning['channel_max'] = kwargs.get('channel_max')
        if 'frame_max' in kwargs:
            self.connection_tunning['frame_max'] = kwargs.get('frame_max')
        if 'heartbeat' in kwargs:
            self.connection_tunning['heartbeat'] = kwargs.get('heartbeat')

        self.connecting = asyncio.Future()
        self.connection_closed = trio.Event()
        self.state = CONNECTING
        self.version_major = None
        self.version_minor = None
        self.server_properties = None
        self.server_mechanisms = None
        self.server_locales = None
        self.worker = None
        self.worker_done = trio.Event()
        self.server_heartbeat = None
        self._heartbeat_timer_recv = None
        self._heartbeat_timer_send = None
        self._heartbeat_trigger_send = trio.Event()
        self._heartbeat_worker = None
        self.channels = {}
        self.server_frame_max = None
        self.server_channel_max = None
        self.channels_ids_ceil = 0
        self.channels_ids_free = set()
        self._drain_lock = trio.Lock()

    def connection_made(self, transport):
        super().connection_made(transport)
        self._stream_writer = _StreamWriter(transport, self, self._stream_reader, self._loop)

    def eof_received(self):
        super().eof_received()
        # Python 3.5+ started returning True here to keep the transport open.
        # We really couldn't care less so keep the behavior from 3.4 to make
        # sure connection_lost() is called.
        return False

    def connection_lost(self, exc):
        if exc:
            logger.exception("Connection lost")
        else:
            logger.debug("Connection lost")
        self.connection_closed.set()
        self.state = CLOSED
        self._close_channels(exception=exc)
        self._heartbeat_stop()
        super().connection_lost(exc)

    def data_received(self, data):
        self._heartbeat_timer_recv_reset()
        super().data_received(data)

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
        raise exceptions.AioamqpException("connection isn't established yet.")

    async def _drain(self):
        with (await self._drain_lock):
            # drain() cannot be called concurrently by multiple coroutines:
            # http://bugs.python.org/issue29930. Remove this lock when no
            # version of Python where this bugs exists is supported anymore.
            await self._stream_writer.drain()

    async def _write_frame(self, frame, encoder, drain=True):
        frame.write_frame(encoder)
        if drain:
            await self._drain()

    async def close(self, no_wait=False, timeout=None):
        """Close connection (and all channels)"""
        await self.ensure_open()
        self.state = CLOSING
        frame = amqp_frame.AmqpRequest(self._stream_writer, amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(
            amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_CLOSE)
        encoder = amqp_frame.AmqpEncoder()
        # we request a clean connection close
        encoder.write_short(0)
        encoder.write_shortstr('')
        encoder.write_short(0)
        encoder.write_short(0)
        await self._write_frame(frame, encoder)
        self._stop_heartbeat()
        if not no_wait:
            await self.wait_closed(timeout=timeout)

    async def wait_closed(self, timeout=None):
        await asyncio.wait_for(self.connection_closed.wait(), timeout=timeout)
        if self._heartbeat_worker is not None:
            try:
                await asyncio.wait_for(self._heartbeat_worker, timeout=timeout)
            except asyncio.CancelledError:
                pass
        if self.worker is not None:
            try:
                with trio.fail_after(timeout):
                    await self.worker_done.wait()
            except Exception as exc:
                logger.exception("Worker")

    async def close_ok(self, frame):
        logger.debug("Recv close ok")
        self._stream_writer.close()

    async def start_connection(self, host, port, login, password, virtualhost, ssl=False,
            login_method='AMQPLAIN', insist=False):
        """Initiate a connection at the protocol level
            We send `PROTOCOL_HEADER'
        """

        if login_method != 'AMQPLAIN':
            # TODO
            logger.warning('only AMQPLAIN login_method is supported, falling back to AMQPLAIN')

        self._stream_writer.write(amqp_constants.PROTOCOL_HEADER)

        # Wait 'start' method from the server
        await self.dispatch_frame()

        client_properties = {
            'capabilities': {
                'consumer_cancel_notify': True,
                'connection.blocked': False,
            },
            'copyright': 'BSD',
            'product': version.__package__,
            'product_version': version.__version__,
        }
        client_properties.update(self.client_properties)
        auth = {
            'LOGIN': login,
            'PASSWORD': password,
        }

        # waiting reply start with credentions and co
        await self.start_ok(client_properties, 'AMQPLAIN', auth, self.server_locales[0])

        # wait for a "tune" reponse
        await self.dispatch_frame()

        tune_ok = {
            'channel_max': self.connection_tunning.get('channel_max', self.server_channel_max),
            'frame_max': self.connection_tunning.get('frame_max', self.server_frame_max),
            'heartbeat': self.connection_tunning.get('heartbeat', self.server_heartbeat),
        }
        # "tune" the connexion with max channel, max frame, heartbeat
        await self.tune_ok(**tune_ok)

        # update connection tunning values
        self.server_frame_max = tune_ok['frame_max']
        self.server_channel_max = tune_ok['channel_max']
        self.server_heartbeat = tune_ok['heartbeat']

        if self.server_heartbeat > 0:
            self._heartbeat_timer_recv_reset()
            self._heartbeat_timer_send_reset()

        # open a virtualhost
        await self.open(virtualhost, capabilities='', insist=insist)

        # wait for open-ok
        frame = await self.get_frame()
        await self.dispatch_frame(frame)
        if (frame.frame_type == amqp_constants.TYPE_METHOD and
                frame.class_id == amqp_constants.CLASS_CONNECTION and
                frame.method_id == amqp_constants.CONNECTION_CLOSE):
            raise exceptions.AmqpClosedConnection()

        # for now, we read server's responses asynchronously
        self.worker = ensure_future(self.run())

    async def get_frame(self):
        """Read the frame, and only decode its header

        """
        frame = amqp_frame.AmqpResponse(self._stream_reader)
        await frame.read_frame()

        return frame

    async def dispatch_frame(self, frame=None):
        """Dispatch the received frame to the corresponding handler"""

        method_dispatch = {
            (amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_CLOSE): self.server_close,
            (amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_CLOSE_OK): self.close_ok,
            (amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_TUNE): self.tune,
            (amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_START): self.start,
            (amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_OPEN_OK): self.open_ok,
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
                reply_text:     str, the text associated to the error_code
                exc:            the exception responsible of this error

        """
        if exception is None:
            exception = exceptions.ChannelClosed(reply_code, reply_text)

        if self._on_error_callback:
            if asyncio.iscoroutinefunction(self._on_error_callback):
                ensure_future(self._on_error_callback(exception))
            else:
                self._on_error_callback(exceptions.ChannelClosed(exception))

        for channel in self.channels.values():
            channel.connection_closed(reply_code, reply_text, exception)

    async def run(self, task_status=trio.STATUS_IGNORED):
        self.worker_done.clear()
        task_status.started()
        try:
            while True:
                try:
                    if self._stream_reader is None:
                        raise exceptions.AmqpClosedConnection
                    await self.dispatch_frame()
                except exceptions.AmqpClosedConnection as exc:
                    logger.debug("Closed connection")

                    self._close_channels(exception=exc)
                    break
                except Exception:  # pylint: disable=broad-except
                    logger.exception('error on dispatch')
        finally:
            self.worker_done.set()

    async def send_heartbeat(self):
        """Sends an heartbeat message.
        It can be an ack for the server or the client willing to check for the
        connexion timeout
        """
        frame = amqp_frame.AmqpRequest(self._stream_writer, amqp_constants.TYPE_HEARTBEAT, 0)
        request = amqp_frame.AmqpEncoder()
        await self._write_frame(frame, request)

    def _heartbeat_timer_recv_timeout(self):
        # 4.2.7 If a peer detects no incoming traffic (i.e. received octets) for
        # two heartbeat intervals or longer, it should close the connection
        # without following the Connection.Close/Close-Ok handshaking, and log
        # an error.
        # TODO(rcardona) raise a "timeout" exception somewhere
        self._stream_writer.close()

    def _heartbeat_timer_recv_reset(self):
        if self.server_heartbeat is None:
            return
        if self._heartbeat_timer_recv is not None:
            self._heartbeat_timer_recv.cancel()
        self._heartbeat_timer_recv = self._loop.call_later(
            self.server_heartbeat * 2,
            self._heartbeat_timer_recv_timeout)

    def _heartbeat_timer_send_reset(self):
        if self.server_heartbeat is None:
            return
        if self._heartbeat_timer_send is not None:
            self._heartbeat_timer_send.cancel()
        self._heartbeat_timer_send = self._loop.call_later(
            self.server_heartbeat,
            self._heartbeat_trigger_send.set)
        if self._heartbeat_worker is None:
            self._heartbeat_worker = ensure_future(self._heartbeat())

    def _heartbeat_stop(self):
        self.server_heartbeat = None
        if self._heartbeat_timer_recv is not None:
            self._heartbeat_timer_recv.cancel()
        if self._heartbeat_timer_send is not None:
            self._heartbeat_timer_send.cancel()
        if self._heartbeat_worker is not None:
            self._heartbeat_worker.cancel()

    async def _heartbeat(self):
        while self.state != CLOSED:
            await self._heartbeat_trigger_send.wait()
            self._heartbeat_trigger_send.clear()
            await self.send_heartbeat()

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
        frame = amqp_frame.AmqpRequest(self._stream_writer, amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(
            amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_START_OK)
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
        self._stop_heartbeat()
        logger.warning("Server closed connection: %s, code=%s, class_id=%s, method_id=%s",
            reply_text, reply_code, class_id, method_id)
        self._close_channels(reply_code, reply_text)
        self._close_ok()
        self._stream_writer.close()

    def _stop_heartbeat(self):
        # prevent new timers from being created by overlapping traffic
        self.server_heartbeat = 0
        if self._heartbeat_timer_recv is not None:
            self._heartbeat_timer_recv.cancel()
            self._heartbeat_timer_recv = None
        if self._heartbeat_timer_send is not None:
            self._heartbeat_timer_send.cancel()
            self._heartbeat_timer_send = None

    def _close_ok(self):
        frame = amqp_frame.AmqpRequest(self._stream_writer, amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(
            amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_CLOSE_OK)
        request = amqp_frame.AmqpEncoder()
        await self._write_frame(frame, request)

    async def tune(self, frame):
        decoder = amqp_frame.AmqpDecoder(frame.payload)
        self.server_channel_max = decoder.read_short()
        self.server_frame_max = decoder.read_long()
        self.server_heartbeat = decoder.read_short()

    async def tune_ok(self, channel_max, frame_max, heartbeat):
        frame = amqp_frame.AmqpRequest(self._stream_writer, amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(
            amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_TUNE_OK)
        encoder = amqp_frame.AmqpEncoder()
        encoder.write_short(channel_max)
        encoder.write_long(frame_max)
        encoder.write_short(heartbeat)

        await self._write_frame(frame, encoder)

    async def secure_ok(self, login_response):
        pass

    async def open(self, virtual_host, capabilities='', insist=False):
        """Open connection to virtual host."""
        frame = amqp_frame.AmqpRequest(self._stream_writer, amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(
            amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_OPEN)
        encoder = amqp_frame.AmqpEncoder()
        encoder.write_shortstr(virtual_host)
        encoder.write_shortstr(capabilities)
        encoder.write_bool(insist)

        await self._write_frame(frame, encoder)

    async def open_ok(self, frame):
        self.state = OPEN
        logger.debug("Recv open ok")

    #
    ## trio_amqp public methods
    #

    async def channel(self, **kwargs):
        """Factory to create a new channel

        """
        await self.ensure_open()
        try:
            channel_id = self.channels_ids_free.pop()
        except KeyError:
            assert self.server_channel_max is not None, 'connection channel-max tuning not performed'
            # channel-max = 0 means no limit
            if self.server_channel_max and self.channels_ids_ceil > self.server_channel_max:
                raise exceptions.NoChannelAvailable()
            self.channels_ids_ceil += 1
            channel_id = self.channels_ids_ceil
        channel = self.CHANNEL_FACTORY(self, channel_id, **kwargs)
        self.channels[channel_id] = channel
        await channel.open()
        return channel

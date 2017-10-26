"""
    Amqp Protocol
"""

import trio
import logging

from . import channel as amqp_channel
from . import constants as amqp_constants
from . import frame as amqp_frame
from . import exceptions
from . import version
from . import acontextmanager, async_generator, yield_


logger = logging.getLogger(__name__)


CONNECTING, OPEN, CLOSING, CLOSED = range(4)


class NoHeartbeatError(Exception):
    pass


class AmqpProtocol(trio.abc.AsyncResource):
    """The AMQP protocol for trio.
    """

    CHANNEL_FACTORY = amqp_channel.Channel

    def __init__(self, *args, **kwargs):
        """Defines our new protocol instance

        Args:
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

        self.connecting = trio.Event()
        self.connection_closed = trio.Event()
        self.state = CONNECTING
        self.version_major = None
        self.version_minor = None
        self.server_properties = None
        self.server_mechanisms = None
        self.server_locales = None
        self.server_heartbeat = None
        self._heartbeat_timer_recv = None
        self._heartbeat_timer_send = None
        self._heartbeat_trigger_send = trio.Event()
        self.channels = {}
        self.server_frame_max = None
        self.server_channel_max = None
        self.channels_ids_ceil = 0
        self.channels_ids_free = set()
        self._drain_lock = trio.Lock()
        self._nursery = None
        self._nursery_mgr = None
        self._write_lock = trio.Lock()

    def connection_lost(self, exc=None):
        if self.state == CLOSED:
            return
        if exc:
            logger.exception("Connection lost")
        else:
            logger.debug("Connection lost")
        self.connection_closed.set()
        self.state = CLOSED
        self._close_channels(exception=exc)
        self._stop_heartbeat()
        if self._nursery is not None:
            self._nursery.cancel_scope.cancel()

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

        #with (await self._drain_lock):
        #    # drain() cannot be called concurrently by multiple coroutines:
        #    # http://bugs.python.org/issue29930. Remove this lock when no
        #    # version of Python where this bugs exists is supported anymore.
        #    await self._stream_writer.drain()

    async def _write_frame(self, frame, encoder, drain=True):
        f = frame.get_frame(encoder)
        try:
            async with self._write_lock:
                await self._stream.send_all(f)
                if drain:
                    await self._drain()
                await self._heartbeat_timer_send_reset()
        except BaseException as exc:
            self.connection_lost(exc)
            raise

    async def aclose(self, no_wait=False):
        """Close connection (and all channels)"""
        if self.state == CLOSED:
            return
        if self.state == CLOSING:
            if not no_wait:
                await self.wait_closed()
            return

        # If the closing handshake is in progress, let it complete.
        try:
            self.state = CLOSING
            frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, 0)
            frame.declare_method(
                amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_CLOSE)
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
            else:
                if not no_wait:
                    await self.wait_closed()

        except BaseException as exc:
            self.connection_lost(exc)
            if self._nursery_mgr is not None:
                await self._nursery_mgr.__aexit__(type(exc),exc,exc.__traceback__)
        else:
            self.connection_lost()
            if self._nursery_mgr is not None:
                await self._nursery_mgr.__aexit__(None,None,None)
        finally:
            self._nursery = None
            self._nursery_mgr = None

    async def wait_closed(self):
        await self.connection_closed.wait()

    async def close_ok(self, frame):
        logger.debug("Recv close ok")
        await self._stream.aclose()
        self.connection_lost()

    @trio.hazmat.enable_ki_protection
    async def start_connection(self, stream, login, password, virtualhost, ssl=False,
            channel_max=None, frame_max=None, heartbeat=None, client_properties=None,
            login_method='AMQPLAIN', insist=False):
        """Initiate a connection at the protocol level
            We send `PROTOCOL_HEADER'
        """
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
            logger.warning('only AMQPLAIN login_method is supported, falling back to AMQPLAIN')

        self._stream = stream
        self._virtualhost = virtualhost
        #self._ssl = ssl
        self._login_method=login_method
        self._insist = insist
        self._auth = {
            'LOGIN': login,
            'PASSWORD': password,
        }
        return self

    def __enter__(self):
        raise TypeError("You need to use an async context")

    def __exit__(self, a,b,c):
        raise TypeError("You need to use an async context")

    async def __aenter__(self):
        self._nursery_mgr = trio.open_nursery()
        self._nursery = await self._nursery_mgr.__aenter__()
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
                'product': version.__package__,
                'product_version': version.__version__,
            }
            client_properties.update(self.client_properties)

            # waiting reply start with credentions and co
            await self.start_ok(client_properties, 'AMQPLAIN', self._auth, self.server_locales[0])

            # wait for a "tune" reponse
            await self.dispatch_frame()
            if self.server_channel_max is None:
                raise RuntimeError("Server didn't send a TUNE packet")

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
                await self._heartbeat_timer_recv_reset()
                await self._heartbeat_timer_send_reset()

            # open a virtualhost
            await self.open(self._virtualhost, capabilities='', insist=self._insist)

            # wait for open-ok
            await self.dispatch_frame()
            if self.state != OPEN:
                raise exceptions.AmqpClosedConnection()
        except BaseException as exc:
            await self.aclose(no_wait=True)
            raise

        # read other server's responses asynchronously
        await self._nursery.start(self.run)
        return self

    async def __aexit__(self, typ,exc,tb):
        await self.aclose()

    async def get_frame(self):
        """Read the frame, and only decode its header

        """
        frame = amqp_frame.AmqpResponse(self._stream)
        try:
            await frame.read_frame()
            await self._heartbeat_timer_recv_reset()
        except Exception as exc:
            self.connection_lost(exc)
            raise

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

        for channel in self.channels.values():
            channel.connection_closed(reply_code, reply_text, exception)

    async def run(self, task_status=trio.STATUS_IGNORED):
        task_status.started()
        while True:
            try:
                if self._stream is None:
                    raise exceptions.AmqpClosedConnection
                await self.dispatch_frame()
            except exceptions.AmqpClosedConnection as exc:
                logger.debug("Closed connection")

                self._close_channels(exception=exc)
                break
            except Exception as exc:  # pylint: disable=broad-except
                self._nursery.cancel_scope.cancel()
                raise

    async def send_heartbeat(self):
        """Sends an heartbeat message.
        It can be an ack for the server or the client willing to check for the
        connexion timeout
        """
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_HEARTBEAT, 0)
        request = amqp_frame.AmqpEncoder()
        await self._write_frame(frame, request)

    async def _heartbeat_timer_recv_timeout(self, task_status=trio.STATUS_IGNORED):
        # 4.2.7 If a peer detects no incoming traffic (i.e. received octets) for
        # two heartbeat intervals or longer, it should close the connection
        # without following the Connection.Close/Close-Ok handshaking, and log
        # an error.
        with trio.open_cancel_scope() as scope:
            task_status.started(scope)

            await trio.sleep(self.server_heartbeat * 2)
            raise NoHeartbeatError(self)

    async def _heartbeat_timer_recv_reset(self):
        if self.server_heartbeat is None:
            return
        if self._heartbeat_timer_recv is not None:
            self._heartbeat_timer_recv.cancel()
        if self.state != OPEN:
            return
        self._heartbeat_timer_recv = await self._nursery.start(
            self._heartbeat_timer_recv_timeout)

    async def _heartbeat_timer_send_timeout(self, task_status=trio.STATUS_IGNORED):
        if self.server_heartbeat is None:
            task_status.started()
            return
        with trio.open_cancel_scope() as scope:
            task_status.started(scope)
            while self.state != CLOSED:
                await trio.sleep(self.server_heartbeat)
                await self.send_heartbeat()

    async def _heartbeat_timer_send_reset(self):
        if self.server_heartbeat is None:
            return
        if self._heartbeat_timer_send is not None:
            self._heartbeat_timer_send.cancel()
        if self.state != OPEN:
            return
        self._heartbeat_timer_send = await self._nursery.start(
            self._heartbeat_timer_send_timeout)

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
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, 0)
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
        logger.warning("Server closed connection: %s, code=%s, class_id=%s, method_id=%s",
            reply_text, reply_code, class_id, method_id)
        self._close_channels(reply_code, reply_text)
        await self._close_ok()

    def _stop_heartbeat(self):
        # prevent new timers from being created by overlapping traffic
        self.server_heartbeat = 0
        if self._heartbeat_timer_recv is not None:
            self._heartbeat_timer_recv.cancel()
            self._heartbeat_timer_recv = None
        if self._heartbeat_timer_send is not None:
            self._heartbeat_timer_send.cancel()
            self._heartbeat_timer_send = None

    async def _close_ok(self):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, 0)
        frame.declare_method(
            amqp_constants.CLASS_CONNECTION, amqp_constants.CONNECTION_CLOSE_OK)
        request = amqp_frame.AmqpEncoder()
        await self._write_frame(frame, request)
        if self._nursery is not None:
            self._nursery.cancel_scope.cancel()

    async def tune(self, frame):
        decoder = amqp_frame.AmqpDecoder(frame.payload)
        self.server_channel_max = decoder.read_short()
        self.server_frame_max = decoder.read_long()
        self.server_heartbeat = decoder.read_short()

    async def tune_ok(self, channel_max, frame_max, heartbeat):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, 0)
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
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, 0)
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

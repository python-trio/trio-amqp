"""
    Amqp channel specification
"""

import sys
import trio
import logging
import uuid
import io
import inspect
from itertools import count

from . import constants as amqp_constants
from . import frame as amqp_frame
from . import exceptions
from .envelope import Envelope, ReturnEnvelope
from .future import Future

logger = logging.getLogger(__name__)


class BasicListener:
    """This class is returned by :meth:Channel.new_consumer`.
    It is responsible for telling AMQP to start sending data.
    """

    def __init__(self, channel, consumer_tag, **kwargs):
        self.channel = channel
        self.kwargs = kwargs
        self.consumer_tag = consumer_tag

    async def _data(self, channel, msg, env, prop):
        if msg is None:
            await self._q.put(None)
        else:
            await self._q.put((msg, env, prop))

    if sys.version_info >= (3,5,3):
        def __aiter__(self):
            return self
    else:
        async def __aiter__(self):
            return self

    async def __anext__(self):
        res = await self._q.get()
        if res is None:
            raise StopAsyncIteration
        return res

    async def __aenter__(self):
        await self.channel.basic_consume(self._data, consumer_tag=self.consumer_tag, **self.kwargs)
        self._q = trio.Queue(30)  # TODO: 2 + possible prefetch
        return self

    async def __aexit__(self, *tb):
        with trio.open_cancel_scope(shield=True):
            await self.channel.basic_cancel(self.consumer_tag)
        del self._q
        # these messages are not acknowledged, thus deleting the queue will
        # not lose them

    def __enter__(self):
        raise RuntimeError("You need to use 'async with'.")

    def __exit__(self, *tb):
        raise RuntimeError("You need to use 'async with'.")

    def __iter__(self):
        raise RuntimeError("You need to use 'async for'.")


class Channel:
    _q = None # for returned messages

    def __init__(self, protocol, channel_id):
        self.protocol = protocol
        self.channel_id = channel_id
        self.consumer_queues = {}
        self.consumer_callbacks = {}
        self.response_future = None
        self.close_event = trio.Event()
        self.cancelled_consumers = set()
        self.last_consumer_tag = None
        self.publisher_confirms = False

        self.delivery_tag_iter = None
        # counting iterator, used for mapping delivered messages
        # to publisher confirms

        self._write_lock = trio.Lock()

        self._futures = {}
        self._ctag_events = {}

    def __aiter__(self):
        if self._q is None:
            self._q = trio.Queue(30)  # TODO: 2 + possible prefetch
        return self

    if sys.version_info < (3,5,3):
        _aiter = __aiter__
        async def __aiter__(self):
            return self._aiter()

    async def __anext__(self):
        res = await self._q.get()
        if res is None:
            raise StopAsyncIteration
        return res

    def _add_future(self, fut):
        self._futures[fut.rpc_name] = fut

    def _set_waiter(self, rpc_name):
        if rpc_name in self._futures:
            raise exceptions.SynchronizationError("Waiter already exists")

        return Future(self, rpc_name)

    def _get_waiter(self, rpc_name):
        fut = self._futures.pop(rpc_name, None)
        if not fut:
            raise exceptions.SynchronizationError("Call %s didn't set a waiter" % rpc_name)
        return fut

    @property
    def is_open(self):
        if self.protocol.connection_closed.is_set():
            return False
        return not self.close_event.is_set()

    def connection_closed(self, server_code=None, server_reason=None, exception=None):
        for future in self._futures.values():
            if future.done():
                continue
            if exception is None:
                kwargs = {}
                if server_code is not None:
                    kwargs['code'] = server_code
                if server_reason is not None:
                    kwargs['message'] = server_reason
                exception = exceptions.ChannelClosed(**kwargs)
            future.set_exception(exception)

        self.protocol.release_channel_id(self.channel_id)
        self.close_event.set()
        if self._q is not None:
            self._q.put_nowait(None)

    async def dispatch_frame(self, frame):
        methods = {
            (amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_OPEN_OK):
                self.open_ok,
            (amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_FLOW_OK):
                self.flow_ok,
            (amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_CLOSE_OK):
                self.close_ok,
            (amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_CLOSE):
                self.server_channel_close,
            (amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DECLARE_OK):
                self.exchange_declare_ok,
            (amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_BIND_OK):
                self.exchange_bind_ok,
            (amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_UNBIND_OK):
                self.exchange_unbind_ok,
            (amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DELETE_OK):
                self.exchange_delete_ok,
            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DECLARE_OK):
                self.queue_declare_ok,
            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DELETE_OK):
                self.queue_delete_ok,
            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_BIND_OK):
                self.queue_bind_ok,
            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_UNBIND_OK):
                self.queue_unbind_ok,
            (amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_PURGE_OK):
                self.queue_purge_ok,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_QOS_OK):
                self.basic_qos_ok,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CONSUME_OK):
                self.basic_consume_ok,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CANCEL_OK):
                self.basic_cancel_ok,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_GET_OK):
                self.basic_get_ok,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_GET_EMPTY):
                self.basic_get_empty,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_DELIVER):
                self.basic_deliver,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CANCEL):
                self.server_basic_cancel,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_ACK):
                self.basic_server_ack,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_NACK):
                self.basic_server_nack,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_RECOVER_OK):
                self.basic_recover_ok,
            (amqp_constants.CLASS_BASIC, amqp_constants.BASIC_RETURN):
                self.basic_return,
            (amqp_constants.CLASS_CONFIRM, amqp_constants.CONFIRM_SELECT_OK):
                self.confirm_select_ok,
        }

        if (frame.class_id, frame.method_id) not in methods:
            raise NotImplementedError(
                "Frame (%s, %s) is not implemented" % (frame.class_id, frame.method_id)
            )
        await methods[(frame.class_id, frame.method_id)](frame)

    async def _write_frame(self, frame, request, check_open=True, drain=True):
        await self.protocol.ensure_open()
        if not self.is_open and check_open:
            raise exceptions.ChannelClosed()
        try:
            await self.protocol._write_frame(frame, request)
            if drain:
                await self.protocol._drain()
        except Exception as exc:
            self.protocol.connection_lost(exc)
            raise

    async def _write_frame_awaiting_response(
        self, waiter_id, frame, request, no_wait, check_open=True, drain=True
    ):
        '''Write a frame and set a waiter for the response
        (unless no_wait is set)
        '''
        if no_wait:
            await self._write_frame(frame, request, check_open=check_open, drain=drain)
            return None

        f = self._set_waiter(waiter_id)
        try:
            await self._write_frame(frame, request, check_open=check_open, drain=drain)
        except BaseException as exc:
            self._get_waiter(waiter_id)
            f.cancel()
            raise
        res = await f()
        return res

#
# Channel class implementation
#

    async def open(self):
        """Open the channel on the server."""
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_OPEN)
        request = amqp_frame.AmqpEncoder()
        request.write_shortstr('')
        return (
            await self._write_frame_awaiting_response(
                'open', frame, request, no_wait=False, check_open=False
            )
        )

    async def open_ok(self, frame):
        self.close_event.clear()
        fut = self._get_waiter('open')
        fut.set_result(True)
        logger.debug("Channel is open")

    async def close(self, reply_code=0, reply_text="Normal Shutdown"):
        """Close the channel."""
        if not self.is_open:
            raise exceptions.ChannelClosed("channel already closed or closing")
        self.close_event.set()
        if self._q is not None:
            self._q.put_nowait(None)
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_CLOSE)
        request = amqp_frame.AmqpEncoder()
        request.write_short(reply_code)
        request.write_shortstr(reply_text)
        request.write_short(0)
        request.write_short(0)
        async with self._write_lock:
            return (
                await self._write_frame_awaiting_response(
                    'close', frame, request, no_wait=False, check_open=False
                )
            )

    async def close_ok(self, frame):
        self._get_waiter('close').set_result(True)
        logger.info("Channel closed")
        self.protocol.release_channel_id(self.channel_id)

    async def _send_channel_close_ok(self):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_CLOSE_OK)
        request = amqp_frame.AmqpEncoder()
        # intentionally not locked
        await self._write_frame(frame, request)

    async def server_channel_close(self, frame):
        await self._send_channel_close_ok()
        results = {
            'reply_code': frame.payload_decoder.read_short(),
            'reply_text': frame.payload_decoder.read_shortstr(),
            'class_id': frame.payload_decoder.read_short(),
            'method_id': frame.payload_decoder.read_short(),
        }
        self.connection_closed(results['reply_code'], results['reply_text'])

    async def flow(self, active):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_CHANNEL, amqp_constants.CHANNEL_FLOW)
        request = amqp_frame.AmqpEncoder()
        request.write_bits(active)
        async with self._write_lock:
            return (
                await self._write_frame_awaiting_response(
                    'flow', frame, request, no_wait=False, check_open=False
                )
            )

    async def flow_ok(self, frame):
        decoder = amqp_frame.AmqpDecoder(frame.payload)
        active = bool(decoder.read_octet())
        self.close_event.clear()
        fut = self._get_waiter('flow')
        fut.set_result({'active': active})

        logger.debug("Flow ok")

#
# Exchange class implementation
#

    async def exchange_declare(
        self,
        exchange_name,
        type_name,
        passive=False,
        durable=False,
        auto_delete=False,
        no_wait=False,
        arguments=None
    ):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DECLARE)
        request = amqp_frame.AmqpEncoder()
        # short reserved-1
        request.write_short(0)
        request.write_shortstr(exchange_name)
        request.write_shortstr(type_name)

        internal = False  # internal: deprecated
        request.write_bits(passive, durable, auto_delete, internal, no_wait)
        request.write_table(arguments)

        async with self._write_lock:
            return (
                await
                self._write_frame_awaiting_response('exchange_declare', frame, request, no_wait)
            )

    async def exchange_declare_ok(self, frame):
        future = self._get_waiter('exchange_declare')
        future.set_result(True)
        logger.debug("Exchange declared")
        return future

    async def exchange_delete(self, exchange_name, if_unused=False, no_wait=False):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_DELETE)
        request = amqp_frame.AmqpEncoder()
        # short reserved-1
        request.write_short(0)
        request.write_shortstr(exchange_name)
        request.write_bits(if_unused, no_wait)

        async with self._write_lock:
            return (
                await
                self._write_frame_awaiting_response('exchange_delete', frame, request, no_wait)
            )

    async def exchange_delete_ok(self, frame):
        future = self._get_waiter('exchange_delete')
        future.set_result(True)
        logger.debug("Exchange deleted")

    async def exchange_bind(
        self, exchange_destination, exchange_source, routing_key, no_wait=False, arguments=None
    ):
        if arguments is None:
            arguments = {}
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_EXCHANGE, amqp_constants.EXCHANGE_BIND)

        request = amqp_frame.AmqpEncoder()
        request.write_short(0)  # reserved
        request.write_shortstr(exchange_destination)
        request.write_shortstr(exchange_source)
        request.write_shortstr(routing_key)

        request.write_bits(no_wait)
        request.write_table(arguments)
        async with self._write_lock:
            return (
                await
                self._write_frame_awaiting_response('exchange_bind', frame, request, no_wait)
            )

    async def exchange_bind_ok(self, frame):
        future = self._get_waiter('exchange_bind')
        future.set_result(True)
        logger.debug("Exchange bound")

    async def exchange_unbind(
        self, exchange_destination, exchange_source, routing_key, no_wait=False, arguments=None
    ):
        if arguments is None:
            arguments = {}
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.EXCHANGE_UNBIND, amqp_constants.EXCHANGE_UNBIND)

        request = amqp_frame.AmqpEncoder()
        request.write_short(0)  # reserved
        request.write_shortstr(exchange_destination)
        request.write_shortstr(exchange_source)
        request.write_shortstr(routing_key)

        request.write_bits(no_wait)
        request.write_table(arguments)
        async with self._write_lock:
            return (
                await
                self._write_frame_awaiting_response('exchange_unbind', frame, request, no_wait)
            )

    async def exchange_unbind_ok(self, frame):
        future = self._get_waiter('exchange_unbind')
        future.set_result(True)
        logger.debug("Exchange bound")

#
# Queue class implementation
#

    async def queue_declare(
        self,
        queue_name=None,
        passive=False,
        durable=False,
        exclusive=False,
        auto_delete=False,
        no_wait=False,
        arguments=None
    ):
        """Create or check a queue on the broker
           Args:
                queue_name:
                    str, the queue to receive message from. The server generate
                    a queue_name if not specified.
                passive:
                    bool, if set, the server will reply with Declare-Ok if
                    the queue already exists with the same name, and raise
                    an error if not. Checks for the same parameter as well.
                durable:
                    bool: If set when creating a new queue, the queue will
                    be marked as durable. Durable queues remain active when
                    a server restarts.
                exclusive:
                    bool, request exclusive consumer access, meaning only
                    this consumer can access the queue
                no_wait:
                    bool, if set, the server will not respond to the method
                arguments:
                    dict, AMQP arguments to be passed when creating the
                    queue.
        """
        if arguments is None:
            arguments = {}

        if not queue_name:
            queue_name = ''
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DECLARE)
        request = amqp_frame.AmqpEncoder()
        request.write_short(0)  # reserved
        request.write_shortstr(queue_name)
        request.write_bits(passive, durable, exclusive, auto_delete, no_wait)
        request.write_table(arguments)
        async with self._write_lock:
            return (
                await
                self._write_frame_awaiting_response('queue_declare', frame, request, no_wait)
            )

    async def queue_declare_ok(self, frame):
        results = {
            'queue': frame.payload_decoder.read_shortstr(),
            'message_count': frame.payload_decoder.read_long(),
            'consumer_count': frame.payload_decoder.read_long(),
        }
        future = self._get_waiter('queue_declare')
        future.set_result(results)
        logger.debug("Queue declared")

    async def queue_delete(self, queue_name, if_unused=False, if_empty=False, no_wait=False):
        """Delete a queue in RabbitMQ
            Args:
                queue_name:
                    str, the queue to receive message from
                if_unused:
                    bool, the queue is deleted if it has no consumers.
                    Raise if not.
                if_empty:
                    bool, the queue is deleted if it has no messages. Raise
                    if not.
                no_wait:
                    bool, if set, the server will not respond to the method
        """
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_DELETE)

        request = amqp_frame.AmqpEncoder()
        request.write_short(0)  # reserved
        request.write_shortstr(queue_name)
        request.write_bits(if_unused, if_empty, no_wait)
        async with self._write_lock:
            return (
                await self._write_frame_awaiting_response('queue_delete', frame, request, no_wait)
            )

    async def queue_delete_ok(self, frame):
        future = self._get_waiter('queue_delete')
        future.set_result(True)
        logger.debug("Queue deleted")

    async def queue_bind(
        self, queue_name, exchange_name, routing_key, no_wait=False, arguments=None
    ):
        """Bind a queue to an exchange."""
        if arguments is None:
            arguments = {}
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_BIND)

        request = amqp_frame.AmqpEncoder()
        # short reserved-1
        request.write_short(0)
        request.write_shortstr(queue_name)
        request.write_shortstr(exchange_name)
        request.write_shortstr(routing_key)
        request.write_octet(int(no_wait))
        request.write_table(arguments)
        async with self._write_lock:
            return (
                await self._write_frame_awaiting_response('queue_bind', frame, request, no_wait)
            )

    async def queue_bind_ok(self, frame):
        future = self._get_waiter('queue_bind')
        future.set_result(True)
        logger.debug("Queue bound")

    async def queue_unbind(self, queue_name, exchange_name, routing_key, arguments=None):
        if arguments is None:
            arguments = {}
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_UNBIND)

        request = amqp_frame.AmqpEncoder()
        # short reserved-1
        request.write_short(0)
        request.write_shortstr(queue_name)
        request.write_shortstr(exchange_name)
        request.write_shortstr(routing_key)
        request.write_table(arguments)
        async with self._write_lock:
            return (
                await
                self._write_frame_awaiting_response('queue_unbind', frame, request, no_wait=False)
            )

    async def queue_unbind_ok(self, frame):
        future = self._get_waiter('queue_unbind')
        future.set_result(True)
        logger.debug("Queue unbound")

    async def queue_purge(self, queue_name, no_wait=False):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_QUEUE, amqp_constants.QUEUE_PURGE)

        request = amqp_frame.AmqpEncoder()
        # short reserved-1
        request.write_short(0)
        request.write_shortstr(queue_name)
        request.write_octet(int(no_wait))
        async with self._write_lock:
            return (
                await self._write_frame_awaiting_response(
                    'queue_purge', frame, request, no_wait=no_wait
                )
            )

    async def queue_purge_ok(self, frame):
        decoder = amqp_frame.AmqpDecoder(frame.payload)
        message_count = decoder.read_long()
        future = self._get_waiter('queue_purge')
        future.set_result({'message_count': message_count})

#
# Basic class implementation
#

    async def basic_publish(
        self,
        payload,
        exchange_name,
        routing_key,
        properties=None,
        mandatory=False,
        immediate=False
    ):
        assert payload, "Payload cannot be empty"

        async with self._write_lock:
            method_frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
            method_frame.declare_method(amqp_constants.CLASS_BASIC, amqp_constants.BASIC_PUBLISH)
            method_request = amqp_frame.AmqpEncoder()
            method_request.write_short(0)
            method_request.write_shortstr(exchange_name)
            method_request.write_shortstr(routing_key)
            method_request.write_bits(mandatory, immediate)
            await self._write_frame(method_frame, method_request, drain=False)

            header_frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_HEADER, self.channel_id)
            header_frame.declare_class(amqp_constants.CLASS_BASIC)
            header_frame.set_body_size(len(payload))
            encoder = amqp_frame.AmqpEncoder()
            encoder.write_message_properties(properties)
            await self._write_frame(header_frame, encoder, drain=False)

            # split the payload

            frame_max = self.protocol.server_frame_max or len(payload)
            for chunk in (payload[0 + i:frame_max + i] for i in range(0, len(payload), frame_max)):

                content_frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_BODY, self.channel_id)
                content_frame.declare_class(amqp_constants.CLASS_BASIC)
                encoder = amqp_frame.AmqpEncoder()
                if isinstance(chunk, str):
                    encoder.payload.write(chunk.encode())
                else:
                    encoder.payload.write(chunk)
                await self._write_frame(content_frame, encoder, drain=False)

        await self.protocol._drain()

    async def basic_qos(self, prefetch_size=0, prefetch_count=0, connection_global=None):
        """Specifies quality of service.

        Args:
            prefetch_size:
                int, request that messages be sent in advance so that when
                the client finishes processing a message, the following
                message is already held locally
            prefetch_count:
                int: Specifies a prefetch window in terms of whole
                messages. This field may be used in combination with the
                prefetch-size field; a message will only be sent in advance
                if both prefetch windows (and those at the channel and
                connection level) allow it
            connection_global:
                bool: global=false means that the QoS settings should apply
                per-consumer channel; and global=true to mean that the QoS
                settings should apply per-channel.
        """
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_BASIC, amqp_constants.BASIC_QOS)
        request = amqp_frame.AmqpEncoder()
        request.write_long(prefetch_size)
        request.write_short(prefetch_count)
        request.write_bits(connection_global)

        async with self._write_lock:
            return (
                await
                self._write_frame_awaiting_response('basic_qos', frame, request, no_wait=False)
            )

    async def basic_qos_ok(self, frame):
        future = self._get_waiter('basic_qos')
        future.set_result(True)
        logger.debug("Qos ok")

    async def basic_server_nack(self, frame, delivery_tag=None):
        if delivery_tag is None:
            decoder = amqp_frame.AmqpDecoder(frame.payload)
            delivery_tag = decoder.read_long_long()
        fut = self._get_waiter('basic_server_ack_{}'.format(delivery_tag))
        logger.debug('Received nack for delivery tag %r', delivery_tag)
        fut.set_exception(exceptions.PublishFailed(delivery_tag))

    def new_consumer(
        self,
        queue_name='',
        consumer_tag='',
        no_local=False,
        no_ack=False,
        exclusive=False,
        no_wait=False,
        arguments=None
    ):
        """Starts the consumption of message from a queue.

            Usage::

                async with chan.new_consumer(callback, queue_name="my_queue") \
                        as listener:
                    async for body, envelope, properties in listener:
                        await process_message(body, envelope, properties)

            Arguments:
                queue_name:
                    str, the queue to receive message from
                consumer_tag:
                    str, optional consumer tag
                no_local:
                    bool, if set the server will not send messages to the
                    connection that published them.
                no_ack:
                    bool, if set the server does not expect
                    acknowledgements for messages
                exclusive:
                    bool, request exclusive consumer access, meaning only
                    this consumer can access the queue
                no_wait:
                    bool, if set, the server will not respond to the method
                arguments:
                    dict, AMQP arguments to be passed to the server

        If no callback is given, return an iterable which returns (message,
        envelope, properties) triples. You must call its :meth:`aclose`
        coroutine to stop receiving messages.

        Otherwise, the callback function will be called with these three
        arguments, once for each message. Callbacks may be simple functions,
        async coroutines, or Trio tasks.

        In either case, unless you have set :param:`no_ack` to ``True``,
        your code is responsible for calling :meth:`basic_client_ack` or
        :meth:`basic_client_nack` on the envelope's :attribute:`delivery_tag`.
        """
        # If a consumer tag was not passed, create one
        consumer_tag = consumer_tag or 'ctag%i.%s' % (self.channel_id, uuid.uuid4().hex)

        if arguments is None:
            arguments = {}

        return BasicListener(
            self,
            queue_name=queue_name,
            consumer_tag=consumer_tag,
            no_local=no_local,
            no_ack=no_ack,
            exclusive=exclusive,
            no_wait=no_wait,
            arguments=arguments
        )

    async def basic_consume(
        self,
        callback,
        queue_name='',
        consumer_tag='',
        no_local=False,
        no_ack=False,
        exclusive=False,
        no_wait=False,
        arguments=None
    ):
        """Starts the consumption of message from a queue.
        The callback will be called each time we're receiving a message.

            Arguments:
                callback:
                    coroutine, the callback to be executed for each message.
                queue_name:
                    str, the queue to receive message from
                consumer_tag:
                    str, optional consumer tag
                no_local:
                    bool, if set the server will not send messages to the
                    connection that published them.
                no_ack:
                    bool, if set the server does not expect
                    acknowledgements for messages
                exclusive:
                    bool, request exclusive consumer access, meaning only
                    this consumer can access the queue
                no_wait:
                    bool, if set, the server will not respond to the method
                arguments:
                    dict, AMQP arguments to be passed to the server

        The callback function will be called with three arguments,
        once for each message. Callbacks may be simple functions, async
        coroutines, or Trio tasks.

            Callback Args:
                Message:
                    The body of the AMQP message.
                Envelope:
                    An instance of :class:`trio_amqp.envelope.Envelope`.
                Properties:
                    An instance of :class:`trio_amqp.properties.Properties`.

        Unless you have set :param:`no_ack` to ``True``, your code is
        responsible for calling :meth:`basic_client_ack` or
        :meth:`basic_client_nack` on the envelope's
        :attribute:`delivery_tag`.
        """
        # If a consumer tag was not passed, create one
        consumer_tag = consumer_tag or 'ctag%i.%s' % (self.channel_id, uuid.uuid4().hex)

        if arguments is None:
            arguments = {}

        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CONSUME)
        request = amqp_frame.AmqpEncoder()
        request.write_short(0)
        request.write_shortstr(queue_name)
        request.write_shortstr(consumer_tag)
        request.write_bits(no_local, no_ack, exclusive, no_wait)
        request.write_table(arguments)

        self.consumer_callbacks[consumer_tag] = callback
        self.last_consumer_tag = consumer_tag

        async with self._write_lock:
            return_value = await self._write_frame_awaiting_response(
                'basic_consume', frame, request, no_wait
            )
        if no_wait:
            return_value = {'consumer_tag': consumer_tag}
        else:
            self._ctag_events[consumer_tag].set()
        return return_value

    async def basic_consume_ok(self, frame):
        ctag = frame.payload_decoder.read_shortstr()
        results = {
            'consumer_tag': ctag,
        }
        future = self._get_waiter('basic_consume')
        future.set_result(results)
        self._ctag_events[ctag] = trio.Event()

    async def basic_deliver(self, frame):
        response = amqp_frame.AmqpDecoder(frame.payload)
        consumer_tag = response.read_shortstr()
        delivery_tag = response.read_long_long()
        is_redeliver = response.read_bit()
        exchange_name = response.read_shortstr()
        routing_key = response.read_shortstr()
        content_header_frame = await self.protocol.get_frame()

        buffer = io.BytesIO()
        while (buffer.tell() < content_header_frame.body_size):
            content_body_frame = await self.protocol.get_frame()
            buffer.write(content_body_frame.payload)

        body = buffer.getvalue()
        envelope = Envelope(consumer_tag, delivery_tag, exchange_name, routing_key, is_redeliver)
        properties = content_header_frame.properties

        callback = self.consumer_callbacks[consumer_tag]

        event = self._ctag_events.get(consumer_tag)
        if event:
            await event.wait()
            del self._ctag_events[consumer_tag]

        if not inspect.iscoroutinefunction(callback):
            callback(self, body, envelope, properties)
        else:
            cbi = inspect.getfullargspec(callback)
            if 'task_status' in cbi.args or 'task_status' in cbi.kwonlyargs:
                await self.protocol._nursery.start(callback, self, body, envelope, properties)
            else:
                await callback(self, body, envelope, properties)

    async def server_basic_cancel(self, frame):
        # https://www.rabbitmq.com/consumer-cancel.html
        consumer_tag = frame.payload_decoder.read_shortstr()
        _no_wait = frame.payload_decoder.read_bit()  # noqa: F841
        self.cancelled_consumers.add(consumer_tag)
        logger.info("consume cancelled received")
        callback = self.consumer_callbacks.get(consumer_tag, None)
        if callback is None:
            pass
        elif not inspect.iscoroutinefunction(callback):
            callback(self, None, None, None)
        else:
            await callback(self, None, None, None)


    async def basic_cancel(self, consumer_tag, no_wait=False):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_BASIC, amqp_constants.BASIC_CANCEL)
        request = amqp_frame.AmqpEncoder()
        request.write_shortstr(consumer_tag)
        request.write_bits(no_wait)
        async with self._write_lock:
            return (
                await self._write_frame_awaiting_response(
                    'basic_cancel', frame, request, no_wait=no_wait
                )
            )

    async def basic_cancel_ok(self, frame):
        results = {
            'consumer_tag': frame.payload_decoder.read_shortstr(),
        }
        future = self._get_waiter('basic_cancel')
        future.set_result(results)
        logger.debug("Cancel ok")

    async def basic_return(self, frame):
        response = amqp_frame.AmqpDecoder(frame.payload)
        reply_code = response.read_short()
        reply_text = response.read_shortstr()
        exchange_name = response.read_shortstr()
        routing_key = response.read_shortstr()
        content_header_frame = await self.protocol.get_frame()

        buffer = io.BytesIO()
        while buffer.tell() < content_header_frame.body_size:
            content_body_frame = await self.protocol.get_frame()
            buffer.write(content_body_frame.payload)

        body = buffer.getvalue()
        envelope = ReturnEnvelope(reply_code, reply_text,
                                  exchange_name, routing_key)
        properties = content_header_frame.properties
        if self._q is None:
            # they have set mandatory bit, but havent added a callback
            logger.warning('You have received a returned message, but dont iterate the channel for returns.')
        else:
            self._q.put((body, envelope, properties))

    async def basic_get(self, queue_name='', no_ack=False):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_BASIC, amqp_constants.BASIC_GET)
        request = amqp_frame.AmqpEncoder()
        request.write_short(0)
        request.write_shortstr(queue_name)
        request.write_bits(no_ack)
        async with self._write_lock:
            return (
                await
                self._write_frame_awaiting_response('basic_get', frame, request, no_wait=False)
            )

    async def basic_get_ok(self, frame):
        data = {}
        decoder = amqp_frame.AmqpDecoder(frame.payload)
        data['delivery_tag'] = decoder.read_long_long()
        data['redelivered'] = bool(decoder.read_octet())
        data['exchange_name'] = decoder.read_shortstr()
        data['routing_key'] = decoder.read_shortstr()
        data['message_count'] = decoder.read_long()
        content_header_frame = await self.protocol.get_frame()

        buffer = io.BytesIO()
        while (buffer.tell() < content_header_frame.body_size):
            content_body_frame = await self.protocol.get_frame()
            buffer.write(content_body_frame.payload)

        data['message'] = buffer.getvalue()
        data['properties'] = content_header_frame.properties
        future = self._get_waiter('basic_get')
        future.set_result(data)

    async def basic_get_empty(self, frame):
        future = self._get_waiter('basic_get')
        future.set_exception(exceptions.EmptyQueue)

    async def basic_client_ack(self, delivery_tag, multiple=False):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_BASIC, amqp_constants.BASIC_ACK)
        request = amqp_frame.AmqpEncoder()
        request.write_long_long(delivery_tag)
        request.write_bits(multiple)
        async with self._write_lock:
            await self._write_frame(frame, request)

    async def basic_client_nack(self, delivery_tag, multiple=False, requeue=True):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_BASIC, amqp_constants.BASIC_NACK)
        request = amqp_frame.AmqpEncoder()
        request.write_long_long(delivery_tag)
        request.write_bits(multiple, requeue)
        async with self._write_lock:
            await self._write_frame(frame, request)

    async def basic_server_ack(self, frame):
        decoder = amqp_frame.AmqpDecoder(frame.payload)
        delivery_tag = decoder.read_long_long()
        fut = self._get_waiter('basic_server_ack_{}'.format(delivery_tag))
        logger.debug('Received ack for delivery tag %s', delivery_tag)
        fut.set_result(True)

    async def basic_reject(self, delivery_tag, requeue=False):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_BASIC, amqp_constants.BASIC_REJECT)
        request = amqp_frame.AmqpEncoder()
        request.write_long_long(delivery_tag)
        request.write_bits(requeue)
        async with self._write_lock:
            await self._write_frame(frame, request)

    async def basic_recover_async(self, requeue=True):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_BASIC, amqp_constants.BASIC_RECOVER_ASYNC)
        request = amqp_frame.AmqpEncoder()
        request.write_bits(requeue)
        async with self._write_lock:
            await self._write_frame(frame, request)

    async def basic_recover(self, requeue=True):
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_BASIC, amqp_constants.BASIC_RECOVER)
        request = amqp_frame.AmqpEncoder()
        request.write_bits(requeue)
        async with self._write_lock:
            return (
                await self._write_frame_awaiting_response(
                    'basic_recover', frame, request, no_wait=False
                )
            )

    async def basic_recover_ok(self, frame):
        future = self._get_waiter('basic_recover')
        future.set_result(True)
        logger.debug("Cancel ok")


#
# convenient aliases
#

    queue = queue_declare
    exchange = exchange_declare

    async def publish(
        self,
        payload,
        exchange_name,
        routing_key,
        properties=None,
        mandatory=False,
        immediate=False
    ):
        assert payload, "Payload cannot be empty"

        async with self._write_lock:
            if self.publisher_confirms:
                delivery_tag = next(self.delivery_tag_iter)
                fut = self._set_waiter('basic_server_ack_{}'.format(delivery_tag))

            method_frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
            method_frame.declare_method(amqp_constants.CLASS_BASIC, amqp_constants.BASIC_PUBLISH)
            method_request = amqp_frame.AmqpEncoder()
            method_request.write_short(0)
            method_request.write_shortstr(exchange_name)
            method_request.write_shortstr(routing_key)
            method_request.write_bits(mandatory, immediate)
            await self._write_frame(method_frame, method_request, drain=False)

            header_frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_HEADER, self.channel_id)
            header_frame.declare_class(amqp_constants.CLASS_BASIC)
            header_frame.set_body_size(len(payload))
            encoder = amqp_frame.AmqpEncoder()
            encoder.write_message_properties(properties)
            await self._write_frame(header_frame, encoder, drain=False)

            # split the payload

            frame_max = self.protocol.server_frame_max or len(payload)
            for chunk in (payload[0 + i:frame_max + i] for i in range(0, len(payload), frame_max)):

                content_frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_BODY, self.channel_id)
                content_frame.declare_class(amqp_constants.CLASS_BASIC)
                encoder = amqp_frame.AmqpEncoder()
                if isinstance(chunk, str):
                    encoder.payload.write(chunk.encode())
                else:
                    encoder.payload.write(chunk)
                await self._write_frame(content_frame, encoder, drain=False)

        await self.protocol._drain()

        if self.publisher_confirms:
            await fut()

    async def confirm_select(self, *, no_wait=False):
        if self.publisher_confirms:
            raise ValueError('publisher confirms already enabled')
        frame = amqp_frame.AmqpRequest(amqp_constants.TYPE_METHOD, self.channel_id)
        frame.declare_method(amqp_constants.CLASS_CONFIRM, amqp_constants.CONFIRM_SELECT)
        request = amqp_frame.AmqpEncoder()
        request.write_shortstr('')

        async with self._write_lock:
            return (
                await
                self._write_frame_awaiting_response('confirm_select', frame, request, no_wait)
            )

    async def confirm_select_ok(self, frame):
        self.publisher_confirms = True
        self.delivery_tag_iter = count(1)
        fut = self._get_waiter('confirm_select')
        fut.set_result(True)
        logger.debug("Confirm selected")

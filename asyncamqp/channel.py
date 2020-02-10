"""
    Amqp channel specification
"""

import sys
import anyio
import logging
import uuid
import io
import inspect
from itertools import count

from . import constants as amqp_constants
from . import frame as amqp_frame
from . import exceptions
from . import properties as amqp_properties
from .envelope import Envelope, ReturnEnvelope
from .future import Future
from .exceptions import AmqpClosedConnection

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

    async def get(self):
        return await self._q.get()

    async def __aenter__(self):
        await self.channel.basic_consume(self._data, consumer_tag=self.consumer_tag, **self.kwargs)
        self._q = anyio.create_queue(30)  # TODO: 2 + possible prefetch
        return self

    async def __aexit__(self, *tb):
        async with anyio.open_cancel_scope(shield=True):
            try:
                await self.channel.basic_cancel(self.consumer_tag)
            except AmqpClosedConnection:
                pass
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
        self.close_event = anyio.create_event()
        self.cancelled_consumers = set()
        self.last_consumer_tag = None
        self.publisher_confirms = False

        self.delivery_tag_iter = None
        # counting iterator, used for mapping delivered messages
        # to publisher confirms

        self._write_lock = anyio.create_lock()

        self._futures = {}
        self._ctag_events = {}

    def __aiter__(self):
        if self._q is None:
            self._q = anyio.create_queue(30)  # TODO: 2 + possible prefetch
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
            raise exceptions.SynchronizationError("Call %r didn't set a waiter" % rpc_name)
        return fut

    @property
    def is_open(self):
        if self.protocol.connection_closed.is_set():
            return False
        return not self.close_event.is_set()

    async def connection_closed(self, server_code=None, server_reason=None, exception=None):
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
            await future.set_exception(exception)

        self.protocol.release_channel_id(self.channel_id)
        await self.close_event.set()
        if self._q is not None:
            await self._q.put(None)

    async def dispatch_frame(self, frame):
        methods = {
            pamqp.specification.Channel.OpenOk.name: self.open_ok,
            pamqp.specification.Channel.FlowOk.name: self.flow_ok,
            pamqp.specification.Channel.CloseOk.name: self.close_ok,
            pamqp.specification.Channel.Close.name: self.server_channel_close,

            pamqp.specification.Exchange.DeclareOk.name: self.exchange_declare_ok,
            pamqp.specification.Exchange.BindOk.name: self.exchange_bind_ok,
            pamqp.specification.Exchange.UnbindOk.name: self.exchange_unbind_ok,
            pamqp.specification.Exchange.DeleteOk.name: self.exchange_delete_ok,

            pamqp.specification.Queue.DeclareOk.name: self.queue_declare_ok,
            pamqp.specification.Queue.DeleteOk.name: self.queue_delete_ok,
            pamqp.specification.Queue.BindOk.name: self.queue_bind_ok,
            pamqp.specification.Queue.UnbindOk.name: self.queue_unbind_ok,
            pamqp.specification.Queue.PurgeOk.name: self.queue_purge_ok,

            pamqp.specification.Basic.QosOk.name: self.basic_qos_ok,
            pamqp.specification.Basic.ConsumeOk.name: self.basic_consume_ok,
            pamqp.specification.Basic.CancelOk.name: self.basic_cancel_ok,
            pamqp.specification.Basic.GetOk.name: self.basic_get_ok,
            pamqp.specification.Basic.GetEmpty.name: self.basic_get_empty,
            pamqp.specification.Basic.Deliver.name: self.basic_deliver,
            pamqp.specification.Basic.Cancel.name: self.server_basic_cancel,
            pamqp.specification.Basic.Ack.name: self.basic_server_ack,
            pamqp.specification.Basic.Nack.name: self.basic_server_nack,
            pamqp.specification.Basic.RecoverOk.name: self.basic_recover_ok,
            pamqp.specification.Basic.Return.name: self.basic_return,

            pamqp.specification.Confirm.SelectOk.name: self.confirm_select_ok,
        }

        if frame.name not in methods:
            raise NotImplementedError("Frame %s is not implemented" % frame.name)

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

        async with self._write_lock:
            f = self._set_waiter(waiter_id)
            try:
                await self._write_frame(frame, request, check_open=check_open, drain=drain)
            except BaseException as exc:
                self._get_waiter(waiter_id)
                await f.cancel()
                raise
            res = await f()
            return res

#
# Channel class implementation
#

    async def open(self):
        """Open the channel on the server."""
        request = pamqp.specification.Channel.Open()
        return (
            await self._write_frame_awaiting_response(
                'open', self.channel_id, request, no_wait=False, check_open=False
            )
        )

    async def open_ok(self, frame):
        self.close_event = anyio.create_event()
        fut = self._get_waiter('open')
        await fut.set_result(True)
        logger.debug("Channel is open")

    async def close(self, reply_code=0, reply_text="Normal Shutdown"):
        """Close the channel."""
        if not self.is_open:
            raise exceptions.ChannelClosed("channel already closed or closing")
        await self.close_event.set()
        if self._q is not None:
            self._q.put_nowait(None)
        request = pamqp.specification.Channel.Close(reply_code, reply_text, class_id=0, method_id=0)
        return await self._write_frame_awaiting_response('close', self.channel_id, request, no_wait=False, check_open=False)

    async def close_ok(self, frame):
        try:
            w = self._get_waiter('close')
        except asyncamqp.exceptions.SynchronizationError:
            pass
        else:
            await w.set_result(True)
        logger.info("Channel closed")
        self.protocol.release_channel_id(self.channel_id)

    async def _send_channel_close_ok(self):
        request = pamqp.specification.Channel.CloseOk()
        # intentionally not locked
        await self._write_frame(channel_id, request)

    async def server_channel_close(self, frame):
        try:
            await self._send_channel_close_ok()
        except exceptions.ChannelClosed:
            pass
        results = {
            'reply_code': frame.reply_code,
            'reply_text': frame.reply_text,
            'class_id': frame.class_id,
            'method_id': frame.method_id,
        }
        await self.connection_closed(results['reply_code'], results['reply_text'])

    async def flow(self, active):
        request = pamqp.specification.Channel.Flow(active)
        return await self._write_frame_awaiting_response('flow', self.channel_id, request, no_wait=False, check_open=False)

    async def flow_ok(self, frame):
        self.close_event = anyio.create_event()
        fut = self._get_waiter('flow')
        await fut.set_result({'active': frame.active})

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
        request = pamqp.specification.Exchange.Declare(
            exchange=exchange_name,
            exchange_type=type_name,
            passive=passive,
            durable=durable,
            auto_delete=auto_delete,
            nowait=no_wait,
            arguments=arguments
        )

        return await self._write_frame_awaiting_response('exchange_declare', self.channel_id, request, no_wait)

    async def exchange_declare_ok(self, frame):
        future = self._get_waiter('exchange_declare')
        await future.set_result(True)
        logger.debug("Exchange declared")
        return future

    async def exchange_delete(self, exchange_name, if_unused=False, no_wait=False):
        request = pamqp.specification.Exchange.Delete(exchange=exchange_name, if_unused=if_unused, nowait=no_wait)

        return await self._write_frame_awaiting_response('exchange_delete', frame, request, no_wait)

    async def exchange_delete_ok(self, frame):
        future = self._get_waiter('exchange_delete')
        await future.set_result(True)
        logger.debug("Exchange deleted")

    async def exchange_bind(
        self, exchange_destination, exchange_source, routing_key, no_wait=False, arguments=None
    ):
        if arguments is None:
            arguments = {}
        request = pamqp.specification.Exchange.Bind(
            destination=exchange_destination,
            source=exchange_source,
            routing_key=routing_key,
            nowait=no_wait,
            arguments=arguments
        )
        return await self._write_frame_awaiting_response('exchange_bind', self.channel_id, request, no_wait)

    async def exchange_bind_ok(self, frame):
        future = self._get_waiter('exchange_bind')
        await future.set_result(True)
        logger.debug("Exchange bound")

    async def exchange_unbind(
        self, exchange_destination, exchange_source, routing_key, no_wait=False, arguments=None
    ):
        if arguments is None:
            arguments = {}

        request = pamqp.specification.Exchange.Unbind(
            destination=exchange_destination,
            source=exchange_source,
            routing_key=routing_key,
            nowait=no_wait,
            arguments=arguments,
        )
        return await self._write_frame_awaiting_response('exchange_unbind', self.channel_id, request, no_wait)

    async def exchange_unbind_ok(self, frame):
        future = self._get_waiter('exchange_unbind')
        await future.set_result(True)
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
        request = pamqp.specification.Queue.Declare(
            queue=queue_name,
            passive=passive,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            nowait=no_wait,
            arguments=arguments
        )
        return await self._write_frame_awaiting_response('queue_declare', self.channel_id, request, no_wait)

    async def queue_declare_ok(self, frame):
        results = {
            'queue': frame.queue,
            'message_count': frame.message_count,
            'consumer_count': frame.consumer_count,
        }
        future = self._get_waiter('queue_declare')
        await future.set_result(results)
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
        request = pamqp.specification.Queue.Delete(
            queue=queue_name,
            if_unused=if_unused,
            if_empty=if_empty,
            nowait=no_wait
        )
        return await self._write_frame_awaiting_response('queue_delete', self.channel_id, request, no_wait)

    async def queue_delete_ok(self, frame):
        future = self._get_waiter('queue_delete')
        await future.set_result(True)
        logger.debug("Queue deleted")

    async def queue_bind(
        self, queue_name, exchange_name, routing_key, no_wait=False, arguments=None
    ):
        """Bind a queue to an exchange."""
        if arguments is None:
            arguments = {}
        request = pamqp.specification.Queue.Bind(
            queue=queue_name,
            exchange=exchange_name,
            routing_key=routing_key,
            nowait=no_wait,
            arguments=arguments
        )
        return await self._write_frame_awaiting_response('queue_bind', self.channel_id, request, no_wait)

    async def queue_bind_ok(self, frame):
        future = self._get_waiter('queue_bind')
        await future.set_result(True)
        logger.debug("Queue bound")

    async def queue_unbind(self, queue_name, exchange_name, routing_key, arguments=None):
        if arguments is None:
            arguments = {}
        request = pamqp.specification.Queue.Unbind(
            queue=queue_name,
            exchange=exchange_name,
            routing_key=routing_key,
            arguments=arguments
        )
        return await self._write_frame_awaiting_response('queue_unbind', self.channel_id, request, no_wait=False)

    async def queue_unbind_ok(self, frame):
        future = self._get_waiter('queue_unbind')
        await future.set_result(True)
        logger.debug("Queue unbound")

    async def queue_purge(self, queue_name, no_wait=False):
        request = pamqp.specification.Queue.Purge(
            queue=queue_name, nowait=no_wait
        )
        return await self._write_frame_awaiting_response('queue_purge', self.channel_id, request, no_wait=no_wait)

    async def queue_purge_ok(self, frame):
        future = self._get_waiter('queue_purge')
        await future.set_result({'message_count': frame.message_count})

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
        async with self._write_lock:
            if properties is None:
                properties = {}

            method_request = pamqp.specification.Basic.Publish(
                exchange=exchange_name,
                routing_key=routing_key,
                mandatory=mandatory,
                immediate=immediate
            )

            await self._write_frame(self.channel_id, method_request, drain=False)

            header_request = pamqp.header.ContentHeader(
                body_size=len(payload),
                properties=pamqp.specification.Basic.Properties(**properties)
            )
            await self._write_frame(self.channel_id, header_request, drain=False)

            frame_max = self.protocol.server_frame_max or len(payload)
            for chunk in (payload[0 + i:frame_max + i] for i in range(0, len(payload), frame_max)):

                content_request = pamqp.body.ContentBody(chunk)
                await self._write_frame(self.channel_id, content_request, drain=False)

        await self.protocol._drain()

    async def basic_qos(self, prefetch_size=0, prefetch_count=0, connection_global=False):
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
        request = pamqp.specification.Basic.Qos(
            prefetch_size, prefetch_count, connection_global
        )
        return await self._write_frame_awaiting_response('basic_qos', self.channel_id, request, no_wait=False)

    async def basic_qos_ok(self, frame):
        future = self._get_waiter('basic_qos')
        await future.set_result(True)
        logger.debug("Qos ok")

    async def basic_server_nack(self, frame, delivery_tag=None):
        if delivery_tag is None:
            delivery_tag = frame.delivery_tag
        fut = self._get_waiter('basic_server_ack_{}'.format(delivery_tag))
        logger.debug('Received nack for delivery tag %r', delivery_tag)
        await fut.set_exception(exceptions.PublishFailed(delivery_tag))

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

                async with chan.new_consumer(queue_name="my_queue") \
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
        envelope, properties) triples.

        Otherwise, the callback function will be called with these three
        arguments, once for each message. Callbacks may be simple functions,
        async coroutines, or AnyIO tasks.

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
        coroutines, or AnyIO tasks.

            Callback Args:
                Message:
                    The body of the AMQP message.
                Envelope:
                    An instance of :class:`asyncamqp.envelope.Envelope`.
                Properties:
                    An instance of :class:`asyncamqp.properties.Properties`.

        Unless you have set :param:`no_ack` to ``True``, your code is
        responsible for calling :meth:`basic_client_ack` or
        :meth:`basic_client_nack` on the envelope's
        :attribute:`delivery_tag`.
        """
        # If a consumer tag was not passed, create one
        consumer_tag = consumer_tag or 'ctag%i.%s' % (self.channel_id, uuid.uuid4().hex)

        if arguments is None:
            arguments = {}

        request = pamqp.specification.Basic.Consume(
            queue=queue_name,
            consumer_tag=consumer_tag,
            no_local=no_local,
            no_ack=no_ack,
            exclusive=exclusive,
            nowait=no_wait,
            arguments=arguments
        )

        self.consumer_callbacks[consumer_tag] = callback
        self.last_consumer_tag = consumer_tag

        return_value = await self._write_frame_awaiting_response('basic_consume', self.channel_id, request, no_wait)
        if no_wait:
            return_value = {'consumer_tag': consumer_tag}
        else:
            await self._ctag_events[consumer_tag].set()
        return return_value

    async def basic_consume_ok(self, frame):
        results = {
            'consumer_tag': frame.consumer_tag
        }
        future = self._get_waiter('basic_consume')
        await future.set_result(results)
        self._ctag_events[ctag] = anyio.create_event()

    async def basic_deliver(self, frame):
        consumer_tag = frame.consumer_tag
        delivery_tag = frame.delivery_tag
        is_redeliver = frame.redelivered
        exchange_name = frame.exchange
        routing_key = frame.routing_key
        channel, content_header_frame = await self.protocol.get_frame()

        buffer = io.BytesIO()
        while (buffer.tell() < content_header_frame.body_size):
            _channel, content_body_frame = await self.protocol.get_frame()
            buffer.write(content_body_frame.value)

        body = buffer.getvalue()
        envelope = Envelope(consumer_tag, delivery_tag, exchange_name, routing_key, is_redeliver)
        properties = amqp_properties.from_pamqp(content_header_frame.properties)

        callback = self.consumer_callbacks[consumer_tag]

        event = self._ctag_events.get(consumer_tag)
        if event:
            await event.wait()
            del self._ctag_events[consumer_tag]

        res = callback(self, body, envelope, properties)
        if inspect.iscoroutine(res):
            res = await res

    async def server_basic_cancel(self, frame):
        # https://www.rabbitmq.com/consumer-cancel.html
        consumer_tag = frame.consumer_tag
        _no_wait = frame.nowait
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
        request = pamqp.specification.Basic.Cancel(consumer_tag, no_wait)
        return await self._write_frame_awaiting_response('basic_cancel', self.channel_id, request, no_wait=no_wait)

    async def basic_cancel_ok(self, frame):
        results = {
            'consumer_tag': frame.consumer_tag,
        }
        future = self._get_waiter('basic_cancel')
        await future.set_result(results)
        logger.debug("Cancel ok")

    async def basic_return(self, frame):
        reply_code = frame.reply_code
        reply_text = frame.reply_text
        exchange_name = frame.exchange
        routing_key = frame.routing_key
        channel, content_header_frame = await self.protocol.get_frame()

        buffer = io.BytesIO()
        while buffer.tell() < content_header_frame.body_size:
            _channel, content_body_frame = await self.protocol.get_frame()
            buffer.write(content_body_frame.value)

        body = buffer.getvalue()
        envelope = ReturnEnvelope(reply_code, reply_text,
                                  exchange_name, routing_key)
        properties = amqp_properties.from_pamqp(content_header_frame.properties)
        if self._q is None:
            # they have set mandatory bit, but aren't reading
            logger.warning("You don't iterate the channel for returned messages!")
        else:
            await self._q.put((body, envelope, properties))

    async def basic_get(self, queue_name='', no_ack=False):
        request = pamqp.specification.Basic.Get(queue=queue_name, no_ack=no_ack)
        return await self._write_frame_awaiting_response('basic_get', self.channel_id, request, no_wait=False)

    async def basic_get_ok(self, frame):
        data = {
            'delivery_tag': frame.delivery_tag,
            'redelivered': frame.redelivered,
            'exchange_name': frame.exchange,
            'routing_key': frame.routing_key,
            'message_count': frame.message_count,
        }
        _channel, content_header_frame = await self.protocol.get_frame()

        buffer = io.BytesIO()
        while (buffer.tell() < content_header_frame.body_size):
            _channel, content_body_frame = await self.protocol.get_frame()
            buffer.write(content_body_frame.payload)

        data['message'] = buffer.getvalue()
        data['properties'] = amqp_properties.from_pamqp(content_header_frame.properties)
        future = self._get_waiter('basic_get')
        await future.set_result(data)

    async def basic_get_empty(self, frame):
        future = self._get_waiter('basic_get')
        await future.set_exception(exceptions.EmptyQueue)

    async def basic_client_ack(self, delivery_tag, multiple=False):
        request = pamqp.specification.Basic.Ack(delivery_tag, multiple)
        request.write_bits(multiple)
        async with self._write_lock:
            await self._write_frame(self.channel_id, request)

    async def basic_client_nack(self, delivery_tag, multiple=False, requeue=True):
        request = pamqp.specification.Basic.Nack(delivery_tag, multiple, requeue)
        async with self._write_lock:
            await self._write_frame(self.channel_id, request)

    async def basic_server_ack(self, frame):
        delivery_tag = frame.delivery_tag
        fut = self._get_waiter('basic_server_ack_{}'.format(delivery_tag))
        logger.debug('Received ack for delivery tag %s', delivery_tag)
        await fut.set_result(True)

    async def basic_reject(self, delivery_tag, requeue=False):
        request = pamqp.specification.Basic.Reject(delivery_tag, requeue)
        async with self._write_lock:
            await self._write_frame(self.channel_id, request)

    async def basic_recover_async(self, requeue=True):
        request = pamqp.specification.Basic.RecoverAsync(requeue)
        async with self._write_lock:
            await self._write_frame(self.channel_id, request)

    async def basic_recover(self, requeue=True):
        request = pamqp.specification.Basic.Recover(requeue)
        return await self._write_frame_awaiting_response('basic_recover', self.channel_id, request, no_wait=False)

    async def basic_recover_ok(self, frame):
        future = self._get_waiter('basic_recover')
        await future.set_result(True)
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
        async with self._write_lock:
            if self.publisher_confirms:
                delivery_tag = next(self.delivery_tag_iter)
                fut = self._set_waiter('basic_server_ack_{}'.format(delivery_tag))

            method_request = pamqp.specification.Basic.Publish(
                exchange=exchange_name,
                routing_key=routing_key,
                mandatory=mandatory,
                immediate=immediate
            )
            await self._write_frame(self.channel_id, method_request, drain=False)

            properties = pamqp.specification.Basic.Properties(**properties)
            header_request = pamqp.header.ContentHeader(
                body_size=len(payload), properties=properties
            )

            await self._write_frame(self.channel_id, encoder, drain=False)

            # split the payload

            frame_max = self.protocol.server_frame_max or len(payload)
            for chunk in (payload[0 + i:frame_max + i] for i in range(0, len(payload), frame_max)):
                content_request = pamqp.body.ContentBody(chunk)
                await self._write_frame(self.channel_id, encoder, drain=False)

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

        return await self._write_frame_awaiting_response('confirm_select', frame, request, no_wait)

    async def confirm_select_ok(self, frame):
        self.publisher_confirms = True
        self.delivery_tag_iter = count(1)
        fut = self._get_waiter('confirm_select')
        await fut.set_result(True)
        logger.debug("Confirm selected")

"""AsyncAmqp tests utilities

Provides the test case to simplify testing
"""

import anyio
import inspect
import logging
import os
import time
import uuid
import pytest
from functools import wraps
try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager

import pyrabbit2 as pyrabbit

from . import testcase
from async_amqp import exceptions, connect_amqp
from async_amqp.channel import Channel
from async_amqp.protocol import AmqpProtocol, OPEN
from anyio._core._compat import DeprecatedAwaitable

logger = logging.getLogger(__name__)


def use_full_name(f, arg_names):
    sig = inspect.signature(f)
    for arg_name in arg_names:
        if arg_name not in sig.parameters:
            raise ValueError(
                '%s is not a valid argument name for function %s' % (arg_name, f.__qualname__)
            )

    @wraps(f)
    def wrapper(self, *args, **kw):
        ba = sig.bind_partial(self, *args, **kw)
        for param in sig.parameters.values():
            if param.name in arg_names and param.name in ba.arguments:
                ba.arguments[param.name] = self.full_name(ba.arguments[param.name])
        return f(*(ba.args), **(ba.kwargs))

    return wrapper


class ProxyChannel(Channel):
    exchange_declare = use_full_name(Channel.exchange_declare, ['exchange_name'])
    exchange_delete = use_full_name(Channel.exchange_delete, ['exchange_name'])
    queue_declare = use_full_name(Channel.queue_declare, ['queue_name'])
    queue_delete = use_full_name(Channel.queue_delete, ['queue_name'])
    queue_bind = use_full_name(Channel.queue_bind, ['queue_name', 'exchange_name'])
    queue_unbind = use_full_name(Channel.queue_unbind, ['queue_name', 'exchange_name'])
    queue_purge = use_full_name(Channel.queue_purge, ['queue_name'])

    exchange_bind = use_full_name(
        Channel.exchange_bind, ['exchange_source', 'exchange_destination']
    )
    exchange_unbind = use_full_name(
        Channel.exchange_unbind, ['exchange_source', 'exchange_destination']
    )
    publish = use_full_name(Channel.publish, ['exchange_name'])
    basic_get = use_full_name(Channel.basic_get, ['queue_name'])
    basic_consume = use_full_name(Channel.basic_consume, ['queue_name'])

    def full_name(self, name):
        conn = self.protocol
        if conn is None:
            return name
        return conn.full_name(name)


_seq = 0


class ProxyAmqpProtocol(AmqpProtocol):
    test_seq = None
    CHANNEL_FACTORY = ProxyChannel

    def __init__(self, *args, test_seq=None, **kwargs):
        if test_seq is None:
            global _seq
            _seq += 1
            test_seq = _seq
        super().__init__(*args, **kwargs)
        self.test_seq = test_seq

    @property
    def shortname(self):
        return "tc_%03d_" % self.test_seq

    async def _stop(self):
        pass

    def full_name(self, name):
        if self.is_full_name(name):
            return name
        return self.shortname + name

    def local_name(self, name):
        if self.is_full_name(name):
            return name[len(self.shortname):]
        return name

    def is_full_name(self, name):
        return name.startswith(self.shortname)


def reset_vhost():
    host = os.environ.get('AMQP_HOST', 'localhost')
    port = int(os.environ.get('AMQP_PORT', 5672))
    vhost = os.environ.get('AMQP_VHOST', 'test' + str(uuid.uuid4()))
    http_client = pyrabbit.api.Client(
        '%s:%s' % (host, 10000 + port), 'guest', 'guest', timeout=20
    )

    try:
        http_client.delete_vhost(vhost)
    except Exception:  # pylint: disable=broad-except
        pass

    http_client.create_vhost(vhost)
    http_client.set_vhost_permissions(
        vname=vhost, username='guest', config='.*', rd='.*', wr='.*',
    )


def connect(*a, **kw):
    return connect_amqp(*a, protocol=ProxyAmqpProtocol, **kw)


class FakeScope:
    def __init__(self, scope):
        self.scope = scope

    def cancel(self):
        self.scope.cancel()


class TaskGroup:
    def __init__(self, nursery) -> None:
        self._nursery = nursery
        self.cancel_scope = FakeScope(nursery.cancel_scope)
             
    def start_soon(self, func, *args, name=None) -> None:
        self._nursery.start_soon(func, *args, name=name)

    async def start(self, func, *args, name=None) -> None:
        return await self._nursery.start(func, *args, name=name)



@pytest.fixture
async def amqp(request, nursery):
    reset_vhost()
    async with ProxyAmqpProtocol(
        nursery=TaskGroup(nursery),
        host=os.environ.get('AMQP_HOST', 'localhost'),
        port=int(os.environ.get('AMQP_PORT', 5672)),
        virtualhost=os.environ.get('AMQP_VHOST', 'test' + str(uuid.uuid4())),
    ) as conn:
        yield conn


@pytest.fixture
async def channel(request, nursery):
    reset_vhost()
    # XXX using another async fixture does not work yet
    async with ProxyAmqpProtocol(
        nursery=TaskGroup(nursery),
        host=os.environ.get('AMQP_HOST', 'localhost'),
        port=int(os.environ.get('AMQP_PORT', 5672)),
        virtualhost=os.environ.get('AMQP_VHOST', 'test' + str(uuid.uuid4())),
    ) as conn:
        async with conn.new_channel() as channel:
            yield channel


class RabbitTestCase:
    """TestCase with a rabbit running in background"""

    RABBIT_TIMEOUT = 1.0
    amqp = None

    def setup(self):
        self.host = os.environ.get('AMQP_HOST', 'localhost')
        self.port = int(os.environ.get('AMQP_PORT', 5672))
        self.vhost = os.environ.get('AMQP_VHOST', 'test' + str(uuid.uuid4()))
        self.http_client = pyrabbit.api.Client(
            '%s:%s' % (self.host, 10000 + self.port), 'guest', 'guest', timeout=20
        )

        self.amqps = []
        self.exchanges = {}
        self.queues = {}
        self.transports = []

    async def start(self, rc=None):
        pass

    async def stop(self):
        for queue_name, channel in self.queues.values():
            logger.debug('Delete queue %s', self.full_name(queue_name))
            await self.safe_queue_delete(queue_name, channel)
        for exchange_name, channel in self.exchanges.values():
            logger.debug('Delete exchange %s', self.full_name(exchange_name))
            await self.safe_exchange_delete(exchange_name, channel)
        for amqp in self.amqps:
            if amqp.state != OPEN:
                continue
            logger.debug('Kill off amqp %s', amqp)
            amqp._nursery.cancel_scope.cancel()
            await amqp.close()

    def teardown(self):
        pass
#       try:
#           self.http_client.delete_vhost(self.vhost)
#       except Exception as exc:  # pylint: disable=broad-except
#           pass

    def reset_vhost(self):
        reset_vhost()  # global

    def server_version(self, amqp):
        server_version = tuple(int(x) for x in amqp.server_properties['version'].split('.'))
        return server_version

    async def check_exchange_exists(self, exchange_name):
        """Check if the exchange exist"""
        try:
            await self.exchange_declare(exchange_name, passive=True)
        except exceptions.ChannelClosed:
            return False

        return True

    async def assertExchangeExists(self, exchange_name):
        if not self.check_exchange_exists(exchange_name):
            self.fail("Exchange {} does not exists".format(exchange_name))

    async def check_queue_exists(self, queue_name):
        """Check if the queue exist"""
        try:
            await self.queue_declare(queue_name, passive=True)
        except exceptions.ChannelClosed:
            return False

        return True

    async def assertQueueExists(self, queue_name):
        if not self.check_queue_exists(queue_name):
            self.fail("Queue {} does not exists".format(queue_name))

    async def list_queues(self, amqp, vhost=None, fully_qualified_name=False, delay=None):
        # wait for the http client to get the correct state of the queue
        if delay is None:
            delay = int(os.environ.get('AMQP_REFRESH_TIME', 1.1))
        await anyio.sleep(delay)
        queues_list = self.http_client.get_queues(vhost=vhost or self.vhost)
        queues = {}
        for queue_info in queues_list:
            queue_name = queue_info['name']
            if fully_qualified_name is False:
                queue_name = amqp.local_name(queue_info['name'])
                queue_info['name'] = queue_name

            queues[queue_name] = queue_info
        return queues

    async def check_messages(self, amqp, queue_name, num_msg):
        for x in range(20):
            try:
                queues = await self.list_queues(amqp)
                assert queue_name in queues
                q = queues[queue_name]
                try:
                    q['messages_ready_ram'] == 1
                except (KeyError, AssertionError):
                    try:
                        assert q['messages'] == 1
                    except (KeyError, AssertionError):
                        assert q['message_stats']['publish'] == 1
            except (KeyError, AssertionError) as exc:
                ex = exc
            else:
                break
            await anyio.sleep(0.5)
        else:
            raise ex

    async def safe_queue_delete(self, queue_name, channel):
        """Delete the queue but does not raise any exception if it fails

        The operation has a timeout as well.
        """
        full_queue_name = channel.protocol.full_name(queue_name)
        try:
            await channel.queue_delete(full_queue_name, no_wait=False)
        except TimeoutError:
            logger.warning('Timeout on queue %s deletion', full_queue_name, exc_info=True)
        except Exception:  # pylint: disable=broad-except
            logger.exception('Unexpected error on queue %s deletion', full_queue_name)

    async def safe_exchange_delete(self, exchange_name, channel=None):
        """Delete the exchange but does not raise any exception if it fails

        The operation has a timeout as well.
        """
        channel = channel or self.channel
        full_exchange_name = self.full_name(exchange_name)
        try:
            await channel.exchange_delete(full_exchange_name, no_wait=False)
        except TimeoutError:
            logger.warning('Timeout on exchange %s deletion', full_exchange_name, exc_info=True)
        except Exception:  # pylint: disable=broad-except
            logger.exception('Unexpected error on exchange %s deletion', full_exchange_name)

    async def queue_declare(self, queue_name, *args, channel=None, safe_delete_before=True, **kw):
        channel = channel or self.channel
        if safe_delete_before:
            await self.safe_queue_delete(queue_name, channel=channel)
        # prefix queue_name with the test name
        full_queue_name = self.full_name(queue_name)
        try:
            rep = await channel.queue_declare(full_queue_name, *args, **kw)

        finally:
            self.queues[queue_name] = (queue_name, channel)
        return rep

    async def exchange_declare(
        self, exchange_name, *args, channel=None, safe_delete_before=True, **kw
    ):
        channel = channel or self.channel
        if safe_delete_before:
            await self.safe_exchange_delete(exchange_name, channel=channel)
        # prefix exchange name
        full_exchange_name = self.full_name(exchange_name)
        try:
            rep = await channel.exchange_declare(full_exchange_name, *args, **kw)
        finally:
            self.exchanges[exchange_name] = (exchange_name, channel)
        return rep

    @asynccontextmanager
    async def create_amqp(self, vhost=None, test_seq=None):
        async with testcase.connect(
            host=self.host, port=self.port, virtualhost=vhost or self.vhost, test_seq=test_seq
        ) as protocol:
            self.amqps.append(protocol)
            yield protocol

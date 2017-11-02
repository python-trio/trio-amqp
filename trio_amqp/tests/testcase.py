"""TrioAmqp tests utilities

Provides the test case to simplify testing
"""

import trio
import inspect
import logging
import os
import time
import uuid
import pytest
from functools import wraps

import pyrabbit.api

from . import testing
from .. import connect as trio_amqp_connect
from .. import exceptions
from ..channel import Channel
from ..protocol import AmqpProtocol, OPEN


logger = logging.getLogger(__name__)


def use_full_name(f, arg_names):
    sig = inspect.signature(f)
    for arg_name in arg_names:
        if arg_name not in sig.parameters:
            raise ValueError('%s is not a valid argument name for function %s' % (arg_name, f.__qualname__))

    @wraps(f)
    def wrapper(self, *args, **kw):
        ba = sig.bind_partial(self, *args, **kw)
        for param in sig.parameters.values():
            if param.name in arg_names and param.name in ba.arguments:
                ba.arguments[param.name] = self.full_name(ba.arguments[param.name])
        return f(*(ba.args), **(ba.kwargs))

    return wrapper


class ProxyChannel(Channel):
    def __init__(self, test_case, *args, **kw):
        super().__init__(*args, **kw)
        self.test_case = test_case
        self.test_case.register_channel(self)

    exchange_declare = use_full_name(Channel.exchange_declare, ['exchange_name'])
    exchange_delete = use_full_name(Channel.exchange_delete, ['exchange_name'])
    queue_declare = use_full_name(Channel.queue_declare, ['queue_name'])
    queue_delete = use_full_name(Channel.queue_delete, ['queue_name'])
    queue_bind = use_full_name(Channel.queue_bind, ['queue_name', 'exchange_name'])
    queue_unbind = use_full_name(Channel.queue_unbind, ['queue_name', 'exchange_name'])
    queue_purge = use_full_name(Channel.queue_purge, ['queue_name'])

    exchange_bind = use_full_name(Channel.exchange_bind, ['exchange_source', 'exchange_destination'])
    exchange_unbind = use_full_name(Channel.exchange_unbind, ['exchange_source', 'exchange_destination'])
    publish = use_full_name(Channel.publish, ['exchange_name'])
    basic_get = use_full_name(Channel.basic_get, ['queue_name'])
    basic_consume = use_full_name(Channel.basic_consume, ['queue_name'])

    def full_name(self, name):
        tc = self.test_case
        if tc is None:
            return name
        return tc.full_name(name)


class ProxyAmqpProtocol(AmqpProtocol):
    test_case = None
    def channel_factory(self, protocol, channel_id):
        return ProxyChannel(self.test_case, protocol, channel_id)
    CHANNEL_FACTORY = channel_factory

    def __init__(self, *args, test_case=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_case = test_case

@pytest.fixture
def amqp():
    conn = ProxyAmqpProtocol(
        host = os.environ.get('AMQP_HOST', 'localhost'),
        port = int(os.environ.get('AMQP_PORT', 5672)),
        virtualhost = os.environ.get('AMQP_VHOST','test' + str(uuid.uuid4())),
    )
    return conn

class RabbitTestCase(testing.AsyncioTestCaseMixin):
    """TestCase with a rabbit running in background"""

    RABBIT_TIMEOUT = 1.0

    def setup(self):
        self.host = os.environ.get('AMQP_HOST', 'localhost')
        self.port = int(os.environ.get('AMQP_PORT', 5672))
        self.vhost = os.environ.get('AMQP_VHOST', 'test' + str(uuid.uuid4()))
        self.http_client = pyrabbit.api.Client(
            '%s:%s/' % (self.host, 10000+self.port), 'guest', 'guest', timeout=20
        )

        self.amqps = []
        self.channels = []
        self.exchanges = {}
        self.queues = {}
        self.transports = []

    async def start(self):
        channel = await self.create_channel()
        self.channels.append(channel)
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
            logger.debug('Delete amqp %s', amqp)
            await amqp.aclose()
            del amqp

    def teardown(self):
        try:
            self.http_client.delete_vhost(self.vhost)
        except Exception as exc:  # pylint: disable=broad-except
            pass

    def reset_vhost(self):
        try:
            self.http_client.delete_vhost(self.vhost)
        except Exception:  # pylint: disable=broad-except
            pass

        try:
            self.http_client.create_vhost(self.vhost)
            self.http_client.set_vhost_permissions(
                vname=self.vhost, username='guest', config='.*', rd='.*', wr='.*',
            )
        except Exception:  # pylint: disable=broad-except
            pass

    async def initial_channel(self):
        channel = await self.create_channel()
        self.channels.append(channel)

    @property
    def channel(self):
        return self.channels[0]

    def server_version(self, amqp=None):
        if amqp is None:
            amqp = self.amqp

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

    def list_queues(self, vhost=None, fully_qualified_name=False, delay=None):
        # wait for the http client to get the correct state of the queue
        if delay is None:
            delay = int(os.environ.get('AMQP_REFRESH_TIME', 1.1))
        time.sleep(delay)
        queues_list = self.http_client.get_queues(vhost=vhost or self.vhost)
        queues = {}
        for queue_info in queues_list:
            queue_name = queue_info['name']
            if fully_qualified_name is False:
                queue_name = self.local_name(queue_info['name'])
                queue_info['name'] = queue_name

            queues[queue_name] = queue_info
        return queues

    async def check_messages(self, queue_name, num_msg):
        for x in range(20):
            try:
                queues = self.list_queues()
                assert queue_name in queues
                q = queues[queue_name]
                try:
                    q['messages_ready_ram'] == 1
                except (KeyError,AssertionError):
                    try:
                        assert q['messages'] == 1
                    except (KeyError,AssertionError):
                        assert q['message_stats']['publish'] == 1
            except (KeyError,AssertionError) as exc:
                ex = exc
            else:
                break
            await trio.sleep(0.5)
        else:
            raise ex

    async def safe_queue_delete(self, queue_name, channel=None):
        """Delete the queue but does not raise any exception if it fails

        The operation has a timeout as well.
        """
        channel = channel or self.channel
        full_queue_name = self.full_name(queue_name)
        try:
            await channel.queue_delete(full_queue_name, no_wait=False)
        except trio.TooSlowError:
            logger.warning('Timeout on queue %s deletion', full_queue_name, exc_info=True)
        except Exception:  # pylint: disable=broad-except
            logger.error('Unexpected error on queue %s deletion', full_queue_name, exc_info=True)

    async def safe_exchange_delete(self, exchange_name, channel=None):
        """Delete the exchange but does not raise any exception if it fails

        The operation has a timeout as well.
        """
        channel = channel or self.channel
        full_exchange_name = self.full_name(exchange_name)
        try:
            await channel.exchange_delete(full_exchange_name, no_wait=False)
        except trio.TooSlowError:
            logger.warning('Timeout on exchange %s deletion', full_exchange_name, exc_info=True)
        except Exception:  # pylint: disable=broad-except
            logger.error('Unexpected error on exchange %s deletion', full_exchange_name, exc_info=True)

    def full_name(self, name):
        if self.is_full_name(name):
            return name
        return repr(self) + '.' + name

    def local_name(self, name):
        if self.is_full_name(name):
            return name[len(repr(self)) + 1:]  # +1 because of the '.'
        return name

    def is_full_name(self, name):
        return name.startswith(repr(self))

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

    async def exchange_declare(self, exchange_name, *args, channel=None, safe_delete_before=True, **kw):
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

    def register_channel(self, channel):
        self.channels.append(channel)

    async def create_channel(self, amqp=None):
        amqp = amqp or self.amqp
        channel = await amqp.channel()
        return channel

    def create_amqp(self, vhost=None):
        vhost = vhost or self.vhost
        protocol = ProxyAmqpProtocol(host=self.host, port=self.port, virtualhost=vhost,
            test_case=self)
        self.amqps.append(protocol)
        return protocol


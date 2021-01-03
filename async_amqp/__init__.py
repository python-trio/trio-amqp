from urllib.parse import urlparse
try:
    from contextlib import asynccontextmanager
except ImportError:
    from async_generator import asynccontextmanager

from .exceptions import *  # pylint: disable=wildcard-import  # noqa: F401,F403
from .protocol import AmqpProtocol  # noqa: F401

from . import protocol
connect_amqp = protocol.connect_amqp


@asynccontextmanager
async def connect_from_url(url, **kwargs):
    """Connect to the AMQP using a single url parameter.

        @url:    amqp:// or amqps:// URL with connection parameters
        @kwargs: Further arguments for async_amqp.connect_amqp()

        Usage:
            async with connect_from_url(
                "amqp://user:password@hostname:port/vhost"
            ) as amqp:
                await do_whatever(amqp)
    """
    url = urlparse(url)

    if url.scheme not in ('amqp', 'amqps'):
        raise ValueError('Invalid protocol %s, valid protocols are amqp or amqps' % url.scheme)

    if url.hostname:
        kwargs['host'] = url.hostname
    if url.port:
        kwargs['port'] = url.port
    if url.username:
        kwargs['login'] = url.username
    if url.password:
        kwargs['password'] = url.password
    async with connect_amqp(
        virtualhost=(url.path[1:] if len(url.path) > 1 else '/'),
        ssl=(url.scheme == 'amqps'),
        **kwargs
    ) as amqp:
        yield amqp

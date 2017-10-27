import trio
import sys
from urllib.parse import urlparse

from .exceptions import *  # pylint: disable=wildcard-import
from .protocol import AmqpProtocol

from .version import __version__
from .version import __packagename__

connect = protocol.AmqpProtocol

def connect_from_url(url, **kwargs):
    """ Connect to the AMQP using a single url parameter and return the client.

        For instance:

            amqp://user:password@hostname:port/vhost

        @kwargs:        Further arguments for trio-amqp.connect()

        Returns:        the AmqpProtocol instance
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
    return connect(
            virtualhost=(url.path[1:] if len(url.path) > 1 else '/'),
            ssl=(url.scheme == 'amqps'),
            **kwargs)


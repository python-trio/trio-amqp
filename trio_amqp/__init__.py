import trio
import socket
import sys
import ssl as ssl_module  # import as to enable argument named ssl in connect
from urllib.parse import urlparse

from trio._util import acontextmanager
from async_generator import async_generator, yield_

from .exceptions import *  # pylint: disable=wildcard-import
from .protocol import AmqpProtocol

from .version import __version__
from .version import __packagename__

@trio.hazmat.enable_ki_protection
async def connect(host='localhost', port=None, login='guest', password='guest',
            virtualhost='/', ssl=False,
            protocol_factory=AmqpProtocol, *, verify_ssl=True, **kwargs):
    """Convenient method to connect to an AMQP broker

        @host:          the host to connect to
        @port:          broker port
        @login:         login
        @password:      password
        @virtualhost:   AMQP virtualhost to use for this connection
        @ssl:           the SSL context to use
        @login_method:  AMQP auth method
        @insist:        Insist on connecting to a server
        @protocol_factory:
                        Factory to use, if you need to subclass AmqpProtocol

        @kwargs:        Arguments to be given to the protocol_factory instance

        Returns:        a tuple (transport, protocol) of an AmqpProtocol instance
    """

    if ssl:
        ssl_context = ssl_module.create_default_context()
        if not verify_ssl:
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl_module.CERT_NONE

    if port is None:
        if ssl:
            port = 5671
        else:
            port = 5672

    if ssl:
        stream = trio.open_ssl_over_tcp_stream(host, port, ssl_context=ssl_context)
        sock = stream.transport_stream
    else:
        sock = stream = await trio.open_tcp_stream(host, port)

    protocol = protocol_factory()

    # these 2 flags *may* show up in sock.type. They are only available on linux
    # see https://bugs.python.org/issue21327
    nonblock = getattr(socket, 'SOCK_NONBLOCK', 0)
    cloexec = getattr(socket, 'SOCK_CLOEXEC', 0)
    if sock is not None and (sock.socket.type & ~nonblock & ~cloexec) == socket.SOCK_STREAM:
        sock.socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    p = await protocol.start_connection(stream, login, password, virtualhost, ssl=ssl, **kwargs)
    assert p is  protocol, (p,protocol)
    return protocol

async def connect_from_url(
        url, **kwargs):
    """ Connect to the AMQP using a single url parameter and return the client.

        For instance:

            amqp://user:password@hostname:port/vhost

        @insist:        Insist on connecting to a server
        @protocol_factory:
                        Factory to use, if you need to subclass AmqpProtocol
        @verify_ssl:    Verify server's SSL certificate (True by default)

        @kwargs:        Arguments to be given to the protocol_factory instance

        Returns:        a tuple (transport, protocol) of an AmqpProtocol instance
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
    return await connect(
            virtualhost=(url.path[1:] if len(url.path) > 1 else '/'),
            ssl=(url.scheme == 'amqps'),
            **kwargs)


"""
    Test our Protocol class
"""

import trio
import unittest
from unittest import mock
from functools import partial

from . import testing
from . import testcase
from .. import exceptions
from .. import connect as amqp_connect
from .. import connect_from_url as amqp_from_url
from ..protocol import AmqpProtocol, OPEN


class TestProtocol(testcase.RabbitTestCase):


    async def test_connect(self):
        async with amqp_connect(virtualhost=self.vhost) as protocol:
            assert protocol.state == OPEN

    async def test_connect_products_info(self):
        client_properties = {
            'program': 'trio-amqp-tests',
            'program_version': '0.1.1',
        }
        async with amqp_connect(
            virtualhost=self.vhost,
            client_properties=client_properties,
        ) as protocol:
            assert protocol.client_properties == client_properties

    async def test_connection_unexistant_vhost(self):
        with pytest.raises(exceptions.AmqpClosedConnection):
            async with amqp_connect(virtualhost='/unexistant') as protocol:
                pass

    async def test_connection_wrong_login_password(self):
        with pytest.raises(exceptions.AmqpClosedConnection):
            async with amqp_connect(login='wrong', password='wrong') as protocol:
                pass

    async def test_connection_from_url(self):
        with mock.patch('trio_amqp.connect') as connect:
            async def func(*x, **y):
                return 1, 2
            connect.side_effect = func
            await amqp_from_url('amqp://tom:pass@example.com:7777/myvhost')
            connect.assert_called_once_with(
                insist=False,
                password='pass',
                login_method='AMQPLAIN',
                ssl=False,
                login='tom',
                host='example.com',
                protocol_factory=AmqpProtocol,
                virtualhost='myvhost',
                port=7777,
                verify_ssl=True,
            )

    async def test_from_url_raises_on_wrong_scheme(self):
        with pytest.raises(ValueError):
            await amqp_from_url('invalid://')


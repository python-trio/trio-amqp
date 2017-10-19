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
from .. import from_url as amqp_from_url
from ..protocol import AmqpProtocol, OPEN


class ProtocolTestCase(testcase.RabbitTestCase, unittest.TestCase):


    async def test_connect(self):
        _transport, protocol = await amqp_connect(virtualhost=self.vhost)
        self.assertEqual(protocol.state, OPEN)
        await protocol.close()

    async def test_connect_products_info(self):
        client_properties = {
            'program': 'trio-amqp-tests',
            'program_version': '0.1.1',
        }
        _transport, protocol = await amqp_connect(
            virtualhost=self.vhost,
            client_properties=client_properties,
        )

        self.assertEqual(protocol.client_properties, client_properties)
        await protocol.close()

    async def test_connection_unexistant_vhost(self):
        with self.assertRaises(exceptions.AmqpClosedConnection):
            await amqp_connect(virtualhost='/unexistant')

    def test_connection_wrong_login_password(self):
        with self.assertRaises(exceptions.AmqpClosedConnection):
            trio.run(partial(amqp_connect,login='wrong', password='wrong'))

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
        with self.assertRaises(ValueError):
            await amqp_from_url('invalid://')

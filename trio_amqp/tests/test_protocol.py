"""
    Test our Protocol class
"""

import trio
import pytest
import mock
from functools import partial

from . import testcase
from .. import exceptions
from .. import connect_amqp
from .. import connect_from_url as amqp_from_url
from ..protocol import AmqpProtocol, OPEN


class TestProtocol(testcase.RabbitTestCase):

    @pytest.mark.trio
    async def test_connect(self):
        self.reset_vhost()
        amqp = connect_amqp(virtualhost=self.vhost)
        async with amqp as protocol:
            assert protocol.state == OPEN

    @pytest.mark.trio
    async def test_connect_products_info(self):
        self.reset_vhost()
        client_properties = {
            'program': 'trio-amqp-tests',
            'program_version': '0.1.1',
        }
        amqp = connect_amqp(
            virtualhost=self.vhost,
            client_properties=client_properties,
        )
        async with amqp as protocol:
            assert protocol.client_properties == client_properties

    @pytest.mark.trio
    async def test_connection_unexistant_vhost(self):
        self.reset_vhost()
        with pytest.raises(exceptions.AmqpClosedConnection):
            amqp = connect_amqp(virtualhost='/unexistant')
            async with amqp as protocol:
                pass

    @pytest.mark.trio
    async def test_connection_wrong_login_password(self):
        self.reset_vhost()
        with pytest.raises(exceptions.AmqpClosedConnection):
            amqp = connect_amqp(login='wrong', password='wrong')
            async with amqp as protocol:
                pass

    @pytest.mark.trio
    async def test_connection_from_url(self):
        self.reset_vhost()
        with mock.patch('trio_amqp.connect') as connect:
            async def func(*x, **y):
                return 1, 2
            connect.side_effect = func
            await amqp_from_url('amqp://tom:pass@example.com:7777/myvhost')
            connect.assert_called_once_with(
                password='pass',
                ssl=False,
                login='tom',
                host='example.com',
                virtualhost='myvhost',
                port=7777,
            )

    @pytest.mark.trio
    async def test_from_url_raises_on_wrong_scheme(self):
        self.reset_vhost()
        with pytest.raises(ValueError):
            await amqp_from_url('invalid://')


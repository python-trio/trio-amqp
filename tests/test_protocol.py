"""
    Test our Protocol class
"""

import pytest
import mock

from . import testcase
from async_amqp import exceptions
from async_amqp import connect_from_url as amqp_from_url
from async_amqp.protocol import OPEN


class TestProtocol(testcase.RabbitTestCase):
    @pytest.mark.trio
    async def test_connect(self):
        self.reset_vhost()
        amqp = testcase.connect(host=self.host, port=self.port, virtualhost=self.vhost)
        async with amqp as protocol:
            assert protocol.state == OPEN

    @pytest.mark.trio
    async def test_connect_products_info(self):
        self.reset_vhost()
        client_properties = {
            'program': 'async_amqp-tests',
            'program_version': '0.1.1',
        }
        amqp = testcase.connect(
            host=self.host, port=self.port, virtualhost=self.vhost,
            client_properties=client_properties,
        )
        async with amqp as protocol:
            assert protocol.client_properties == client_properties

    @pytest.mark.trio
    async def test_connection_unexistant_vhost(self):
        self.reset_vhost()
        with pytest.raises(exceptions.AmqpClosedConnection):
            amqp = testcase.connect(host=self.host, port=self.port, virtualhost='/unexistant')
            async with amqp:
                pass

    @pytest.mark.trio
    async def test_connection_wrong_login_password(self):
        self.reset_vhost()
        with pytest.raises(exceptions.AmqpClosedConnection):
            amqp = testcase.connect(login='wrong', password='wrong')
            async with amqp:
                pass

    @pytest.mark.trio
    async def test_connection_from_url(self):
        self.reset_vhost()
        with mock.patch('async_amqp.connect_amqp') as connect:

            class func:
                def __init__(self):
                    pass

                async def __aenter__(self):
                    return self

                async def __aexit__(self, *tb):
                    pass

            connect.return_value = func()
            res = amqp_from_url('amqp://tom:pass@example.com:7777/myvhost')
            async with res:
                pass
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
            async with amqp_from_url('invalid://'):
                assert False, "does not go here"

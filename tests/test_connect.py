"""Connection tests"""

import socket
import pytest

from trio_amqp.protocol import OPEN, CLOSED

from . import testcase


class TestAmqpConnection(testcase.RabbitTestCase):
    @pytest.mark.trio
    async def test_connect(self, amqp):
        assert amqp.state == OPEN
        assert amqp.server_properties is not None

    @pytest.mark.trio
    async def test_connect_tuning(self):
        # frame_max should be higher than 131072
        self.reset_vhost()
        frame_max = 131072
        channel_max = 10
        heartbeat = 100
        proto = testcase.connect(
            virtualhost=self.vhost,
            channel_max=channel_max,
            frame_max=frame_max,
            heartbeat=heartbeat,
        )
        async with proto as amqp:
            assert amqp.state == OPEN, amqp.state
            assert amqp.server_properties is not None

            assert amqp.connection_tunning == {
                'frame_max': frame_max,
                'channel_max': channel_max,
                'heartbeat': heartbeat
            }

            assert amqp.server_channel_max == channel_max
            assert amqp.server_frame_max == frame_max
            assert amqp.server_heartbeat == heartbeat
        assert amqp.state == CLOSED, amqp.state

    @pytest.mark.trio
    async def test_socket_nodelay(self):
        self.reset_vhost()
        proto = testcase.connect(virtualhost=self.vhost)
        async with proto as amqp:
            sock = amqp._stream.socket
            opt_val = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
            assert opt_val == 1, opt_val

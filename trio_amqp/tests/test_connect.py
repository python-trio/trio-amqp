"""Connection tests"""

import socket

from trio_amqp import connect
from trio_amqp.protocol import OPEN, CLOSED

from . import testing, testcase


class TestAmqpConnection(testcase.RabbitTestCase):

    async def test_connect(self, amqp):
        assert amqp.state == OPEN
        assert amqp.server_properties is not None

    async def test_connect_tuning(self):
        # frame_max should be higher than 131072
        self.reset_vhost()
        frame_max = 131072
        channel_max = 10
        heartbeat = 100
        proto = connect(
            virtualhost=self.vhost,
            channel_max=channel_max,
            frame_max=frame_max,
            heartbeat=heartbeat,
        )
        async with proto:
            assert proto.state == OPEN, proto.state
            assert proto.server_properties is not None

            assert proto.connection_tunning == {
                'frame_max': frame_max,
                'channel_max': channel_max,
                'heartbeat': heartbeat
            }

            assert proto.server_channel_max == channel_max
            assert proto.server_frame_max == frame_max
            assert proto.server_heartbeat == heartbeat
        assert proto.state == CLOSED, proto.state

    async def test_socket_nodelay(self):
        self.reset_vhost()
        proto = connect(virtualhost=self.vhost)
        async with proto:
            sock = proto._stream.socket
            opt_val = sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)
            assert opt_val == 1, opt_val

"""
    Tests the heartbeat methods
"""

import anyio
import pytest

from async_amqp import exceptions

from . import testcase


class TestHeartbeat(testcase.RabbitTestCase):
    @pytest.mark.trio
    async def test_recv_heartbeat(self):
        conn = testcase.connect(virtualhost=self.vhost,)
        self.reset_vhost()
        with pytest.raises(exceptions.HeartbeatTimeoutError):
            async with conn as amqp:

                async def mock_send():
                    self.send_called += 1

                self.send_called = 0

                amqp.send_heartbeat = mock_send
                amqp.server_heartbeat = 0.01

                async with amqp.new_channel():
                    # this ensures that the send and recv loops use the new
                    # heartbeat value
                    await anyio.sleep(0.1)
                    assert False, "not reached"
        assert self.send_called > 2

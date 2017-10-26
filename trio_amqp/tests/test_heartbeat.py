"""
    Tests the heartbeat methods
"""

import trio
import pytest
import mock

from trio_amqp import connect, exceptions
from trio_amqp.protocol import CLOSED

from . import testcase
from . import testing


class TestHeartbeat(testcase.RabbitTestCase):

    async def test_heartbeat(self):
        amqp = await connect(
            virtualhost=self.vhost,
        )
        self.reset_vhost()
        with pytest.raises(exceptions.HeartbeatTimeoutError):
            async with amqp:
                async def mock_send():
                    self.send_called += 1
                self.send_called = 0
                amqp.send_heartbeat = mock_send
                # reset both timers to 1) make them 'see' the new heartbeat value
                # 2) so that the mock is actually called back from the main loop
                amqp.server_heartbeat = 1
                await amqp._heartbeat_timer_send_reset()
                await amqp._heartbeat_timer_recv_reset()

                await trio.sleep(1.1)
                assert self.send_called == 1

                await trio.sleep(1.1)
                # not reached
                assert False,"not reached"
        

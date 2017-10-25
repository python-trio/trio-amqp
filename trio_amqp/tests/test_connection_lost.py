import unittest
import unittest.mock

import trio
from trio_amqp.protocol import OPEN, CLOSED

from . import testcase
from . import testing


class TestConnectionLost(testcase.RabbitTestCase):

    _multiprocess_can_split_ = True

    async def test_connection_lost(self, amqp):

        self.callback_called = False

        def callback(*args, **kwargs):
            self.callback_called = True

        amqp._on_error_callback = callback
        channel = self.channel
        self.assertEqual(amqp.state, OPEN)
        self.assertTrue(channel.is_open)
        amqp._stream_reader._transport.close()  # this should have the same effect as the tcp connection being lost

        with trio.fail_after(1):
            await amqp.worker_done.wait()
        self.assertEqual(amqp.state, CLOSED)
        self.assertFalse(channel.is_open)
        self.assertTrue(self.callback_called)

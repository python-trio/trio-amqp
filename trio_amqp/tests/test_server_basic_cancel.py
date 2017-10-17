"""
    Server received requests handling tests

"""

import unittest

from . import testcase
from . import testing


class ServerBasicCancelTestCase(testcase.RabbitTestCase, unittest.TestCase):
    _multiprocess_can_split_ = True

    async def test_cancel_whilst_consuming(self):
        queue_name = 'queue_name'
        await self.channel.queue_declare(queue_name)

        # None is non-callable.  We want to make sure the callback is
        # unregistered and never called.
        await self.channel.basic_consume(None)
        await self.channel.queue_delete(queue_name)

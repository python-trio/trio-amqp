"""
    Server received requests handling tests

"""

import pytest

from . import testcase


class TestServerBasicCancel(testcase.RabbitTestCase):
    _multiprocess_can_split_ = True

    @pytest.mark.trio
    async def test_cancel_whilst_consuming(self, amqp):
        queue_name = 'queue_name'
        await self.channel.queue_declare(queue_name)

        # None is non-callable.  We want to make sure the callback is
        # unregistered and never called.
        await self.channel.basic_consume(None)
        await self.channel.queue_delete(queue_name)

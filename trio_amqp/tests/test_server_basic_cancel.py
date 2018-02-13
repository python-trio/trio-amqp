"""
    Server received requests handling tests

"""

import pytest

from . import testcase


class TestServerBasicCancel(testcase.RabbitTestCase):
    _multiprocess_can_split_ = True

    @pytest.mark.trio
    async def test_cancel_whilst_consuming(self, channel):
        queue_name = 'queue_name'
        await channel.queue_declare(queue_name)

        # None is non-callable.  We want to make sure the callback is
        # unregistered and never called.
        await channel.basic_consume(None)
        await channel.queue_delete(queue_name)

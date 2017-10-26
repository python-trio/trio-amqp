"""
    Amqp basic tests for recover methods
"""

import unittest

from . import testcase
from . import testing


class TestRecover(testcase.RabbitTestCase):

    async def test_basic_recover_async(self, amqp):
        await self.channel.basic_recover_async(requeue=True)

    async def test_basic_recover_async_no_requeue(self, amqp):
        await self.channel.basic_recover_async(requeue=False)

    async def test_basic_recover(self, amqp):
        result = await self.channel.basic_recover(requeue=True)
        assert result

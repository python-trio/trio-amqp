"""
    Amqp basic tests for recover methods
"""

import pytest

from . import testcase


class TestRecover(testcase.RabbitTestCase):

    @pytest.mark.trio
    async def test_basic_recover_async(self, amqp):
        await self.channel.basic_recover_async(requeue=True)

    @pytest.mark.trio
    async def test_basic_recover_async_no_requeue(self, amqp):
        await self.channel.basic_recover_async(requeue=False)

    @pytest.mark.trio
    async def test_basic_recover(self, amqp):
        result = await self.channel.basic_recover(requeue=True)
        assert result

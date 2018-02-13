import pytest

from . import testcase


class TestPublish(testcase.RabbitTestCase):

    _multiprocess_can_split_ = True

    @pytest.mark.trio
    async def test_publish(self, channel):
        # declare
        await channel.queue_declare("q", exclusive=True, no_wait=False)
        await channel.exchange_declare("e", "fanout")
        await channel.queue_bind("q", "e", routing_key='')

        # publish
        await channel.publish("coucou", "e", routing_key='')

        await self.check_messages(channel.protocol, "q", 1)

    @pytest.mark.trio
    async def test_big_publish(self, channel):
        # declare
        await channel.queue_declare("q", exclusive=True, no_wait=False)
        await channel.exchange_declare("e", "fanout")
        await channel.queue_bind("q", "e", routing_key='')

        # publish
        await channel.publish("a" * 1000000, "e", routing_key='')

        await self.check_messages(channel.protocol, "q", 1)

    @pytest.mark.trio
    async def test_confirmed_publish(self, channel):
        # declare
        await channel.confirm_select()
        assert channel.publisher_confirms
        await channel.queue_declare("q", exclusive=True, no_wait=False)
        await channel.exchange_declare("e", "fanout")
        await channel.queue_bind("q", "e", routing_key='')

        # publish
        await channel.publish("coucou", "e", routing_key='')

        await self.check_messages(channel.protocol, "q", 1)

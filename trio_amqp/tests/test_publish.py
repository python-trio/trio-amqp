import pytest

from . import testcase


class TestPublish(testcase.RabbitTestCase):

    _multiprocess_can_split_ = True

    @pytest.mark.trio
    async def test_publish(self, amqp):
        # declare
        await self.channel.queue_declare("q", exclusive=True, no_wait=False)
        await self.channel.exchange_declare("e", "fanout")
        await self.channel.queue_bind("q", "e", routing_key='')

        # publish
        await self.channel.publish("coucou", "e", routing_key='')

        await self.check_messages("q", 1)

    @pytest.mark.trio
    async def test_big_publish(self, amqp):
        # declare
        await self.channel.queue_declare("q", exclusive=True, no_wait=False)
        await self.channel.exchange_declare("e", "fanout")
        await self.channel.queue_bind("q", "e", routing_key='')

        # publish
        await self.channel.publish("a"*1000000, "e", routing_key='')

        await self.check_messages("q", 1)

    @pytest.mark.trio
    async def test_confirmed_publish(self, amqp):
        # declare
        await self.channel.confirm_select()
        assert self.channel.publisher_confirms
        await self.channel.queue_declare("q", exclusive=True, no_wait=False)
        await self.channel.exchange_declare("e", "fanout")
        await self.channel.queue_bind("q", "e", routing_key='')

        # publish
        await self.channel.publish("coucou", "e", routing_key='')

        await self.check_messages("q", 1)

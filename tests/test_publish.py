import anyio
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


    @pytest.mark.skip("This callback doesn't exist in asyncamqp")
    @pytest.mark.trio
    async def test_return_from_publish(self, channel):
        called = False

        async def callback(channel, body, envelope, properties):
            nonlocal called
            called = True
        channel.return_callback = callback

        # declare
        await channel.exchange_declare("e", "topic")

        # publish
        await channel.publish("coucou", "e", routing_key="not.found",
                                   mandatory=True)

        for i in range(10):
            if called:
                break
            await anyio.sleep(0.1)

        assert called


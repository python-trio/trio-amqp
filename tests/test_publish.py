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
        await channel.publish(b"coucou", "e", routing_key='')

        await self.check_messages(channel.protocol, "q", 1)

    @pytest.mark.trio
    async def test_empty_publish(self, channel):
        # declare
        await channel.queue_declare("q", exclusive=True, no_wait=False)
        await channel.exchange_declare("e", "fanout")
        await channel.queue_bind("q", "e", routing_key='')

        # publish
        await channel.publish(b"", "e", routing_key='')

        await self.check_messages(channel.protocol, "q", 1)

    @pytest.mark.trio
    async def test_big_publish(self, channel):
        # declare
        await channel.queue_declare("q", exclusive=True, no_wait=False)
        await channel.exchange_declare("e", "fanout")
        await channel.queue_bind("q", "e", routing_key='')

        # publish
        await channel.publish(b"a" * 1000000, "e", routing_key='')

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
        await channel.publish(b"coucou", "e", routing_key='')

        await self.check_messages(channel.protocol, "q", 1)


    @pytest.mark.skip("This callback doesn't exist in async_amqp")
    @pytest.mark.trio
    async def test_return_from_publish(self, channel):
        called = trio.Event()

        async def logger(task_status=trio.TASK_STATUS_IGNORED):
            task_status.started()
            async for a,b,c in channel:
                called.set()

        async def sender(task_status=trio.TASK_STATUS_IGNORED):
            task_status.started()

            # declare
            await channel.exchange_declare("e", "topic")

            # publish
            await channel.publish("coucou", "e", routing_key="not.found",
                                  mandatory=True)
 
        with trio.fail_after(1):
            async with trio.open_nursery() as n:
                await n.start(logger)
                await n.start(sender)
                await called.wait()
                n.cancel_scope.cancel()
 

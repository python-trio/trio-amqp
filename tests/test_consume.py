import anyio
import pytest

from . import testcase
from async_amqp import exceptions

from async_amqp.properties import Properties


class TestConsume(testcase.RabbitTestCase):

    _multiprocess_can_split_ = True

#   def setup(self):
#       super().setup()
#       self.consume_future = anyio.Event()

    async def callback(self, channel, body, envelope, properties):
        self.consume_result = (body, envelope, properties)
        self.consume_future.set()

    async def get_callback_result(self):
        await self.consume_future.wait()
        result = self.consume_result
        self.consume_future = anyio.Event()
        return result

    @pytest.mark.trio
    async def test_wrong_callback_argument(self):
        self.consume_future = anyio.Event()
        def badcallback():
            pass

        self.reset_vhost()
        proto = testcase.connect(virtualhost=self.vhost,)
        with pytest.raises(TypeError):
            async with proto as amqp:
                async with amqp.new_channel() as chan:
                    await chan.queue_declare("q", exclusive=True, no_wait=False)
                    await chan.exchange_declare("e", "fanout")
                    await chan.queue_bind("q", "e", routing_key='')

                    # get a different channel
                    async with amqp.new_channel() as channel:
                        # publish
                        await channel.publish(
                            "coucou",
                            "e",
                            routing_key='',
                        )

                        # assert there is a message to consume
                        await self.check_messages(amqp, "q", 1)

                        # start consume
                        await channel.basic_consume(badcallback, queue_name="q")
                        await anyio.sleep(1)

    @pytest.mark.trio
    async def test_consume(self, amqp):
        self.consume_future = anyio.Event()
        # declare
        async with amqp.new_channel() as channel:
            await channel.queue_declare("q", exclusive=True, no_wait=False)
            await channel.exchange_declare("e", "fanout")
            await channel.queue_bind("q", "e", routing_key='')

            # get a different channel
            async with amqp.new_channel() as channel:
                # publish
                await channel.publish(
                    b"coucou",
                    "e",
                    routing_key='',
                )

                # start consume
                await channel.basic_consume(self.callback, queue_name="q")

                # get one
                body, envelope, properties = await self.get_callback_result()
                assert envelope.consumer_tag is not None
                assert envelope.delivery_tag is not None
                assert b"coucou" == body
                assert isinstance(properties, Properties)

    @pytest.mark.trio
    async def test_big_consume(self, amqp):
        self.consume_future = anyio.Event()
        # declare
        async with amqp.new_channel() as channel:
            await channel.queue_declare("q", exclusive=True, no_wait=False)
            await channel.exchange_declare("e", "fanout")
            await channel.queue_bind("q", "e", routing_key='')

            # get a different channel
            async with amqp.new_channel() as channel:
                # publish
                await channel.publish(
                    b"a" * 1000000,
                    "e",
                    routing_key='',
                )

                # start consume
                await channel.basic_consume(self.callback, queue_name="q")

                # get one
                body, envelope, properties = await self.get_callback_result()
                assert envelope.consumer_tag is not None
                assert envelope.delivery_tag is not None
                assert b"a" * 1000000 == body
                assert isinstance(properties, Properties)

    @pytest.mark.trio
    async def test_consume_multiple_queues(self, amqp):
        self.consume_future = anyio.Event()
        async with amqp.new_channel() as channel:
            await channel.queue_declare("q1", exclusive=True, no_wait=False)
            await channel.queue_declare("q2", exclusive=True, no_wait=False)
            await channel.exchange_declare("e", "direct")
            await channel.queue_bind("q1", "e", routing_key="q1")
            await channel.queue_bind("q2", "e", routing_key="q2")

            # get a different channel
            async with amqp.new_channel() as channel:

                q1_future = anyio.Event()

                async def q1_callback(channel, body, envelope, properties):
                    self.q1_result = (body, envelope, properties)
                    q1_future.set()

                q2_future = anyio.Event()

                async def q2_callback(channel, body, envelope, properties):
                    self.q2_result = (body, envelope, properties)
                    q2_future.set()

                # start consumers
                result = await channel.basic_consume(q1_callback, queue_name="q1")
                ctag_q1 = result['consumer_tag']
                result = await channel.basic_consume(q2_callback, queue_name="q2")
                ctag_q2 = result['consumer_tag']

                # put message in q1
                await channel.publish(b"coucou1", "e", "q1")

                # get it
                await q1_future.wait()
                body1, envelope1, properties1 = self.q1_result
                assert ctag_q1 == envelope1.consumer_tag
                assert envelope1.delivery_tag is not None
                assert b"coucou1" == body1
                assert isinstance(properties1, Properties)

                # put message in q2
                await channel.publish(b"coucou2", "e", "q2")

                # get it
                await q2_future.wait()
                body2, envelope2, properties2 = self.q2_result
                assert ctag_q2 == envelope2.consumer_tag
                assert b"coucou2" == body2
                assert isinstance(properties2, Properties)

    @pytest.mark.trio
    @pytest.mark.xfail(msg="Needs debugging")
    async def test_duplicate_consumer_tag(self, channel):
        self.consume_future = anyio.Event()
        await channel.queue_declare("q1", exclusive=True, no_wait=False)
        await channel.queue_declare("q2", exclusive=True, no_wait=False)
        await channel.basic_consume(self.callback, queue_name="q1", consumer_tag='tag')

        with pytest.raises(exceptions.ChannelClosed) as cm:
            await channel.basic_consume(self.callback, queue_name="q2", consumer_tag='tag')

        assert cm.value.code == 530

    @pytest.mark.trio
    async def test_consume_callaback_synced(self, amqp):
        self.consume_future = anyio.Event()
        # declare
        async with amqp.new_channel() as channel:
            await channel.queue_declare("q", exclusive=True, no_wait=False)
            await channel.exchange_declare("e", "fanout")
            await channel.queue_bind("q", "e", routing_key='')

            # get a different channel
            async with amqp.new_channel() as channel:

                # publish
                await channel.publish(
                    b"coucou",
                    "e",
                    routing_key='',
                )

                sync_future = anyio.Event()

                async def callback(channel, body, envelope, properties):
                    assert sync_future.is_set()

                await channel.basic_consume(callback, queue_name="q")
                sync_future.set()

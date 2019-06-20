import unittest
import asyncio

from . import testcase
from . import testing


class PublishTestCase(testcase.RabbitTestCase, unittest.TestCase):

    _multiprocess_can_split_ = True

    @testing.coroutine
    def test_publish(self):
        # declare
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # publish
        yield from self.channel.publish("coucou", "e", routing_key='')

        queues = self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(1, queues["q"]['messages'])

    @testing.coroutine
    def test_big_publish(self):
        # declare
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # publish
        yield from self.channel.publish("a"*1000000, "e", routing_key='')

        queues = self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(1, queues["q"]['messages'])

    @testing.coroutine
    def test_big_unicode_publish(self):
        # declare
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # publish
        yield from self.channel.publish("Ы"*1000000, "e", routing_key='')
        yield from self.channel.publish("Ы"*1000000, "e", routing_key='')

        queues = self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(2, queues["q"]['messages'])

    @testing.coroutine
    def test_confirmed_publish(self):
        # declare
        yield from self.channel.confirm_select()
        self.assertTrue(self.channel.publisher_confirms)
        yield from self.channel.queue_declare("q", exclusive=True, no_wait=False)
        yield from self.channel.exchange_declare("e", "fanout")
        yield from self.channel.queue_bind("q", "e", routing_key='')

        # publish
        yield from self.channel.publish("coucou", "e", routing_key='')

        queues = self.list_queues()
        self.assertIn("q", queues)
        self.assertEqual(1, queues["q"]['messages'])

    @testing.coroutine
    def test_return_from_publish(self):
        called = anyio.create_event()

        async def logger():
            for a,b,c in self.channel:
                await called.set()

        async def sender():

            # declare
            yield from channel.exchange_declare("e", "topic")

            # publish
            yield from channel.publish("coucou", "e", routing_key="not.found",
                                       mandatory=True)

        async def run_test():
            async with anyio.create_task_group() as n:
                await n.spawn(logger)
                await n.spawn(sender)
                await called.wait()
                await n.cancel_scope.cancel()

        async with anyio.fail_after(1):
            await run_test()


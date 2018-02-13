"""
    Amqp Future implementation
"""

import trio
import logging

logger = logging.getLogger(__name__)


class FutureCancelled(Exception):
    pass


class Future:
    def __init__(self, channel, rpc_name):
        self.channel = channel
        self.rpc_name = rpc_name
        self.event = trio.Event()
        self.result = None
        self.exc = None
        channel._add_future(self)

    async def __call__(self):
        await self.event.wait()
        if self.exc is None:
            return self.result
        else:
            raise self.exc

    def set_result(self, value):
        if self.event.is_set():
            raise RuntimeError("future already set")
        self.result = value
        self.event.set()

    def set_exception(self, exc):
        if self.event.is_set():
            raise RuntimeError("future already set")
        self.exc = exc
        self.event.set()

    def cancel(self):
        try:
            raise FutureCancelled()
        except FutureCancelled as exc:
            self.set_exception(exc)

    def done(self):
        return self.event.is_set()

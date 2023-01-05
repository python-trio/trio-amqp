"""
    Amqp Future implementation
"""

import anyio
import logging
import outcome

logger = logging.getLogger(__name__)


class FutureCancelled(Exception):
    pass


class Future:
    def __init__(self, channel, rpc_name):
        self.channel = channel
        self.rpc_name = rpc_name
        self.event = anyio.Event()
        self.result = None
        channel._add_future(self)

    async def __call__(self):
        await self.event.wait()
        return self.result.unwrap()

    def set_result(self, value):
        if self.event.is_set():
            raise RuntimeError("future already set")
        self.result = outcome.Value(value)
        self.event.set()

    def set_exception(self, exc):
        if self.event.is_set():
            raise RuntimeError("future already set")
        if isinstance(exc, type):
            exc = exc()
        self.result = outcome.Error(exc)
        self.event.set()

    def cancel(self):
        try:
            raise FutureCancelled()
        except FutureCancelled as exc:
            self.set_exception(exc)

    def done(self):
        return self.event.is_set()

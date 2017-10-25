from functools import wraps
import logging

import trio
from functools import partial


class AsyncioErrors(AssertionError):
    def __repr__(self):
        return "<AsyncioErrors: Got asyncio errors: %r" % self.args[0]


class Handler(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.ERROR)
        self.messages = []

    def emit(self, record):
        message = record.msg % record.args
        print(message)
        self.messages.append(message)


asyncio_logger = logging.getLogger('trio')
handler = Handler()
asyncio_logger.addHandler(handler)


def timeout(t):
    def wrapper(func):
        setattr(func, '__timeout__', t)
        return func
    return wrapper


class AsyncioTestCaseMixin:
    __timeout__ = 10

    def setUp(self):
        super().setUp()
        #self.loop = asyncio.new_event_loop()
        #asyncio.set_event_loop(self.loop)

    def tearDown(self):
        super().tearDown()
        #self.loop.close()


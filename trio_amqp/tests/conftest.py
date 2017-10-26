import pytest
import inspect
import trio
from .testcase import amqp # side effect (fixture)
from functools import wraps,partial
from weakref import ref

class Runner:
	def __init__(self, proc):
		self.proc = proc
	def __call__(self, *args, **kwargs):
		return trio.run(self.resolve, args, kwargs)
	async def resolve(self, args, kwargs):
		amqp = kwargs.get('amqp',None)
		if amqp is not None:
			amqp = await amqp
			async with amqp as conn:
				kwargs['amqp'] = conn
				obj = self.proc.__self__
				obj.amqp = conn
				conn.test_case = ref(obj)
				try:
					await obj.reset_vhost()
					return await self.proc(*args, **kwargs)
				finally:
					del obj.amqp
		else:
			return await self.proc(*args, **kwargs)

@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem):
	if inspect.iscoroutinefunction(pyfuncitem.obj):
		pyfuncitem.obj = Runner(pyfuncitem.obj)


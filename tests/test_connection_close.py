import pytest

from async_amqp.protocol import OPEN, CLOSED

from . import testcase


class TestClose(testcase.RabbitTestCase):
    @pytest.mark.trio
    @pytest.mark.xfail(reason="this triggers a cancellation error")
    async def test_close(self, amqp):
        assert amqp.state == OPEN
        # grab a ref here because py36 sets _stream_reader to None in
        # StreamReaderProtocol.connection_lost()
        sock = amqp._stream.socket
        await amqp.close()
        assert amqp.state == CLOSED
        assert sock.fileno() == -1
        # make sure those 2 tasks/futures are properly set as finished
        assert amqp.connection_closed.is_set()
        assert amqp._heartbeat_timer_recv is None
        assert amqp._heartbeat_timer_send is None

    @pytest.mark.trio
    @pytest.mark.xfail(reason="this triggers a cancellation error")
    async def test_multiple_close(self, amqp):
        # close is supposed to be idempotent
        await amqp.close()
        assert amqp.state == CLOSED
        await amqp.close()
        assert amqp.state == CLOSED

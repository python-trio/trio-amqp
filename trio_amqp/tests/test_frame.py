"""
    Test frame format.
"""

import io
import unittest
import sys

from .. import constants as amqp_constants
from .. import frame as frame_module
from ..frame import AmqpEncoder
from ..frame import AmqpResponse


class TestEncoder:
    """Test encoding of python builtin objects to AMQP frames."""

    _multiprocess_can_split_ = True

    def setup(self):
        self.encoder = AmqpEncoder()

    def test_write_string(self):
        self.encoder.write_value("foo")
        assert self.encoder.payload.getvalue() == \
                         b'S\x00\x00\x00\x03foo'
                         # 'S' + size (4 bytes) + payload

    def test_write_bool(self):
        self.encoder.write_value(True)
        assert self.encoder.payload.getvalue() == b't\x01'

    def test_write_dict(self):
        self.encoder.write_value({'foo': 'bar', 'bar': 'baz'})
        assert self.encoder.payload.getvalue() in \
            (b'F\x00\x00\x00\x18\x03barS\x00\x00\x00\x03baz\x03fooS\x00\x00\x00\x03bar',
             b'F\x00\x00\x00\x18\x03fooS\x00\x00\x00\x03bar\x03barS\x00\x00\x00\x03baz')
            # 'F' + total size + key (always a string) + value (with type) + ...
            # The keys are not ordered, so the output is not deterministic (two possible values below)

    def test_write_message_properties_dont_crash(self):
        properties = {
            'content_type': 'plain/text',
            'content_encoding': 'utf8',
            'headers': {'key': 'value'},
            'delivery_mode': 2,
            'priority': 10,
            'correlation_id': '122',
            'reply_to': 'joe',
            'expiration': 'someday',
            'message_id': 'm_id',
            'timestamp': 12345,
            'type': 'a_type',
            'user_id': 'joe_42',
            'app_id': 'roxxor_app',
            'cluster_id': 'a_cluster',
        }
        self.encoder.write_message_properties(properties)
        assert len(self.encoder.payload.getvalue()) != 0

    def test_write_message_correlation_id_encode(self):
        properties = {
            'delivery_mode': 2,
            'priority': 0,
            'correlation_id': '122',
            }
        self.encoder.write_message_properties(properties)
        assert self.encoder.payload.getvalue() == b'\x1c\x00\x02\x00\x03122'

    def test_write_message_priority_zero(self):
        properties = {
            'delivery_mode': 2,
            'priority': 0,
        }
        self.encoder.write_message_properties(properties)
        assertEqual self.encoder.payload.getvalue() == \
                         b'\x18\x00\x02\x00'

    def test_write_message_properties_raises_on_invalid_property_name(self):
        properties = {
            'invalid': 'coucou',
        }
        with pytest.raises(ValueError):
            self.encoder.write_message_properties(properties)


class TestAmqpResponse:
    def test_dump_dont_crash(self):
        frame = AmqpResponse(None)
        frame.frame_type = amqp_constants.TYPE_METHOD
        frame.class_id = 0
        frame.method_id = 0
        saved_stout = sys.stdout
        frame_module.DUMP_FRAMES = True
        sys.stdout = io.StringIO()
        try:
            last_len = len(sys.stdout.getvalue())
            print(self)
            # assert something has been writen
            assert len(sys.stdout.getvalue()) > last_len
        finally:
            frame_module.DUMP_FRAMES = False
            sys.stdout = saved_stout

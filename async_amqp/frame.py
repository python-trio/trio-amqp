"""
    Helper class to decode AMQP responses

AMQP Frame implementations


0      1         3         7                      size+7      size+8
+------+---------+---------+    +-------------+     +-----------+
| type | channel |   size  |    |   payload   |     | frame-end |
+------+---------+---------+    +-------------+     +-----------+
 octets   short     long         'size' octets          octet

The frame-end octet MUST always be the hexadecimal value %xCE

type:

Type = 1, "METHOD": method frame.
Type = 2, "HEADER": content header frame.
Type = 3, "BODY": content body frame.
Type = 4, "HEARTBEAT": heartbeat frame.


Method Payload

0          2           4
+----------+-----------+-------------- - -
| class-id | method-id | arguments...
+----------+-----------+-------------- - -
    short     short       ...

Content Payload

0          2        4           12               14
+----------+--------+-----------+----------------+------------- - -
| class-id | weight | body size | property flags | property list...
+----------+--------+-----------+----------------+------------- - -
   short     short    long long       short        remainder...

"""

import io
import struct
import socket
import os
import datetime
from anyio import ClosedResourceError, IncompleteRead
from itertools import count
from decimal import Decimal

import pamqp.encode
import pamqp.frame

from . import exceptions
from . import constants as amqp_constants
from .properties import Properties

DUMP_FRAMES = False


async def read(reader):
    """Read a new frame from the wire

        reader:     anyio Stream

    Returns (channel, frame) a tuple containing both channel and the pamqp frame,
                             the object describing the frame
    """
    if not reader:
        raise exceptions.AmqpClosedConnection()
    try:
        data = await reader.receive_exactly(7)
    except (ClosedResourceError, IncompleteRead) as ex:
        raise exceptions.AmqpClosedConnection() from ex

    frame_type, channel, frame_length = pamqp.frame.frame_parts(data)

    payload_data = await reader.receive_exactly(frame_length)
    frame = None

    if frame_type == amqp_constants.TYPE_METHOD:
        frame = pamqp.frame._unmarshal_method_frame(payload_data)

    elif frame_type == amqp_constants.TYPE_HEADER:
        frame = pamqp.frame._unmarshal_header_frame(payload_data)

    elif frame_type == amqp_constants.TYPE_BODY:
        frame = pamqp.frame._unmarshal_body_frame(payload_data)

    elif frame_type == amqp_constants.TYPE_HEARTBEAT:
        frame = pamqp.heartbeat.Heartbeat()

    frame_end = await reader.receive_exactly(1)
    assert frame_end == amqp_constants.FRAME_END
    return channel, frame

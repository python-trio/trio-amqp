API
===

.. module:: trio_amqp
    :synopsis: public trio_amqp API


Basics
------

There are two principal objects when using trio_amqp:

 * The protocol object, used to begin a connection to trio_amqp,
 * The channel object, used when creating a new channel to effectively use an AMQP channel.


Starting a connection
---------------------

.. py:function:: connect(host, port, login, password, virtualhost, ssl, login_method, insist, verify_ssl, …) -> AmqpProtocol

   Convenience method to set up a connection to an AMQP broker. It is an
   alias for the AmqpProtocol_ class.

   :param str host:          the host to connect to
   :param int port:          broker port
   :param str login:         login
   :param str password:      password
   :param str virtualhost:   AMQP virtualhost to use for this connection
   :param bool ssl:          create an SSL connection instead of a plain unencrypted one
   :param bool verify_ssl:   verify server's SSL certificate (True by default)
   :param str login_method:  AMQP auth method
   :param bool insist:       insist on connecting to a server
   :param AmqpProtocol protocol_factory: factory to use, if you need to subclass AmqpProtocol
   :param int channel_max: specifies highest channel number that the server permits.
                      Usable channel numbers are in the range 1..channel-max.
                      Zero indicates no specified limit.
   :param int frame_max: the largest frame size that the server proposes for the connection,
                    including frame header and end-byte. The client can negotiate a lower value.
                    Zero means that the server does not impose any specific limit
                    but may reject very large frames if it cannot allocate resources for them.
   :param int heartbeat: the delay, in seconds, of the connection heartbeat that the server wants.
                    Zero means the server does not want a heartbeat.
   :param dict client_properties: configure the client to connect to the AMQP server.

   The actual connection will then be established by an async context manager.

.. _AmqpProtocol: :

.. code::

    import trio
    import trio_amqp

    async def connect():
        async with trio_amqp.connect_amqp() as conn: # use default parameters
            print("connected !")
            await trio.sleep(1)

            print("close connection")

    trio.run(connect)

Channels
--------

A channel links your connection to a specific exchange or queue. The server
closes your channel when it detects an error. Flow control is also applied
per channel.

Creating a channel is easy::

    async with conn.new_channel() as chan:
        do_whatever()

Publishing messages
-------------------

When you want to produce some content, you declare an exchange, then publish message into it::

    await chan.exchange_declare("my_exch","topic")
    await chan.publish("trio_amqp hello", "my_exch", "hello.there")

Here we're publishing a message to the "my_exch" exchange.

If you need guaranteed delivery, you can set the ``mandatory=True`` flag on :meth:`channel.Channel.publish`.
Returned messages will be delivered to your code in an async iterator over the channel::

    async def do_returns(chan, task_status=trio.TASK_STATUS_IGNORED):
        task_status.started()
        # DO NOT add any async statements here
        async for r_body, r_envelope, r_properties in chan:
            await process_your_return(r_body, r_envelope, r_properties)

    async with conn.new_channel() as chan:
        await nursery.start_soon(do_returns, chan)
        do_whatever()

The code above ensures that the iterator is started before calling ``do_whatever()``.
Returned messages arriving before that will be logged and discarded.

Consuming messages
------------------

When consuming message, you either create a queue (and hook it up to an
exchange), or read from an existing one; see below. Then you read messages
from the queue::

    async with chan.new_consumer(queue_name="my_queue") as listener:
        async for body, envelope, properties in listener:
            process_message(body, envelope, properties)

* the ``body`` parameter is the actual message
* the ``envelope`` is an instance of :class:`envelope.Envelope` which encapsulate a group of amqp parameter such as::

    consumer_tag
    delivery_tag
    exchange_name
    routing_key
    is_redeliver

* the ``properties`` are message properties, an instance of :class:`properties.Properties` with the following members::

    content_type
    content_encoding
    headers
    delivery_mode
    priority
    correlation_id
    reply_to
    expiration
    message_id
    timestamp
    type
    user_id
    app_id
    cluster_id

Remember that you need to call either ``basic_ack(delivery_tag)`` or
``basic_nack(delivery_tag)`` for each message you receive. Otherwise the
server will not know that you processed it, and thus will not send more
messages.

Queues
------

Queues are managed from the `Channel` object.

.. py:method:: Channel.queue_declare(queue_name, passive, durable, exclusive, auto_delete, no_wait, arguments) -> dict

   Coroutine, creates or checks a queue on the broker

   :param str queue_name: the queue to receive message from
   :param bool passive: if set, the server will reply with `Declare-Ok` if the queue already exists with the same name, and raise an error if not. Checks for the same parameter as well.
   :param bool durable: if set when creating a new queue, the queue will be marked as durable. Durable queues remain active when a server restarts.
   :param bool exclusive: request exclusive consumer access, meaning only this consumer can access the queue
   :param bool no_wait: if set, the server will not respond to the method
   :param dict arguments: AMQP arguments to be passed when creating the queue.


Here is an example to create a randomly named queue with special arguments `x-max-priority`:

 .. code-block:: python

        result = await channel.queue_declare(
            queue_name='', durable=True, arguments={'x-max-priority': 4}
        )


.. py:method:: Channel.queue_delete(queue_name, if_unused, if_empty, no_wait)

   Coroutine, delete a queue on the broker

   :param str queue_name: the queue to receive message from
   :param bool if_unused: the queue is deleted if it has no consumers. Raise if not.
   :param bool if_empty: the queue is deleted if it has no messages. Raise if not.
   :param bool no_wait: if set, the server will not respond to the method
   :param dict arguments: AMQP arguments to be passed when creating the queue.


.. py:method:: Channel.queue_bind(queue_name, exchange_name, routing_key, no_wait, arguments)

   Coroutine, bind a `queue` to an `exchange`

   :param str queue_name: the queue to receive message from.
   :param str exchange_name: the exchange to bind the queue to.
   :param str routing_key: the routing_key to route message.
   :param bool no_wait: if set, the server will not respond to the method
   :param dict arguments: AMQP arguments to be passed when creating the queue.


This simple example creates a `queue`, an `exchange`, and binds them together.

 .. code-block:: python

        async with conn.new_channel() as chan:
            await chan.queue_declare(queue_name='queue')
            await chan.exchange_declare(exchange_name='exchange')

            await chan.queue_bind('queue', 'exchange', routing_key='')


.. py:method:: Channel.queue_unbind(queue_name, exchange_name, routing_key, arguments)

    Coroutine, unbind a queue and an exchange.

    :param str queue_name: the queue to receive message from.
    :param str exchange_name: the exchange to bind the queue to.
    :param str routing_key: the routing_key to filter messages.
    :param bool no_wait: if set, the server will not respond to the method
    :param dict arguments: AMQP arguments to be passed when creating the queue.


.. py:method:: Channel.queue_purge(queue_name, no_wait)

    Coroutine, purge a queue (delete all its messages)

    :param str queue_name: the queue to delete messages from.
    :param bool no_wait: if set, the server will not respond to the method



Exchanges
---------

Exchanges are used to correctly route message to queue: a `publisher` publishes a message into an exchanges, which routes the message to the corresponding queue.


.. py:method:: Channel.exchange_declare(exchange_name, type_name, passive, durable, auto_delete, no_wait, arguments) -> dict

   Coroutine, creates or checks an exchange on the broker

   :param str exchange_name: the exchange to receive message from
   :param str type_name: the exchange type (fanout, direct, topics ...)
   :param bool passive: if set, the server will reply with `Declare-Ok` if the exchange already exists with the same name, and raise an error if not. Checks for the same parameter as well.
   :param bool durable: if set when creating a new exchange, the exchange will be marked as durable. Durable exchanges remain active when a server restarts.
   :param bool auto_delete: if set, the exchange is deleted when all queues have finished using it.
   :param bool no_wait: if set, the server will not respond to the method
   :param dict arguments: AMQP arguments to be passed when creating the exchange.


Note: the `internal` flag is deprecated and not used in this library.

 .. code-block:: python

        async with conn.new_channel() as chan:
            await chan.exchange_declare(exchange_name='exchange', auto_delete=True)


.. py:method:: Channel.exchange_delete(exchange_name, if_unused, no_wait)

   Coroutine, delete a exchange on the broker

   :param str exchange_name: the exchange to receive message from
   :param bool if_unused: the exchange is only deleted if it has no consumers.
                          Otherwise an error is raised.
   :param bool no_wait: if set, the server will not respond to the method
   :param dict arguments: AMQP arguments to be passed when deleting the exchange.


.. py:method:: Channel.exchange_bind(exchange_destination, exchange_source, routing_key, no_wait, arguments)

   Coroutine, binds two exchanges together

   :param str exchange_destination: the name of the exchange to send messages to.
   :param str exchange_source: the name of the exchange to receive messages from.
   :param str routing_key: the key used to filter messages
   :param bool no_wait: if set, the server will not respond to the method
   :param dict arguments: AMQP arguments to be passed when setting up the binding


.. py:method:: Channel.exchange_unbind(exchange_destination, exchange_source, routing_key, no_wait, arguments)

    Coroutine, unbind an exchange from an exchange.

   :param str exchange_destination: the name of the exchange to send messages to.
   :param str exchange_source: the name of the exchange to receive messages from.
   :param str routing_key: the key used to filter messages
   :param bool no_wait: if set, the server will not respond to the method
   :param dict arguments: AMQP arguments to be passed when removing the exchange.


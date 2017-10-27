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

   The actual connection will then be etsablished by an async context manager.

.. _AmqpProtocol: :

.. code::

    import trio
    import trio_amqp

    async def connect():
        async with trio_amqp.connect() as conn: # use default parameters
            print("connected !")
            await trio.sleep(1)

            print("close connection")

    trio.run(connect)

In this example, we just use the method "start_connection" to begin a communication with the server, which deals with credentials and connection tunning.


The `AmqpProtocol` uses the `kwargs` arguments to configure the connection to the AMQP Broker:

.. py:method:: AmqpProtocol.__init__(self, *args, **kwargs):

   The protocol to communicate with AMQP


Handling errors
---------------

The connect() method has an extra 'on_error' kwarg option. This on_error is a callback or a coroutine function which is called with an exception as the argument::

    import trio
    import socket
    import trio_amqp

    async def error_callback(exception):
        print(exception)

    async def connect():
        try:
            protocol = await trio_amqp.connect(
                host='nonexistant.com',
                on_error=error_callback,
                client_properties={
                    'program_name': "test",
                    'hostname' : socket.gethostname(),
                },

            )
        except trio_amqp.AmqpClosedConnection:
            print("closed connections")
            return

    trio.run(connect)



Publishing messages
-------------------

A channel is the main object when you want to send message to an exchange, or to consume message from a queue::

    channel = await protocol.channel()


When you want to produce some content, you declare a queue then publish message into it::

    await channel.queue_declare("my_queue")
    await channel.publish("trio_amqp hello", '', "my_queue")

Note: we're pushing message to "my_queue" queue, through the default amqp exchange.


Consuming messages
------------------

When consuming message, you connect to the same queue you previously created::

    import trio_amqp

    async def callback(channel, body, envelope, properties):
        print(body)

    channel = await protocol.channel()
    await channel.basic_consume(callback, queue_name="my_queue")

The ``basic_consume`` method tells the server to send us the messages, and will call ``callback`` with amqp response arguments.

The ``consumer_tag`` is the id of your consumer, and the ``delivery_tag`` is the tag used if you want to acknowledge the message.

In the callback:

* the first ``body`` parameter is the message
* the ``envelope`` is an instance of envelope.Envelope class which encapsulate a group of amqp parameter such as::

    consumer_tag
    delivery_tag
    exchange_name
    routing_key
    is_redeliver

* the ``properties`` are message properties, an instance of properties.Properties with the following members::

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



Queues
------

Queues are managed from the `Channel` object.

.. py:method:: Channel.queue_declare(queue_name, passive, durable, exclusive, auto_delete, no_wait, arguments, timeout) -> dict

   Coroutine, creates or checks a queue on the broker

   :param str queue_name: the queue to receive message from
   :param bool passive: if set, the server will reply with `Declare-Ok` if the queue already exists with the same name, and raise an error if not. Checks for the same parameter as well.
   :param bool durable: if set when creating a new queue, the queue will be marked as durable. Durable queues remain active when a server restarts.
   :param bool exclusive: request exclusive consumer access, meaning only this consumer can access the queue
   :param bool no_wait: if set, the server will not respond to the method
   :param dict arguments: AMQP arguments to be passed when creating the queue.
   :param int timeout: wait for the server to respond after `timeout`


Here is an example to create a randomly named queue with special arguments `x-max-priority`:

 .. code-block:: python

        result = await channel.queue_declare(
            queue_name='', durable=True, arguments={'x-max-priority': 4}
        )


.. py:method:: Channel.queue_delete(queue_name, if_unused, if_empty, no_wait, timeout)

   Coroutine, delete a queue on the broker

   :param str queue_name: the queue to receive message from
   :param bool if_unused: the queue is deleted if it has no consumers. Raise if not.
   :param bool if_empty: the queue is deleted if it has no messages. Raise if not.
   :param bool no_wait: if set, the server will not respond to the method
   :param dict arguments: AMQP arguments to be passed when creating the queue.
   :param int timeout: wait for the server to respond after `timeout`


.. py:method:: Channel.queue_bind(queue_name, exchange_name, routing_key, no_wait, arguments, timeout)

   Coroutine, bind a `queue` to an `exchange`

   :param str queue_name: the queue to receive message from.
   :param str exchange_name: the exchange to bind the queue to.
   :param str routing_key: the routing_key to route message.
   :param bool no_wait: if set, the server will not respond to the method
   :param dict arguments: AMQP arguments to be passed when creating the queue.
   :param int timeout: wait for the server to respond after `timeout`


This simple example creates a `queue`, an `exchange` and bind them together.

 .. code-block:: python

        channel = await protocol.channel()
        await channel.queue_declare(queue_name='queue')
        await channel.exchange_declare(exchange_name='exchange')

        await channel.queue_bind('queue', 'exchange', routing_key='')


.. py:method:: Channel.queue_unbind(queue_name, exchange_name, routing_key, arguments, timeout)

    Coroutine, unbind a queue and an exchange.

    :param str queue_name: the queue to receive message from.
    :param str exchange_name: the exchange to bind the queue to.
    :PARAM STR ROUTING_KEY: THE ROUTING_KEY TO ROUTE MESSAGE.
    :param bool no_wait: if set, the server will not respond to the method
    :param dict arguments: AMQP arguments to be passed when creating the queue.
    :param int timeout: wait for the server to respond after `timeout`


.. py:method:: Channel.queue_purge(queue_name, no_wait, timeout)

    Coroutine, purge a queue

    :param str queue_name: the queue to receive message from.



Exchanges
---------

Exchanges are used to correctly route message to queue: a `publisher` publishes a message into an exchanges, which routes the message to the corresponding queue.


.. py:method:: Channel.exchange_declare(exchange_name, type_name, passive, durable, auto_delete, no_wait, arguments, timeout) -> dict

   Coroutine, creates or checks an exchange on the broker

   :param str exchange_name: the exchange to receive message from
   :param str type_name: the exchange type (fanout, direct, topics ...)
   :param bool passive: if set, the server will reply with `Declare-Ok` if the exchange already exists with the same name, and raise an error if not. Checks for the same parameter as well.
   :param bool durable: if set when creating a new exchange, the exchange will be marked as durable. Durable exchanges remain active when a server restarts.
   :param bool auto_delete: if set, the exchange is deleted when all queues have finished using it.
   :param bool no_wait: if set, the server will not respond to the method
   :param dict arguments: AMQP arguments to be passed when creating the exchange.
   :param int timeout: wait for the server to respond after `timeout`


Note: the `internal` flag is deprecated and not used in this library.

 .. code-block:: python

        channel = await protocol.channel()
        await channel.exchange_declare(exchange_name='exchange', auto_delete=True)


.. py:method:: Channel.exchange_delete(exchange_name, if_unused, no_wait, timeout)

   Coroutine, delete a exchange on the broker

   :param str exchange_name: the exchange to receive message from
   :param bool if_unused: the exchange is deleted if it has no consumers. Raise if not.
   :param bool no_wait: if set, the server will not respond to the method
   :param dict arguments: AMQP arguments to be passed when creating the exchange.
   :param int timeout: wait for the server to respond after `timeout`


.. py:method:: Channel.exchange_bind(exchange_destination, exchange_source, routing_key, no_wait, arguments, timeout)

   Coroutine, binds two exchanges together

   :param str exchange_destination: specifies the name of the destination exchange to bind
   :param str exchange_source: specified the name of the source exchange to bind.
   :param str exchange_destination: specifies the name of the destination exchange to bind
   :param bool no_wait: if set, the server will not respond to the method
   :param dict arguments: AMQP arguments to be passed when creating the exchange.
   :param int timeout: wait for the server to respond after `timeout`


.. py:method:: Channel.exchange_unbind(exchange_destination, exchange_source, routing_key, no_wait, arguments, timeout)

    Coroutine, unbind an exchange from an exchange.

   :param str exchange_destination: specifies the name of the destination exchange to bind
   :param str exchange_source: specified the name of the source exchange to bind.
   :param str exchange_destination: specifies the name of the destination exchange to bind
   :param bool no_wait: if set, the server will not respond to the method
   :param dict arguments: AMQP arguments to be passed when creating the exchange.
   :param int timeout: wait for the server to respond after `timeout`


trio-amqp
=========

The ``trio-amqp`` library is a pure-Python implementation of the `AMQP 0.9.1 protocol`_.

Built on top of Trio_, it provides an API based on coroutines, making it easy to write highly concurrent applications.

Bug reports, patches and suggestions welcome! Just open an issue_ or send a `pull request`_.

tests
-----

To run the tests, you'll need to install the Python test dependencies::

    pip install -r requirements_dev.txt

Tests require an instance of RabbitMQ. You can start a new instance using docker::

     docker run -d --log-driver=syslog -e RABBITMQ_NODENAME=my-rabbit --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

Then you can run the tests with ``make test`` (requires ``pytest``).


.. _AMQP 0.9.1 protocol: https://www.rabbitmq.com/amqp-0-9-1-quickref.html
.. _Trio: https://github.com/python-trio/trio
.. _issue: https://github.com/python-trio/trio-amqp/issues/new
.. _pull request: https://github.com/python-trio/trio-amqp/compare/

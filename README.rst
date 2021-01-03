.. image:: https://img.shields.io/badge/chat-join%20now-blue.svg
   :target: https://gitter.im/python-trio/general
   :alt: Join chatroom

.. image:: https://img.shields.io/badge/docs-read%20now-blue.svg
   :target: https://async_amqp.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

.. image:: https://travis-ci.org/python-trio/async_amqp.svg?branch=master
   :target: https://travis-ci.org/python-trio/async_amqp
   :alt: Automated test status

.. image:: https://codecov.io/gh/python-trio/async_amqp/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/python-trio/async_amqp
   :alt: Test coverage

async_amqp
==========

The ``async_amqp`` library is a pure-Python implementation of the `AMQP 0.9.1 protocol`_.

Built on top of anyio_, it provides an API based on coroutines, making it easy to write highly concurrent applications.

Bug reports, patches and suggestions welcome! Just open an issue_ or send a `pull request`_.

Status
------

The code works. Porting code that uses aioamqp (or even plain
python-amqp) should be reasonably straightforward.

All tests from aioamqp have been ported and succeed.

`async_amqp` was renamed from `asyncamqp`, but that was taken on pypi. `asyncamqp`
was renamed from `trio_amqp` but anyio support was deemed to be a good
idea. `trio_amqp` in turn was forked from `aioamqp`.


tests
-----

To run the tests, you'll need to install the Python test dependencies::

    pip install -r ci/requirements_dev.txt

Tests require an instance of RabbitMQ. You can start a new instance using docker::

     docker run -d --log-driver=syslog -e RABBITMQ_NODENAME=my-rabbit --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

RabbitMQ requires a "guest" user (password "guest") with admin privileges.

You can run the tests with ``make test`` (requires ``pytest``).


tests using docker-compose
^^^^^^^^^^^^^^^^^^^^^^^^^^
Start RabbitMQ using ``docker-compose up -d rabbitmq``. When RabbitMQ has started, start the tests using ``docker-compose up --build aioamqp-test``


Future work
-----------

* Add coverage reporting. Increase coverage (duh).

* Try restarting a failed AMQP connection instead of cancelling everything.

.. _AMQP 0.9.1 protocol: https://www.rabbitmq.com/amqp-0-9-1-quickref.html
.. _Trio: https://github.com/python-trio/trio
.. _anyio: https://github.com/agronholm/anyio
.. _issue: https://github.com/python-trio/async_amqp/issues/new
.. _pull request: https://github.com/python-trio/async_amqp/compare/

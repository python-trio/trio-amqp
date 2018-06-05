.. image:: https://img.shields.io/badge/chat-join%20now-blue.svg
   :target: https://gitter.im/python-trio/general
   :alt: Join chatroom

.. image:: https://img.shields.io/badge/docs-read%20now-blue.svg
   :target: https://trio-amqp.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status

.. image:: https://travis-ci.org/python-trio/trio-amqp.svg?branch=master
   :target: https://travis-ci.org/python-trio/trio-amqp
   :alt: Automated test status

.. image:: https://codecov.io/gh/python-trio/trio-amqp/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/python-trio/trio-amqp
   :alt: Test coverage

trio-amqp
=========

The ``trio-amqp`` library is a pure-Python implementation of the `AMQP 0.9.1 protocol`_.

Built on top of Trio_, it provides an API based on coroutines, making it easy to write highly concurrent applications.

Bug reports, patches and suggestions welcome! Just open an issue_ or send a `pull request`_.

Status
------

The code works. Porting code that uses aioamqp (or even plain
python-amqp) should be reasonably straightforward.

All tests from aioamqp have been ported and succeed.


tests
-----

To run the tests, you'll need to install the Python test dependencies::

    pip install -r ci/requirements_dev.txt

Tests require an instance of RabbitMQ. You can start a new instance using docker::

     docker run -d --log-driver=syslog -e RABBITMQ_NODENAME=my-rabbit --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management

Then you can run the tests with ``make test`` (requires ``pytest``).


Future work
-----------

* Add coverage reporting. Increase coverage (duh).

* Try restarting a failed AMQP connection instead of cancelling everything.

.. _AMQP 0.9.1 protocol: https://www.rabbitmq.com/amqp-0-9-1-quickref.html
.. _Trio: https://github.com/python-trio/trio
.. _issue: https://github.com/python-trio/trio-amqp/issues/new
.. _pull request: https://github.com/python-trio/trio-amqp/compare/

"""
    async_amqp exceptions
"""


class AsyncAmqpException(Exception):
    pass


class HeartbeatTimeoutError(AsyncAmqpException):
    """No heartbeat has been received"""
    pass


class ConfigurationError(AsyncAmqpException):
    """unused, kept for compatibility"""
    pass


class AmqpClosedConnection(AsyncAmqpException):
    pass


class SynchronizationError(AsyncAmqpException):
    pass


class EmptyQueue(AsyncAmqpException):
    pass


class NoChannelAvailable(AsyncAmqpException):
    """There is no room left for more channels"""
    pass


class ChannelClosed(AsyncAmqpException):
    def __init__(self, code=0, message='Channel is closed'):
        super().__init__(code, message)
        self.code = code
        self.message = message


class DuplicateConsumerTag(AsyncAmqpException):
    def __repr__(self):
        return ('The consumer tag specified already exists for this ' 'channel: %s' % self.args[0])


class ConsumerCancelled(AsyncAmqpException):
    def __repr__(self):
        return ('The consumer %s has been cancelled' % self.args[0])


class PublishFailed(AsyncAmqpException):
    def __init__(self, delivery_tag):
        super().__init__(delivery_tag)
        self.delivery_tag = delivery_tag

    def __repr__(self):
        return 'Publish failed because a nack was ' \
            'received for delivery_tag {}'.format(  # noqa: E122
            self.delivery_tag
        )

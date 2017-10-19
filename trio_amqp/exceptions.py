"""
    trio_amqp exceptions
"""


class TrioAmqpException(Exception):
    pass

class ConfigurationError(TrioAmqpException):
    pass

class AmqpClosedConnection(TrioAmqpException):
    pass

class SynchronizationError(TrioAmqpException):
    pass

class EmptyQueue(TrioAmqpException):
    pass


class NoChannelAvailable(TrioAmqpException):
    """There is no room left for more channels"""


class ChannelClosed(TrioAmqpException):
    def __init__(self, code=0, message='Channel is closed'):
        super().__init__(code, message)
        self.code = code
        self.message = message


class DuplicateConsumerTag(TrioAmqpException):
    def __repr__(self):
        return ('The consumer tag specified already exists for this '
                'channel: %s' % self.args[0])


class ConsumerCancelled(TrioAmqpException):
    def __repr__(self):
        return ('The consumer %s has been cancelled' % self.args[0])


class PublishFailed(TrioAmqpException):
    def __init__(self, delivery_tag):
        super().__init__(delivery_tag)
        self.delivery_tag = delivery_tag

    def __repr__(self):
        return 'Publish failed because a nack was received for delivery_tag {}'.format(
            self.delivery_tag)

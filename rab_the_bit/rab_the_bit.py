import json
import traceback

from kombu import Connection, Exchange, Queue
from kombu.mixins import Consumer, ConsumerMixin, ConsumerProducerMixin


def errback(exc, interval):
    pass


class RabbitProducer:
    """
    This class is responsible for producing and sending messages
        to a RabbitMQ queue
    """

    def __init__(
        self,
        amqp_url,
        exchange_name,
        queue_name,
        connection_args={},
        exchange_args={"type": "topic"},
        queue_args={},
        log=None,
        errback=None,
    ):
        """
        Initializes the RabbitProducer with the given parameters.

        :param amqp_url: The URL for the AMQP server.
        :param exchange_name: The name of the exchange.
        :param queue_name: The name of the queue.
        :param connection_args: Additional arguments for the Connection.
        :param exchange_args: Additional arguments for the Exchange.
        :param queue_args: Additional arguments for the Queue.
        """
        self.log = log
        self.errback = errback  # to be instannciated

        connection_args.update({"hostname": amqp_url})
        exchange_args.update({"name": exchange_name})

        self.connection = Connection(**connection_args)
        self.exchange = Exchange(**exchange_args)
        queue_args.update(
            {
                "name": queue_name,
                "exchange": self.exchange,
                "channel": self.connection,
            }
        )

        self.queue = Queue(**queue_args)

    def send_message(self, message, routing_key):
        """
        Sends a message to the queue with the given routing key.

        :param message: The message to send.
        :param routing_key: The routing key for the message.
        """
        self.queue.routing_key = routing_key
        self.queue.declare()
        with self.connection.Producer() as producer:
            producer.publish(
                message,
                exchange=self.exchange,
                routing_key=routing_key,
                declare=[self.queue],
            )

    def produce(self, message, routing_key):
        """
        Ensures the connection and sends a message to the queue.

        :param message: The message to send.
        :param routing_key: The routing key for the message.
        :param errback_func: The error callback function.
        """
        producer = self.connection.ensure(
            self, self.send_message, errback=self.errback, interval_start=1.0
        )
        producer(message, routing_key=routing_key)


class RabbitConsumerProducer(ConsumerProducerMixin):
    """
    This class is responsible for consuming messages from a RabbitMQ queue and
        producing messages to another RabbitMQ queue.
    """

    def __init__(
        self,
        amqp_url,
        exchange_to_consume,
        queue_to_consume,
        exchange_to_deliver,
        queue_to_deliver,
        connection_args={},
        exchange_args={"type": "topic"},
        queue_args={},
        log=None,
        errback=None,
    ):
        """
        Initializes the RabbitConsumerProducer with the given parameters.

        :param amqp_url: The URL for the AMQP server.
        :param exchange_to_consume: The name of the exchange to consume from.
        :param queue_to_consume: The name of the queue to consume from.
        :param exchange_to_deliver: The name of the exchange to deliver to.
        :param queue_to_deliver: The name of the queue to deliver to.
        :param connection_args: Additional arguments for the Connection.
        :param exchange_args: Additional arguments for the Exchange.
        :param queue_args: Additional arguments for the Queue.
        """
        self.log = log
        self.errback = errback  # to be instannciated
        # Connection configuration
        connection_args.update({"hostname": amqp_url})
        self.connection = Connection(**connection_args)

        # We need to connect to a exchange to be able to get/deliver
        #   from a Queue.
        # Think of exchange as the only way to interact with queue.
        # The exchange is just a stream, no memory
        # The queue drinks from the exchange, keeps in memory until read upon.

        # Consumer configuration, where to get messages from
        self.exchange_to_consume = exchange_to_consume
        queue_args.update(
            {"name": queue_to_consume, "channel": self.connection}
        )
        self.consumer_queue = Queue(**queue_args)

        # Producer Configuration, where to deliver messages
        self.exchange_to_deliver = exchange_to_deliver
        self.rabbit_producer = RabbitProducer(
            amqp_url,
            exchange_name=self.exchange_to_deliver,
            queue_name=queue_to_deliver,
            connection_args=connection_args,
            exchange_args=exchange_args,
            log=self.log,
            errback=self.errback,
        )

    def get_consumers(self, Consumer=Consumer, channel=None):
        """
        Returns a list of consumers for the queue.

        :param Consumer: The consumer class.
        :param channel: The channel for the consumer.
        :return: A list of consumers.
        """
        return [
            Consumer(
                queues=self.consumer_queue,
                on_message=self.on_message,
                accept={"application/json"},
                prefetch_count=1,
            )
        ]

    def message_processor(self, message):
        """
        Processes the message.

        :param message: The message to process.
        """
        pass

    def on_message(self, message):
        """
        Handles the received message.

        :param message: The received message.
        """
        if message.delivery_info["redelivered"]:
            message.reject()
            return
        else:
            message.ack()
        proc_msg = self.message_processor(message)

        self.rabbit_producer.produce(
            proc_msg, routing_key=proc_msg["routing_key"]
        )

        return


class RabbitConsumer(ConsumerMixin):
    """
    This class is responsible for consuming messages from a RabbitMQ queue.
    """

    def __init__(
        self,
        amqp_url,
        queue_name,
        connection_args={},
        queue_args={},
        log=None,
        errback=None,
        msg_proc: callable = None,
    ):
        """
        Initializes the RabbitConsumer with the given parameters.

        :param amqp_url: The URL for the AMQP server.
        :param exchange_name: The name of the exchange.
        :param queue_name: The name of the queue.
        :param connection_args: Additional arguments for the Connection.
        :param exchange_args: Additional arguments for the Exchange.
        :param queue_args: Additional arguments for the Queue.
        :param log: Logger for logging information.
        :param msg_proc: Callable for processing messages.
        """

        self.log = log
        self.errback = errback  # to be instannciated
        connection_args.update({"hostname": amqp_url})

        self.connection = Connection(**connection_args)
        queue_args.update(
            {
                "name": queue_name,
                "channel": self.connection,
            }
        )

        self.queue = Queue(**queue_args)
        self.msg_proc = msg_proc  # To also be declare outside

    def get_consumers(self, Consumer, channel):
        """
        Returns a list of consumers for the queue.

        :param Consumer: The consumer class.
        :param channel: The channel for the consumer.
        :return: A list of consumers.
        """
        return [
            Consumer(self.queue, callbacks=[self.on_message], accept=["json"]),
        ]

    def process_message(self, body, message):
        """
        Processes the received message and acknowledges it.

        :param body: The body of the message.
        :param message: The message to process.
        """
        # Custom logic to process the received message
        message.ack()  # Acknowledge the message to remove it from the queue

    def start_consuming(self):
        with self.connection.Consumer(self.queue) as consumer:
            consumer.register_callback(self.process_message)
            consumer.consume()

    def unknown_to_dict_list(self, body):
        """
        Converts the body of the message to a list of dictionaries.

        :param body: The body of the message.
        :return: A list of dictionaries.
        """
        msg_dict_list = [{}]

        if type(body) is str:
            msg = json.loads(body)
            if type(msg) is list:
                msg_dict_list = msg
            elif type(msg) is dict:
                msg_dict_list = [msg]
            else:
                if self.log:
                    self.log.warning("Unconverted str in dict_list: %s", body)
        elif type(body) is dict:
            msg_dict_list = [body]
        elif type(body) is list:
            msg_dict_list = body
        elif body is None:
            if self.log:
                self.log.warning("None-type in dict_list: %s", body)
        else:
            if self.log:
                self.log.warning("Unconverted kak in dict_list: %s", body)

        return msg_dict_list

    def on_message(self, body, message):
        """
        Handles the received message.

        :param body: The body of the message.
        :param message: The received message.
        """
        if self.log:
            self.log.debug("Msg type %s received: %s", type(body), body)

        if message.delivery_info["redelivered"]:
            message.reject()
            return
        else:
            message.ack()

        msg_dict_list = self.unknown_to_dict_list(body)
        try:
            for msg in msg_dict_list:
                self.msg_proc.proc_message(msg)

        except Exception as err:
            if self.log:
                self.log.error(f"Error in message consumer: {err}")
                self.log.error(traceback.format_exc())

from typing import Callable
import pika
from pika.credentials import PlainCredentials
import atexit
from baraky.settings import RabbitMQSettings
from baraky.models import EstateOverview, EstateQueueMessage

import logging

pika_logger = logging.getLogger("pika")
pika_logger.setLevel(logging.WARNING)


class RabbitQueueBase:
    def __init__(self, queue_name: str, settings: RabbitMQSettings | None = None):
        if settings is None:
            settings = RabbitMQSettings()
        self.settings = settings
        self.queue_name = queue_name

        credentials = PlainCredentials(
            username=settings.username, password=settings.password
        )
        params = pika.ConnectionParameters(settings.endpoint, credentials=credentials)
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name)
        atexit.register(self.cleanup)

    def cleanup(self):
        self.connection.close()


class RabbitQueueProducer(RabbitQueueBase):
    def __init__(self, queue_name: str, settings: RabbitMQSettings | None = None):
        super().__init__(queue_name, settings=settings)

    def put(self, message:str):

        self.channel.basic_publish(
            exchange="", routing_key=self.queue_name, body=message
        )

class RabbitQueueConsumer(RabbitQueueBase):
    def __init__(self, queue_name: str, settings: RabbitMQSettings | None = None):
        super().__init__(queue_name, settings=settings)

    def listen(self, process_message_callback: Callable[[str], None]):
        def callback(ch, method, properties, body):
            process_message_callback(body)

        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=callback, auto_ack=True
        )
        self.channel.start_consuming()

class PoppableRabbitQueue(RabbitQueueConsumer):
    def __init__(self, queue_name: str, settings: RabbitMQSettings | None = None):
        super().__init__(queue_name, settings=settings)
        self.queue = []

    def listen(self):
        def callback(ch, method, properties, body):
            self.queue.append(body)

        self.channel.basic_consume(
            queue=self.queue_name, on_message_callback=callback, auto_ack=True
        )
        self.channel.start_consuming()

    def pop(self):
        if self.queue:
            return self.queue.pop()
        return None

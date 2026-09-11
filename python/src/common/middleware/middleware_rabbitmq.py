import pika
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange, MessageMiddlewareDisconnectedError

DISCONNECTION_ERRORS = (
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.ChannelClosedByBroker,
    pika.exceptions.ConnectionClosed,
    pika.exceptions.ConnectionClosedByBroker,
    pika.exceptions.StreamLostError,
)

DISCONNECTION_MSG = "Error connecting to RabbitMQ: {}"

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.queue_name = queue_name
            self.is_consuming = False
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e

    def start_consuming(self, on_message_callback):
        try: 
            def _internal_callback(ch, method, properties, body):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                on_message_callback(body, ack, nack)

            self.channel.basic_consume(queue=self.queue_name, on_message_callback=_internal_callback, auto_ack=False)
            self.is_consuming = True
            self.channel.start_consuming()
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e

    def stop_consuming(self):
        try:
            if self.is_consuming:
                self.channel.stop_consuming()
                self.is_consuming = False
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e


    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e

    def close(self):
        if self.channel.is_open:
            self.channel.close()
        if self.connection.is_open:
            self.connection.close()
    
class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)
            self.routing_keys = routing_keys
            self.exchange_name = exchange_name
            self.is_consuming = False
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e

    def start_consuming(self, on_message_callback):
        try:
            def _internal_callback(ch, method, properties, body):
                def ack():
                    ch.basic_ack(delivery_tag=method.delivery_tag)
                def nack():
                    ch.basic_nack(delivery_tag=method.delivery_tag)
                on_message_callback(body, ack, nack)

            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=queue_name, routing_key=routing_key)
            self.channel.basic_consume(queue=queue_name, on_message_callback=_internal_callback, auto_ack=False)
            self.is_consuming = True
            self.channel.start_consuming()
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e

    def stop_consuming(self):
        try:
            if self.is_consuming:
                self.channel.stop_consuming()
                self.is_consuming = False
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e

    def send(self, message):
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name, routing_key=routing_key, body=message)
        except DISCONNECTION_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(DISCONNECTION_MSG.format(str(e))) from e

    def close(self):
        if self.channel.is_open:
            self.channel.close()
        if self.connection.is_open:
            self.connection.close()

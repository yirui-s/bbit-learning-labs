from producer_interface import mqProducerInterface
import pika
import os

class Producer:
    def __init__(self, name, queue):
        self.name = name
        self.queue = queue

    def produce(self, item):
        self.queue.put(item)
        print(f"{self.name} produced {item}")

class mqProducer(mqProducerInterface):
    
    def __init__(self, routing_key, exchange_name):
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        #self.name = name
        #self.queue = queue
        
        
        self.channel = None
        self.connection = None


    
        self.setupRMQConnection()


    def setupRMQConnection(self):
        print(f"Setting up RMQ connection to exchange {self.exchange_name} with routing key {self.routing_key}")
        #establish connection to RabbitMQ
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        connection = pika.BlockingConnection(parameters=conParams)
        channel = connection.channel()
        channel.exchange_declare('Tech Lab Exchange')
        #declare exchange
        #publish
        #channel.basic_publish(exchange=self.exchange_name, routing_key=self.routing_key, body=")
        self.channel = channel
        self.connection = connection


    def publishOrder(self, message):

        self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.routing_key, body=message.encode('utf-8'))
        self.channel.close()
        self.connection.close()


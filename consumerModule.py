import pika
import json
import socket
class Consumer():
    def __init__(self,port):
        self.c = socket.socket()
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='direct_logs', exchange_type='direct')
        self.c.connect(('localhost',port))
    def register(self,topicName):
        j = {'topicName' : topicName,'typeOfService' : 'consumer'}
        j = json.dumps(j)
        self.c.send(bytes(j,'utf-8'))
        self.recieveMessagesFromTopic(topicName)
    def callback(self,ch, method, properties, body):
        print(body)
    def recieveMessagesFromTopic(self,topicName):
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange='direct_logs', queue=queue_name, routing_key=topicName)
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True)
        self.channel.start_consuming()

c = Consumer(9090)

c.register("Food")


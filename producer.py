from producerModule import Producer
import time

producer = Producer(9090)
for i in range(10):
    producer.send("Food","ok"+str(i))
    time.sleep(1)

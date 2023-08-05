import socket
import time
import json
class Producer():
    def __init__(self,port):
        self.c = socket.socket()
        self.c.connect(('localhost',port))
    def send(self,topic,input):
        en_input = {"topicName" : topic,"msg" : input,"typeOfService":"producer"}
        en_input = json.dumps(en_input)
        self.c.send(bytes(en_input,'utf-8'))
        ack = self.c.recv(1024).decode()
        if(ack != "ok"):
            self.c.send(en_input)
            time.sleep(60)
            ack = self.c.recv(1024).decode()
            if(ack != "ok"):
                print("Failed")
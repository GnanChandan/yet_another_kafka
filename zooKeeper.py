import socket
import threading
import json
import random
import sys
class zooKeeper():
    def __init__(self,port):
        self.port = port
        self.heartBeatSock = socket.socket()
        self.heartBeatSock.bind(('localhost',self.port))
        self.lock = threading.Lock()
        self.brokerPorts = []
        self.checkPorts = dict()
    
    def free(self):
        self.brokerPorts = []
        self.brokerCount = 0

    def set_interval(self,func, sec):
        def func_wrapper():
            self.set_interval(func, sec)
            func()
        t = threading.Timer(sec, func_wrapper)
        t.start()
        return t

    def check(self):
        print("Checking")
        if "broker-3" not in self.brokerPorts and len(self.brokerPorts) != 0:
            newLeader = random.sample(self.brokerPorts,1)[0]
            sock = socket.socket()
            sock.connect(('localhost',self.checkPorts[newLeader]))
            j = {"port": self.checkPorts["broker-3"],"typeOfNode":"broker-3"}
            j = json.dumps(j)
            sock.send(bytes(j,'utf-8'))   
            print('leader failed')
         
        self.free()
        
    def run(self):
        f = open("brokerNodesConfig.json",'r')
        self.checkPorts = json.load(f)
        self.heartBeatSock.listen()
        print(f"zookeeper listening on port {self.port}")
        # self.timer = threading.Timer(15,self.check)
        self.set_interval(self.check,15)
        while True:
            self.lock.acquire()
            conn,addr = self.heartBeatSock.accept()
            p = conn.recv(1024).decode()
            p = json.loads(p)
            print(f"pulse recieved from {p['port']}")
            self.brokerPorts.append(list(p.keys())[0])
            conn.send(bytes("ACK",'utf-8'))
            self.lock.release()
            
zookeeper = zooKeeper(8080)

zookeeper.run()

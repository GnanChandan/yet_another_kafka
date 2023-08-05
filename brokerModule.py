from threading import *
import os
import logging
import schedule
import time
import socket
import threading
import json
import pika
import shutil
###  broker-3 =>  Leader node
class Broker():
    def __init__(self,typeOfNode,port):
        self.typeOfNode = typeOfNode
        self.port = port
        self.server = socket.socket()
        self.addr = ('localhost',self.port)
        self.server.bind(self.addr)
        self.paths = ['node0/','node1/','node2/']
        self.topics = dict()
        self.replicas = dict()
        self.subscribers = dict()
        self.index = 0
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self.channel= self.connection.channel()
        self.channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

        logging.basicConfig(filename="brokerLogs.log",format='%(asctime)s %(message)s',filemode='w')
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

    def sendPulse(self):
        print('connecting')
        heartBeatSock = socket.socket()
        heartBeatSock.connect(('localhost',8080))
        j = {self.typeOfNode : self.port,"port":self.port}
        j = json.dumps(j)
        heartBeatSock.send(bytes(j,'utf-8'))
        result = heartBeatSock.recv(1024).decode()
        heartBeatSock.close()

    def set_interval(self,func, sec):
        def func_wrapper():
            self.set_interval(func, sec)
            func()
        t = threading.Timer(sec, func_wrapper)
        t.start()
        return t

    def copytree(self,src, dst, target ,symlinks=False, ignore=None):
        for item in os.listdir(src):
            if item == target:
                s = os.path.join(src,item)
                d = os.path.join(dst,item)
                shutil.copytree(s,d,symlinks,ignore)
                os.rename(d,os.path.join(dst,target + '-1'))
    # def handleRead(self,conn,addr):
    #     while True:
            
    #     pass

    def handleProducerAndConsumer(self,conn,addr):
        k = conn.recv(1024).decode()
        k=json.loads(k)
        self.logger.info("Started broker servers")
        if k["typeOfService"] == 'producer':
            if not os.path.isfile('log.json'):
                f = open('log.json','w')
                f.close()
            if os.stat("log.json").st_size != 0:
                f = open('log.json','r')
                self.logger.info("configuring files storage info in partition")
                self.topics = json.load(f)
                # print('loaded')
                f.close()
            elif os.stat('log.json').st_size == 0:
                # print('empty')
                self.topics = dict()
            replicatePaths = dict()
            while True:
                    if k:
                        # k=json.loads(k)
                        print(k,type(k))
                        topicName = k["topicName"]
                        msg = k["msg"]
                        # if msg == 'stop':
                        #     connected = False
                        if topicName in self.topics:
                            fileName = topicName + str(self.index)
                            self.logger.info(f"written to {self.paths[self.index]} partition")
                            savePath = self.paths[self.index] + fileName
                            if fileName not in replicatePaths:
                                replicatePaths[fileName] = [self.paths[self.index],self.index]
                            self.topics[topicName].append(self.index)
                            if not os.path.exists(savePath):
                                os.mkdir(savePath)
                            fd = open(savePath+'/file','a')
                            if type(msg) == 'dict':
                                fd.write(json.dumps(msg))
                            else:
                                fd.write(str(msg) + '\n')
                            self.channel.basic_publish(exchange='direct_logs', routing_key = topicName, body=json.dumps(msg))
                            self.index = (self.index + 1) % len(self.paths)
                            fd.close()
                        else:
                            fileName = topicName + str(self.index)
                            savePath = self.paths[self.index] + fileName
                            self.logger.info(f"written to {self.paths[self.index]} partition")
                            if fileName not in replicatePaths:
                                replicatePaths[fileName] = [self.paths[self.index],self.index]
                            self.topics[topicName] = [self.index]
                            if not os.path.exists(savePath):
                                os.mkdir(savePath)
                            fd = open(savePath+'/file','a')
                            if type(msg) == 'dict':
                                fd.write(json.dumps(msg))
                            else:
                                fd.write(str(msg) + '\n')
                            self.channel.basic_publish(exchange='direct_logs', routing_key = topicName, body=json.dumps(msg))
                            self.index = (self.index + 1) % len(self.paths)
                            fd.close()  
                        conn.send(bytes("ok",'utf-8'))
                    else:
                        #src dst target
                        if not os.path.isfile('replicas.json'):
                        	fi = open('replicas.json','w')
                        	fi.close()
                        if os.stat('replicas.json').st_size != 0:
                            self.logger.info("setting up replicas file")
                            rf = open('replicas.json','r')
                            self.replicas = json.load(rf)
                            rf.close()
                        rf = open('replicas.json','w')
                        for fileName in replicatePaths:
                            src = replicatePaths[fileName][0]
                            dst = self.paths[(replicatePaths[fileName][1] + 1) % len(self.paths)]
                            self.logger.info(f"copied {fileName} from {src} to {dst}")
                            target = fileName
                            dir = os.path.join(dst,target+'-1')
                            partitionNum = (replicatePaths[fileName][1] + 1) % len(self.paths)
                            if fileName not in self.replicas:
                                self.replicas[fileName] = [partitionNum]
                            elif partitionNum not in self.replicas[fileName]:
                                self.replicas[fileName].append(partitionNum)

                            if os.path.exists(dir): 
                                shutil.rmtree(dir) 
                            self.copytree(src,dst,target)
                        rf.write(json.dumps(self.replicas))
                        rf.close()
                        self.logger.info("Shutting down")
                        break
                    k = conn.recv(1024).decode() 
                    if k:
                        k = json.loads(k) 
            f = open('log.json','w')
            f.write(json.dumps(self.topics))
            f.close()       
            shutil.copy('log.json','node0/') 
            shutil.copy('log.json','node1/') 
            shutil.copy('log.json','node2/') 
        else:
            if not os.path.isfile('subscribers.json'):
                f = open('subscribers.json','w')
                f.close()
            if os.stat('subscribers.json').st_size != 0:
                cr = open('subscribers.json','r')
                self.subscribers = json.load(cr)
                cr.close()
            cr = open('subscribers.json','w')
            topicName = k["topicName"]
            if topicName not in self.subscribers:
                self.subscribers[topicName] = [addr[1]]
            elif addr[1] not in self.subscribers[topicName]:
                self.subscribers[topicName].append(addr[1])
            cr.write(json.dumps(self.subscribers))
            cr.close()
            # self.handleRead(conn,addr)


    def run(self):
        self.server.listen()
        self.set_interval(self.sendPulse,10)
        print(f"running[broker] on {self.port}")
        print(self.typeOfNode)
        while True:
            if (self.typeOfNode == 'broker-3'):
                # listen for calls
                print('listening') 
                conn,addr = self.server.accept()       
                print(addr)             
                thread = threading.Thread(target = self.handleProducerAndConsumer,args=(conn,addr))
                thread.start()
            else:
                conn,addr = self.server.accept()
                L = conn.recv(1024).decode()
                L = json.loads(L)
                if L:
                    self.port = L["port"]
                    self.typeOfNode = L["typeOfNode"]
                    print(f"New leader {self.port} elected")
                    self.server.close()
                    self.server = socket.socket()
                    self.server.bind(('localhost',self.port))
                    self.server.listen()
                    print(f'listening on port {self.port}')

        self.server.close()



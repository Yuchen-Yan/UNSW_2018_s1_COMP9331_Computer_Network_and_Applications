from socket import *
import threading
import time
import sys
import os

class UDPServerThread(threading.Thread):
    def __init__(self,UDPserverSocket):
        super().__init__()
        self.socket = UDPserverSocket
        self.__flag = threading.Event()
        self.__flag.set()
        self.__running = threading.Event()
        self.__running.set()
    def run(self):
        serverPort = 50000 + int(sys.argv[1])
        self.socket.bind(('', serverPort))
        i = 0
        while self.__running.isSet():
            message, clientAddress = UDPserverSocket.recvfrom(2048)
            message = str(message,'utf-8')
            m = message.split()
            if m[0] not in pre:
                pre.append(int(m[0]))
            print('A ping request message was received from Peer ', m[0])
            print()
            s = str(sys.argv[1]) + ' ' + str(m[1])
            s = bytes(s, 'utf-8')
            UDPserverSocket.sendto(s, clientAddress)
    def stop(self):
        self.__flag.set()
        self.__running.clear()





class UDPClientThread(threading.Thread):
    def __init__(self,UDPclientSocket, serverName):
        super().__init__()
        self.socket = UDPclientSocket
        self.__flag = threading.Event()
        self.__flag.set()
        self.__running = threading.Event()
        self.__running.set()
    def run(self):
        i = 0
        while self.__running.isSet():
            if request[0] - response[0] >= 4:
                flag[0] = 1
            if request[1] - response[1] >= 4:
                flag[1] = 1
            s = str(sys.argv[1]) + ' ' + str(i)
            send = bytes(s, 'utf-8')
            self.socket.sendto(send,(serverName, first_successor))
            request[0] = i
            self.socket.sendto(send,(serverName, second_successor))
            request[1] = i
            i += 1
            time.sleep(10)
    def stop(self):
        self.__flag.set()
        self.__running.clear()





class UDPresponseSocket(threading.Thread):
    def __init__(self,UDPclientSocket):
        super().__init__()
        self.socket = UDPclientSocket
        self.__flag = threading.Event()
        self.__flag.set()
        self.__running = threading.Event()
        self.__running.set()
    def run(self):
        while self.__running.isSet():
            message, serverAddress = self.socket.recvfrom(2048)
            message = str(message,'utf-8')
            m = message.split()
            
            if int(m[0]) == first_successor - 50000:
                response[0] = int(m[1])
            elif int(m[0]) == second_successor - 50000:
                response[1] = int(m[1])

            print('A ping response message was received from Peer ', m[0])
            print()
    def stop(self):
        self.__flag.set()
        self.__running.clear()





class TCPfileClientSocket(threading.Thread):
    def __init__(self, TCPclientSocket, serverName):
        super().__init__()
        self.socket = TCPclientSocket
        self.__flag = threading.Event()
        self.__flag.set()
        self.__running = threading.Event()
        self.__running.set()
    def run(self):
        TCPclientSocket.connect((serverName,first_successor))
        pre.sort()
        sentence = input()
        if sentence == 'quit':
            s = 'quit ' + str(sys.argv[1])+' '+str(first_successor)+' '+str(second_successor)
            s = bytes(s, 'utf-8')
            port = 50000 + int(pre[0])
            TCPclientSocket1 = socket(AF_INET, SOCK_STREAM)
            TCPclientSocket2 = socket(AF_INET, SOCK_STREAM)
            TCPclientSocket1.connect((serverName, port))
            TCPclientSocket1.send(s)
            port1 = 50000 + int(pre[1])
            TCPclientSocket2.connect((serverName, port1))
            TCPclientSocket2.send(s)
            for i in L:
                i.stop()
        else:
            file = sentence.split()
            file[1] = int(file[1])
            hash = file[1] % 256
            if (hash < int(sys.argv[1]) and hash > pre[1]) or (int(hash) == int(sys.argv[1])):
                print('File',file[1] ,'is here.')
            elif hash > pre[1] and hash > int(sys.argv[1]) and int(sys.argv[1]) < pre[1]:
                print('File',file[1] ,'is here.')
            else:
                print('File request message has been forwarded to my successor.')
                s = str(file[1]) + ' ' + sys.argv[1]
                s = bytes(s, 'utf-8')
                TCPclientSocket.send(s)
    def stop(self):
        self.__flag.set()
        self.__running.clear()





class TCPfileServerSocket(threading.Thread):
    def __init__(self, TCPclientSocket, TCPserverSocket):
        super().__init__()
        self.socket = TCPserverSocket
        self.__flag = threading.Event()
        self.__flag.set()
        self.__running = threading.Event()
        self.__running.set()
    def run(self):
        TCPserverSocket.bind(('',serverPort))
        TCPserverSocket.listen(10)
        while self.__running.isSet():
            global first_successor
            global second_successor
            connectionSocket, addr = TCPserverSocket.accept()
            s = connectionSocket.recv(1024)
            s = str(s, 'utf-8')
            l = s.split()
            if len(l) == 1:
                sent = str(first_successor) + ' ' + str(second_successor)
                sent = bytes(sent, 'utf-8')
                connectionSocket.send(sent)
                continue
            elif len(l) == 2:
                hash = int(l[0]) % 256
                peer = l[1]
            elif len(l) == 3:
                if l[1] == sys.argv[1]:
                    print('Received a response message from peer',l[0] ,', which has the file',l[2],'.')
                else:
                    s = bytes(s, 'utf-8')
                    TCPclientSocket.send(s)
                continue
            elif len(l) == 4:
                print('Peer',l[1] ,'will depart from the network.')
                first = first_successor - 50000
                second = second_successor - 50000
                if l[1] == str(first):
                    first_successor = int(l[2])
                    second_successor = int(l[3])
                    first = first_successor - 50000
                    second = second_successor - 50000
                    print('My first successor is now peer',first,'.')
                    print('My second successor is now peer',second,'.')
                    continue
                elif l[1] == str(second):
                    second_successor = int(l[2])
                    first = first_successor - 50000
                    second = second_successor - 50000
                    print('My first successor is now peer',first,'.')
                    print('My second successor is now peer',second,'.')
                    continue
            if (int(hash) < int(sys.argv[1]) and int(hash) > pre[1]) or (int(hash) == int(sys.argv[1])):
                print('File',l[0] ,'is here.')
                print('A response message, destined for peer',peer ,', has been sent.')
                res = str(sys.argv[1]) + ' '+ str(peer) + ' ' + str(l[0])
                res = bytes(res, 'utf-8')
                TCPclientSocket.send(res)
                continue
            elif int(hash) > pre[1] and int(hash) > int(sys.argv[1]) and int(sys.argv[1]) < pre[1]:
                print('File',l[0] ,'is here.')
                print('A response message, destined for peer ',peer ,', has been sent.')
                res = str(sys.argv[1]) + ' '+ str(peer) + ' ' + str(l[0])
                res = bytes(res, 'utf-8')
                TCPclientSocket.send(res)
                continue
            else:
                print('File',l[0] ,'is not stored here.')
                print('File request message has been forwarded to my successor.')
                s = bytes(s, 'utf-8')
                TCPclientSocket.send(s)
    def stop(self):
        self.__flag.set()
        self.__running.clear()


class TCPkillSocket(threading.Thread):
    def __init__(self, TCPclientSocket, TCPserverSocket):
        super().__init__()
        self.socket = TCPserverSocket
        self.__flag = threading.Event()
        self.__flag.set()
        self.__running = threading.Event()
        self.__running.set()
    def run(self):
        while 1:
            global first_successor
            global second_successor
            if flag[0]:
                o = first_successor-50000
                s = bytes('ask','utf-8')
                first_successor = second_successor
                TCPclientSocket3 = socket(AF_INET, SOCK_STREAM)
                TCPclientSocket3.connect((serverName, first_successor))
                TCPclientSocket3.send(s)
                sentence = TCPclientSocket3.recv(2048)
                sentence = str(sentence,'utf-8')
                s = sentence.split()
                second_successor = int(s[0])
                print('Peer',str(o) ,'is no longer alive.')
                print('My first successor is now peer',str(first_successor-50000) ,'.')
                print('My second successor is now peer',str(second_successor-50000) ,'.')
                break
            if flag[1]:
                r = second_successor-50000
                s = bytes('ask','utf-8')
                TCPclientSocket4 = socket(AF_INET, SOCK_STREAM)
                TCPclientSocket4.connect((serverName, first_successor))
                TCPclientSocket4.send(s)
                sentence = TCPclientSocket4.recv(2048)
                sentence = str(sentence,'utf-8')
                s = sentence.split()
                second_successor = int(s[0])
                print('Peer',str(r) ,'is no longer alive.')
                print('My first successor is now peer',str(first_successor-50000) ,'.')
                print('My second successor is now peer',str(second_successor-50000) ,'.')
                break
    def stop(self):
        self.__flag.set()
        self.__running.clear()



#initialization
global serverPort
global first_successor
global second_successor
global pre
global serverName
global L
global request
global response
global flag
flag = [0, 0]
request = [0, 0]
response = [0, 0]

serverPort = 50000 + int(sys.argv[1])
first_successor = 50000 + int(sys.argv[2])
second_successor = 50000 + int(sys.argv[3])
pre = []
serverName = gethostname()

#The udp and tcp client and server sockets created.
UDPclientSocket = socket(AF_INET, SOCK_DGRAM)
UDPserverSocket = socket(AF_INET, SOCK_DGRAM)
TCPserverSocket = socket(AF_INET, SOCK_STREAM)
TCPclientSocket = socket(AF_INET, SOCK_STREAM)

thread1 = UDPServerThread(UDPserverSocket)
thread2 = UDPClientThread(UDPclientSocket, serverName)
thread3 = UDPresponseSocket(UDPclientSocket)
thread4 = TCPfileServerSocket(TCPclientSocket, TCPserverSocket)
thread5 = TCPfileClientSocket(TCPclientSocket, serverName)
thread6 = TCPkillSocket(TCPclientSocket, TCPserverSocket)
L = [thread1, thread2, thread3, thread4, thread5]

thread1.start()
thread2.start()
thread3.start()
thread4.start()
time.sleep(5)
thread5.start()
thread6.start()

thread1.join()
thread2.join()
thread3.join()
thread4.join()
thread5.join()
thread6.join()

UDPclientSocket.close()
UDPserverSocket.close()
TCPserverSocket.close()
TCPclientSocket.close()




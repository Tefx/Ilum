from gevent.socket import socket, AF_INET, SOCK_DGRAM, SOCK_STREAM
import gevent
from cPickle import dumps, loads
import os,random

def getPort(start, end):  
    pscmd = "netstat -ntl |grep -v Active| grep -v Proto|awk '{print $4}'|awk -F: '{print $NF}'"  
    procs = os.popen(pscmd).read()  
    procarr = procs.split("\n")  
    tt= random.randint(start,end)  
    if tt not in procarr:  
        return tt  
    else:  
        getPort()  

COORD_PORT = 8523
MIN_PORT_NO = 50000
MAX_PORT_NO = 60000

class BaseWorker(object):

    class CoordClient(object):
        def __init__(self, coord_addr):
            self.coord_addr = (coord_addr, COORD_PORT)
            self.coon = socket(AF_INET, SOCK_DGRAM)

        def add(self, port):
            self.coon.sendto(dumps(("ADD", port)), self.coord_addr)

        def require(self, num):
            self.coon.sendto(dumps(("REQ", num)), self.coord_addr)
            data = ""
            while True:
                data_buf, addr = self.coon.recvfrom(4096)
                data += data_buf
                if data[-4:] == "\r\n\r\n": break
            return [worker for worker in [self.make_worker(addr) for addr in loads(data[:-4])] if worker]

        def make_worker(self, addr):
            sock = socket(AF_INET, SOCK_STREAM)
            if sock.connect_ex(addr) != 0:
                return None
            else:
                return WorkerClient(sock)


    def __init__(self, coord_addr, maintain_port):
        self.coord = self.CoordClient(coord_addr)
        self.work_port = getPort(MIN_PORT_NO, MAX_PORT_NO)
        self.maintain_port = maintain_port
        self.sock = socket(AF_INET, SOCK_STREAM) 
        self.sock.bind(("", self.work_port)) 
        self.sock.listen(100) 

    def handle(self, data):
        pass

    def require(self, n):
        return self.coord.require(n)

    def run(self):
        gevent.joinall([gevent.spawn(self._maintain), gevent.spawn(self._server)])

    def _server(self):
        while True:
            conn, address = self.sock.accept()  
            data = ""
            while True:
                buf = conn.recv(4096)
                data += buf
                if data[-4:] == "\r\n\r\n": break
            res = self.handle(data[:-4])
            conn.sendall(dumps(res, HIGHEST_PROTOCOL)+"\r\n\r\n")
            conn.close()
            self.coord.add(self.work_port)

    def _maintain(self):
        self.coord.add(self.work_port)
        conn = socket(AF_INET, SOCK_DGRAM)
        conn.bind(('', self.maintain_port))
        while True:
            data, addr = conn.recvfrom(1024)
            if data == "ASK":
                self.coord = self.CoordClient(addr[0])
                self.coord.add(self.work_port)

class WorkerClient(object):
    def __init__(self, sock):
        self.sock = sock

    def process(self, data):
        self.sock.sendall(dumps(data, HIGHEST_PROTOCOL)+"\r\n\r\n")
        result = ""
        while True:
            buf = self.sock.recv(4096)
            result += buf
            if result[-4:] == "\r\n\r\n": break
        self.sock.close()
        return loads(result[:-4])

if __name__ == '__main__':
    BaseWorker("localhost", getPort(MIN_PORT_NO, MAX_PORT_NO)).run()


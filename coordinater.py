from gevent.socket import socket, AF_INET, SOCK_DGRAM, SOCK_STREAM, SOL_SOCKET, SO_BROADCAST
from gevent.pool import Pool
from cPickle import dumps, loads, HIGHEST_PROTOCOL

MAX_LETS = 1000

class Coordinater(object):
    def __init__(self, port=8523):
        self.workers = []   
        self.sock = socket(AF_INET, SOCK_DGRAM)
        self.sock.bind(('', port))
        self.pool = Pool(MAX_LETS)

    def add_worker(self, worker):
        if worker not in self.workers:
            print "added:", worker
            self.workers.append(worker)

    def require(self, num):
        res = []
        got = 0
        while got < num and self.workers:
            res.append(self.workers.pop(0))
            got += 1
        return res

    def cry(self):
        conn = socket(AF_INET, SOCK_DGRAM)
        conn.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
        for port in range(50000, 60000):
            conn.sendto('CRY', ('<broadcast>', port))

    def run(self):
        self.cry()
        while True:
            data, addr = self.sock.recvfrom(1024)
            self.handle(loads(data), addr)

    def handle(self, data, addr):
        if data[0] == "ADD":
            self.add_worker((addr[0], data[1]))
            self.sock.sendto("GOT", addr)
        elif data[0] == "REQ":
            res = self.require(data[1])
            self.sock.sendto(dumps(res, HIGHEST_PROTOCOL)+"\r\n\r\n", addr)
        elif data[0] == "CRY":
            self.cry()
            #self.sock.sendto("CRY", (addr[0], data[1]))

if __name__ == '__main__':
    Coordinater().run()
        
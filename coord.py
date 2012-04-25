from gevent.socket import socket, AF_INET, SOCK_DGRAM, SOCK_STREAM, SOL_SOCKET, SO_BROADCAST
from gevent.pool import Pool
from cPickle import dumps, loads

MAX_LETS = 1000

class Coordinater(object):
    def __init__(self, port=8523):
        self.workers = []   
        self.sock = socket(AF_INET, SOCK_DGRAM)
        self.sock.bind(('', port))
        self.pool = Pool(MAX_LETS)

    def require(self, no):
        if self.workers:
            worker = self.workers.pop(0)
            sock = socket(AF_INET, SOCK_STREAM)
            if sock.connect_ex(worker) != 0:
                if self.workers: 
                    return self.require(no)
                else:
                    return None
            else:
                sock.close()
                return worker
        else:
            return None

    def require(self, num):
        if num > len(self.workers): num = len(self.workers)
        return [worker for worker in self.pool.map(self.require, range(num)) if worker]

    def run(self):
        conn = socket(AF_INET, SOCK_DGRAM)
        conn.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
        print "starting..."
        for port in range(50000, 60000):
            conn.sendto('ASK', ('<broadcast>', port))
        print "started."
        while True:
            data, addr = self.sock.recvfrom(1024)
            self.handle(loads(data), addr)

    def handle(self, data, addr):
        if data[0] == "ADD":
            print "added", (addr[0], data[1])
            self.workers.append((addr[0], data[1]))
        elif data[0] == "REQ":
            res = self.require(data[1])
            self.sock.sendto(dumps(res, HIGHEST_PROTOCOL)+"\r\n\r\n", addr)

if __name__ == '__main__':
    Coordinater().run()
        
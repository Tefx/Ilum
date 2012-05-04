from config import *
import gevent.socket as socket
import gevent
import cPickle as pickle
from uuid import uuid1


class Coordinator(object):
    def __init__(self):
        self.uuid = uuid1()
        self.workers = []
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', COORD_PORT))

    def run(self):
        self.cry()
        while True:
            data, addr = self.sock.recvfrom(1024)
            gevent.spawn(self.handle, pickle.loads(data), addr)

    def handle(self, data, addr):
        if data[0] == "CRY":
            self.cry()
        elif data[0] == "ADD":
            self.add_worker((addr[0], data[1]), addr)
        elif data[0] == "REQ":
            self.require(data[1], addr)

    def cry(self):
        conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        conn.sendto(pickle.dumps(('CRY', self.uuid)), ('<broadcast>', PROXY_PORT))

    def add_worker(self, worker, addr):
        if worker not in self.workers:
            self.workers.append(worker)
        conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        conn.sendto('GOT', addr)

    def require(self, num, addr):
        res = []
        got = 0
        while got < num and self.workers:
            res.append(self.workers.pop(0))
            got += 1
        conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        conn.sendto(pickle.dumps(res, pickle.HIGHEST_PROTOCOL) + "\r\n\r\n", addr)

if __name__ == '__main__':
    Coordinator().run()

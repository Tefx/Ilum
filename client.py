from cPickle import dumps, loads, HIGHEST_PROTOCOL
from gevent.socket import socket, AF_INET, SOCK_DGRAM, SOCK_STREAM

####################CoordClient######################

COORD_PORT = 8523

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
        got = loads(data[:-4])
        if not got: return []
        cur = [worker for worker in [self.make_worker(addr) for addr in got] if worker]
        if len(cur) < num:
            return cur + self.require(num-len(cur))
        else:
            return cur

    def make_worker(self, addr):
        sock = socket(AF_INET, SOCK_STREAM)
        if sock.connect_ex(addr) != 0:
            return None
        else:
            return WorkerClient(sock)


####################WorkerClient######################

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


####################StorageClient######################

from httplib import HTTPConnection
from ujson import dumps as udumps
from ujson import loads as uloads

class StorageClient(object):
    def __init__(self, host, port=8080):
        self.base_addr = host+":"+str(port)

    def add_data(self, data):
        body = udumps(data)
        conn = HTTPConnection(self.base_addr)
        conn.request('POST', '/data', body)
        return conn.getresponse().read()

    def get_data(self, id, start=0, end=0):
        url = "/data/%s/%d/%s" % (id, start, end)
        conn = HTTPConnection(self.base_addr)
        conn.request('GET', url, "")
        return uloads(conn.getresponse().read())

    def delete_data(self, id):
        conn = HTTPConnection(self.base_addr)
        conn.request('DELETE', '/data/'+id, "")
        return conn.getresponse().read()

    def __call__(self, data):
        return Data(self.add_data(data), 0, len(data), self)

class Data(object):
    def __init__(self, data_id, start, end, source):
        self.source = source
        self.data_id = data_id
        self.start = start
        self.end = end
        self.data = None
        self.len = self.end-self.start

    def __del__(self):
        self.source.delete_data(self.data_id)

    def __len__(self):
        return self.len

    def __getslice__(self, i, j):
        if i < 0: 
            start = self.end + i 
        else:
            start = self.start + i

        if j < 0:
            end = self.end + j
        else:
            end = self.start + j
        return Data(self.data_id, start, end, self.source)

    def __getitem__(self, key):
        if not self.data:
            self.data = self.source.get_data(self.data_id, self.start, self.end)
        return self.data[key]

####################Client######################

class Client(object):
    def __init__(self, coord_addr):
        self.coord = CoordClient(coord_addr)

    def eval(self, E):
        workers = self.coord.require(1)
        if len(workers) == 1:
            worker = workers[0]
            res = worker.process(E)
            if isinstance(res, Exception): print res
            return res
        else:
            return None

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
from marshal import dumps as mdumps
from marshal import loads as mloads
from ujson import dumps as udumps
from ujson import loads as uloads
from types import FunctionType


class StorageClient(object):
    def __init__(self, host, port=8080):
        self.base_addr = (host, port)
        self.conn = HTTPConnection(host+":"+str(port))

    def add_func(self, func):
        body = mdumps(func.func_code)
        self.conn.request('POST', '/func/'+func.func_name, body)
        return self.conn.getresponse().read()

    def get_func(self, id):
        self.conn.request('GET', '/func/'+id, "")
        code = self.conn.getresponse().read()
        return FunctionType(mloads(code), globals()) 

    def delete_func(self, id):
        self.conn.request('DELETE', '/func/'+id, "")
        return self.conn.getresponse().read()

    def add_data(self, data):
        body = udumps(data)
        self.conn.request('POST', '/data', body)
        return self.conn.getresponse().read()

    def get_data(self, id, start=0, end=0):
        url = "/data/%s/%d/%s" % (id, start, end)
        self.conn.request('GET', url, "")
        return uloads(self.conn.getresponse().read())

    def delete_data(self, id):
        self.conn.request('DELETE', '/data/'+id, "")
        return self.conn.getresponse().read()
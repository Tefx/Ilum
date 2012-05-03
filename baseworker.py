from config import *
import gevent
import gevent.socket as socket
import cPickle as pickle
from utils import RemoteException

def getPort(start, end):
    import os,random  
    pscmd = "netstat -ntl |grep -v Active| grep -v Proto|awk '{print $4}'|awk -F: '{print $NF}'"  
    procs = os.popen(pscmd).read()  
    procarr = procs.split("\n")  
    tt= random.randint(start,end)  
    if tt not in procarr:  
        return tt  
    else:  
        getPort()

class BaseWorkerClient(object):
    def __init__(self, sock):
        self.sock = sock

    def process(self, data):
        self.sock.sendall(pickle.dumps(data, pickle.HIGHEST_PROTOCOL)+"\r\n\r\n")
        result = ""
        while True:
            buf = self.sock.recv(4096)
            result += buf
            if result[-4:] == "\r\n\r\n": break
        self.sock.close()
        return pickle.loads(result[:-4])

class BaseWorker(object):
    def __init__(self, maintain_port, baseWorkerClientCls = BaseWorkerClient):
        self.maintain_port = maintain_port
        self.coord_uuid = None
        self.coord_addr = None
        self.work_port = getPort(MIN_WORK_PORT_NO, MAX_WORK_PORT_NO)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        self.sock.bind(("", self.work_port)) 
        self.sock.listen(1)
        self.WorkerClientCls = baseWorkerClientCls

    def handle(self, data):
        pass

    def run(self):
        gevent.joinall([gevent.spawn(self._server), gevent.spawn(self._maintain)])

    def make_worker(self, addr):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sock.connect_ex(addr) != 0:
            return None
        else:
            return self.WorkerClientCls(sock)

    def require_workers(self, num):
        n = 0
        while True:
            try:
                n += 1
                return self.do_require(num)
            except socket.timeout:
                if n < 5: continue
                return []

    def do_require(self, num):
        conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        conn.sendto(pickle.dumps(("REQ", num)), self.coord_addr)
        data = ""
        while True:
            conn.settimeout(1)
            data_buf, addr = conn.recvfrom(4096)
            data += data_buf
            if data[-4:] == "\r\n\r\n": break
        got = pickle.loads(data[:-4])
        if not got: return []
        cur = [worker for worker in [self.make_worker(addr) for addr in got] if worker]
        if len(cur) < num:
            return cur + self.require_workers(num-len(cur))
        else:
            return cur

    def _server(self):
        while True:
            conn, address = self.sock.accept()  
            data = ""
            while True:
                buf = conn.recv(4096)
                if not buf: break
                data += buf
                if data[-4:] == "\r\n\r\n": break
            if not data: break
            cmd = pickle.loads(data[:-4])
            try:
                res = self.handle(cmd)
            except Exception as e:
                res = RemoteException(e, cmd)
            conn.sendall(pickle.dumps(res, pickle.HIGHEST_PROTOCOL)+"\r\n\r\n")
            conn.close()
            self.add_self()

    def _maintain(self):
        conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        conn.bind(('', self.maintain_port))
        self.cry()
        while True:
            data, addr = pickle.loads(conn.recvfrom(1024)[0])
            gevent.spawn(self.handle_maintain_request, pickle.loads(data), addr)

    def handle_maintain_request(self, data, addr):
        if data[0] == "CRY":
            if self.coord_uuid != data[1]:
                self.coord_uuid = data[1]
                self.coord_addr = (addr[0], COORD_PORT)
                self.add_self()

    def add_self(self):
        conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        data = pickle.dumps(("ADD", self.work_port))
        n = 0
        while True:
            try:
                n += 1
                conn.settimeout(1)
                conn.sendto(data, self.coord_addr)
                data, addr = conn.recvfrom(4096)
                if data == "GOT":
                    break
            except socket.timeout:
                if n > 10: break
        conn.close()

    def cry(self):
        conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        conn.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        conn.sendto(pickle.dumps(('CRY', self.maintain_port)), ('<broadcast>', COORD_PORT))
        conn.close()

if __name__ == '__main__':
    from sys import argv, exit
    if len(argv) != 2:
        exit('Usage: %s maintain_port\nYou should ALWAYS use a monitoring script to launch workers.' % __file__)
    BaseWorker(int(argv[1]), BaseWorkerClient).run()



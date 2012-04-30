from config import *
import cPickle as pickle
import gevent.socket as socket

####################Client######################
class Client(object):
    def __init__(self, coord_addr):
        self.coord_addr = (coord_addr, COORD_PORT)

    def require(self):
        conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        conn.sendto(pickle.dumps(("REQ", 1)), self.coord_addr)
        data = ""
        while True:
            data_buf, addr = conn.recvfrom(4096)
            data += data_buf
            if data[-4:] == "\r\n\r\n": break
        got = pickle.loads(data[:-4])
        if not got: 
            return None
        else:
            return got[0]

    def eval(self, E):
        worker_addr = self.require()
        if not worker_addr: return None
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if sock.connect_ex(worker_addr) != 0: return None
        sock.sendall(pickle.dumps(E, pickle.HIGHEST_PROTOCOL)+"\r\n\r\n")
        result = ""
        while True:
            buf = sock.recv(4096)
            result += buf
            if result[-4:] == "\r\n\r\n": break
        sock.close()
        return pickle.loads(result[:-4])

if __name__ == '__main__':
    from utils import Fun as F

    def fun(item):
        from hashlib import sha512
        res = ""
        for i in range(1000):
            res = sha512(item*i+res).hexdigest()
        return len(res)

    e1 = (cmp, 0, (cmp, 4, 6))
    e2 = ("map2", F(fun), map(str, range(100)))
    e3 = (cmp, (cmp, 0), 1)
    e4 = ("seq", (cmp, 3, 1), (cmp, 6, 8))
    e5 = ("local", "seq", (cmp, 3, 1), (cmp, 6, 6))
    e7 = ("map", F(fun), ['1','2','3','4','5','6','7','8','9'])
    e8 = (range, 10)
    e9 = ("seq", e7, e7, e7)

    print Client("localhost").eval(e2)

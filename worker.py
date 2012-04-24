import gevent
from gevent import monkey; monkey.patch_all()
from gevent.socket import socket, AF_INET, SOCK_STREAM, SOCK_DGRAM, gethostname, gethostbyname
from cPickle import dumps, loads, HIGHEST_PROTOCOL
from sys import argv
import coordinater
from storage import StorageClient
import random, os

class Worker(object):

	def __init__(self, local_port, coord_addr, stor_addr):
		self.coord = coordinater.CoordinaterClient(coord_addr)
		self.stor = StorageClient(stor_addr)
		self.local_port = local_port
		self.sock = socket(AF_INET, SOCK_STREAM) 
		self.sock.bind(("", self.local_port)) 
		self.sock.listen(100) 
		self.coord.add(self.local_port)
		self.func_buf = {}
		
	def run(self):
		while True:
			conn, address = self.sock.accept()  
			data = ""
			while True:
				buf = conn.recv(4096)
				data += buf
				if data[-4:] == "\r\n\r\n": break
			res = self.eval(loads(data[:-4]))
			conn.sendall(dumps(res, HIGHEST_PROTOCOL)+"\r\n\r\n")
			self.coord.add(self.local_port)
			conn.close()

	def eval(self, e, local=False):
		if isinstance(e, tuple):
			if e[0] == "local":
				return self.eval(e[1:], True)
			else:
				return self.apply(e[0], self.apply("seq", e[1:], local), local)
		else:
			return e

	def apply(self, func, args, local=False):
		if isinstance(func, str):
			if hasattr(self, func):
				f = getattr(self, func)
				return f(args, local)
			else:
				if func not in self.func_buf:
					self.func_buf[func] = self.stor.get_func(func)
				f = self.func_buf[func]
				return f(*args)
		else:
			return func(*args)

	def map(self, l, local=False):
		if local: return [self.eval((l[0], item), True) for item in l[1]]
		workers = self.coord.require(len(l[1]))
		if not workers: return self.apply_map(self, l, True)
		jobs = [(worker, ("local", "map", l[0], sl)) for worker, sl in zip(workers, self.split_n(l[1], len(workers)))]
		return self.distribute(jobs)
		
	def seq(self, l, local=False):
		if len(l) == 1: return [self.eval(l[0], True)]
		if local: return [self.eval(item, True) for item in l]
		workers = self.coord.require(len(l))
		if not workers: return self.apply_seq(self, l, True)
		jobs = [(worker, ("local", "seq") + tuple(sl)) for worker, sl in zip(workers, self.split_n(l, len(workers)))]
		return self.distribute(jobs)

	def split_n(self, l, n):
		len_l = len(l)
		num = len_l / n
		k = len_l - num * n
		end = 0
		res = []
		while end < len_l:
			start = end
			end = start + num
			if len(res) < k: end += 1
			res.append(l[start:end])
		return res

	def distribute(self, jobs):
		lets = [gevent.spawn(worker.eval, item) for worker, item in jobs]
		gevent.joinall(lets)
		return [flatten for inner in [item.value for item in lets] for flatten in inner]

class WorkerClient(object):
	def __init__(self, sock):
		self.sock = sock

	def eval(self, E):
		self.sock.sendall(dumps(E, HIGHEST_PROTOCOL)+"\r\n\r\n")
		result = ""
		while True:
			buf = self.sock.recv(4096)
			result += buf
			if result[-4:] == "\r\n\r\n": break
		self.sock.close()
		return loads(result[:-4])

def getPort(start, end):  
    pscmd = "netstat -ntl |grep -v Active| grep -v Proto|awk '{print $4}'|awk -F: '{print $NF}'"  
    procs = os.popen(pscmd).read()  
    procarr = procs.split("\n")  
    tt= random.randint(start,end)  
    if tt not in procarr:  
        return tt  
    else:  
        getPort(start, end) 

if __name__ == '__main__':
	local_port = getPort(32300, 32400)
	coord_addr, stor_addr = argv[1:]

	worker = Worker(local_port, coord_addr, stor_addr)
	worker.run()



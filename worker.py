import gevent
from gevent import monkey; monkey.patch_all()
from gevent.socket import socket, AF_INET, SOCK_STREAM, SOCK_DGRAM, gethostname, gethostbyname
from sys import argv
from client import CoordClient, StorageClient
from utils import getPort, split_n
from cPickle import dumps, loads, HIGHEST_PROTOCOL

MIN_PORT_NO = 50000
MAX_PORT_NO = 60000

class BaseWorker(object):
	def __init__(self, coord_addr, maintain_port):
		self.coord = CoordClient(coord_addr)
		self.work_port = getPort(MIN_PORT_NO, MAX_PORT_NO)
		self.maintain_port = maintain_port
		self.sock = socket(AF_INET, SOCK_STREAM) 
		self.sock.bind(("", self.work_port)) 
		self.sock.listen(100) 

	def handle(self, data):
		pass

	def require_workers(self, n):
		return self.coord.require(n)

	def run(self):
		gevent.joinall([gevent.spawn(self._server), gevent.spawn(self._maintain)])

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
			res = self.handle(loads(data[:-4]))
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
				self.coord = CoordClient(addr[0])
				self.coord.add(self.work_port)

class Worker(BaseWorker):
	def __init__(self, maintain_port, coord_addr, stor_addr):
		super(Worker, self).__init__(coord_addr, maintain_port)
		self.stor = StorageClient(stor_addr)

	def handle(self, data):
		self.func_buf = {}
		return self.eval(data)
		
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
		workers = self.require_workers(len(l[1]))
		if not workers: return self.map(l, True)
		jobs = [(worker, ("local", "map", l[0], sl)) for worker, sl in zip(workers, split_n(l[1], len(workers)))]
		return self.distribute(jobs)
		
	def seq(self, l, local=False):
		if len(l) == 1: return [self.eval(l[0], True)]
		if local: return [self.eval(item, True) for item in l]
		workers = self.require_workers(len(l))
		if not workers: return self.seq(l, True)
		jobs = [(worker, ("local", "seq") + tuple(sl)) for worker, sl in zip(workers, split_n(l, len(workers)))]
		return self.distribute(jobs)

	def distribute(self, jobs):
		lets = [gevent.spawn(worker.process, item) for worker, item in jobs]
		gevent.joinall(lets)
		return [flatten for inner in [item.value for item in lets] for flatten in inner]

if __name__ == '__main__':
	from sys import argv
	local_port = getPort(MIN_PORT_NO, MAX_PORT_NO)
	if len(argv) < 3:
		coord_addr = "localhost"
		stor_addr = "localhost"
	else:
		coord_addr, stor_addr = argv[1:]

	worker = Worker(local_port, coord_addr, stor_addr)
	worker.run()



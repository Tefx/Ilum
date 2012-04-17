import gevent
from gevent.socket import socket, AF_INET, SOCK_STREAM, SOCK_DGRAM, gethostname, gethostbyname
from cPickle import dumps, loads
from sys import argv
import coordinater
from funcs import *

class Worker(object):

	def __init__(self, local_addr, coord_addr):
		self.coord = coordinater.CoordinaterClient(coord_addr)
		self.local_addr = local_addr
		self.sock = socket(AF_INET, SOCK_STREAM) 
		self.sock.bind(("", self.local_addr[1])) 
		self.sock.listen(100) 
		self.coord.add(self.local_addr)
		
	def run(self):
		while True:
			conn, address = self.sock.accept()  
			data = loads(conn.recv(40960000))
			res = self.eval(data)
			conn.sendall(dumps(res))
			self.coord.add(self.local_addr)
			conn.close()

	def eval(self, e, local=False):
		if not isinstance(e, list): return e
		if e[0] == "local": return self.eval(e[1:], True)

		if local:
			return self.apply(e[0], [self.eval(item, True) for item in e[1:]], True)
		else:
			return self.apply(e[0], self.map_eval(e[1:]))

	def apply(self, func, args, local=False):
		if local: return func(*args)

		if func == map:
			el = [[args[0], arg] for arg in args[1]]
			return self.map_eval(el)
		elif func == "dot":
			return dot(*args)
		return func(*args)

	def split_jobs(self, jobs, workers):
		num_each = len(jobs) / len(workers)
		if num_each * len(workers) < len(jobs):
			num_each += 1
		res = []

	def map_eval(self, L):
		if not L: return []
		num_job = len(L)
		if num_job == 1: 
			return [self.eval(L[0])]

		workers = self.coord.require(num_job)
		num_worker = len(workers)
		if num_worker == 0:
			result_first = self.eval(L.pop())
			return [result_first] + self.map_eval(L)
		else:
			jobs = [gevent.spawn(worker.eval, job) for (worker, job) in zip(workers, L[:num_worker])]
			gevent.joinall(jobs)
			return [job.value for job in jobs] + self.map_eval(L[num_worker:])

class WorkerClient(object):
	def __init__(self, sock):
		self.sock = sock


	def eval(self, E):
		self.sock.sendall(dumps(E))
		result = self.sock.recv(40960000)
		self.sock.close()
		return loads(result)

if __name__ == '__main__':
	local_ip, local_port_str = argv[1].split(':')
	local_addr = (local_ip, int(local_port_str))
	coord_ip, coord_port_str = argv[2].split(':')
	coord_addr = (coord_ip, int(coord_port_str))

	worker = Worker(local_addr, coord_addr)
	worker.run()



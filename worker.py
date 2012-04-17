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
		return self.apply(e[0], self.map_eval(e[1:], local), local)

	def apply(self, func, args, local=False):
		if func == map and not local:
			el = [[args[0], arg] for arg in args[1]]
			return self.map_eval(el)
		elif func == "seq":
			return self.map_eval(args, local)
		return func(*args)

	def split_jobs(self, jobs, workers):
		num_each = len(jobs) / len(workers)
		if num_each * len(workers) < len(jobs):
			num_each += 1
		res = []
		while jobs:
			if len(jobs) > num_each:
				res.append(jobs[:num_each])
				del jobs[:num_each]
			else:
				res.append(jobs)
				break
		return [(worker, ["local", "seq"] + job) for worker, job in zip(workers, res)]

	def map_eval(self, jobs, local=False):
		if not jobs: 
			return []
		if len(jobs) == 1: 
			return [self.eval(jobs[0])]
		if local: 
			return [self.eval(item, local) for item in jobs]
		workers = self.coord.require(len(jobs))
		if len(workers) == 0: 
			return [self.eval(item, local) for item in jobs]

		splited = self.split_jobs(jobs, workers)
		lets = [gevent.spawn(worker.eval, job) for (worker, job) in splited]
		gevent.joinall(lets)
		nested_res = [job.value for job in lets]
		return [flatten for inner in nested_res for flatten in inner]

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



import gevent
from gevent.socket import socket, AF_INET, SOCK_STREAM, SOCK_DGRAM, gethostname, gethostbyname
from cPickle import dumps, loads
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
			conn.sendall(dumps(res)+"\r\n\r\n")
			self.coord.add(self.local_port)
			conn.close()

	def eval(self, e, local=False):
		if isinstance(e, tuple):
			if e[0] == "local":
				return self.eval(e[1:], True)
			else:
				return self.apply(e[0], self.map_eval(list(e[1:]), local), local)
		else:
			return e

	def apply(self, func, args, local=False):
		if func == map:
			el = [(args[0], arg) for arg in args[1]]
			return self.map_eval(el, local)
		elif func == "seq":
			return list(self.map_eval(args, local))

		if isinstance(func, str):
			if func not in self.func_buf:
				self.func_buf[func] = self.stor.get_func(func)
			f = self.func_buf[func]
			return f(*args)

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
		return [(worker, tuple(["seq"] + job)) for worker, job in zip(workers, res)]

	def map_eval(self, jobs, local=False):
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
		self.sock.sendall(dumps(E)+"\r\n\r\n")
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



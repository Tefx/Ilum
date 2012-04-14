import gevent
from gevent.socket import socket, AF_INET, SOCK_STREAM, SOCK_DGRAM, gethostname, gethostbyname
from cPickle import dumps, loads
from sys import argv

class Worker(object):

	def __init__(self, local_addr, coord_addr):
		self.local_ip, self.port = local_addr
		self.coord_addr = coord_addr

		self.sock = socket(AF_INET, SOCK_STREAM) 
		self.sock.bind(("", self.port))  
		self.sock.listen(100) 


	def cry_when_born(self):
		udpSock = socket(AF_INET, SOCK_DGRAM)
		udpSock.sendto(dumps(["ADD", (self.local_ip, self.port)]), self.coord_addr)

	def run(self):
		while True:
			conn, address = self.sock.accept()  
        	fileobj = conn.makefile()
        	line = fileobj.readline()
        	if line:
        		result = self.eval(loads(line.strip()))
        		fileobj.write(dumps(result)+"\r\n")
        		fileobj.flush()
        	conn.close()

	def eval(self, E):
		if not isinstance(E, list):
			return E
		else:
			return self.apply(E[0], self.map_eval(E[1:]))

	def apply(self, func, args):
		return func(*args)

	def map_eval(self, L):
		if not L: return []

		num_job = len(L)
		if num_job == 1: 
			return [self.eval(L[0])]

		num_worker, workers = self.require_workers(num_job)
		if num_worker == 0:
			result_first = self.eval(L.pop())
			return [result_first] + self.map_eval(L)
		else:
			jobs = [gevent.spawn(worker.eval, job) for (worker, job) in zip(workers, L[:num_worker])]
			gevent.joinall(jobs)
			return [job.value for job in jobs].extend(self.map_eval(L[num_worker:]))

	def require_workers(self, Num):
		udpSock = socket(AF_INET, SOCK_DGRAM)
		udpSock.sendto(dumps(["REQ", Num]), self.coord_addr)
		data, addr = udpSock.recvfrom(40960000)
		num, worker_addr = data
		return num, [WorkerClient(addr) for addr in worker_addr]


class WorkerClient(object):
	def __init__(self, address):
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.connect(address)
		self.fileobj = socket.makefile()

	def eval(self, E):
		self.fileobj.write(dumps(E)+"\r\n")
		self.fileobj.flush()
		result = self.fileobj.readline()
		self.sock.close()
		return loads(result.strip())

if __name__ == '__main__':
	local_ip, local_port_str = argv[1].split(':')
	local_addr = (local_ip, int(local_port_str))
	coord_ip, coord_port_str = argv[2].split(':')
	coord_addr = (coord_ip, int(coord_port_str))

	worker = Worker(local_addr, coord_addr)
	worker.cry_when_born()
	worker.run()



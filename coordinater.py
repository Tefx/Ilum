import gevent
from gevent.socket import socket, AF_INET, SOCK_DGRAM, SOCK_STREAM
from cPickle import dumps, loads
from sys import argv
import worker


class Coordinater(object):
	def __init__(self, port):
		self.workers = []	
		self.sock = socket(AF_INET, SOCK_DGRAM)
		self.sock.bind(('', port))

	def add_worker(self, worker):
		self.workers.append(worker)

	def require(self, num):
		res = []
		got = 0
		while got < num and self.workers:
			res.append(self.workers.pop(0))
			got += 1
		return res

	def run(self):
		while True:
			data, addr = self.sock.recvfrom(1024)
			#gevent.spawn(self.handle, loads(data), addr)
			self.handle(loads(data), addr)

	def handle(self, data, addr):
		if data[0] == "ADD":
			self.add_worker((addr[0], data[1]))
		elif data[0] == "REQ":
			res = self.require(data[1])
			self.sock.sendto(dumps(res)+"\r\n\r\n", addr)

class CoordinaterClient(object):
	def __init__(self, coord_addr, coord_port = 8523):
		self.coord_addr = (coord_addr, coord_port)
		self.coon = socket(AF_INET, SOCK_DGRAM)

	def add(self, port):
		self.coon.sendto(
			dumps(["ADD", port]), 
			self.coord_addr
			)

	def make_worker(self, addr):
		sock = socket(AF_INET, SOCK_STREAM)
		if sock.connect_ex(addr) != 0:
			return None
		else:
			return worker.WorkerClient(sock)

	def require(self, num):
		self.coon.sendto(
			dumps(["REQ", num]), 
			self.coord_addr
			)
		data = ""
		while True:
			data_buf, addr = self.coon.recvfrom(4096)
			data += data_buf
			if data[-4:] == "\r\n\r\n": break
		return [worker for worker in [self.make_worker(addr) for addr in loads(data[:-4])] if worker]

if __name__ == "__main__":
	if len(argv) < 2:
		port = 8523
	else:
		port = int(argv[1])

	Coordinater(port).run()


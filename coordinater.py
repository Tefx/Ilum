import gevent
from gevent.socket import socket, AF_INET, SOCK_DGRAM
from cPickle import dumps, loads


class Coordinater(object):
	def __init__(self):
		self.workers = []	
		self.sock = socket(AF_INET, SOCK_DGRAM)
		self.sock.bind(('', 8523))
		self.release = self.add_worker

	def add_worker(self, worker):
		print "New worker: ", worker
		self.workers.append(worker)

	def require(self, num):
		res = []
		got = 0
		while got < num and self.workers:
			res.append(self.workers.pop(0))
			got += 1
		print got, "workers going:", res
		return (got, res)

	def run(self):
		while True:
			data, addr = self.sock.recvfrom(1024)
			gevent.spawn(self.handle, loads(data), addr)

	def handle(self, data, addr):
		if data[0] == "ADD":
			self.add_worker(data[1])
		elif data[0] == "REQ":
			res = self.require(data[1])
			self.sock.sendto(dumps(res), addr)

if __name__ == "__main__":
	Coordinater().run()
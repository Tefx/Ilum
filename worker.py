from gevent import monkey
monkey.patch_all()

import gevent
from socket import socket, AF_INET, SOCK_STREAM

class Worker(object):

	def eval(self, E):
		if not isinstance(E, list):
			return E
		else:
			return self.apply(E[0], self.map_eval(E[1:]))
	
	def apply(self, func, args):
		return func(*args)

	def map_eval(self, L):
		num_job = len(L)

		if num_job == 1: 
			return [self.eval(L[0])]

		num_worker, workers = self.ask_for_workers(num_job)

		if num_worker == 0:
			result_first = self.eval(L.pop())
			return [result_first] + self.map_eval(L)

		elif num_worker < num_job:
			jobs = [gevent.spawn(worker.eval, job) for (worker, job) in zip(workers, L[:num_worker])]
			gevent.joinall(jobs)
			return [job.value for job in jobs].extend(self.map_eval(L[num_worker:]))

		else:
			jobs = [gevent.spawn(worker.eval, job) for (worker, job) in zip(workers, L)]
			gevent.joinall(jobs)
			return [job.value for job in jobs]


	def ask_for_workers(self, Num):
		return 0, []


class WorkerClient(object):
	def __init__(self, address):
		self.sock = socket(AF_INET, SOCK_STREAM)
		self.sock.connect(address)

	def eval(self, E):
		self.sock.sendall(pickle.dumps(E))
		return sock.recv(pickle.loads(4096))

	def release(self):
		self.sock.close()

if __name__ == '__main__':
	worker = Worker()

	E = [cmp, 0, [cmp, 4, 4]]

	print worker.eval(E)
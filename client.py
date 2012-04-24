from worker import WorkerClient
from coordinater import CoordinaterClient

class Client(object):
	def __init__(self, coord_addr):
		self.coord = CoordinaterClient(coord_addr)

	def eval(self, E):
		workers = self.coord.require(1)
		if len(workers) == 1:
			worker = workers[0]
			res = worker.eval(E)
			if isinstance(res, Exception): print res
			return res
		else:
			return None

if __name__ == '__main__':
	from storage import StorageClient, Data
	sc = StorageClient("localhost")

	def fun(item):
		from hashlib import sha512
		res = ""
		for i in range(10000):
			res = sha512(item*i+res).hexdigest()
		return res

	sc.add_func(fun)

	data = Data.warp(sc, map(str, range(100)))


	e1 = (cmp, 0, (cmp, 4, 6))
	e2 = ("map", "fun", data)
	e3 = (cmp, (cmp, 0, (cmp, 0)), (cmp, 0, 4))
	e4 = ("seq", (cmp, 3, 1), (cmp, 6, 8))
	e5 = ("local", "seq", (cmp, 3, 1), (cmp, 6, 6))
	e7 = ("map", "fun", ['1','2','3','4','5','6','7','8','9'])
	e8 = (range, 10)
	e9 = ("local", "seq", e7, e7, e7)

	print Client("localhost").eval(e1)

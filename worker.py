import gevent
from baseworker import BaseWorker
from utils import RemoteException


def split_n(l, n):
    len_l = len(l)
    num = len_l / n
    k = len_l - num * n
    end = 0
    res = []
    while end < len_l:
        start = end
        end = start + num
        if len(res) < k:
            end += 1
        res.append(l[start:end])
    return res


class Worker(BaseWorker):
    def __init__(self, maintain_port):
        super(Worker, self).__init__(maintain_port)

    def handle(self, data):
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
        for arg in args:
            if isinstance(arg, RemoteException):
                return arg
        if isinstance(func, str) and hasattr(self, func):
            return getattr(self, func)(args, local)
        else:
            return func(*args)

    def map(self, l, local=False):
        if local:
            return [self.eval((l[0], item), True) for item in l[1]]
        workers = self.require_workers(len(l[1]))
        if not workers:
            return self.map(l, True)
        jobs = [(worker, ("local", "map", l[0], sl)) for worker, sl in zip(workers, split_n(l[1], len(workers)))]
        return self.distribute(jobs)

    def seq(self, l, local=False):
        if len(l) == 1:
            return [self.eval(l[0], True)]
        if local:
            return [self.eval(item, True) for item in l]
        workers = self.require_workers(len(l))
        if not workers:
            return self.seq(l, True)
        jobs = [(worker, ("local", "seq") + tuple(sl)) for worker, sl in zip(workers, split_n(l, len(workers)))]
        return self.distribute(jobs)

    def distribute(self, jobs):
        lets = [gevent.spawn(worker.process, item) for worker, item in jobs]
        gevent.joinall(lets)
        res = [flatten for inner in [item.value for item in lets] for flatten in inner]
        return res

if __name__ == '__main__':
    from sys import argv, exit
    if len(argv) != 2:
        exit('Usage: %s maintain_port\nYou should ALWAYS use a monitoring script to launch workers.' % __file__)
    Worker(int(argv[1])).run()

from config import *
import gevent
import gevent.socket as socket
import signal
from subprocess import Popen
from multiprocessing import cpu_count
import cPickle as pickle 

class Proxy(object):
    def __init__(self):
        self.dest_ports = []
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("", PROXY_PORT))
        self.greenlet = None

    def run(self):
        while True:
            data, addr = self.sock.recvfrom(1024)
            gevent.spawn(self.handle, data)
            
    def handle(self, data):
        conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for port in self.dest_ports:
            conn.sendto(data, ("localhost", port))

    def add_port(self, port):
        if port not in self.dest_ports:
            self.dest_ports.append(port)

    def del_port(self, port):
        if port in self.dest_ports:
            self.dest_ports.remove(port)

    def start(self):
        self.greentlet = gevent.spawn(self.run)

    def stop(self):
        self.greentlet.kill()

class Monitor(object):
    def __init__(self):
        self.proxy = Proxy()
        self.proxy.start()
        self.runing = True
        gevent.signal(signal.SIGTERM, self.stop)
        gevent.signal(signal.SIGQUIT, self.stop)
        gevent.signal(signal.SIGINT, self.stop)

    def run(self):
        self.workers = [gevent.spawn(self.run_worker, no) for no in range(cpu_count()*2-1)]
        gevent.joinall(self.workers)

    def stop(self):
        self.runing = False
        self.proxy.stop()
        
    def run_worker(self, no):
        maintain_port = MAINTAIN_PORT_START+no
        self.proxy.add_port(maintain_port)
        print "Starting worker[%d]" % (no,)
        p = Popen(["python", "worker.py", str(maintain_port)], shell=False,stdout=False)
        while self.runing:
            gevent.sleep(5)
            if  p.poll():
                print "Worker[%d] terminated! Resatrting" % (no,)
                p = Popen(["python", "worker.py", str(maintain_port)], shell=False,stdout=False)
        print "Stopping worker[%d]" % (no,)
        if p.poll(): p.kill()

if __name__ == '__main__':
    Monitor().run()

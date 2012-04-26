from multiprocessing import cpu_count
from os import popen
from sys import argv
import threading

def run_worker(no, coord_addr):
    print "Worker ", no, ": starting..."
    popen("python worker.py " + coord_addr)
    print "Worker ", no, ": ended"

if __name__ == '__main__':
    threads = [threading.Thread(target=run_worker, args=(i, argv[1])) for i in range(cpu_count()*2)]
    for t in threads: t.start()
from multiprocessing import cpu_count
from os import system
from sys import argv
import threading

def run_worker(no, coord_addr, stor_addr):
    print "Worker ", no, ": starting..."
    system("python worker.py " + coord_addr + " " + stor_addr)
    print "Worker ", no, ": ended"

if __name__ == '__main__':
    threads = [threading.Thread(target=run_worker, args=(i, argv[1], argv[2])) for i in range(cpu_count()*2-1)]
    for t in threads: t.start()
    for t in threads: t.join()
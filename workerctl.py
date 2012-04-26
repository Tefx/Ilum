from multiprocessing import cpu_count
from os import popen
import threading

def run_worker(no):
    print "Worker ", no, ": starting..."
    popen("python worker.py")
    print "Worker ", no, ": ended"

if __name__ == '__main__':
    threads = [threading.Thread(target=run_worker, args=(i,)) for i in range(cpu_count()*2)]
    for t in threads: t.start()
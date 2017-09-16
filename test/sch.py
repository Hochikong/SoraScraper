import schedule
import time
import threading
from multiprocessing.dummy import Pool as ThreadPool

data = list(range(10))


def job():
    print(list(map(lambda x: x+1, data)))

def add(x):
    return x+2

def job2():
    pool = ThreadPool(4)
    result = pool.map(add, data)
    print(result)
    pool.close()
    pool.join()


def run_process(func):
    job_process = threading.Thread(target=func)
    job_process.start()


schedule.every(2).seconds.do(run_process,job)
schedule.every(3).seconds.do(job2)

while 1:
    schedule.run_pending()
    time.sleep(2)
import shelve
import time

conn = shelve.open('test.dat')
db = conn['data']


def printx(x):
    print(db[x])

if __name__ == "__main__":
    i = 0
    while True:
        printx(i)
        i += 1
        if i > 199:
            i = 0
        time.sleep(2)

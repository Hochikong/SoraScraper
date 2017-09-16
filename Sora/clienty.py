# -*- coding: utf-8 -*-
# @Time    : 2017/9/13 1:04
# @Author  : Hochikong
# @Email   : hochikong@foxmail.com
# @File    : clienty.py
# @Software: PyCharm
import rpyc
import time


def ondatacome(x):
    print(x)

connect = rpyc.connect('localhost', 18871)
bgsrv = rpyc.BgServingThread(connect)
try:
    mon = connect.root.Scraper('000725',ondatacome,'test')
except Exception:
    pass

if __name__ == '__main__':
    while 1:
        pass






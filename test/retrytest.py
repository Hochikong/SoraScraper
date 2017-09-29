# -*- coding: utf-8 -*-
# @Time    : 2017/9/28 14:11
# @Author  : Hochikong
# @Email   : hochikong@foxmail.com
# @File    : retrytest.py
# @Software: PyCharm

from retrying import retry
import random


@retry(stop_max_attempt_number=9)
def have_a_try():
    r = random.randint(0, 10)
    print(r)
    if r != 5:
        raise Exception('It s not 5!')
    print('It s 5!')

if __name__ == "__main__":
    have_a_try()
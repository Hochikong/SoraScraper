# -*- coding: utf-8 -*-
# @Time    : 2017/9/1 22:33
# @Author  : Hochikong
# @Email   : hochikong@foxmail.com
# @File    : publib.py
# @Software: PyCharm

from setuptools import setup, find_packages
setup(
    name='StockCLib',
    version='0.1',
    package=find_packages(),
    include_package_data=True,
    install_requires=['pymongo',
                      'neo4j-driver',
                      'bs4',
                      'jieba'
                      ],

    description='The basic library for Sora',
    author='Hochikong',
    author_email='hochikong@foxmail.com',
)

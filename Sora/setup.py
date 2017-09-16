# Copyright (c) 2017 Hochikong


from setuptools import setup, find_packages
setup(
    name='',
    version='1.0',
    package=find_packages(),
    include_package_data=True,
    install_requires=['pymongo',
                      'neo4j-driver',
                      'bs4',
                      'jieba'
                      ],
    description='A automate tool for investor analysing news',
    author='Hochikong',
    author_email='hochikong@foxmail.com',
)

# -*- coding: utf-8 -*-
# @Time    : 2017/9/27 0:15
# @Author  : Hochikong
# @Email   : hochikong@foxmail.com
# @File    : RPCServer.py
# @Software: PyCharm

import rpyc
import pymongo
from bs4 import BeautifulSoup
from retrying import retry  # 爬虫基础函数不做重试操作
from configparser import ConfigParser
from .dtLib import *

# --------------------------------
# Set the constant and read the configuration file
# 设置常量并读取配置文件
CONFIG_FILE = 'config.ini'
DB_SECTION = 'DB'

# --------------------------------
# ConfigParser configuration
# 配置ConfigParser对象
cfg = ConfigParser()
cfg.read(CONFIG_FILE)

# --------------------------------
# MongoDB connection
# 连接MongoDB
connection = pymongo.MongoClient(cfg.get(DB_SECTION, 'address'), int(cfg.get(DB_SECTION, 'port')))
db = connection[cfg.get(DB_SECTION, 'database')]
collect_stock_url = db['stock_url']
collect_stock_news = db['stock_news']
collect_stocks = db['stocks']



# --------------------------------
# RPyC framework
# RPyC RPC服务基本模版
class ScrapeService(rpyc.Service):
    class exposed_Wedengta(object):
        def exposed_searchURL(self):  # 提供股票url搜索服务
            pass

        def exposed_openTransaction(self):  # 建仓准备，爬取现在的新闻url列表和到两天前的新闻
            # 分为多个部分，首先是获取所有股票的新闻首页的url构造一个url列表
            # 然后执行cacheURL的获取、过滤（分别返回原始版本和过滤掉无用数据的版本的cache）（要实现一个通用的cache获取函数） ok
            # 然后执行爬取今天与两天前的股票的新闻并写入（实现一个日期计算函数、一个通用新闻爬取函数）
            # 数据库查询部分
            query = list(collect_stocks.find())
            query = [{'stock_id':stock['stock_id'], 'url': stock['url']} for stock in query]

            # cacheURL处理
            data = generalCacheGet(query)

            # 爬取今天和前两天的新闻
            ndata = paraGetNews(data)

            # cache写入数据库
            paraUpdateCache(ndata, collect_stock_url)

            # 把新闻写入数据库
            paraInsert(ndata, collect_stock_news)

        def exposed_compareURLs(self):  # 对比现在的新闻url列表和原来的url的差值
            pass

        def exposed_updateNEWSandURLs(self):  # 更新url列表和插入新的新闻


if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    ThreadedServer(ScrapeService, port=18871).start()
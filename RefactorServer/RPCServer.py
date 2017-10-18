# -*- coding: utf-8 -*-
# @Time    : 2017/9/27 0:15
# @Author  : Hochikong
# @Email   : hochikong@foxmail.com
# @File    : RPCServer.py
# @Software: PyCharm

import rpyc
import pymongo
from configparser import ConfigParser
from stockclib.dtLib import *

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
        def exposed_openAllTransaction(self):  # 建仓准备，爬取现在的新闻url列表和到两天前的新闻
            # 分为多个部分，首先是获取所有股票的新闻首页的url构造一个url列表
            # 然后执行cacheURL的获取、过滤（分别返回原始版本和过滤掉无用数据的版本的cache）（要实现一个通用的cache获取函数） ok
            # 然后执行爬取今天与两天前的股票的新闻并写入（实现一个日期计算函数、一个通用新闻爬取函数）
            # 数据库查询部分
            query = list(collect_stocks.find())
            query = [{'stock_id': stock['stock_id'], 'url': stock['url']} for stock in query]

            # cacheURL处理
            data = generalCacheGet(query)

            # 爬取今天和前两天的新闻
            ndata = paraGetNews(data)

            # cache写入数据库
            paraInsertCache(ndata, collect_stock_url)

            # 把新闻写入数据库
            paraInsert(ndata, collect_stock_news)
            return 'Done'

        def exposed_openSingleTransaction(self, stock_number):
            """
            提供给分析器，只对刚临时添加的股票进行建仓操作
            :param stock_number: 用户添加的股票的字符串编号
            :return:
            """
            query = collect_stocks.find_one({'stock_id': '002230'})
            query = [{'stock_id': query['stock_id'], 'url': query['url']}]
            data = generalCacheGet(query)
            ndata = paraGetNews(data)
            paraInsertCache(ndata, collect_stock_url)
            paraInsert(ndata, collect_stock_news)
            return 'Done'

        def exposed_compareURLs(self):  # 对比现在的新闻url列表和原来的url的差值
            # 分为读多个部分，首先获取旧的cache
            # 然后获取新的cache
            # 对每个股票的新旧cache进行比对并返回
            qdata = list(collect_stock_url.find())  # 先从数据库查询cache数据
            ndata = generalCacheGet(qdata)  # 添加cache字段，保存最新的cache_urls
            adddelta = paraCacheDiff(ndata)  # 添加cache_delta字段，保存差值
            afterfilter = paraFilter(adddelta)  # 对差值进行过滤，在ready_for_news字段保存准备用来爬取新闻内容的url。
            return afterfilter

        def exposed_updateNEWSandURLs(self, afterfilter):  # 更新url列表和插入新的新闻
            # 爬取ready_for_news的新闻并保存
            for stock in afterfilter:
                newssoup = generalGet(stock['ready_for_news'], soup=True)
                stock['news_content'] = [analysisPage(soup) for soup in newssoup]

            # 更新cache_urls
            paraUpdateCache(afterfilter, collect_stock_url)

            # 更新新闻
            paraExtend(afterfilter, collect_stock_news)

if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    ThreadedServer(ScrapeService, port=18871).start()

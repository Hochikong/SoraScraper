import rpyc
import pymongo
from bs4 import BeautifulSoup
from retrying import retry
from publib import *

# ---------------------------------
# 数据库连接
connection = pymongo.MongoClient('localhost', 27017)
db = connection.soraDB
collect_stock_url = db.stock_url
collect_stock_news = db.stock_news

# ---------------------------------
# 数据库辅助函数


def update_cacheurls(collection, stock_id, urls):  # 指定集合stock_url更新每一只股票的cache_url
    collection.update_one({'stock_id': stock_id}, {'$set': {'cache_urls': urls}})


def query_cacheurls(collection, stock_id):   # 查找特定股票的cache_urls数据
    if collection.find_one({'stock_id': stock_id}):
        result = collection.find_one({'stock_id': stock_id})
        if result.get('cache_urls'):  # 如果股票已经有cache_urls就返回
            return result['cache_urls']
        else:   # 否则返回空列表
            return []
    else:  # 如果没有这只股票
        return 'No such stock'


def query_stocknews(collection, stock_id):   # 返回指定股票的新闻内容，如果没有该股票就返回空列表
    if collection.find_one({'stock_id': stock_id}):
        return collection.find_one({'stock_id': stock_id})['news_content']
    else:
        return []

# ----------------------------------
# 爬虫辅助


def request_caches(queryurls):  # 根据数据库的股票url记录为每只股票爬取cache_urls
    raw_request_data = list(map(get_page_text, queryurls['urls']))  # 长度等于股票数,为requests爬取的原始内容
    cache_urls = list(map(get_urls, raw_request_data))
    cache_urls = list(map(filter_url, cache_urls))  # 过滤掉不属于新闻的url
    cache_urls = list(map(filter_and_add, cache_urls))  # 添加url prefix
    return cache_urls


def filter_url(urls):  # 只保留带'newsDetail'的url
    news_urls = list(filter(lambda x: 'newsDetail' in x, urls))
    return news_urls


def query_allurls(collection):   # 查找stock_url表的全部url条目和对应的股票编号
    if len(list(collection.find())) > 0:
        query_result = list(collection.find())
        urls = [i['url'] for i in query_result]
        id = [i['stock_id'] for i in query_result]
        return {'urls': urls, 'stock_id': id}


@retry
def get_page_text(url):   # 对url进行爬取新闻首页并获取页面文本，被query_today函数调用
    result = requests.get(url, timeout=20).text
    return result


def write_today_and_2days_ago_data(allurls):   # 爬取当日和前两天的历史数据，目前判定前两天的只针对第一页
    # 根据新闻首页的文本提取全部新闻详情的soup对象
    # page_template = 'http://www.wedengta.com/stockDetail/0001000050/news/%d.html'
    all_page_text = list(map(get_page_text, allurls['urls']))
    all_raw_url = list(map(filter_and_add, list(map(get_urls, all_page_text))))  # 从每个股票的新闻页面提取新闻链接并处理
    all_news_soup = list(map(filter_by_title, all_raw_url))  # 一个长度为stock_url数量的列表，每一个元素是股票的全部新闻的soup对象

    # 根据soup对象提取新闻详情,已经通过日期差过滤掉数据，但数据里还有些None值
    all_news_detail = [list(map(analysis_page, i)) for i in all_news_soup]
    # 进行过滤，去掉None值
    all_news_detail = [[element for element in news if element] for news in all_news_detail]
    target_news_detail = dict(zip(allurls['stock_id'], all_news_detail))
    # 根据id把一条一条的新闻详情写入数据库的stock_news表
    for stock_id in target_news_detail:
        old_news = query_stocknews(collect_stock_news, stock_id)
        if len(old_news) != 0:  # 如果有该股票的记录就update
            old_news.extend([msg for msg in target_news_detail[stock_id]])
            collect_stock_news.update_one({'stock_id': stock_id}, {'$set':{'news_content': old_news}})
        else:   # 否则insert
            old_news.extend([msg for msg in target_news_detail[stock_id]])
            collect_stock_news.insert_one({'stock_id': stock_id, 'news_content': old_news})
    return 'Done'


def update_news(content):   # 把content里的数据追加到数据库中，被incrementScraper调用,数据是把旧的数据追加到新数据后面，所以新数据是在列表前
    for stock in content:
        if len(content[stock]) > 0:
            query_result = collect_stock_news.find_one({'stock_id': stock})
            if query_result:
                oldnews = query_result['news_content']
                newnews = content[stock]
                newnews.extend(oldnews)
                collect_stock_news.update_one({'stock_id': stock}, {'$set': {'news_content': newnews}})


class ScrapeService(rpyc.Service):
    def exposed_initScraper(self):  # 执行两步操作：1.对全部url进行当日和前两天的历史数据爬取  2.爬取新的cache_url并写入
        # 第一步
        queryurls = query_allurls(collect_stock_url)
        write_today_and_2days_ago_data(queryurls)

        # 第二步
        # ids = queryurls['stock_id']  # 股票编号列表
        # cache_urls = [query_cacheurls(collect_stock_url,i) for i in ids]
        cache_urls = request_caches(queryurls)
        # 写入数据库
        tmp = dict(zip(queryurls['stock_id'], cache_urls))
        for sid in tmp:
            update_cacheurls(collect_stock_url, sid, tmp[sid])
        return 'Done'

    def exposed_compareCache(self):  # 检查现在的cache_url与原来的有没有不同，返回差值
        queryurls = query_allurls(collect_stock_url)
        ids = queryurls['stock_id']
        old_cache_urls = [query_cacheurls(collect_stock_url, i) for i in ids]  # 从数据库中查找
        new_cache_urls = request_caches(queryurls)  # 实时在线查找
        delta = []
        for index in range(len(old_cache_urls)):  # 构造返回结果
            delta.append(get_url_delta(new_cache_urls[index], old_cache_urls[index]))
        for stock, urls in zip(ids, new_cache_urls):  # 把cache_url更新到数据库里,不论有没有新增的数据
            collect_stock_url.update_one({'stock_id': stock}, {'$set': {'cache_urls': urls}})
        return dict(zip(ids, delta))

# data = {'000050': [],
# '002285': [],
# '601002': ['http://www.wedengta.com/news/newsDetail/1/1505383522_10896657_9_1.html',
#  'http://www.wedengta.com/news/newsDetail/1/1505298444_10882695_9_1.html']}

    def exposed_incrementScraper(self,delta_url):   # 对增量进行爬取并写入数据库
        content = {}
        for stock in delta_url:
            tmp = []
            if len(delta_url[stock]) != 0:
                urls = yet_another_filterbyt(delta_url[stock])
                for url in urls:
                    tmp.append(analysis_page(BeautifulSoup(get_page_text(url), 'lxml')))
            tmp = [i for i in tmp if i]  # 过虑掉None
            content[stock] = tmp
        update_news(content)
        return 'Done'


if __name__ == "__main__":
    from rpyc.utils.server import ThreadedServer
    ThreadedServer(ScrapeService, port=18871).start()

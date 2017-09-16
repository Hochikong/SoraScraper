# -*- coding: utf-8 -*-
# @Time    : 2017/9/1 22:33
# @Author  : Hochikong
# @Email   : hochikong@foxmail.com
# @File    : publib.py
# @Software: PyCharm

from bs4 import BeautifulSoup
from functools import reduce
from datetime import datetime
import re
import requests
import jieba

"""
本程序为公共代码库
执行顺序：先由外部根据新闻页面的url爬取内容（本页和前2页的），把text提取的文本交给get_url提取url列表，然后由filter_and_mark(先add url prefix)标记不需要的数据，
通过filter_by_title进行过滤，剩下有参考意义的新闻和研报链接，返回新闻soup
列表。然后外部用map执行爬取然后调用bs4处理，再找analysis_page返回数据，再由外部存入数据库，或者由外部对列表进行对比，决定爬取哪些内容
"""


def get_urls(page_text):  # 提供requests库get回来的text进行url提取所有新闻的链接
    url_list = re.findall(
        r"(?<=href=\").+?(?=\")|(?<=href=\').+?(?=\')",
        page_text)
    return url_list  # 返回的url,每次暂停操盘后重启爬虫都要爬取当前url和后2页的全部带情绪的数据


def get_sentiment(soupobj):  # 获取页面情绪
    data = set(
        soupobj.find(
            attrs={
                'class': 'news_tag'}).get_text().split('\n'))
    data.remove('')
    attibute = list(data)[0]
    diffcase = {'利好': 'positive', '买入': 'strong pos', '强烈利空': 'strong neg',
                '增持': 'buy', '中性': 'neutral', '利空': 'negative', '强烈利好': 'strong pos'}
    return diffcase[attibute]
    #if attibute == '利好':
    #    return 'positive'
    #if attibute == '利空':
    #    return 'negative'
    #if attibute == '中性':
    #    return 'neutral'
    #if attibute == '买入':
    #    return 'strong pos'
    #if attibute == '强烈利空':
    #    return 'strong neg'
    #if attibute == '增持':
    #    return 'buy'


def get_data_and_source(soupobj):  # 提取来源和发布时间(以字符串存入数据库)
    data = soupobj.find(attrs={'class': 'news_info'})
    result = data.get_text()
    result = result.split(' ', 1)  # 提取出原始数据列表

    msg_source_tmp = jieba.lcut(result[0])
    msg_source = reduce(lambda x, y: x + y, msg_source_tmp[2:])  # 获得来源

    msg_time = jieba.lcut(result[1])
    date_tmp = msg_time[3:8]
    time_tmp = msg_time[9:]
    date = reduce(lambda x, y: x + y, date_tmp)  # 获得消息发布的日期
    time = reduce(lambda x, y: x + y, time_tmp)  # 获得消息发布的具体时间

    return {'source': msg_source, 'data': date, 'time': time}


def get_content(soupobj):  # 返回页面文本内容
    tmp = []
    for i in soupobj.find_all('p'):
        tmp.append(i)
    tmp = [i.get_text() for i in tmp]  # 提取纯文本
    tmp.pop()  # 删掉股票灯塔的Copyright
    return tmp  # 返回的是一个列表，每一个段落都带\r或者\rn,方便打印出来


#def get_stock_number(page_text):  # 使用page_text转换为soup对象然后返回股票id
#    soupobj = BeautifulSoup(page_text, 'lxml')
#    block = soupobj.find(attrs={'class': 'num'})
#    return block.get_text()


def filter_and_add(urls):  # 把不带头部的新闻url添加头部
    base_url = 'http://www.wedengta.com'
    result = list(map(lambda x: base_url + x, urls))
    return result


def filter_by_title(urls):  # 根据条件把link_list进行过滤,返回一个包含所有页面soup对象的列表
    new_list = filter(lambda x: 'newsDetail' in x, urls)  # 筛选出包含新闻的url
    result = list(map(filter_and_mark, new_list))  # 筛选出不含行情回顾的url
    result = list(filter(lambda x: x != 'No', result))
    return result


def filter_and_mark(url):  # 对所有url进行过滤，凡是标题包含'行情回顾'的都记为No (被filter_by_title调用)
    cond = '行情回顾'
    try:
        data = requests.get(url, timeout=10).text
    except Exception as e:
        return "error:", e
    soup = BeautifulSoup(data, 'lxml')
    if cond in soup.h3.get_text():
        return 'No'
    else:
        return soup


def analysis_page(soupobj):  # 把每个页面的soup对象提取分别提取一些数据：标题、情绪、摘要、原文
    title = soupobj.h3.get_text()
    sentiment = get_sentiment(soupobj)
    msg_source = get_data_and_source(soupobj)
    content = get_content(soupobj)
    return {
        'title': title,
        'sentiment': sentiment,
        'msg_source': msg_source,
        'content': content}


def count_day_delta(soupobj):   # 通过获取当前页面的日期，和程序检测的当前时间计算时间差，根据时间差爬取近日的页面
    msg_source = get_data_and_source(soupobj)
    raw_that_day = msg_source['data']
    that_day = datetime.strptime(raw_that_day, '%Y-%m-%d')
    today = datetime.now()
    delta = (today - that_day).days
    return delta


def get_url_delta(newurls, oldurls):   # 用旧的url列表来与新获得的url对比，获得最近更新的url用于爬取
    delta = list(set(newurls)-set(oldurls))
    return delta

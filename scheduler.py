#!/usr/bin/python
# -*- coding:utf-8 -*-
# author: dsc1697
# CreateTime: 2019/3/19
# software-version: python 3.7

from queue import Queue
from redis_queue import RedisQueue
import six
import w3lib.url  # 修改requirements.txt文件
from hashlib import sha1

from config import logger
from constants import *
from set import NoramlFilterContainer, RedisFilterContainer


class Scheduler(object):
    """
    缓存请求对象(Request)，并为下载器提供请求对象，实现请求的调度：
    对请求对象进行去重判断：实现去重方法_filter_request，该方法对内提供，因此设置为私有方法
    """
    def __init__(self):
        if SCHEDULER_PERSIST:  # 如果使用分布式或者是持久化，使用redis的队列
            self.queue = RedisQueue()
            self._filter_container = RedisFilterContainer()  # 使用redis作为python的去重的容器
        else:
            self.queue = Queue()
            self._filter_container = NoramlFilterContainer()  # 使用Python的set()集合
        self.repeate_request_num = 0  # 统计重复的数量
        # 在engine中阻塞的位置判断程序结束的条件：成功的响应数+重复的数量>=总的请求数量
        # self._filter_container = set() # 去重容器,是一个集合,存储已经发过的请求的特征 url

    def add_request(self, url):
        """添加去重后的url"""
        if self._filter_request(url):
            self.queue.put(url)

    def add_data(self, data):
        """对数据不进行去重处理"""
        self.queue.put(data)

    def get_data(self):
        """队列中获取数据"""
        try:
            data = self.queue.get()  # 从队列获取请求对象设置为非阻塞，否则会造成程序无法退出
        except:
            return None
        else:
            return data

    def task_done(self):
        """完成任务时，做标记"""
        self.queue.task_done()

    def join(self):
        """等待子线程结束"""
        self.queue.join()

    def _filter_request(self, url):
        """对数据进行去重"""
        fp = self._gen_fp(url)  # 给request对象增加一个fp指纹属性
        print(fp)
        if not self._filter_container.exists(fp):
            # 此处修改
            self._filter_container.add_fp(fp)  # 向指纹容器集合添加一个指纹
            return True
        else:
            self.repeate_request_num += 1
            logger.info("发现重复的请求：<{}>".format(fp))
            print("发现重复的请求：<{}>".format(fp))
            return False

    def _gen_fp(self, url):
        """
        生成并返回request对象的指纹
        用来判断请求是否重复的属性：url，method，params(在url中)，data
        为保持唯一性，需要对他们按照同样的排序规则进行排序
        """
        # url排序：借助w3lib.url模块中的canonicalize_url方法
        url = w3lib.url.canonicalize_url(url)

        # 利用sha1计算获取指纹
        s1 = sha1()
        s1.update(self._to_bytes(url))  # sha1计算的对象必须是字节类型

        fp = s1.hexdigest()
        return fp

    def _to_bytes(self, string):
        """为了兼容py2和py3，利用_to_bytes方法，把所有的字符串转化为字节类型"""
        if six.PY2:
            if isinstance(string, str):
                return string
            else: # 如果是python2的unicode类型，转化为字节类型
                return string.encode('utf-8')
        elif six.PY3:
            if isinstance(string, str):  # 如果是python3的str类型，转化为字节类型
                return string.encode("utf-8")
            else:
                return string

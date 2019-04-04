#!/usr/bin/python
# -*- coding:utf-8 -*-
# author: dsc1697
# CreateTime: 2019/3/19
# software-version: python 3.7


import json
import time
from queue import Queue
from multiprocessing.dummy import Pool

from config import logger
from middlewares import Middlewares
from db import MongoDBConfig, MysqlConfig


class CompetitiveSpider(object):
    """赛事数据爬取"""
    def __init__(self):
        """初始化参数"""
        # 设置要爬的网站
        self.url = "http://www.1zplay.com/league?format=json&category=all&page={}"
        # 初始化四个队列
        self.url_queue = Queue()
        self.page_queue = Queue()
        self.detail_data_queue = Queue()
        self.detail_url_queue = Queue()
        # 创建线程池对象
        self.pool = Pool(5)
        # 初始化中间件
        self.middlewares = Middlewares()
        # 初始化mysql
        self.mysql_conn = MysqlConfig()
        # 初始化MongoDB
        self.mongo_detail = MongoDBConfig('test', 'detail')
        self.mongo_info = MongoDBConfig('test', 'info')

    # 将URL添加到队列
    def add_url_to_queue(self):
        """生成URL列表"""
        for url_count in range(1, 6):
            url = self.url.format(url_count)
            self.url_queue.put(url)
            logger.info("<{}> put url_queue success!".format(url))

    # 将detail_url添加进队列
    def add_detail_url_to_queue(self):
        while True:
            # 从page_queue中取出数据
            page = self.page_queue.get()
            dic = {
                "league_id": page["id"],
                "img": page["logo"],
                "cname": page["name"],
                "game_name": page["category"],
                "game_src": "http://cdn.1zplay.com/public/img/game/" + page["category"] + ".png",
                "status": page["state"],
                "prize_pool": page["prize_pool"],
                "star": page["stars"],
                "start_time": page["start_time"][:10] + " " + page["start_time"][11:19]
                if page[ "start_time"] is not None else "",
                "finish_time": page["finish_time"][:10] + " " + page["finish_time"][11:19]
                if page["start_time"] is not None else ""
            }
            # 保存列表数据到json
            # self.save_data_to_json("competitive", dic)
            # 保存数据到mysql
            self.mysql_conn.add_data('competition', dic)
            logger.info("list data <{}> save success!".format(dic["cname"]))
            # 拼接url
            url = "http://www.1zplay.com/league/" + page["id"] + "?format=json"
            # 添加到detail_url_queue中
            self.detail_url_queue.put(url)
            logger.info("<{}> put detail_url_queue success!".format(url))
            self.page_queue.task_done()

    # 取出url_queue中的URL得到页面内容
    def add_detail_to_queue(self):
        """根据URL获取页面内容"""
        while True:
            # 取出URL
            url = self.url_queue.get()
            # 发出请求获取响应内容
            dic = self.middlewares.retry_request(url)
            # 拿出list数据
            page_list = dic["list"]
            for page in page_list:
                # 将页面数据添加到page_queue中
                self.page_queue.put(page)
                logger.info("page <{}> put page_queue success!".format(page["name"]))
            # 完成该url的任务
            self.url_queue.task_done()

    # 取出page_queue中的数据，解析数据并添加到detail_data_queue中
    def add_detail_data_to_queue(self):
        """解析详情页数据"""
        while True:
            url = self.detail_url_queue.get()
            # 发送请求，获取页面
            dic = self.middlewares.retry_request(url)
            # 构建json字典准备存入mongodb
            # 创建一个空字典存储赛事信息
            item = {}
            # 创建一个空列表存储item
            details = []
            # 将赛事组遍历出来
            for t in dic["tiers_names"]:
                if len(t["subtitle"]) > 1:
                    for d in t["subtitle"]:
                        item["rname"] = t["name"] + d["name"]
                        item["tiers_id"] = d["_id"]
                        details.append(item)
                        item = {}
                else:
                    item["rname"] = t["name"]
                    item["tiers_id"] = t["subtitle"][0]["_id"]
                    details.append(item)
                    item = {}

            detail_data = {
                "league_id": dic["league"]["id"],
                "game_name": dic["league"]["category"],
                "cname": dic["league"]["name"],
                "start_time": dic["league"]["start_time"][:10] + " " + dic["league"]["start_time"][11:19] if dic["league"]["start_time"] is not None else "",
                "finish_time": dic["league"]["finish_time"][:10] + " " + dic["league"]["finish_time"][11:19] if dic["league"]["finish_time"] is not None else "",
                "star": dic["league"]["stars"],
                "bonus": dic["league"]["prize_pool"],
                "sponsor": dic["league"]["sponsor"],
                "address": dic["league"]["address"],
                "promoted_teams": [{"tname": t["name"], "logo": t["logo"]} for t in dic["league"]["promoted_teams"]],
                "invited_teams": [{"tname": t["name"], "logo": t["logo"]} for t in dic["league"]["invited_teams"]],
                "placements": [{"tname": t["team"]["name"] if t["team"] is not None else "", "logo": t["team"]["logo"] if t["team"] is not None else "", "order_number":t["order_number"],
                                "placement":t["placement"], "prize": t["prize"], "ratings":t["ratings"]} for t in dic["placements"]],
                "event_detail": details
            }
            # 将详情页以及赛事组数据添加到队列
            self.detail_data_queue.put(detail_data)
            logger.info("data <{}> put detail_data_queue success!".format(detail_data["cname"]))
            self.detail_url_queue.task_done()

    # 取出详情页数据，拼接url得到赛事组数据
    def add_info_data_to_queue(self):
        """解析赛事组数据"""
        while True:
            # 取出detail_data
            detail_data = self.detail_data_queue.get()
            # 保存数据到json
            # self.save_data_to_json("competitive_detail", detail_data)
            # 保存数据到数据库
            self.mongo_detail.add_one_data('league_id', detail_data)
            logger.info("detail data <{}> save success!".format(detail_data["cname"]))
            # 获取event_detail
            event_details = detail_data["event_detail"]
            for event_detail in event_details:
                # 拼接url
                url = "http://www.1zplay.com/template/tier/" + event_detail["tiers_id"] + "?format=json"
                # 发送请求到url
                dic = self.middlewares.retry_request(url)
                # 构建json字典准备存入mongodb
                info_data = {
                    "tiers_id": event_detail["tiers_id"],
                    "league_id": detail_data["league_id"],
                    "data": dic["data"]
                }
                # self.save_data_to_json("competitive_info", info_data)
                # 保存数据到数据库
                self.mongo_info.add_one_data('tiers_id', info_data)
                logger.info("info data <{}> save success!".format(info_data["tiers_id"]))
            self.detail_data_queue.task_done()

    # 保存赛事信息到json
    def save_data_to_json(self, file_name, data):
        """取出赛事组页数据存入json"""
        with open("../Data/" + file_name + ".jsonlines", "a", encoding="utf8") as f:
            json.dump(data, f, ensure_ascii=False)
            f.write(",\n")

    # 线程开启入口
    def execute_task(self, task, count=1):
        """
        开启新线程来执行任务
        :param task: 要执行的任务(函数)
        :param count: 开启多少个线程
        :return:
        """
        for i in range(count):
            # 使用线程池, 执行异步任务
            self.pool.apply_async(task)

    # 对象销毁时关闭数据库
    def __del__(self):
        self.mongo_detail.close_db()
        self.mysql_conn.close_db()
        self.mongo_info.close_db()

    # 执行函数
    def run(self):
        self.execute_task(self.add_url_to_queue)
        self.execute_task(self.add_detail_to_queue)
        self.execute_task(self.add_detail_url_to_queue)
        self.execute_task(self.add_detail_data_to_queue)
        self.execute_task(self.add_info_data_to_queue, 5)

        # 让主线稍微睡一下, 分配时间给子线程执行, 防止还没有执行就结束了!!!
        time.sleep(0.1)
        # 让主线等待队列任务完成
        self.url_queue.join()
        self.page_queue.join()
        self.detail_url_queue.join()
        self.detail_data_queue.join()


if __name__ == '__main__':
    spider = CompetitiveSpider()
    spider.run()

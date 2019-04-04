#!/usr/bin/python
# -*- coding:utf-8 -*-
# author: dsc1697
# CreateTime: 2019/3/19
# software-version: python 3.7

import json
import time
from queue import Queue
from multiprocessing.dummy import Pool

from middlewares import Middlewares
from config import logger


class CsSpider(object):
    """csgo比分数据抓取"""
    def __init__(self):
        """初始化参数"""
        self.url = "http://www.1zplay.com/api/schedules?category=csgo&leagues=5c736b572fd72b55d45bebb0&leagues=5c6df" \
                   "7a12fd72b55d45beba8&leagues=5c64c9252fd72b55d45beb75&leagues=5c00f10143f6b0500d93cbf8&leagues=5c" \
                   "00f42143f6b0500d93cbfa&leagues=5bcd6d8d94d802261e19d190&leagues=5bbb89e14df3e65863922170&leagues" \
                   "=5bcd739394d802261e19d195&leagues=5bcd6f7f94d802261e19d193&leagues=5bb1b42b7f54e1073e225d5a&leag" \
                   "ues=5b96238ece11f15aa1c42425&leagues=5b8fe0dbce11f15aa1c4241d&leagues=5b62a6a53cc11326453526ec&l" \
                   "eagues=5b5582515b7fbe042094b9fd&leagues=5b3b938014f6b42a273bb6a2&leagues=5b36008a14f6b42a273bb68" \
                   "c&leagues=5ae9a2ccef81f73dd17441a1&leagues=5ad71968ef81f73dd174410c&leagues=5abdb529ba32ff22d32b" \
                   "c7ad&leagues=5a9769c92df6fd3ea9144e2d&leagues=5a9765ef2df6fd3ea9144e22&leagues=5aa114082bbf0c1a3" \
                   "9139f38&leagues=5a95201b8f736d6cc624ed88&leagues=5aa0e6d52bbf0c1a39139f33&leagues=5aa2003f2bbf0c" \
                   "1a39139f3b&leagues=5a7285f80828146fe8a40343&leagues=5abdb69dba32ff22d32bc7af&leagues=5a658d9dfb2" \
                   "8ec5d94913e2b&leagues=5a659727fb28ec5d94913e2d&leagues=5a56ce871d8cef02d4211977&leagues=5a27b801" \
                   "54548c2adcaf3778&leagues=null&page={}&state=finish"
        # 初始化中间件
        self.middlewares = Middlewares()
        # 记录页数
        self.url_count = 1
        # 创建队列
        self.url_queue = Queue()
        self.list_queue = Queue()
        # 初始化线程池
        self.pool = Pool(5)

    def add_url_to_queue(self):
        """添加url到队列"""
        while True:
            url = self.url.format(self.url_count)
            self.url_count += 1
            # 判断页面是否有内容，没有内容不再加入url_queue
            if len(self.middlewares.retry_request(url)["list"]) == 0:
                break
            self.url_queue.put(url)
            logger.info("<{}> put url_queue success!".format(url))

    def add_list_to_queue(self):
        """获取csgo列表页数据|"""
        while True:
            # 从url_queue中取出url，得到list页面
            url = self.url_queue.get()
            # 发送请求，获取响应
            dic = self.middlewares.retry_request(url)
            # 取出其中的list列表
            content_list = dic['list']
            for content in content_list:
                # 组装字典存入mongodb
                datas = {
                    "game_name": content["category"],
                    "logo": "http://cdn.1zplay.com/public/img/game/" + content["category"] + ".png",
                    "schedules_id": content["id"],
                    "cname": content["league"]["name"] if content["league"] is not None else "",
                    "index": content["live_match"]["index"],
                    "competition_system": "BO" + str(content["round"]),
                    "start_time": content["start_time"][:10] + " " + content["start_time"][11:19],
                    "map": content["live_match"]["map"] if "map" in content["live_match"].keys() else "",
                    "state": content["state"],
                    "left_team": {"tname": content["left_team"]["name"] if "name" in content["left_team"].keys() else "",
                                  "logo": content["left_team"]["logo"],
                                  "score": content["left_score"],
                                  "odd": content["odd_data"]["left_odd"],
                                  "side": content["live_match"]["team1_side"]
                                  if "team1_side" in content["live_match"].keys() else "",
                                  "this_score": content["live_match"]["team1_score"]
                                  if "team1_score" in content["live_match"] else "",
                                  "firstHalf": {"left_score": content["live_match"]["firstHalf"]["team1_score"],
                                                "left_side": content["live_match"]["firstHalf"]["team1_side"]
                                                } if "firstHalf" in content["live_match"] else "",
                                  "secondHalf": {
                                      "left_score": content["live_match"]["secondHalf"]["team1_score"],
                                      "left_side": content["live_match"]["secondHalf"]["team1_side"]
                                  } if "secondHalf" in content["live_match"] else "",
                                  "overtime": content["live_match"]["overtime"]["team1_score"]
                                  if "overtime" in content["live_match"].keys() else "",
                                  } if content["left_team"] is not None else "",
                    "right_team": {"tname": content["right_team"]["name"] if "name" in content["right_team"].keys() else "",
                                   "logo": content["right_team"]["logo"],
                                   "score": content["right_score"],
                                   "odd": content["odd_data"]["right_odd"],
                                   "side": content["live_match"]["team2_side"]
                                   if "team2_side" in content["live_match"].keys() else "",
                                   "this_score": content["live_match"]["team2_score"]
                                   if "team1_score" in content["live_match"] else "",
                                   "firstHalf": {"right_score": content["live_match"]["firstHalf"]["team2_score"],
                                                 "right_side": content["live_match"]["firstHalf"]["team2_side"],
                                                 } if "firstHalf" in content["live_match"] else "",
                                   "secondHalf": {
                                       "right_score": content["live_match"]["secondHalf"]["team2_score"],
                                       "right_side": content["live_match"]["secondHalf"]["team2_side"]
                                   } if "secondHalf" in content["live_match"] else "",
                                   "overtime": content["live_match"]["overtime"]["team2_score"]
                                   if "overtime" in content["live_match"].keys() else "",
                                   } if content["right_team"] is not None else "",
                    "r1": content["live_match"]["r1"],
                    "r16": content["live_match"]["r16"],
                    "win5": content["live_match"]["win5"]
                }
                self.list_queue.put(datas)
            self.url_queue.task_done()

    def get_detail_from_list(self):
        """发送详情页请求，解析响应数据"""
        while True:
            # 取出schedules_id
            datas = self.list_queue.get()
            self.save_data_to_json("csgo_list", datas)
            # 拼接URL，发送响应
            url = "http://www.1zplay.com/score/{}?format=json".format(datas["schedules_id"])
            # 发送请求，获取响应
            dic = self.middlewares.retry_request(url)
            jishu = 0
            stats = []
            for maps in dic["maps"]:
                if len(dic["stats"]) <= jishu:
                    break
                if len(dic["stats"][jishu]) == 10:
                    left_data = [dic["stats"][jishu][0], dic["stats"][jishu][1], dic["stats"][jishu][2],
                                 dic["stats"][jishu][3], dic["stats"][jishu][4]]
                    right_data = [dic["stats"][jishu][5], dic["stats"][jishu][6], dic["stats"][jishu][7],
                                  dic["stats"][jishu][8], dic["stats"][jishu][9]]
                elif len(dic["stats"][jishu]) == 8:
                    left_data = [dic["stats"][jishu][0], dic["stats"][jishu][1], dic["stats"][jishu][2],
                                 dic["stats"][jishu][3]]
                    right_data = [dic["stats"][jishu][4], dic["stats"][jishu][5], dic["stats"][jishu][6],
                                  dic["stats"][jishu][7]]
                else:
                    left_data = ""
                    right_data = ""
                jishu += 1
                stat = {"maps": maps,
                        "left_team": {
                            "tname": datas["left_team"]["tname"] if datas["left_team"] is not "" else "",
                            "data": left_data},
                        "right_team": {"tname": datas["right_team"]["tname"] if datas["right_team"] is not "" else "",
                                       "data": right_data}
                        }
                stats.append(stat)
            detail_datas = {
                "schedules_id": datas["schedules_id"],
                "stats": stats,
                "map_bp": dic["map_bp"],
                "round_info": dic["round_info"],
                "results": dic["results"],
            }
            self.save_data_to_json("csgo_detail", detail_datas)
            self.list_queue.task_done()

    # 保存赛事信息到json
    def save_data_to_json(self, file_name, data):
        """取出赛事组页数据存入json"""
        with open("../Data/" + file_name + ".jsonlines", "a", encoding="utf8") as f:
            json.dump(data, f, ensure_ascii=False)
            f.write(",\n")
        print("保存 <{}> 成功！".format(file_name))

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

    def run(self):
        self.execute_task(self.add_url_to_queue, 5)
        self.execute_task(self.add_list_to_queue, 5)
        self.execute_task(self.get_detail_from_list, 5)

        # 让主线稍微睡一下, 分配时间给子线程执行, 防止还没有执行就结束了!!!
        time.sleep(5)
        # 让主线等待队列任务完成
        self.url_queue.join()
        self.list_queue.join()


if __name__ == '__main__':
    spider = CsSpider()
    spider.run()

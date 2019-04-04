
import json
import requests
from queue import Queue
from multiprocessing.dummy import Pool
import time

from middlewares import Middlewares
from config import logger


class ScoreSpider(object):
    """比分列表页爬虫"""
    def __init__(self):
        """初始化参数"""
        # 初始url
        self.url = "http://www.1zplay.com/api/schedules?category=dota2&leagues=5c864ec22fd72b55d45bec79&leagues=5c7fc2732fd72b55d45bec16&leagues=5c7f0a572fd72b55d45bec0d&leagues=5c6df5352fd72b55d45beba7&leagues=5c7524b32fd72b55d45bebb9&leagues=5c7510472fd72b55d45bebb3&leagues=5c2b8ea543f6b0500d93cd28&leagues=5c2b8fd343f6b0500d93cd29&leagues=5c4d31252fd72b55d45bea97&leagues=5c2b8ccd43f6b0500d93cd27&leagues=5c2b89ae43f6b0500d93cd26&leagues=5c34228543f6b0500d93cd5d&leagues=5c2b93fb43f6b0500d93cd2e&leagues=5c29993d43f6b0500d93cd23&leagues=5c22f00943f6b0500d93ccf9&leagues=5c206e2143f6b0500d93cce6&leagues=5c1db3e743f6b0500d93ccdf&leagues=5c18bb5643f6b0500d93cccd&leagues=5c11ec1b43f6b0500d93ccb9&leagues=5c109c7b43f6b0500d93cca4&leagues=5c0bb60943f6b0500d93cc8a&leagues=5c035fa543f6b0500d93cc03&leagues=5bf7e3d343f6b0500d93cb5d&leagues=5bf5345c43f6b0500d93cb48&leagues=5beb049d43f6b0500d93cafd&leagues=5bf3999243f6b0500d93cb22&leagues=5be3ab4e43f6b0500d93caed&leagues=5bd6610f43f6b0500d93cabf&leagues=5be3763643f6b0500d93cae9&leagues=5bccb6bb4df3e6586392220a&leagues=5bce485d94d802261e19d1a0&leagues=5bc7be1b4df3e658639221d5&leagues=5bc457274df3e65863922199&leagues=5bc29ce74df3e65863922183&leagues=5bbd654d4df3e65863922175&leagues=5bba67314df3e65863922168&leagues=5bc4d14d4df3e658639221bb&leagues=5bb280364df3e65863922137&leagues=5bb19d567f54e1073e225d53&leagues=5bac55f87f54e1073e225cdb&leagues=5bac53d87f54e1073e225cda&leagues=5bae48f47f54e1073e225d05&leagues=5ba846eb7f54e1073e225cbb&leagues=5b9a15dfce11f15aa1c4242a&leagues=5b8d3a87ce11f15aa1c42418&leagues=5b557e1e5b7fbe042094b9fa&leagues=5b63ee6c3cc11326453526f5&leagues=5b62b9473cc11326453526f0&leagues=5b52e6135b7fbe042094b9f5&leagues=5b3cc75914f6b42a273bb6a6&leagues=5b35fa8014f6b42a273bb689&leagues=5b2728267eec3c4f6a380484&leagues=5b03d5f086b7d72865fe5aa7&leagues=5af6e3ce86b7d72865fe5a5d&leagues=5adcadd3ef81f73dd1744154&leagues=5aef2c5def81f73dd17441af&leagues=5a85b4db92abd971ffdb028b&leagues=5ad5e38eef81f73dd17440ed&leagues=5ad412802461157a983c4f36&leagues=5ac496512461157a983c4eeb&leagues=5ac98c012461157a983c4f10&leagues=5a85b73592abd971ffdb0291&leagues=5a85b0b192abd971ffdb0284&leagues=5aafc89ae6aa9371f2dba5ec&leagues=5a85b5c192abd971ffdb028c&leagues=5aa92d51c6854126efc08866&leagues=5aa6a4f8a08c69431b78a2a7&leagues=5a94c47c8f736d6cc624ed78&leagues=5aa5ffbba08c69431b78a2a5&leagues=5a85b86892abd971ffdb0292&leagues=5a9a0ea5181e6c31c29a0262&leagues=5a97caf59ae79f6cddd8c4bc&leagues=5a38e519f75edf444c5af963&leagues=5a7e68a90828146fe8a40390&leagues=5a5d66a169a0744a0ce16180&leagues=5a66bd82f80e3b5cfcb0d46b&leagues=5a66bfd6f80e3b5cfcb0d474&leagues=5a3a1f98b18c6d2a2471e388&leagues=5a3a2008b18c6d2a2471e389&leagues=5a3a2076b18c6d2a2471e38a&leagues=5a54332afed0441d7ca58698&leagues=5a3a20bcb18c6d2a2471e38b&leagues=5a4b21a3fc6037049878b388&leagues=5a421b7766b59d17f0f3e74c&leagues=5a3b857eb020152bbce0a829&leagues=5a3c8129b936963ee894de00&leagues=5a3221fe9263772950a80636&leagues=5a321f569263772950a80634&leagues=5a24b2183a809ba7d4335f5b&leagues=5a20dcb9f82d48a2a095556f&leagues=5a3caaa9b936963ee894de06&leagues=5a1e725e832921bff0e82575&leagues=5a1e758a832921bff0e82579&leagues=5a1e74dc832921bff0e82578&leagues=5a1e7455832921bff0e82577&leagues=5a1e73ab832921bff0e82576&leagues=5a63346042fc4d599871815d&leagues=null&page={}&state=finish"
        # 初始化中间件
        self.middlewares = Middlewares()
        # 初始化四个队列
        self.url_queue = Queue()
        self.list_queue = Queue()
        # 初始化线程池
        self.pool = Pool(5)
        # 记录页数
        self.url_count = 1
        # 记录局数
        self.ju = 1

    def add_url_to_queue(self):
        """添加url进queue"""
        while True:
            url = self.url.format(self.url_count)
            self.url_count += 1
            # 判断页面是否有内容，没有内容不再加入url_queue
            if len(self.middlewares.retry_request(url)["list"]) == 0:
                break
            self.url_queue.put(url)
            logger.info("<{}> put url_queue success!".format(url))

    def add_list_to_queue(self):
        """拿到json数据"""
        while True:
            # 从url_queue中取出url，得到list页面
            url = self.url_queue.get()
            # 发送请求，获取响应
            dic = self.middlewares.retry_request(url)
            # 取出其中的list列表
            content_list = dic['list']
            for content in content_list:
                # 组装字典存入mongodb
                if content["live_match"]:
                    list_data = {
                        "game_name": content["category"],
                        "logo": "http://cdn.1zplay.com/public/img/game/" + content["category"] + ".png",
                        "schedules_id": content["id"],
                        "cname": content["league"]["name"] if content["league"] is not None else "",
                        "index": content["live_match"]["index"],
                        "competition_system": "BO" + str(content["round"]),
                        "start_time": content["start_time"][:10] + " " + content["start_time"][11:19],
                        "game_time": str(content["live_match"]["game_time"]//60) + "分" + str(content["live_match"]["game_time"]%60) + "秒",
                        "state": content["state"],
                        "left_team": {"tname": content["left_team"]["name"], "logo": content["left_team"]["logo"],
                                      "score": content["left_score"], "odd": content["odd_data"]["left_odd"],
                                      "side": content["live_match"]["left_side"], "struck": content["live_match"]["left_data"]["score"],
                                      "gold": content["live_match"]["left_data"]["gold"], "xp": content["live_match"]["left_data"]["xp"],
                                      "towers": content["live_match"]["right_data"]["towers"].count(0)
                                      } if content["left_team"] is not None else "",
                        "right_team": {"tname": content["right_team"]["name"], "logo": content["right_team"]["logo"],
                                       "score": content["right_score"], "odd": content["odd_data"]["right_odd"],
                                       "side": content["live_match"]["right_side"], "struck": content["live_match"]["right_data"]["score"],
                                       "gold": content["live_match"]["right_data"]["gold"], "xp": content["live_match"]["right_data"]["xp"],
                                       "towers": content["live_match"]["left_data"]["towers"].count(0)
                                       } if content["right_team"] is not None else "",
                        "first_blood": content["live_match"]["first_blood"] if "first_blood" in content["live_match"].keys() else "",
                        "ten_kills": content["live_match"]["ten_kills"] if "ten_kills" in content["live_match"].keys() else "",
                        "win_team": content["live_match"]["win_side"] if "win_side" in content["live_match"].keys() else "",
                        "matchs_id": content["dota2_matches"]
                    }
                else:
                    list_data = {
                        "game_name": content["category"],
                        "logo": "http://cdn.1zplay.com/public/img/game/" + content["category"] + ".png",
                        "schedules_id": content["id"],
                        "cname": content["league"]["name"] if content["league"] is not None else "",
                        "competition_system": "BO" + str(content["round"]),
                        "start_time": content["start_time"][:10] + " " + content["start_time"][11:19],
                        "state": content["state"],
                        "left_team": {"tname": content["left_team"]["name"], "logo": content["left_team"]["logo"],
                                      "score": content["left_score"], "odd": content["odd_data"]["left_odd"]
                                      } if content["left_team"] is not None else "",
                        "right_team": {"tname": content["right_team"]["name"], "logo": content["right_team"]["logo"],
                                       "score": content["right_score"], "odd": content["odd_data"]["right_odd"]
                                       } if content["right_team"] is not None else "",
                        "matchs_id": content["dota2_matches"]
                    }
                # 将list数据添加入list_queue
                self.list_queue.put(list_data)
            # 完成该url的任务
            self.url_queue.task_done()

    def get_battle(self, match_id, array):
        """获取阵容分析胜率"""
        # 拼接url
        url = "http://www.1zplay.com/api/dota2_hero_data?match_id={}&type={}".format(match_id, array)
        # 获取headers
        headers = self.middlewares.get_user_agent()
        # 发送请求，获取响应
        response = requests.get(url, headers=headers, timeout=5)
        # 得到json_str
        json_str = response.content.decode()
        if response.status_code is not 200:
            dic = ""
        else:
            # 将json文件转换为Python文件
            dic = json.loads(json_str)

        return dic

    def get_content_from_detail(self):
        """"详情页数据解析"""
        while True:
            # 取出list数据
            datas = self.list_queue.get()
            self.save_content_to_json("daota2_score_list", datas)
            logger.info("list data <{}> save success!".format(datas["cname"]))
            # 取出matchs_id
            matchs_id = datas["matchs_id"]
            # 记录局数
            self.ju = 1
            for match_id in matchs_id:
                # 拼接url
                url = "http://www.1zplay.com/api/dota2_match/" + str(match_id)
                # 发送请求，获取响应
                dic = self.middlewares.retry_request(url)
                if self.ju == 1:
                    ju = "一"
                elif self.ju == 2:
                    ju = "二"
                elif self.ju == 3:
                    ju = "三"
                elif self.ju == 4:
                    ju = "四"
                elif self.ju == 5:
                    ju = "五"
                else:
                    ju = None
                self.ju += 1
                # 拼接详情页数据
                detail_datas = {
                    "detail_id": match_id,
                    "schedules_id": datas["schedules_id"],
                    "jushu": "第{}局".format(ju),
                    "dire_picks": dic["dire_picks"],
                    "dire_bans": dic["dire_bans"],
                    "radiant_bans": dic["radiant_bans"],
                    "radiant_picks": dic["radiant_picks"],
                    "dire_team": {"logo": dic["dire_team"]["logo"],
                                  "name": dic["dire_team"]["name"],
                                  "tag": dic["dire_team"]["tag"],
                                  "score": dic["dire"]["score"]} if dic["dire_team"] is not None else "",
                    "radiant_team": {"logo": dic["radiant_team"]["logo"], "name": dic["radiant_team"]["name"],
                                  "tag": dic["radiant_team"]["tag"], "score": dic["radiant"]["score"]} if dic["radiant_team"] is not None else "",
                    "ten_kills": dic["ten_kills"] if dic["ten_kills"] is not None else "",
                    "first_blood": dic["first_blood"] if dic["first_blood"] is not None else "",
                    "win_side": dic["win_side"] if dic["win_side"] is not None else "",
                    "finished": dic["finished"] if dic["finished"] is not None else "",
                    "radiant_gold_lead": dic["radiant_gold_lead"],
                    "game_time": str(dic["game_time"]//60) + "分" + str(dic["game_time"]%60) + "秒",
                    "win": self.get_battle(match_id, "win"),
                    "fb": self.get_battle(match_id, "fb"),
                    "tk": self.get_battle(match_id, "tk"),
                    "score": self.get_battle(match_id, "score"),
                    "gtime": self.get_battle(match_id, "gtime"),
                    "players": [{"name": player["account"]["name"] if player["account"] is not None or 0 else "",
                                 "assists": player["assists"] if player["assists"] is not None else "",
                                 "death": player["death"] if player["death"] is not None else "",
                                 "gold": player["gold"] if player["gold"] is not None else "",
                                 "net_worth": player["net_worth"] if player["net_worth"] is not None else "",
                                 "last_hits": player["last_hits"] if player["last_hits"] is not None else "",
                                 "denies": player["denies"] if player["denies"] is not None else "",
                                 "gold_per_min": player["gold_per_min"] if player["gold_per_min"] is not None else "",
                                 "xp_per_min": player["xp_per_min"] if player["xp_per_min"] is not None else "",
                                 "hero": player["hero"] if player["hero"] is not None else "",
                                 "level": player["level"] if player["level"] is not None else "",
                                 "team": player["team"] if player["team"] is not None else "",
                                 "item_0": player["item_0"] if player["item_0"] is not None else "",
                                 "item_1": player["item_1"] if player["item_1"] is not None else "",
                                 "item_2": player["item_2"] if player["item_2"] is not None else "",
                                 "item_3": player["item_3"] if player["item_3"] is not None else "",
                                 "item_4": player["item_4"] if player["item_4"] is not None else "",
                                 "item_5": player["item_5"] if player["item_5"] is not None else ""} for player in dic["players"]]
                }
                # 将数据存入json
                self.save_content_to_json("dota2_score_detail", detail_datas)
                logger.info("detail data <{}> save success!".format(detail_datas["detail_id"]))
            # 列表数据任务已完成
            self.list_queue.task_done()

    def save_content_to_json(self, filename, datas):
        with open("../Data/" + filename + ".jsonlines", "a", encoding="utf-8") as f:
            json.dump(datas, f, ensure_ascii=False)
            f.write(",\n")
        print("保存<{}>成功！".format(filename))

    # 线程池开启入口
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

        self.execute_task(self.add_url_to_queue, 3)
        self.execute_task(self.add_list_to_queue, 5)
        self.execute_task(self.get_content_from_detail, 5)

        # 让主线稍微睡一下, 分配时间给子线程执行, 防止还没有执行就结束了!!!
        time.sleep(5)
        self.url_queue.join()
        self.list_queue.join()


if __name__ == '__main__':
    spider = ScoreSpider()
    spider.run()

#!/usr/bin/python
# -*- coding:utf-8 -*-
# author: dsc1697
# CreateTime: 2019/3/19
# software-version: python 3.7

import re
import time
import pymysql

from selenium import webdriver


class DJSpider(object):
    """电竞比分网赛程页数据抓取"""
    def __init__(self):
        """初始化参数"""
        self.urls = ["http://www.1zplay.com/schedule?category=dota2", "http://www.1zplay.com/schedule?category=csgo"]
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36"
        }
        self.driver = webdriver.Chrome()
        self.conn = pymysql.connect(host='47.107.158.70', user='bluesky', passwd='mysql', db='competitive_score',
                                    charset='utf8')
        self.cursor = self.conn.cursor()

    def get_page_from_url(self, url):
        """发送请求，接收赛程页数据"""
        # 创建一个字典用来存储数据信息
        item = {}
        # 创建一个列表来存储字典
        data_list = []
        self.driver.get(url)
        # 获取赛程页节点数据
        html_list = self.driver.find_elements_by_xpath('//*[@id="container"]/div/div[2]/div/div/div[2]/div')
        for html in html_list:
            # 清空item
            item = {}
            # 比赛日期
            item['datetime'] = "2019年3月21日 - 星期四"
            # 游戏图片
            try:
                img = html.find_element_by_xpath('./div[1]/img').get_attribute('src')
                item['game_src'] = img
                # 游戏名称
                gname = re.match(r"http://cdn.1zplay.com/public/img/game/(\w+)\.png", img)
                item['game_name'] = gname.group(1)
                # 比赛时间
                item['ctime'] = html.find_element_by_xpath('./div[1]').text
                # 赛事名称
                item['league_name'] = html.find_element_by_xpath('./div[2]/span').text
                # 赛制
                item['competition_system'] = html.find_element_by_xpath('./div[4]/small').text
                # 第一队名字
                item['team1_name'] = html.find_element_by_xpath('./div[3]/span').text
                # 第二队名字
                item['team2_name'] = html.find_element_by_xpath('./div[5]/span').text
                # 第一队图标
                item['team1_src'] = html.find_element_by_xpath('./div[3]/img').get_attribute('src')
                # 第二队图标
                item['team2_src'] = html.find_element_by_xpath('./div[5]/img').get_attribute('src')
                # 赛事对应得id
                sche_id = html.get_attribute('data-id')
                if sche_id:
                    item['schedules_id'] = sche_id
                else:
                    item['schedules_id'] = "暂无数据"

            except:
                break
            data_list.append(item)

        return data_list

    def save_data_to_db(self, data_list):
        game_name = data_list["game_name"]
        game_src = data_list["game_src"]
        ctime = data_list["ctime"]
        league_name = data_list["league_name"]
        competition_system = data_list["competition_system"]
        team1_name = data_list["team1_name"]
        team2_name = data_list["team2_name"]
        team1_src = data_list["team1_src"]
        team2_src = data_list["team2_src"]
        schedules_id = data_list["schedules_id"]

        sql = '''insert into schedules(game_src, game_name, ctime, league_name, competition_system, team1_name, team2_name, team1_src, team2_src, schedules_id) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        self.cursor.execute(sql, (game_src, game_name, ctime, league_name, competition_system, team1_name, team2_name, team1_src, team2_src, schedules_id))
        self.conn.commit()
        print("数据插入成功")

    def run(self):
        for url in self.urls:
            data_list = self.get_page_from_url(url)
            for data in data_list:
                self.save_data_to_db(data)
        time.sleep(3)
        self.conn.close()
        self.driver.close()


if __name__ == '__main__':
    spider = DJSpider()
    spider.run()

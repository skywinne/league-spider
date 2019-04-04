#!/usr/bin/python
# -*- coding:utf-8 -*-
# author: dsc1697
# CreateTime: 2019/4/3
# software-version: python 3.7

from retrying import retry
import random
import requests
import json

from constants import *
from config import logger


class Middlewares(object):

    def __init__(self):
        # 记录重试次数
        self.count = 1

    def get_proxy(self):
        """从代理池中获取代理IP"""
        try:
            response = requests.get(PROXY_POOL_URL)
            if response.status_code == 200:
                print('Get Proxy', response.text)
                return response.text
        except requests.ConnectionError:
            return None

    def get_user_agent(self):
        """从user-agent池中取出user-agent"""
        user_agent = random.choice(USER_AGENT_PHONE)
        headers = {
            "Accept": ACCEPT,
            "Accept-Encoding": ACCPPT_ENCODING,
            "Accept-Language": ACCEPT_LANGUAGE,
            "Connection": CONNECTION,
            "Host": HOST,
            "User-Agent": user_agent
        }

        return headers

    # 这个函数用于requests超时重试
    @retry(stop_max_attempt_number=20, wait_fixed=100)
    def retry_request(self, url):
        try:
            # 启用代理
            # proxy = self..get_proxy()
            # 启用user-agent
            headers = self.get_user_agent()
            # if proxy:
            #     proxies = {
            #         'http': 'http://' + proxy,
            #         'https': 'https://' + proxy
            #     }
            #     # 超时的时候回报错并重试
            #     response = requests.get(url, headers=headers, proxies=proxies, timeout=3)
            #     logger.info("use proxy <{}> success!".format(proxies))
            # else:
            response = requests.get(url, headers=headers, timeout=3)
            # 状态码不是200，也会报错并重试
            assert response.status_code == 200
            # 得到json_str
            json_str = response.content.decode()
            # 将json文件转换为Python文件
            dic = json.loads(json_str)
            self.count = 1
            return dic
        except:
            print("<{}> retry {} times".format(url, self.count))
            logger.warning("<{}> retry {} times".format(url, self.count))
            logger.error("<{}> get response failed.".format(url))
            self.count += 1
        raise ("<{}> get response failed.".format(url))

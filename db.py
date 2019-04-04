#!/usr/bin/python
# -*- coding:utf-8 -*-
# author: dsc1697
# CreateTime: 2019/3/19
# software-version: python 3.7

import pymysql
from pymongo import MongoClient

from constants import *


class MysqlConfig(object):
    """mysql数据库初始化"""
    def __init__(self):
        # mysql连接配置
        self.db = pymysql.connect(
            host=MY_HOST,
            user=MYSQL_USER,
            passwd=MYSQL_PASSWD,
            db=MYSQL_DB,
            charset=CHARSET
        )

        # 打开mysql游标
        self.cursor = self.db.cursor()

    def add_data(self, table, data):
        # 添加数据
        table = table
        keys = ', '.join(data.keys())
        values = ', '.join(['%s'] * len(data))
        sql = 'INSERT INTO {table}({keys}) VALUES ({values}) ON DUPLICATE KEY UPDATE'.format(table=table, keys=keys, values=values)
        update = ','.join([" {key} = %s".format(key=key) for key in data])
        sql += update
        try:
            if self.cursor.execute(sql, tuple(data.values()) * 2):
                print('Successful')
                self.db.commit()
        except Exception as e:
            print('Failed info: {}'.format(e))
            self.db.rollback()

    def close_db(self):
        # 关闭mysql数据库
        self.db.close()


class MongoDBConfig(object):
    """配置mongodb"""
    def __init__(self, db, collection):
        # 配置MongoDB
        self.client = MongoClient(host=MY_HOST, port=MONGODB_PORT)
        # 指定集合
        self.collection = self.client[db][collection]

    def add_one_data(self, unique_id, data):
        # 插入数据
        self.collection.update_one({unique_id: data[unique_id]}, {'$set': data}, True)

    def close_db(self):
        # 关闭mongodb数据库
        self.client.close()

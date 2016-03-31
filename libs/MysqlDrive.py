#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author: xiaoL-pkav l@pker.in
@version: 2015/8/27 14:27
"""

import MySQLdb
import MySQLdb.cursors
from common.logger import ES_LOGGER


class MysqlDrive():
    def __init__(self, db_host, db_user, db_pass, db_name, db_port, charset):
        self.DB_QUERY = None
        self.DB_CONNECT = None
        self.DB_CURSOR = None
        self.DB_ARGS = None
        try:
            self.DB_CONNECT = MySQLdb.connect(db_host, db_user, db_pass, db_name, db_port, charset=charset,
                                              cursorclass=MySQLdb.cursors.SSCursor, connect_timeout=36000000)
            self.DB_CURSOR = self.DB_CONNECT.cursor()
            # MySQLdb.cursors.DictCursor 使用MySQLdb.cursors.SSCursor 无法直接返回字典序
        except Exception, e:
            ES_LOGGER.debug("数据库连接错误：%s " % e)

    def query(self, query, value):
        self.DB_QUERY = query
        self.DB_ARGS = value
        return self

    def execute_query(self):
        if self.DB_QUERY is None:
            ES_LOGGER.warning("数据库语句空缺")
        try:
            self.DB_CURSOR.execute(self.DB_QUERY, self.DB_ARGS)
        except Exception, e:
            self.DB_CONNECT.rollback()
            ES_LOGGER.debug("数据库执行错误：%s " % e)

    def format_data(self):
        results = self.DB_CURSOR.fetchall()
        return results

    def run(self):
        self.execute_query()
        try:
            self.DB_CONNECT.commit()
            return self.DB_CURSOR.rowcount
        except Exception, e:
            ES_LOGGER.debug("CURD 错误：%s" % e)

    def select(self):
        self.execute_query()
        return self.format_data()

    def stream_result(self, line):
        self.execute_query()
        return self.stream_data(line)

    def stream_data(self, line):
        try:
            while True:
                rows = self.DB_CURSOR.fetchmany(size=line)
                if not rows:
                   #self.release()
                    yield []
                else:
                    yield rows
        except Exception, e:
            ES_LOGGER.debug("数据读取错误，正在重新连接：%s" % e)
            yield 'Exception'

    def reinitialize(self):
        self.DB_QUERY = None

    def release(self):
        self.DB_CONNECT.close()

# #使用样例 Prepared statements执行查询
# db = MysqlDrive()
# #sql = "select Id,weburl from interface where Id = %s limit 10"
# sql = "update interface set whois ='xiaoL' where Id = %s "
# params = ['2']
# db.query(sql, params).run()

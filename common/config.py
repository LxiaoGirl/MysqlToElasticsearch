#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author: xiaoL-pkav l@pker.in
@version: 2015/8/27 14:38
"""

# 测试ES服务器写入效率
ES_COLONY = ["http://192.168.1.1:6200"]

# 导入数据库集合
DATABASES = [{
    "es_colony": ["http://192.168.1.1:6200"],
    "db_host": "192.168.1.1",
    "db_user": "root",
    "db_pass": "root",
    "db_port": 3306,
    "db_name": ["mysql"],
    "db_tables": "website",
    "db_charset": "utf8",
    "index": "index_test",
    "doc_type": "type_test",
    # 自增主键（例Id） | 'LIMIT'方式（较慢）"index_type": "数字主键" | "index_type": "limit",
    "index_type": "Id",
    "sql": "SELECT * FROM website "},
    {
    "es_colony": ["http://192.168.1.1:6200"],
    "db_host": "192.168.1.2",
    "db_user": "root",
    "db_pass": "root",
    "db_port": 3306,
    "db_name": ["mysql"],
    "db_tables": "website",
    "db_charset": "utf8",
    "index": "index_test",
    "doc_type": "type_test",
    # 自增主键（例Id） | 'LIMIT'方式（较慢）"index_type": "数字主键" | "index_type": "limit",
    "index_type": "Id",
    "sql": "SELECT * FROM website "}]
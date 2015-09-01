#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author: xiaoL-pkav l@pker.in
@version: 2015/8/27 14:38
"""

# 测试ES服务器写入效率
ES_COLONY = ["http://192.168.1.79:6200"]

# 上传文件大小 默认 10M 测试完毕后修改这里
UPLOAD_BYTE = 10 * 1024 * 1024

# 导入数据库集合
DATABASES = [{
    "es_colony": ["http://192.168.198.1:9200"],
    "db_host": "192.168.1.1",
    "db_user": "root",
    "db_pass": "root",
    "db_port": 3306,
    "db_name": ["test","mysql"],
    "db_tables": "website",
    "db_charset": "utf8",
    "index": "website",
    "doc_type": "url",
    "sql": "SELECT * FROM website"},
{
    "es_colony": ["http://192.168.198.1:9200"],
    "db_host": "192.168.1.3",
    "db_user": "root",
    "db_pass": "root",
    "db_port": 3306,
    "db_name": ["test","zhensan","sisi"],
    "db_tables": "website",
    "db_charset": "utf8",
    "index": "website",
    "doc_type": "url",
    "sql": "SELECT * FROM website"}]

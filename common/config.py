#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author: xiaoL-pkav l@pker.in
@version: 2015/8/27 14:38
"""

# 导入数据库集合
DATABASES = [{
    "es_colony": ["http://192.168.1.188:9200"],
    "db_host": "192.168.1.201",
    "db_user": "root",
    "db_pass": "root",
    "db_port": 3306,
    "db_name": ["cf"],
    "db_charset": "utf8",
    "index": "test",
    "doc_type": "website",
    "doc_field":['ip', 'port', 'site', 'url', 'banner', 'os', 'server', 'script', 'charset', 'title'],
    # 使用流式数据库读取，尽量缩小内存使用量
    "sql": "SELECT `IP` AS `ip`,`Port` AS `port`,`URL` AS `site`,`URL` AS `url`,`Banner` AS `banner`,`OS` AS `os`,"
           "`Server` AS `server`,`Script_Type` AS `script`,`Charset` AS `charset`,`Title` AS `title` FROM website"}]
    # ,{
    # "es_colony": ["http://192.168.1.188:9200"],
    # "db_host": "192.168.1.201",
    # "db_user": "root",
    # "db_pass": "root",
    # "db_port": 3306,
    # "db_name": ["cf"],
    # "db_charset": "utf8",
    # "index": "test",
    # "doc_type": "website",
    # "doc_field":['ip', 'port', 'site', 'url', 'banner', 'os', 'server', 'script', 'charset', 'title'],
    # # 使用流式数据库读取，尽量缩小内存使用量
    # "sql": "SELECT `IP` AS `ip`,`Port` AS `port`,`URL` AS `site`,`URL` AS `url`,`Banner` AS `banner`,`OS` AS `os`,"
    #        "`Server` AS `server`,`Script_Type` AS `script`,`Charset` AS `charset`,`Title` AS `title` FROM website"}]

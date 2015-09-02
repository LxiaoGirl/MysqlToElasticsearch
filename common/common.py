#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author: xiaoL-pkav l@pker.in
@version: 2015/8/31 15:49
"""
from datetime import datetime

# 日志目录
LOG_DOC = 'log'

# 上传文件大小 默认 10M 测试完毕后修改这里
UPLOAD_BYTE = 10 * 1024 * 1024

# 队列长度
QUEUE_LENGTH = 10000

# 数据偏移量
OFFSET_LEN = 500

# 测试ES性能建立的index
ES_TEST_INDEX = 'xl_test_index'

# 测试ES性能建立的type
ES_TEST_TYPE = 'xl_test_type'

# 测试ES性能建立的doc 1024B
ES_TEST_DOC = {"test": "entire Pro Git book. written Scott Chacon  Ben Straub and published by Apress. is available here. All content is licensed under the Creative Commons Attribution Non Commercial Share Alike 3.0 license. Print versions of the book are available on Amazon.com."
                       "entire Pro Git book. written Scott Chacon  Ben Straub and published by Apress. is available here. All content is licensed under the Creative Commons Attribution Non Commercial Share Alike 3.0 license. Print versions of the book are available on Amazon.com."
                       "entire Pro Git book. written Scott Chacon  Ben Straub and published by Apress. is available here. All content is licensed under the Creative Commons Attribution Non Commercial Share Alike 3.0 license. Print versions of the book are available on Amazon.com."
                       "entire Pro Git book. written Scott Chacon  Ben Straub and published by Apress. is available here. All content is licensed under the Creative Commons Attribution Non Commercial Share Alike 3.0 license. Print versions of the book are available on Amazon.com."}


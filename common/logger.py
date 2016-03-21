#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author: xiaoL-pkav l@pker.in
@version: 2015/8/31 16:20
"""

import logging
import logging.handlers
import sys
import os
import time

from common import LOG_DOC # 目录

from thirdparty.ansistrm.ansistrm import ColorizingStreamHandler

FORMATTER = logging.Formatter('%(asctime)s [%(name)s][%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
COLOR_LOGGER_HANDLER = ColorizingStreamHandler(sys.stdout)
COLOR_LOGGER_HANDLER.setFormatter(FORMATTER)

LOCAL_LOGGER_HANDLER = logging.handlers.RotatingFileHandler(os.path.join(os.path.join(os.getcwd(), LOG_DOC),
                                                                         time.strftime('%y%m%d')+'-log.log'))
LOCAL_LOGGER_HANDLER.setFormatter(FORMATTER)


def init_logger():
    log_path = os.path.join(os.getcwd(), LOG_DOC)
    if not os.path.isdir(log_path):
        try:
            os.makedirs(log_path)
        except Exception, e:
            raise e


class MyLogger(logging.getLoggerClass()):
    def __init__(self, name):
        """

        :rtype : object
        """
        super(MyLogger, self).__init__(self)
        self.name = name
        self.addHandler(LOCAL_LOGGER_HANDLER)
        self.addHandler(COLOR_LOGGER_HANDLER)

init_logger()
ES_LOGGER = MyLogger("ES_TO_MYSQL")
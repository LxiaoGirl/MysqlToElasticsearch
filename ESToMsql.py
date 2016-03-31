#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author: xiaoL-pkav l@pker.in
@version: 2015/8/27 14:27
"""
import sys
from time import sleep
from multiprocessing import Pool as ProcessPool, Manager

from elasticsearch import Elasticsearch, helpers

from libs.MysqlDrive import MysqlDrive
from common.logger import ES_LOGGER
from common.config import DATABASES
from common.common import QUEUE_LENGTH, BULK_LENGTH, SQL_NUMBER

reload(sys)
sys.setdefaultencoding('utf-8')


def init_database(db_host, db_user, db_pass, db_name, db_port, db_charset):
    ES_LOGGER.info("Connect to %s db: %s" % (db_host, db_name))
    return MysqlDrive(db_host, db_user, db_pass, db_name, db_port, db_charset)


def bulk_elasticsearch(r_queue, w_lock, dbs, db_name):
    ES_LOGGER.info("Bulk Host: %s DB: %s Start" % (dbs['db_host'], db_name))
    es = Elasticsearch(dbs['es_colony'], retry_on_timeout=True, max_retries=3, timeout=3600)
    flag = True
    bulks = []
    data_lines_number = 0
    bulk_length = 0
    while flag:
        while not r_queue.empty():
            if bulk_length == 0:
                w_lock.acquire()
            data = r_queue.get()
            data_lines_number += 1
            bulk_length += 1
            if bulk_length >= BULK_LENGTH or r_queue.empty():
                w_lock.release()
            if isinstance(data, str) and data == 'False':
                try:
                    ES_LOGGER.info("Bulk Host: %s DB: %s Data: %s" % (dbs['db_host'], db_name, bulk_length))
                    streaming_bulks = helpers.streaming_bulk(es, bulks, chunk_size=len(bulks))
                    for streaming_bulk in streaming_bulks:
                        if streaming_bulk[0]:
                            pass
                    bulks = []
                except Exception, e:
                    ES_LOGGER.warning(e)
                flag = False
                break
            bulks.append({
                "_index": dbs['index'],
                "_type": dbs['doc_type'],
                "_source": data
            })
            if bulk_length >= BULK_LENGTH:
                try:
                    ES_LOGGER.info("Bulk Host: %s DB: %s Data: %s" % (dbs['db_host'], db_name, data_lines_number),)
                    streaming_bulks = helpers.streaming_bulk(es, bulks, chunk_size=len(bulks))
                    for streaming_bulk in streaming_bulks:
                        if streaming_bulk[0]:
                            pass
                    bulks = []
                    bulk_length = 0
                except Exception, e:
                    ES_LOGGER.warning("Bulk Error! %s", e)

        ES_LOGGER.info("Queue is empty. Sleep 10")
        sleep(10)
    ES_LOGGER.info("Bulk Host: %s DB: %s Finish! Data: %s" % (dbs['db_host'], db_name, data_lines_number))


def write_database(w_queue, w_lock, dbs, db_name):
    ES_LOGGER.info("Index Host: %s DB: %s Start" % (dbs['db_host'], db_name))
    db_connect = init_database(dbs['db_host'], dbs['db_user'], dbs['db_pass'], db_name, dbs['db_port'],
                               dbs['db_charset'])
    ES_LOGGER.info("Host: %s DB: %s Start!SQL: %s" % (dbs['db_host'], db_name, dbs['sql']))
    data_lines_number = 0
    read_line_number = SQL_NUMBER
    for result_lines in db_connect.query(dbs['sql'], []).stream_result(line=read_line_number):
        if result_lines == 'Exception':
            db_connect.release()
            emergency_process(w_queue, w_lock, dbs, db_name, data_lines_number)
            break
        if len(result_lines) == 0:
            break
        if w_queue.qsize() + read_line_number <= QUEUE_LENGTH:
            w_lock.acquire()
            for result in result_lines:
                bulk_list = dict(zip(dbs['doc_field'], list(result)))
                w_queue.put(bulk_list)
            data_lines_number += len(result_lines)
            ES_LOGGER.info("Index Host: %s DB: %s Data: %s" % (dbs['db_host'], db_name, data_lines_number),)
            w_lock.release()
        else:
            while w_queue.qsize() + read_line_number > QUEUE_LENGTH:
                ES_LOGGER.info("Queue is full. Sleep 5")
                sleep(5)
            else:
                w_lock.acquire()
                for result in result_lines:
                    bulk_list = dict(zip(dbs['doc_field'], list(result)))
                    w_queue.put(bulk_list)
                data_lines_number += len(result_lines)
                ES_LOGGER.info("Index Host: %s DB: %s Data: %s" % (dbs['db_host'], db_name, data_lines_number),)
                w_lock.release()
    w_lock.acquire()
    w_queue.put("False")
    w_lock.release()
    ES_LOGGER.info("Index Host: %s DB: %s Finish! Data: %s" % (dbs['db_host'], db_name, data_lines_number))


def emergency_process(w_queue, w_lock, dbs, db_name, data_lines_number):
    ES_LOGGER.info("Emergency process index Host: %s DB: %s Start" % (dbs['db_host'], db_name))
    db_connect = init_database(dbs['db_host'], dbs['db_user'], dbs['db_pass'], db_name, dbs['db_port'],
                               dbs['db_charset'])

    emergency_sql = "%s limit %s,18446744073709551615" % (dbs['sql'], data_lines_number)
    ES_LOGGER.info("Host: %s DB: %s Start!SQL: %s" % (dbs['db_host'], db_name, emergency_sql))
    read_line_number = SQL_NUMBER
    for result_lines in db_connect.query(emergency_sql, []).stream_result(line=read_line_number):
        if result_lines == 'Exception':
            db_connect.release()
            emergency_process(w_queue, w_lock, dbs, db_name, data_lines_number)
            break
        if len(result_lines) == 0:
            break
        if w_queue.qsize() + read_line_number <= QUEUE_LENGTH:
            w_lock.acquire()
            for result in result_lines:
                bulk_list = dict(zip(dbs['doc_field'], list(result)))
                w_queue.put(bulk_list)
            data_lines_number += len(result_lines)
            ES_LOGGER.info("Index Host: %s DB: %s Data: %s" % (dbs['db_host'], db_name, data_lines_number),)
            w_lock.release()
        else:
            while w_queue.qsize() + read_line_number > QUEUE_LENGTH:
                ES_LOGGER.info("Queue is full. Sleep 5")
                sleep(5)
            else:
                w_lock.acquire()
                for result in result_lines:
                    bulk_list = dict(zip(dbs['doc_field'], list(result)))
                    w_queue.put(bulk_list)
                data_lines_number += len(result_lines)
                ES_LOGGER.info("Index Host: %s DB: %s Data: %s" % (dbs['db_host'], db_name, data_lines_number),)
                w_lock.release()


def main():
    process_pool = ProcessPool(len(DATABASES)*2)
    try:
        for dbs in DATABASES:
            if isinstance(dbs['db_name'], list):
                for db_name in dbs['db_name']:
                    managers = Manager()
                    w_queue = managers.Queue(QUEUE_LENGTH)
                    w_lock = managers.Lock()
                    process_pool.apply_async(write_database, args=(w_queue, w_lock, dbs, db_name))
                    process_pool.apply_async(bulk_elasticsearch, args=(w_queue, w_lock, dbs, db_name))
        process_pool.close()
        process_pool.join()
    except Exception, e:
        ES_LOGGER.error("BIG ERROR! %s",e)


if __name__ == '__main__':
    main()

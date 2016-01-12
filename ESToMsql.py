#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author: xiaoL-pkav l@pker.in
@version: 2015/8/27 14:27
"""
from time import sleep
from multiprocessing import Pool as ProcessPool, Manager, cpu_count

from elasticsearch import Elasticsearch, helpers

from libs.MysqlDrive import MysqlDrive
from common.logger import ES_LOGGER
from common.config import DATABASES
from common.common import QUEUE_LENGTH, BULK_LENGTH, THREAD_NUMBER


def init_database(db_host, db_user, db_pass, db_name, db_port, db_charset):
    ES_LOGGER.info("Connect to %s db: %s" % (db_host, db_name))
    return MysqlDrive(db_host, db_user, db_pass, db_name, db_port, db_charset)


def bulk_elasticsearch(r_queue, dbs, db_name):
    ES_LOGGER.info("Bulk Host: %s DB: %s Start" % (dbs['db_host'], db_name))
    es = Elasticsearch(dbs['es_colony'], retry_on_timeout=True, max_retries=3)
    flag = True
    bulks = []

    bulk_length = 0
    while flag:
        while not r_queue.empty():
            data = r_queue.get()
            if isinstance(data, str) and data == 'False':
                try:
                    ES_LOGGER.info("Bulk Host: %s DB: %s Data: %s" % (dbs['db_host'], db_name, bulk_length))
                    streaming_bulks = helpers.streaming_bulk(es, bulks)
                    for streaming_bulk in streaming_bulks:
                        if streaming_bulk[0]:
                            print "Bulk Id: %s\r" % streaming_bulk[1]['create']['_id'],
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
            bulk_length += 1
            if bulk_length >= BULK_LENGTH:
                try:
                    ES_LOGGER.info("Bulk Host: %s DB: %s Data: %s" % (dbs['db_host'], db_name, bulk_length))
                    streaming_bulks = helpers.streaming_bulk(es, bulks)
                    for streaming_bulk in streaming_bulks:
                        if streaming_bulk[0]:
                            print "Bulk Id: %s\r" % streaming_bulk[1]['create']['_id'],
                    bulks = []
                except Exception, e:
                    ES_LOGGER.warning(e)
                bulk_length = 0
        ES_LOGGER.info("Queue is empty. Sleep 5 ")
        sleep(5)
    ES_LOGGER.info("Bulk Host: %s DB: %s Finish" % (dbs['db_host'], db_name))


def write_database(w_queue, w_lock, dbs, db_name):
    ES_LOGGER.info("Index Host: %s DB: %s Start" % (dbs['db_host'], db_name))
    db_connect = init_database(dbs['db_host'], dbs['db_user'], dbs['db_pass'], db_name, dbs['db_port'],
                               dbs['db_charset'])
    data_lines_number = 0
    for result_lines in db_connect.query(dbs['sql'], []).stream_result():
        while w_queue.full() or w_queue.qsize()+1000 > QUEUE_LENGTH:
            ES_LOGGER.info("Queue is full. Sleep 20 ")
            sleep(20)
        for result in result_lines:
            bulk_list = dict(zip(dbs['doc_field'], list(result)))
            w_queue.put(bulk_list)
        data_lines_number += len(result_lines)
        print "Index Host: %s DB: %s Data: %s\r" % (dbs['db_host'], db_name, data_lines_number),

    thread_num = THREAD_NUMBER
    while thread_num > 0:
        w_queue.put("False")
        thread_num -= 1

    ES_LOGGER.info("Index Host: %s DB: %s Finish" % (dbs['db_host'], db_name))


def main():
    managers = Manager()
    common_queue = managers.Queue(QUEUE_LENGTH)

    common_lock = managers.Lock()
    process_pool = ProcessPool(cpu_count()*2)

    for dbs in DATABASES:
        if isinstance(dbs['db_name'], list):
            for db_name in dbs['db_name']:
                # 读数据库操作
                process_pool.apply_async(write_database, args=(common_queue, common_lock, dbs, db_name))
                process_pool.apply_async(bulk_elasticsearch, args=(common_queue, dbs, db_name))
    process_pool.close()
    process_pool.join()


if __name__ == '__main__':
    main()

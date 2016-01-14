#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author: xiaoL-pkav l@pker.in
@version: 2015/8/27 14:27
"""
import sys
from time import sleep
from multiprocessing import Pool as ProcessPool, Manager, cpu_count
from multiprocessing.dummy import Pool as ThreadPool

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


def bulk_elasticsearch(r_queue, dbs, db_name):
    ES_LOGGER.info("Bulk Host: %s DB: %s Start" % (dbs['db_host'], db_name))
    es = Elasticsearch(dbs['es_colony'], retry_on_timeout=True, max_retries=3, timeout=360)
    flag = True
    bulks = []

    bulk_length = 0
    while flag:
        while not r_queue.empty():
            data_lines_number = 0
            data = r_queue.get()
            data_lines_number += 1
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
                    print "Bulk Host: %s DB: %s Data: %s\r" % (dbs['db_host'], db_name, data_lines_number),
                    streaming_bulks = helpers.streaming_bulk(es, bulks)
                    for streaming_bulk in streaming_bulks:
                        if streaming_bulk[0]:
                            print "Bulk Id: %s\r" % streaming_bulk[1]['create']['_id'],
                    bulks = []
                    bulk_length = 0
                except Exception, e:
                    ES_LOGGER.warning(e)
        ES_LOGGER.info("Queue is empty. Sleep 5 ")
        sleep(5)
    ES_LOGGER.info("Bulk Host: %s DB: %s Finish! Data: %s" % (dbs['db_host'], db_name, data_lines_number))


def write_database(w_queue, dbs, db_name):
    ES_LOGGER.info("Index Host: %s DB: %s Start" % (dbs['db_host'], db_name))
    db_connect = init_database(dbs['db_host'], dbs['db_user'], dbs['db_pass'], db_name, dbs['db_port'],
                               dbs['db_charset'])
    data_lines_number = 0
    for result_lines in db_connect.query(dbs['sql'], []).stream_result(line=SQL_NUMBER):
        while w_queue.full() or w_queue.qsize()+1000 > QUEUE_LENGTH:
            ES_LOGGER.info("Queue is full. Sleep 20")
            sleep(20)
        for result in result_lines:
            bulk_list = dict(zip(dbs['doc_field'], list(result)))
            w_queue.put(bulk_list)
        data_lines_number += len(result_lines)
        print "Index Host: %s DB: %s Data: %s\r" % (dbs['db_host'], db_name, data_lines_number),

    w_queue.put("False")
    ES_LOGGER.info("Index Host: %s DB: %s Finish! Data: %s" % (dbs['db_host'], db_name, data_lines_number))


def main_process((w_queue, dbs, db_name)):
    try:
        process_pool = ThreadPool(2)
        process_pool.apply_async(write_database, args=(w_queue, dbs, db_name))
        process_pool.apply_async(bulk_elasticsearch, args=(w_queue, dbs, db_name))
        process_pool.close()
        process_pool.join()
    except Exception, e:
        ES_LOGGER.error("Thread Error!" % (e))


def main():
    map_queue = []
    map_dba = []
    map_db_name = []
    process_pool = ProcessPool(cpu_count())
    for dbs in DATABASES:
        if isinstance(dbs['db_name'], list):
            for db_name in dbs['db_name']:
                managers = Manager()
                map_dba.append(dbs)
                map_queue.append(managers.Queue(QUEUE_LENGTH))
                map_db_name.append(db_name)
    process_pool.map_async(main_process, map(lambda queue, dba, name: (queue, dba, name), map_queue, map_dba, map_db_name))

    process_pool.close()
    process_pool.join()


if __name__ == '__main__':
    main()

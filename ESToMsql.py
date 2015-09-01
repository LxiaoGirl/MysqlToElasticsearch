#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author: xiaoL-pkav l@pker.in
@version: 2015/8/27 14:27
"""
from time import time
from multiprocessing import Pool, Manager

from elasticsearch import Elasticsearch, helpers

from libs.MysqlDrive import MysqlDrive
from common.logger import ES_LOGGER
from common.config import DATABASES, ES_COLONY, UPLOAD_BYTE
from common.common import ES_TEST_INDEX, ES_TEST_TYPE, ES_TEST_DOC
from datetime import datetime


def check_es_status(elastic_search, es_colony):
    retry = 0
    try:
        es_status = elastic_search.info()
        if es_status['status'] == 200:
            return True, elastic_search
    except Exception, e:
        print e
        while retry <= 3:
            try:
                elastic_search = Elasticsearch(es_colony)
                print "ES_CONNECT restart!"
                return True, elastic_search
            except Exception, e:
                print e
            retry += 1
    return False, ''


#  __init__(self, db_host, db_user, db_pass, db_name, db_port, charset):
def init_database(db_host, db_user, db_pass, db_name, db_port, db_charset):
    ES_LOGGER.info("Connect to %s db: %s" % (db_host, db_name))
    return MysqlDrive(db_host, db_user, db_pass, db_name, db_port, db_charset)


def count_byte(data_list):
    length = 0
    if isinstance(data_list, dict):
        for data in data_list:
            # datetime 特殊处理
            if isinstance(data_list[data], datetime):
                length += 20
            else:
                length += len(data_list[data])
    return length


def start_import():
    for dbs in DATABASES:
        if isinstance(dbs['db_name'], list):
            for db_name in dbs['db_name']:
                db_table = dbs['db_tables']
                db_connect = init_database(dbs['db_host'], dbs['db_user'], dbs['db_pass'], db_name, dbs['db_port'],
                                           dbs['db_charset'])
                es = Elasticsearch(dbs['es_colony'])
                size = 1
                offset = 0

                count_sql = "SELECT COUNT(*) as `line` FROM %s" % db_table
                count_num = list(db_connect.query(count_sql, []).select())
                if len(count_num):
                    count_num = count_num[0]['line']
                else:
                    break
                while True:
                    temp_sql = "%s LIMIT %d,%d" % (dbs['sql'], offset, size)
                    print "\rDB: %s TABLE: %s Count: %s LIMIT %d,%d" % (db_name, db_table, count_num, offset, size),
                    result_lines = list(db_connect.query(temp_sql, []).select())
                    if len(result_lines):
                        bulks = []
                        length = 0
                        for result in result_lines:
                            length += count_byte(result)
                            bulks.append({
                                "_index": dbs['index'],
                                "_type": dbs['doc_type'],
                                "_source": result
                            })
                        print length*500
                        status, es = check_es_status(es, dbs['es_colony'])
                        if status is False:
                            print "ES LOST"
                            break
                        try:
                            temp_bulk = []
                            for r in range(0,1000):
                                temp_bulk.extend(bulks)
                                start_time = time()
                                bulk_status = helpers.bulk(es, temp_bulk)[0]
                                end_time = time()
                                print "count %d use time : %f " % (bulk_status, end_time-start_time)
                            #print bulk_status
                        except Exception, e:
                            print e
                        offset += 1000
                    else:
                        print("\r")
                        ES_LOGGER.info("Index DB: %s TABLES: %s Finish" % (db_name, db_table))
                        break


def test_performance():
    es_test = Elasticsearch(ES_COLONY)
    es_test_doc_length = len(ES_TEST_DOC["test"])
    bulks = [{
        "_index": ES_TEST_INDEX,
        "_type": ES_TEST_TYPE,
        "_source": ES_TEST_DOC}]
    for r in range(0, 9):
        bulks.extend(bulks)
        es_test_doc_length *= 2

    add_bulk = [{
        "_index": ES_TEST_INDEX,
        "_type": ES_TEST_TYPE,
        "_source": ES_TEST_DOC}]
    for r in range(0, 9):
        add_bulk.extend(add_bulk)

    now_rate = 100
    now_size = es_test_doc_length
    for test_time in range(1, 40):
        temp_bulk = add_bulk
        start_time = time()
        bulk_num = helpers.bulk(es_test, bulks)
        size = es_test_doc_length/1024*test_time
        use_time = time()-start_time
        now_rate, now_size = calculate_time(use_time, size, now_rate, now_size)
        if int(use_time)>2:
            break
        print("\rBulks number: %d, param size: %d KB, use time: %f,now siez: %d KB, time/size: %f" %
                       (bulk_num[0], size, use_time, now_size, now_rate)),
        bulks.extend(temp_bulk)
    print("\r")
    ES_LOGGER.info("Best size: %d KB" % now_size)


def calculate_time(use_time, size, now_rate, now_size):
    if int(use_time) <= 2:
        return use_time/size, size

    temp_rate = use_time/size
    if temp_rate > now_rate:
        return now_rate, now_size
    else:
        return temp_rate, size


def bulk_elasticsearch(r_queue, r_lock, dbs, db_name):
    es = Elasticsearch(dbs['es_colony'])
    db_table = dbs['db_tables']
    flag = True
    content_length = 0
    bulks = []
    while flag:
        while not r_queue.empty():
            data = r_queue.get()
            # print data
            if isinstance(data, str) and data == 'False':
                flag = False
                break

            count_length = count_byte(data)
            content_length = content_length + count_length

            if content_length <= UPLOAD_BYTE:
                bulks.append({
                    "_index": dbs['index'],
                    "_type": dbs['doc_type'],
                    "_source": data
                })
            else:
                try:
                    helpers.bulk(es, bulks)
                except Exception, e:
                    ES_LOGGER.warning(e)
                bulks = [{
                    "_index": dbs['index'],
                    "_type": dbs['doc_type'],
                    "_source": data
                }]
                content_length = count_length
                # status, es = check_es_status(es, dbs['es_colony'])
    try:
        helpers.bulk(es, bulks)
    except Exception, e:
        ES_LOGGER.warning(e)
    ES_LOGGER.info("Index DB: %s TABLES: %s Finish" % (db_name, db_table))



def write_database(w_queue, w_lock, dbs, db_name):
    db_table = dbs['db_tables']
    db_connect = init_database(dbs['db_host'], dbs['db_user'], dbs['db_pass'], db_name, dbs['db_port'],
                               dbs['db_charset'])

    size = 50
    offset = 0

    count_sql = "SELECT COUNT(*) as `line` FROM %s" % db_table
    count_num = list(db_connect.query(count_sql, []).select())
    if len(count_num):
        count_num = count_num[0]['line']

    while True:
        temp_sql = "%s LIMIT %d,%d" % (dbs['sql'], offset, size)
        #print temp_sql
        print "\rDB: %s TABLE: %s Count: %s LIMIT %d,%d" % (db_name, db_table, count_num, offset, size),
        result_lines = list(db_connect.query(temp_sql, []).select())
        if len(result_lines):
            w_lock.acquire()
            for result in result_lines:
                w_queue.put(result)
            w_lock.release()
            offset += 50
        else:
            w_lock.acquire()
            w_queue.put("False")
            w_lock.release()
            break
    print("\r")


def main():
    managers = Manager()
    common_queue = managers.Queue(1000)

    common_lock = managers.Lock()
    process_pool = Pool(4)

    for dbs in DATABASES:
        if isinstance(dbs['db_name'], list):
            for db_name in dbs['db_name']:
                # 读数据库操作
                process_pool.apply_async(write_database, args=(common_queue, common_lock, dbs, db_name))
                process_pool.apply_async(bulk_elasticsearch, args=(common_queue, common_lock, dbs, db_name))
    process_pool.close()
    process_pool.join()


if __name__ == '__main__':
    main()
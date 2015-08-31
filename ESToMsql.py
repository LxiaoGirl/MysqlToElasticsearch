#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
@author: xiaoL-pkav l@pker.in
@version: 2015/8/27 14:27
"""
from elasticsearch import Elasticsearch, helpers

from libs.MysqlDrive import MysqlDrive
from common.logger import ES_LOGGER
from common.config import DATABASES


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



def start_import():
    for dbs in DATABASES:
        if isinstance(dbs['db_name'], list):
            for db_name in dbs['db_name']:
                db_table = dbs['db_tables']
                db_connect = init_database(dbs['db_host'], dbs['db_user'], dbs['db_pass'], db_name, dbs['db_port'],
                                           dbs['db_charset'])
                es = Elasticsearch(dbs['es_colony'])
                size = 500
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
                        for result in result_lines:
                            bulks.append({
                                "_index": dbs['index'],
                                "_type": dbs['doc_type'],
                                "_source": result
                            })
                        status, es = check_es_status(es, dbs['es_colony'])
                        if status is False:
                            print "ES LOST"
                            break
                        try:
                            bulk_status = helpers.bulk(es, bulks)[0]
                        except Exception, e:
                            print e
                        offset += 500
                    else:
                        print("\r")
                        ES_LOGGER.info("Index DB: %s TABLES: %s Finish" % (db_name, db_table))
                        break


if __name__ == '__main__':
    start_import()


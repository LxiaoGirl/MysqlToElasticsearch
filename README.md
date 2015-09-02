# MysqlToElasticsearch
用于分库分表，表结构完全相同情况下从Mysql数据到导入数据到Elasticsearch搜索引擎。

# 用法（usage）
所有的配置都在/common/config.py中，完整配置即可运行。

# Python库要求
python 2.7.*  
MysqlDB   
windows下需要安装包，X64位官方没有版本，可参考http://www.codegood.com/archives/129。   
elasticsearch  
pip install elasticsearch  

# 配置样例
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
    
其中保证es_colony、db_name为list类型，db_port为int类型，其余是str类型。数据库配置错误程序会直接报错停止运行。  

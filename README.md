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

其中保证es_colony、db_name为list类型，db_port为int类型，其余是str类型。数据库配置错误程序会直接报错停止运行。  

# 说明
根据个人的服务器配置信息，修改common.py文件中的bulk上传限制跟queue队列大小。服务器性能偏低尽量调小两个参数，比如1000|20000。

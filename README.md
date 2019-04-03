# 基于Python Socket开发cdh告警接收发送


### 环境
1. python2.6, 理论支持2.6以上
2. 第三方库：pymysql
因为company主要跟电信合作，脚本都是部署在电信的生产机上，python版本不能随便更新。


### 库表
    table: 见table.sql自己company项目，部分表结构不方便透露


### 流程
    1. send.py 在cdh大数据服务器上接收告警json，同时从http://###:50070/jmx获取jmx信息，同时通过socket接口传送到采集机上。

    2. disk_warning.py 在采集机上，提供多线程recv数据，因为cdh上可能同时触发几次告警，接收到后判断告警信息某键是否有'DATA_NODE_VOLUME_FAILURES'关键词，有则判断为告警，然后从jmx中获取坏卷信息如坏卷时间、换卷盘路径、ip地址、严重等级等。

    3. 入库、做本地备份、数据库做备份信息。
    数据提供给凯通发送短信给电信相关人员处理。
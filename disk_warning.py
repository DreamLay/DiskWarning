#!/usr/bin/python2.6
# -*- coding: utf-8 -*-
import socket,urllib,urllib2,json,sys,re,time,threading,logging,os
import pymysql
from datetime import datetime
from datetime import timedelta
reload(sys)
sys.setdefaultencoding('utf-8')


logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 设置日志
fh = logging.FileHandler('./SocketServer.log', mode='a+')
fh.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s: *thread-> %(thread)d* %(module)s.%(funcName)s[line:%(lineno)d]: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)


warning_keys = ('DATA_NODE_VOLUME_FAILURES', )


# 解释器
class Resolver():

    def __init__(self, data, expire_delay, mysqlController):
        self.expire_delay = expire_delay
        self.alert_json, self.jmx_json = data.split("###") # 分割告警信息与jmx信息
        self.mysqlController = mysqlController


    # 判断是否需要检查jmx
    def judge_jmx(self, CURRENT_COMPLETE_HEALTH_TEST_RESULTS):
        for result in CURRENT_COMPLETE_HEALTH_TEST_RESULTS:
            result_dict = json.loads(result)
            for key in warning_keys:
                # print(key, result_dict['testName'])
                if result_dict['testName'] == key and result_dict['severity'] == 'CRITICAL':
                    return result_dict['content']
        return None
 

    # 解析数据
    def parse(self):
        try:
            items = []
            # warning_jsons = re.sub(r"\n|\r|\t|\s", "", self.alert_json)
            warning_jsons = "[" + re.sub(r"\]\[", "],[", self.alert_json) + "]"  # 非标准json，需替换字符串
            warning_infos = json.loads(warning_jsons)
            for i in warning_infos:
                for j in i:
                    items.append(j['body']) if 'body' in j else ""


            count = 0
            for item in items:
                count += 1
                alert = item['alert']
                attributes = item['alert']['attributes']
                alert_id = attributes['__uuid'][0]
                CURRENT_COMPLETE_HEALTH_TEST_RESULTS=attributes['CURRENT_COMPLETE_HEALTH_TEST_RESULTS'] if 'CURRENT_COMPLETE_HEALTH_TEST_RESULTS' in attributes else None
                # 判断是否磁盘告警
                is_warning = self.judge_jmx(CURRENT_COMPLETE_HEALTH_TEST_RESULTS) if CURRENT_COMPLETE_HEALTH_TEST_RESULTS else False
                jmx_data = 'HEALTH_TEST_RESULTS is not no abnormal'
                if is_warning:
                    # print('有告警')
                    warning_item = dict(
                        # CLUSTER_DISPLAY_NAME=attributes['CLUSTER_DISPLAY_NAME'][0] if 'CLUSTER_DISPLAY_NAME' in attributes else "",
                        HOSTS=attributes['HOSTS'][0] if 'HOSTS' in attributes else "",  # 时间主机
                        # SEVERITY=attributes['SEVERITY'][0] if 'SEVERITY' in attributes else "",
                        # testName=attributes['testName']['epochMs'] if 'testName' in attributes else "",
                        # HEALTH_TEST_RESULTS=attributes['HEALTH_TEST_RESULTS'] if 'HEALTH_TEST_RESULTS' in attributes else None,
                        alert_id=alert_id,   # 告警编码
                        content=is_warning,   # 告警描述
                        source=alert['source'],   # 告警来源url
                        # HEALTH_TEST_NAME=attributes['HEALTH_TEST_NAME'] if 'HEALTH_TEST_NAME' in attributes else "",
                        timestamp=alert['timestamp'] if 'timestamp' in alert else "",   # 告警发生时间
                        SERVICE_TYPE=attributes['SERVICE_TYPE'][0] if 'SERVICE_TYPE' in attributes else "",   # 告警类别
                        # SERVICE_DISPLAY_NAME=attributes['SERVICE_DISPLAY_NAME'][0] if 'SERVICE_DISPLAY_NAME' in attributes else "",
                        # CURRENT_COMPLETE_HEALTH_TEST_RESULTS=attributes['CURRENT_COMPLETE_HEALTH_TEST_RESULTS'] if 'CURRENT_COMPLETE_HEALTH_TEST_RESULTS' in attributes else "",
                        ROLE=attributes['ROLE'][0] if 'ROLE' in attributes else "",   # 角色
                        # ROLE_TYPE=attributes['ROLE_TYPE'][0] if 'ROLE_TYPE' in attributes else "",
                        ALERT_SUMMARY=attributes['ALERT_SUMMARY'][0] if 'ALERT_SUMMARY' in attributes else "",
                        # HOST_IDS=attributes['HOST_IDS'][0] if 'HOST_IDS' in attributes else "",
                    )
                    self.parse_jmx(warning_item, item)
                alert_json = json.dumps(alert)
                bak_path = BackupsTool(alert_json, self.jmx_json).backups(str(count), is_warning)
                alert_bak_path = os.path.join('/home/sdnmuser/python_script', bak_path[0][1:])
                jmx_bak_path = os.path.join('/home/sdnmuser/python_script', bak_path[1][1:])
                sql = "insert into cdh_bak_info (insert_time, expire_time, alert_id, alert_bak_path, jmx_bak_path,alert_bak_size,jmx_bak_size,save_days) values ( %s, %s, %s, %s, %s, %s, %s, %s); "

                """
                计算过期时间
                """
                insert_date = datetime.now()
                expire_date = insert_date + timedelta(days=self.expire_delay)
                result = self.mysqlController.excute_sql(sql, 
                        (
                            insert_date.strftime('%Y%m%d%H%M%S'),
                            expire_date.strftime('%Y%m%d%H%M%S'),
                            alert_id,alert_bak_path,jmx_bak_path,
                            str(len(alert_json)), str(len(self.jmx_json)), str(self.expire_delay)
                        )
                    )
                if not result:
                    break


        except Exception as e:
            logging.exception(e)

    
    # 获取坏卷信息
    def parse_jmx(self, warning_item, item, request_count=1):
        try:
            info = json.loads(self.jmx_json)
            VolumeFailuresTotal = ''
            for item in info['beans']:
                VolumeFailuresTotal = item['VolumeFailuresTotal'] if 'VolumeFailuresTotal' in item else VolumeFailuresTotal
                if 'LiveNodes' in item:
                    LiveNodes_dict = json.loads(item['LiveNodes'])
                    for host in LiveNodes_dict:
                        if LiveNodes_dict[host]['volfails'] > 0:
                            LiveNodes = True
                            info_dict = dict(
                                infoAddr = LiveNodes_dict[host]['infoAddr'].split(':')[0],
                                failedStorageLocations = ', '.join([i.replace('/dfs/dn','') for i in LiveNodes_dict[host]['failedStorageLocations']]),
                                lastVolumeFailureDate = time.strftime("%Y%m%d%H%M%S", time.localtime(float(LiveNodes_dict[host]['lastVolumeFailureDate'])/1000)),
                                host = host
                            )
                            # actiontime = time.strftime("%Y%m%d%H%M%S", time.localtime(float(LiveNodes_dict[host]['lastVolumeFailureDate'])/1000))
                            self.create_sql(info_dict, warning_item)
     
        except Exception as e:
            logger.error("Failed to parse JMX error. :".format(e))
        

    # 构造sql语句
    def create_sql(self, badroll_info, warning_item):
        if badroll_info['infoAddr'].split('.')[2] == '57':
            ip = self.mysqlController.find_data("select ip from cdh_host where work_ip='132.126.57.15';")
            if len(ip):
                badroll_info['infoAddr'] = ip[0]
            else:
                logger.warning("Ip is not find. Work_ip is %s" % badroll_info['infoAddr'])

        content = "%s|%s|%s" % (warning_item['SERVICE_TYPE'], 'DATANODE', badroll_info['failedStorageLocations']+'磁盘损坏')
        try:
            insert_dict = dict(
                actiontime=badroll_info['lastVolumeFailureDate'],
                content = content,
                dbtime = time.strftime('%Y%m%d%H%M%S'),logtype = '磁盘告警',priority = '0',status = '0',
                branch_id = '77',occurtimes = '0',emailsent = '0',smssent = '0',sendstatus = '1',
                relatestatus = '0',source = badroll_info['host'],
                subject = content,
                warning_position = badroll_info['failedStorageLocations'],warning_cause = 'CDH磁盘告警',
                oldId = '886688',host_id = '20567'
            )
            self.mysqlController.excute_sql("update host set hostname='%s' where id='20567';" % badroll_info['infoAddr'])
            # count = self.mysqlController.find_data(insert_dict['oldId'])
            # if count and count != 'error': # 
            #     sql = "update warning_item set #### where oldId='886688';"
            #     values = (####)
            #     self.mysqlController.excute_sql(sql % values)
            #     return

            keys = ','.join([str(i) for i in insert_dict])
            values = [str(insert_dict[i]) for i in insert_dict]
            placeholder = ','.join(['%s' for i in range(len(values))])
            sql = "insert into warning_item ({0}) values ({1});".format(keys, placeholder)
            self.mysqlController.excute_sql(sql, values)
            return
        except Exception as e:
            logger.error("Create SQL error: {0}".format(e))



# mysql控制器
class MySqlController():

    def __init__(self):
        self.db = pymysql.connect(host='####', user="####", passwd="####", db='####', charset='utf8')
    

    # 查询
    def find_data(self, sql):
        try:
            cursor = self.db.cursor()
            cursor.execute(sql)
            value = cursor.fetchone()
            return value
        except Exception as e:
            return 'error'
            logger.error(e)


    # 插入数据
    def excute_sql(self, sql, arvg=None):
        try:
            cursor = self.db.cursor()
            cursor.execute(sql, arvg)
            self.db.commit()
            cursor.close()
        except Exception as e:
            logger.error("excute SQL failde: {0}. EXCUTE SQL: {1}".format(e, sql))

        return True

    def end(self):
        self.db.close()


# 备份工具
class BackupsTool():
    def __init__(self, alert_data, jmx_data):
        self.alert_data, self.jmx_data = alert_data, jmx_data

    def backups(self, count, is_warning):
        nowdate = time.strftime("%Y%m%d %H%M%S").split()
        alert_bak_path = './bak/alert/%s/' % nowdate[0]
        jmx_bak_path = './bak/jmx/%s/' % nowdate[0]

        # 创建备份目录
        if not os.path.exists(alert_bak_path):
            os.makedirs(alert_bak_path)
        if not os.path.exists(jmx_bak_path):
            os.makedirs(jmx_bak_path)

        # 写入数据
        try:
            alert_bak_file = os.path.join(alert_bak_path,nowdate[1]+'-%s.json' % count)
            jmx_bak_file = os.path.join(jmx_bak_path,nowdate[1]+'-%s.json' % count)
            if is_warning:
                alert_bak_file = os.path.join(alert_bak_path,nowdate[1]+'-%s-warning.json' % count)
                jmx_bak_file = os.path.join(jmx_bak_path,nowdate[1]+'-%s-warning.json' % count)
            else:
                alert_bak_file = os.path.join(alert_bak_path,nowdate[1]+'-%s.json' % count)
                jmx_bak_file = os.path.join(jmx_bak_path,nowdate[1]+'-%s.json' % count)
            with open(alert_bak_file, 'w') as f:
                f.write(self.alert_data)
            with open(jmx_bak_file, 'w') as f:
                f.write(self.jmx_data)
            return (alert_bak_file, jmx_bak_file)
        except Exception as e:
            logger.error(e)


# 进度条显示器
class ProgressBar():
    
    now_step = 0
    all_step = 0
    now = time.strftime('%Y-%m-%d %H:%M:%S')
    def __init__(self, total, batch_size):
        self.total = total
        self.k = 0
        self.step = (float(total) / batch_size)/50

    def move(self):
        self.all_step += 1
        self.now_step += 1
        if self.now_step >= self.step:
            self.k += 1
            i = self.k - 1
            str = '>'*i+' '*(50-self.k)
            sys.stdout.write('\r'+ '%s - %s/%skb[%s%%]' % (self.now,self.all_step,self.total/1024+1,(i*2+1)) +str)
            sys.stdout.flush()
            self.now_step = self.now_step - self.step

    def end(self):
        sys.stdout.write('\r'+ '%s - %s/%skb[%s%%]' % (self.now, self.total/1024+1,self.total/1024+1,100) + '>'*50)
        sys.stdout.flush()
        print("")


# 接收服务器
class SocketServer():

    def __init__(self, host, port, expire_delay):
        self.host = host
        self.port = port
        self.conn_count = 0
        self.thread_count = 0
        self.expire_delay = expire_delay


    # 连接客户端
    def conn(self, sk,conn, address):
        
        self.conn_count += 1
        logger.info('Receive a connection: {0}. Current connections: {1}'.format(address[0], str(self.conn_count)))
        while True:
            data_size = conn.recv(1024)
            logger.info("The size of the data to be received is {0}bytes".format(data_size))
            # print("待接收数据大小：{0}".format(data_size))
            conn.sendall('ok')
            data = ""
            current_size = 0
            # pgb = ProgressBar(int(data_size),1024)
            while len(data) < int(data_size):
                # pgb.move()
                time.sleep(0.05)
                tmp_data = conn.recv(1024)
                current_size += len(tmp_data)
                data += tmp_data
            print(current_size)
            # pgb.end()
            conn.close()
            self.conn_count -= 1
            mysqlController = MySqlController()
            Resolver(data,self.expire_delay,mysqlController).parse() # 启动解析器
            mysqlController.end()
            self.thread_count -= 1
            return


    # 启动监听
    def start(self):
        try:
            ip_port = (self.host, int(self.port))
            sk = socket.socket()
            sk.bind(ip_port)
            sk.listen(5)
            print('The service has started.')
        except Exception as e:
            logger.error(e)
            print("Failed to start service!")
        else:
            while True:
                if self.thread_count < 5:
                    conn, address =  sk.accept()
                    thread = threading.Thread(target=self.conn, args=(sk, conn, address))
                    self.thread_count += 1
                    thread.start()
                    



if __name__ == "__main__":
    

    # host = raw_input('Please input address: ')
    # port = raw_input('Please input port: ')

    host = '######'
    port = '11211'
    expire_delay = int(sys.argv[1])
    server = SocketServer(host,port,expire_delay)
    server.start()
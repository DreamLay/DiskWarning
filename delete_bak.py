# -*- coding: utf-8 -*-
import pymysql,os
from datetime import datetime


class Clearning():

    def __init__(self):
        self.db = pymysql.connect(host='132.96.167.14', user="sendinmuser", passwd="sendinmpw", db='ejb3', charset='utf8')
        

    def clear_folder(self):
        basic_path = "/home/sdnmuser/python_script/bak/"
        for cate in ('alert', 'jmx'):
            folders = os.listdir(basic_path + cate)
            for i in folders:
                if not len(os.listdir(os.path.join(basic_path,cate,i))):
                    os.rmdir(os.path.join(basic_path,cate,i))
    

    def delete_bak_file(self):
        cursor = self.db.cursor()
        cursor.execute("select id,expire_time,alert_bak_path,jmx_bak_path from cdh_bak_info;")
        datas = cursor.fetchall()
        for data in datas:
            bak_id, expire_time, alert_bak_path, jmx_bak_path = data[0],data[1],data[2],data[3]
            if datetime.strptime(expire_time, '%Y%m%d%H%M%S') <= datetime.now():
                os.remove('/home/sdnmuser/python_script' + alert_bak_path)
                os.remove('/home/sdnmuser/python_script' + jmx_bak_path)
                cursor.execute("delete from cdh_bak_info where id='%s';" % bak_id)
                self.db.commit()
        self.db.close()

if __name__ == "__main__":

    clear = Clearning()
    clear.delete_bak_file()
    clear.clear_folder()
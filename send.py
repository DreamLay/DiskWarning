#!/usr/bin/python
# -*- coding: utf-8 -*-
import socket
import time,logging
import urllib,urllib2,sys


logger = logging.getLogger()
logger.setLevel(logging.INFO)
fh = logging.FileHandler('./SocketClient.log', mode='a+')
fh.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(module)s.%(funcName)s[line:%(lineno)d]: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)


def get_response(request_count=1):
    try:
        url = 'http://132.122.85.18:50070/jmx' # cdh
        req = urllib2.Request(url)
        res_data = urllib2.urlopen(req, timeout=10)
        return res_data.read()

    except Exception as e:
        if request_count <= 3:
            logger.error("Get JMX response failed at {0} time(s): {1}".format(str(request_count), e))
            get_response(request_count+1)
        return None
        


if __name__ == "__main__":
    path = sys.argv[1]
    try:
        f = open(path, 'r')
        alert_json = f.read()
        f.close()
    except Exception as e:
        logger.error(e)

    jmx_json = get_response()
    if jmx_json:
        all_info = alert_json + '###' + jmx_json

        ip_port = ('132.122.132.188',11211) 
        sk = socket.socket()
        sk.connect(ip_port)
        sk.sendall(str(len(all_info)))
        server_recall = sk.recv(1204)
        sk.sendall(all_info)
        sk.close()

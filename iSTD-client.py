#!/usr/bin/env python
#coding=utf-8

'''
@author Sam Cheng
@desc this is a test script for sending the played record to the iSTD-platform
@date 2020-06-12
'''

import redis
import time
import datetime
import sqlite3
import os
import requests
import json


class iSTD():
        def __init__(self):
                self.redisConn = redis.Redis(host='127.0.0.1',port=6379,db=0)
                self.conn = sqlite3.connect('/var/opt/bsp/share/bsp/bsp.db')
                self.url = 'http://10.179.245.101:5000/'
                self.proxies = {'http':'http://proxyrelay.jcd.priv:8000/'}

        def sendData(self,msg):
                try:
                        headers = {'Content-Type': 'application/json;charset=UTF-8'}
                        response = requests.post(self.url,data=json.dumps(msg),headers=headers)
                        if response.status_code == 200:
                                return 0
                        else:
                                return -1

                except Exception:
                        return -1


        def saveRedata(self,msg):
                timeScore = int(time.mktime(datetime.datetime.now().timetuple())                                                                                        )
#               mapping = {msg:timeScore}
                self.redisConn.zadd('iSTD/POP',msg,timeScore)

        def loadRedata(self):
                data = self.redisConn.zrange('iSTD/POP',0,10,desc=False,withscores=False)
                return data

        def removeRedata(self,msg):
                self.redisConn.zrem('iSTD/POP',*msg)

        def getBspdata(self):
                bspdata = {}
                cur = self.conn.cursor()
                cur.execute('select content_id,timestamp,duration from monitor_stats_stat order by timestamp desc limit 1')
                result = cur.fetchall()
                bspdata['player_id'] = os.popen('DMS-broadsignID').read().strip()
                bspdata['content_id'] = result[0][0]
                bspdata['timestamp'] = result[0][1]
                bspdata['duration'] = result[0][2]
                cur.close()
                return bspdata

        def checkBsp(self):
                cur = self.conn.cursor()
                cur.execute('select count(*) from monitor_stats_stat')
                result = cur.fetchall()
                nowTime = str(result[0][0])
                beforeTime = self.redisConn.getset('iSTD/bspcheck',nowTime)
                cur.close()
                if nowTime == beforeTime:
                        return 0
                else:
                        return -1


if __name__ == '__main__':
        iSTD = iSTD()
        while True:
                if iSTD.checkBsp():
                        sendAll = {}
                        sendAll['pop'] = []
                        data1 = iSTD.getBspdata()
                        data2 = iSTD.loadRedata()
                        data2.append(data1)
                        sendAll['pop'] = data2
                        if iSTD.sendData(sendAll) == 0:
                                data2 = tuple(data2)
                                iSTD.removeRedata(data2)
                                print('%s:%s'% (datetime.datetime.now(),sendAll))
                        else:
                                iSTD.saveRedata(data1)
                                print('%s:SENDING DATA ERROR' % datetime.datetime.now())

                else:
                        pass

                time.sleep(5)

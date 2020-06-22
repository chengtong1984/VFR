import redis
import time
import datetime
import sqlite3
import os


class iSTD():
    def __init__(self):
        self.redisConn = redis.Redis(host='127.0.0.1',port=6379,db=0)
        self.conn = sqlite3.connect('/var/opt/bsp/share/bsp/bsp.db')

    def saveRedata(self,msg):
        timeScore = int(time.mktime(datetime.datetime.now().timetuple()))
        mapping = {msg:timeScore}
        self.redisConn.zadd('iSTD/POP',mapping)
    def loadRedata(self):
        data = self.redisConn.zrange('iSTD/POP',0,10,desc=False,withscores=False)
        return data
    def removeRedata(self,msg):
        self.redisConn.zrem('iSTD/POP',msg)
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
        self.conn.close()
        return bspdata

if __name__ == '__main__':
    iSTD = iSTD()
    iSTD.saveRedata('sam')
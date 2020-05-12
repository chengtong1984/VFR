from xml.dom import minidom
import datetime
import time
import redis
import os
re_queue = redis.Redis(host='localhost',port=6379)
filePath = '/home/apache/test/'
files = os.listdir(filePath.decode('utf-8'))
for file in files:
        file = file.encode('utf-8')
        playerId = file[0:-4]
        with open(filePath + file,'r') as fh:
                dom=minidom.parse(fh)
                root=dom.documentElement
                saturations=root.getElementsByTagName('override')
                for saturation in saturations:
                        h = 0
                        s=saturation.getAttribute('saturation')
                        c=saturation.getAttribute('campaign_id')
                        starttime=saturation.getAttribute('start')
                        endtime=saturation.getAttribute('end')
                        a=datetime.datetime.strptime(starttime,'%Y-%m-%d'+'T'+'%H:%M:%S')
                        stra = a.strftime('%Y-%m-%d')
                        b=datetime.datetime.strptime(endtime,'%Y-%m-%d'+'T'+'%H:%M:%S')
                        d = b - a
                        e = round((d.seconds + 1)/180*float(s))
                        e = int(e)
                        f = str(c)+':'+str(playerId)+':'+str(stra)
                        re_queue.lpush(f,e)
                        re_queue.expire(f,60)
                        result=re_queue.lrange(f,0,-1)
                        for g in result:
                                h += int(g)
                        re_queue.lpush(str(c)+':'+str(stra),h)
                        re_queue.sadd(str(stra),str(c))
                        print(f+':'+str(h))

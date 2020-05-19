import requests,json
from multiprocessing.dummy import Pool as ThreadPool
import pymysql
import redis
import re

omitList=['saturation.xml','meta.json.js','sc.3.0.0.js','sc.2.3.0.js','sc.2.4.0.js','sc.2.5.0.js']
players = []
myhost='127.0.0.1'
myport=3306
myuser='vrf'
mypasswd='Hu@teng123'
mydb='test'
apiurl='https://content-api.cn.viooh.com/api/v2/players/'
filepath='/home/apache/test/'

redisPool = redis.Redis(host='127.0.0.1',port='6379',db=0)


def readPlayerId():
    conn = pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)
    cursor_1 = conn.cursor()
    cursor_1.execute('select * from player_info')
    rows = cursor_1.fetchall()
    for row in rows:
        player_id=row[1]
        players.append(str(player_id))
    cursor_1.close()
    conn.close()

def savePlayerInfo(playerId):
        res = requests.get(apiurl + str(playerId))
        htmlDict = res.json()
        fileList = htmlDict.get('data').get('attributes').get('files').keys()
        campaignList = list(set(fileList) - set(omitList))
        for campaign in campaignList:
                viooh_id = str(campaign)
                conn = pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)
                cursor_1 = conn.cursor()
                cursor_1.execute('select * from campaign_info where viooh_id = %s',(viooh_id,))
                row = cursor_1.fetchone()
                cursor_1.close()
                conn.close()
                if row is None:
                        campaignUrl = htmlDict.get('data').get('attributes').get('files').get(campaign).get('path')
                        res = requests.get(campaignUrl)
                        campaignDict = res.json()
                        petalFile = campaignDict.get('data').get('attributes').get('files').get('petal.json.js').get('path')
                        res = requests.get(petalFile)
                        content = res.text
                        content = content.replace(' ','').replace('\n','').replace('\r','')
                        p1 = re.compile(r'[(](.*?)[)]', re.S)
                        content = re.findall(p1,content)
                        content = str(content).split(',document.currentScript.id,',1)[0].lstrip('\'[').lstrip('u\'')
                        content  = json.loads(content)
                        cus_name = content.get('campaign').get('name').decode('unicode_escape')
                        sb_num = content.get('campaign').get('booking_engine_id')
                        starts_at = content.get('campaign').get('starts_at')[0:9]
                        ends_at = content.get('campaign').get('ends_at')[0:9]
                        bs_id = content.get('campaign').get('broadsign_campaign_ids')[0]
                        viooh_id = content.get('campaign').get('id')
                        query = 'insert into campaign_info(viooh_id,campaign_id,sb_id,customer_name,start,end) values(%s,%s,%s,%s,%s,%s)'
                        values = (viooh_id,bs_id,sb_num,cus_name,starts_at,ends_at)
                        conn = pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)
                        cursor_1 = conn.cursor()
                        cursor_1.execute(query,values)
                        conn.commit()
                        cursor_1.close()
                        conn.close()

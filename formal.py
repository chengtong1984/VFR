#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
@Author Sam Cheng
@desc this is a script for tomorrow plan of campaigns
@date 2020-05-01
'''

import schedule
import sys
import datetime
from jinja2 import Environment, FileSystemLoader
import requests,json
from multiprocessing.dummy import Pool as ThreadPool
import pymysql
import redis
import re
from xml.dom import minidom
import datetime
import time
import os
from collections import Counter
import smtplib
from email.mime.text import MIMEText
import logging

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s : %(levelname)s  %(message)s',
        datefmt='%Y-%m-%d %A %H:%M:%S',
        filename='/home/apache/python/vfr.log',
        filemode='w')  # 写入模式“w”或“a”

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

mailto_list=["sam.cheng@jcdecaux.com"]
cc_mail=[""]
mail_host="smtp.qq.com"
mail_user="2944329431@qq.com"
mail_pass="kqmrxackdcqldgfh"


def sendMail(to_list,to_cc):
        tomorrow = str(datetime.date.today() + datetime.timedelta(days=1))
        f = open("/var/www/html/viooh/index.html",'r')
        msg = '''VFR4.0测试版'''
        mail_content = msg.encode('utf-8')
        while True:
                line = f.readline()
                msg += line.strip()+'\n'
                if not line:
                        break
        f.close()
        mail_content = msg.encode('utf-8')
        me="2944329431@qq.com"+"<"+mail_user+">"
        msg = MIMEText(mail_content,_subtype='html',_charset='utf8')
        msg['Subject'] = "150块DP双联屏" + tomorrow +"播放预告"
        msg['From'] = "2944329431@qq.com"
        msg['To'] = ";".join(to_list)
        msg['Cc'] = ";".join(to_cc)
        try:
                s = smtplib.SMTP()
                s.connect(mail_host,587)
                s.ehlo()
                s.starttls()
                s.login(mail_user,mail_pass)
                s.sendmail(me,to_list+to_cc,msg.as_string())
                s.close()
                logging.debug('发送邮件成功')
                return True
        except Exception, e:
                logging.error('发送邮件失败，原因: %s' % e)
                return False


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
        return players
def savePlayerInfo(playerId):
        try:
                res = requests.get(apiurl + str(playerId),timeout=(5,10))
                htmlDict = res.json()
                newChecksum = htmlDict.get('data').get('attributes').get('files').get('saturation.xml').get('checksum')
                oldChecksum = redisPool.getset(playerId,newChecksum)
                if newChecksum == oldChecksum:
                        logging.debug('%s saturation文件未发生改变，不下载' % playerId)
                        pass
                else:
                        downloadUrl = htmlDict.get('data').get('attributes').get('files').get('saturation.xml').get('path')
                        try:
                                downloadRes = requests.get(downloadUrl,timeout=(5,20))
                                localPath = filepath + str(playerId) + '.xml'
                                with open(localPath,'wb') as file:
                                        file.write(downloadRes.content)
                                logging.debug('%s saturation新文件下载成功.' % playerId)
                        except Exception,e:
                                logging.error('%s saturation文件下载失败,原因 %s.' % (playerId,e))


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
                        logging.debug('%s 检查广告活动 %s 结束' % (playerId,campaign))
                        if row is None:
                                campaignUrl = htmlDict.get('data').get('attributes').get('files').get(campaign).get('path')
                                res = requests.get(campaignUrl,timeout=(5,10))
                                campaignDict = res.json()
                                petalFile = campaignDict.get('data').get('attributes').get('files').get('petal.json.js').get('path')
                                res = requests.get(petalFile,timeout=(5,10))
                                content = res.text
                                content = content.replace(' ','').replace('\n','').replace('\r','')
                                p1 = re.compile(r'[(](.*)[)]', re.S)
                                content = re.findall(p1,content)
                                content = str(content).split(',document.currentScript.id,',1)[0].lstrip('\'[').lstrip('u\'')
                                try:
                                        content  = json.loads(content)
                                        cus_name = content.get('campaign').get('name').decode('unicode_escape')
                                        sb_num = content.get('campaign').get('booking_engine_id')
                                        starts_at = content.get('campaign').get('starts_at').split('T')[0]
                                        ends_at = content.get('campaign').get('ends_at').split('T')[0]
                                        bs_id = content.get('campaign').get('broadsign_campaign_ids')[0]
                                        viooh_id = content.get('campaign').get('id')
                                        content_len = content.get('frames').values()[0].get('content_length')
                                        query = 'insert into campaign_info(viooh_id,campaign_id,sb_id,customer_name,start,end,content_length) values(%s,%s,%s,%s,%s,%s,%s)'
                                        values = (viooh_id,bs_id,sb_num,cus_name,starts_at,ends_at,content_len)
                                        conn = pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)
                                        cursor_1 = conn.cursor()
                                        cursor_1.execute(query,values)
                                        conn.commit()
                                        cursor_1.close()
                                        conn.close()
                                        logging.debug('新广告活动 %s 已成功保存至数据库' % sb_num)
                                except Exception,e:
                                        logging.error('%s 广告活动处理失败,原因: %s' % (campaignUrl,e))
        except Exception,e:
                logging.error('%s 播放机无法连接，请检查网络' % playerId)

def dayReportInfo(todayTime):
        flist = []
        conn = pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)
        re_queue = redis.Redis(host='localhost',port=6379)
        filePath = '/home/apache/test/'
        files = os.listdir(filePath.decode('utf-8'))
        for file in files:
                flist = set()
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
                                re_queue.expire(str(stra),60)
                                re_queue.sadd(str(stra),str(c))
                                flist.add(f)
                        for m in flist:
                                q = 0
                                o = re_queue.lrange(m,0,-1)
                                for p in o:
                                        q += int(p)
                                j = m.split(':')[0] + ':' + m.split(':')[2]
                                re_queue.expire(j,60)
                                re_queue.lpush(j,q)
                logging.debug('%s 文件已成功转换至缓存redis.' % file)

        cur = conn.cursor()
        cur.execute('delete from daysum_info where show_time = %s',(todayTime,))
        members=re_queue.smembers(todayTime)
        for member in members:
                result=re_queue.lrange(str(member)+':'+str(todayTime),0,-1)
                countAll=Counter(result)
                for countNum,screenNum in countAll.items():
                        values = (member,countNum,screenNum,todayTime)
                        query = 'insert into daysum_info(campaign_id,count_num,screen_num,show_time) values(%s,%s,%s,%s)'
                        cur.execute(query,values)
                        conn.commit()
                logging.debug('%s 广告活动报表数据已成功存入数据库' % member)
        cur.close()
        conn.close()

def generate_html():
        result={}
        body = []
        num = 1
        conn = pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb,use_unicode=True,charset='utf8')
        cursor_1 = conn.cursor()
        cursor_1.execute('select a.campaign_id,b.customer_name,b.sb_id,b.start,b.end,a.count_num,a.screen_num,FORMAT(b.content_length/1000,0) from daysum_info a left join campaign_info b on a.campaign_id = b.campaign_id where a.show_time = %s order by a.campaign_id;',(tomorrow,))
        rows = cursor_1.fetchall()
        cursor_1.close()
        conn.close()

        for row in rows:
                conn2 = pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb,use_unicode=True,charset='utf8')
                cursor_2 = conn2.cursor()
                cursor_2.execute('select count_num from daysum_info where show_time = %s and campaign_id = %s and screen_num = %s',(today,row[0],row[6],))
                rows_2 = cursor_2.fetchone()
                cursor_2.close()
                conn2.close()
                if rows_2 is None:
                        result['status'] = 'new'
                elif rows_2[0] == row[5]:
                        result['status'] = 'normal'
                else:
                        result['status'] = 'diff'

                result['campaign_id']=row[0]
                result['customer_name']=row[1]
                result['sb_id']=row[2]
                result['starts_at']=row[3]
                result['ends_at']=row[4]
                result['count']=row[5]
                result['screen']=row[6]
                result['content_length'] = row[7]
                result['num'] = (num % 2)
                body.append(result.copy())
                num = num + 1

        env = Environment(loader=FileSystemLoader('/home/apache/python/'))
        template = env.get_template('template.html')
        with open("/var/www/html/viooh/index.html",'w+') as fout:
                html_content = template.render(tomorrow=tomorrow,body=body,display_time=display_time)
                fout.write(html_content)
        logging.debug('成功生成明日报表')
def main():
        global tomorrow
        global today
        global display_time
        tomorrow = str(datetime.date.today() + datetime.timedelta(days=1))
        today = str(datetime.date.today())
        display_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        readPlayerId()
        for playerName in players:
                savePlayerInfo(playerName)
        dayReportInfo(tomorrow)
        generate_html()
        sendMail(mailto_list,cc_mail)
'''
if __name__ == "__main__":
        schedule.every().hour.do(main)
        while True:
                schedule.run_pending()
                time.sleep(1)
'''
main()


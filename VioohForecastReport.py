#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
@Author Sam Cheng
@desc this is a script for tomorrow plan of campaigns
@create 2020-09-15
@modify 2020-10-22
'''
import datetime
from jinja2 import Environment,FileSystemLoader
import requests
import json
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
from email.header import Header
import logging
import schedule
import importlib
import configparser
import sys

reload(sys)
sys.setdefaultencoding('utf8')



cur_path = os.path.dirname(os.path.realpath(__file__))
config_path = os.path.join(cur_path,'config.ini')

conf = configparser.ConfigParser()
conf.read(config_path)

logging.basicConfig(
level = logging.DEBUG,
format = '%(asctime)s:%(levelname)s%(message)s',
datefmt = '%Y-%m-%d%A%H:%M:%S',
filename = conf.get('LOG','filename'),
filemode = conf.get('LOG','filemode')
)

players = []

omitList = str(conf.get('PARAS','ignorefile')).split(',')

myhost = conf.get('MYSQL','myhost')
myport = int(conf.get('MYSQL','myport'))
myuser = conf.get('MYSQL','myuser')
mypasswd = conf.get('MYSQL','mypasswd')
mydb = conf.get('MYSQL','mydb')

apiurl = conf.get('PARAS','viooh_url')

filepath = conf.get('PARAS','saturation_dir')

redisPool = redis.Redis(host=conf.get('REDIS','rehost'),port=conf.get('REDIS','report'),db=int(conf.get('REDIS','redb')))

mailto_list = str(conf.get('MAIL','mailto_list')).split(',')
cc_mail = str(conf.get('MAIL','cc_mail')).split(',')
mail_host = conf.get('MAIL','mail_host')
mail_user = conf.get('MAIL','mail_user')
mail_pass = conf.get('MAIL','mail_pass')
send_file = conf.get('MAIL','send_file_path')

tomorrow=''
today=''
display_time=''

                tomorrow = str(datetime.date.today()+datetime.timedelta(days=1))
                n_time = datetime.datetime.now()
                s_time = datetime.datetime.strptime(str(n_time.date())+'15:30', '%Y-%m-%d%H:%M')
                l_time =  datetime.datetime.strptime(str(n_time.date())+'16:30', '%Y-%m-%d%H:%M')
                if n_time > s_time and n_time<l_time:
                msg['Subject'] = "250块电子屏" + tomorrow + "VIOOH明日播放预告"


class sendMail():

        def __init__(self,to_list,to_cc,subject,send_file):
                
                cur_path = os.path.dirname(os.path.realpath(__file__))
                config_path = os.path.join(cur_path,'config.ini')
                conf = configparser.ConfigParser()
                conf.read(config_path)
                self.mail_host = conf.get('MAIL','mail_host')
                self.mail_user = conf.get('MAIL','mail_user')
                self.mail_pass = conf.get('MAIL','mail_pass')
                self.to_list=to_list
                self.to_cc=to_cc
                self.send_file=send_file
                self.subject=subject

        def sendReport(self):

                f = open(self.send_file,'r')
                msg = ''
                while True:
                        line = f.readline()
                        msg += line.strip() +'\n'
                        if not line:
                                 break
                f.close()
                mail_content = msg.encode('utf-8')
                me = mail_user + "<" + mail_user + ">"
                msg = MIMEText(mail_content,_subtype = 'html',_charset = 'utf8')
                msg['Subject'] = self.subject
                msg['From'] = mail_user
                msg['To'] = ";".join(self.to_list)
                msg['Cc'] = ";".join(self.to_cc)
                try:
                        self.s = smtplib.SMTP()
                        self.s.connect(mail_host,587)
                        self.s.ehlo()
                        self.s.starttls()
                        self.s.login(mail_user,mail_pass)
                        self.s.sendmail(me,self.to_list+self.to_cc,msg.as_string())
                        self.s.close()
                        logging.debug('发送邮件成功')
                        return True
                except Exception as e:
                        logging.error('发送邮件失败，原因:%s' % e)
                        return False
'''
        def sendWarning1(self):
                msg = MIMEText(result,'plain','utf-8')
                msg['From'] = mail_user
                msg['To'] = ";".join(self.to_list)
                msg['Cc'] = ";".join(self.to_cc)
                msg['Subject'] = Header('告警！' + title + '放不出来的campaign',charset='utf8')
                try:
                        self.s.sendmail(me,self.to_list+self.to_cc,msg.as_string())
                        self.s.close()
                        logging.debug('发送邮件成功')
                        return True
                except Exception as e:
                        logging.error('发送邮件失败，原因:%s' % e)
                        return False
'''
def readPlayerId():
        conn = pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)
        cursor_1 = conn.cursor()
        cursor_1.execute("select hostName,playerID from relationalTable where mediaType = 'DP' or mediaType = 'LED'")
        rows = cursor_1.fetchall()
        for row in rows:
                player_id=row[1]
                players.append(str(player_id))
        cursor_1.close()
        conn.close()
        return  players

def savePlayerInfo(playerId):
        try:
                res = requests.get(apiurl+str(playerId),timeout=(5,10))
                if res.status_code == 200:
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
                                        if downloadRes.status_code == 200:
                                                localPath = filepath + str(playerId) + '.xml'
                                                with open(localPath,'wb') as file:
                                                        file.write(downloadRes.content)
                                                logging.debug('%s saturation新文件下载成功.' % playerId)
                                        else:
                                                logging.debug('%s viooh-api已阻止saturation文件下载.' % playerId)
                                                redisPool.set(playerId,'ffff')
                                                redisPool.lpush('block_player',playerId)

                                except Exception as e:
                                        logging.error('%s saturation文件下载失败,原因 %s' % (playerId,e))


                        fileList = htmlDict.get('data').get('attributes').get('files').keys()
                        campaignList = list(set(fileList) - set(omitList))
                        for campaign in campaignList:
                                viooh_id = str(campaign)
                                conn = pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)
                                cursor_1 = conn.cursor()
                                cursor_1.execute('select * from vioohCampaign where viooh_id = %s',(viooh_id,))
                                row=cursor_1.fetchone()
                                cursor_1.close()
                                conn.close()
                                logging.debug('%s 检查广告活动 %s 结束' % (playerId,campaign))
                                if row is None:
                                        campaignUrl = htmlDict.get('data').get('attributes').get('files').get(campaign).get('path')
                                        res=requests.get(campaignUrl,timeout=(5,10))
                                        if res.status_code == 200:
                                                campaignDict=res.json()
                                                petalFile=campaignDict.get('data').get('attributes').get('files').get('petal.json.js').get('path')
                                                res=requests.get(petalFile,timeout=(5,10))
                                                if res.status_code == 200:
                                                        content=res.text
                                                        content=content.replace(' ','').replace('\n','').replace('\r','')
                                                        p1=re.compile(r'[(](.*)[)]',re.S)
                                                        content=re.findall(p1,content)
                                                        content=str(content).split(',document.currentScript.id,',1)[0].lstrip('\'[').lstrip('u\'')
                                                        try:
                                                                content=json.loads(content)
                                                                cus_name=content.get('campaign').get('name').decode('unicode_escape')
                                                                sb_num=content.get('campaign').get('booking_engine_id')
                                                                starts_at=content.get('campaign').get('starts_at').split('T')[0]
                                                                ends_at=content.get('campaign').get('ends_at').split('T')[0]
                                                                bs_id=content.get('campaign').get('broadsign_campaign_ids')[0]
                                                                viooh_id=content.get('campaign').get('id')
                                                                content_len=content.get('frames').values()[0].get('content_length')
                                                                query='insert into vioohCampaign(viooh_id,campaign_id,sb_id,customer_name,start,end,content_length) values(%s,%s,%s,%s,%s,%s,%s)'
                                                                values=(viooh_id,bs_id,sb_num,cus_name,starts_at,ends_at,content_len)
                                                                conn=pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)
                                                                cursor_1=conn.cursor()
                                                                cursor_1.execute(query,values)
                                                                conn.commit()
                                                                cursor_1.close()
                                                                conn.close()
                                                                logging.debug('新广告活动 %s 已成功保存至数据库' % sb_num)
                                                        except Exception as e:
                                                                logging.error('%s 广告活动处理失败,原因:%s' % (campaignUrl,e))
                else:
                        redisPool.lpush('block_player',playerId)
                        logging.debug('%s播放机已被阻止网络访问' % playerId)
        except Exception as e:
                logging.error('%s播放机无法连接，请检查网络'%playerId)

def dayReportInfo(todayTime):
        flist=[]
        conn=pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)
        re_queue=redis.Redis(host='localhost',port=6379)
        filePath='/home/apache/test/'
        files=os.listdir(filePath.decode('utf-8'))
        for file in files:
                flist=set()
                file=file.encode('utf-8')
                playerId=file[0:-4]
                with open(filePath+file,'r') as fh:
                        dom=minidom.parse(fh)
                        root=dom.documentElement
                        saturations=root.getElementsByTagName('override')
                        for saturation in saturations:
                                s=saturation.getAttribute('saturation')
                                c=saturation.getAttribute('campaign_id')
                                starttime=saturation.getAttribute('start')
                                endtime=saturation.getAttribute('end')
                                a=datetime.datetime.strptime(starttime,'%Y-%m-%d'+'T'+'%H:%M:%S')
                                stra=a.strftime('%Y-%m-%d')
                                b=datetime.datetime.strptime(endtime,'%Y-%m-%d'+'T'+'%H:%M:%S')
                                d=b-a
                                e=round((d.seconds+1)/180*float(s))
                                e=int(e)
                                f=str(c)+':'+str(playerId)+':'+str(stra)
                                re_queue.lpush(f,e)
                                re_queue.expire(f,60)
                                re_queue.expire(str(stra),60)
                                re_queue.sadd(str(stra),str(c))
                                flist.add(f)
                        for m in flist:
                                q=0
                                o=re_queue.lrange(m,0,-1)
                                for p in o:
                                        q += int(p)
                                j=m.split(':')[0]+':'+m.split(':')[2]
                                re_queue.expire(j,60)
                                re_queue.lpush(j,q)
                logging.debug('%s文件已成功转换至缓存redis.'%file)

        cur=conn.cursor()
        cur.execute('delete from vioohDaysum where show_time = %s',(todayTime,))
        members=re_queue.smembers(todayTime)
        for member in members:
                result=re_queue.lrange(str(member)+':'+str(todayTime),0,-1)
                countAll=Counter(result)
                for countNum,screenNum in countAll.items():
                        values=(member,countNum,screenNum,todayTime)
                        query='insert into vioohDaysum(campaign_id,count_num,screen_num,show_time) values(%s,%s,%s,%s)'
                        cur.execute(query,values)
                        conn.commit()
                logging.debug('%s 广告活动报表数据已成功存入数据库' % member)
        cur.close()
        conn.close()

def generate_html():
        result={}
        body=[]
        num=1
        conn=pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb,use_unicode=True,charset='utf8')
        cursor_1=conn.cursor()
        cursor_1.execute('select a.campaign_id,b.customer_name,b.sb_id,b.start,b.end,a.count_num,a.screen_num,FORMAT(b.content_length/1000,0) from vioohDaysum a left join vioohCampaign b on a.campaign_id=b.campaign_id where a.show_time = %s order by a.campaign_id;',(tomorrow,))
        rows=cursor_1.fetchall()
        cursor_1.close()
        conn.close()

        for row in rows:
                conn2=pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb,use_unicode=True,charset='utf8')
                cursor_2=conn2.cursor()
               #cursor_2.execute('select count_num from vioohDaysum where show_time = %s and campaign_id = %s and screen_num = %s',(today,row[0],row[6],))
                cursor_2.execute('select count_num from vioohDaysum where show_time = %s and campaign_id = %s',(today,row[0],))
                rows_2=cursor_2.fetchone()
                cursor_2.close()
                conn2.close()
                if rows_2 is None:
                        result['status']='new'
                elif rows_2[0] == row[5]:
                        result['status']='normal'
                else:
                        result['status']='diff'

                result['campaign_id']=row[0]
                result['customer_name']=row[1]
                result['sb_id']=row[2]
                result['starts_at']=row[3]
                result['ends_at']=row[4]
                result['count']=row[5]
                result['screen']=row[6]
                result['content_length']=row[7]
                result['num']=(num%2)
                body.append(result.copy())
                num=num+1

        env=Environment(loader=FileSystemLoader(cur_path))
        template=env.get_template('template.html')
        with open(send_file,'w+') as fout:
                html_content=template.render(tomorrow=tomorrow,body=body,display_time=display_time)
                fout.write(html_content)
        logging.debug('成功生成明日报表')


def compareSaturation(table_name):
        wrongEmail=''
        sql = 'select campaignID,customerName,sb_id from %s' % table_name
        conn = pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)
        cursor_1 = conn.cursor()
        cursor_1.execute(sql)
        wrongCampaigns = cursor_1.fetchall()
        cursor_1.close()
        conn.close()
        if wrongCampaigns is not None:
                for wrongCampaign in wrongCampaigns:
                        wrongEmail = str(wrongCampaign) + "\r\n" + wrongEmail
        return wrongEmail

def sendRemind(result,title):
        from_addr = '2944329431@qq.com'
        password = 'kqmrxackdcqldgfh'
        to_addr = 'sam.cheng@jcdecaux.com'
        smtp_server = 'smtp.qq.com'
        msg = MIMEText(result,'plain','utf-8')
        msg['From'] = Header(from_addr)
        msg['To'] = Header(to_addr)
        msg['Subject'] = Header('告警！' + title + '放不出来的campaign',charset='utf8')
        server = smtplib.SMTP()
        server.connect(smtp_server,587)
        server.login(from_addr, password)
        server.sendmail(from_addr, to_addr, msg.as_string())
        server.quit()

def main():

        global tomorrow
        global today
        global display_time
        tomorrow=str(datetime.date.today()+datetime.timedelta(days=1))
        today=str(datetime.date.today())
        display_time=datetime.datetime.now().strftime('%Y-%m-%d%H:%M:%S')
        readPlayerId()
        for i in range(redisPool.llen('block_player')):
                savePlayerInfo(redisPool.rpop('block_player'))
        time.sleep(30)
        for playerName in players:
                savePlayerInfo(playerName)
        time.sleep(30)
        for i in range(redisPool.llen('block_player')):
                savePlayerInfo(redisPool.rpop('block_player'))

        d_time2 = datetime.datetime.strptime(str(datetime.datetime.now().date())+'01:30', '%Y-%m-%d%H:%M')
        d_time3 =  datetime.datetime.strptime(str(datetime.datetime.now().date())+'04:30', '%Y-%m-%d%H:%M')
        n_time1 = datetime.datetime.now()
        if n_time1 > d_time2 and n_time1<d_time3:
                dayReportInfo(today)
        else:
                dayReportInfo(tomorrow)

        generate_html()
        sendMail(mailto_list,cc_mail)

        todayCheck = compareSaturation('view_saturation_today_check')
        tomorrowCheck = compareSaturation('view_saturation_tomorrow_check')
        if todayCheck != "":
                sendRemind(todayCheck,'今天')
        if tomorrowCheck != "":
                sendRemind(tomorrowCheck,'明天')

main()

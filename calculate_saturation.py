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


class calculate_saturation():
        def __init__(self,todayTime):
                cur_path = os.path.dirname(os.path.realpath(__file__))
                config_path = os.path.join(cur_path,'config.ini')
                conf = configparser.ConfigParser()
                conf.read(config_path)
                self.todayTime=todayTime

                logging.basicConfig(
                level = logging.DEBUG,
                format = '%(asctime)s:%(levelname)s%(message)s',
                datefmt = '%Y-%m-%d%A%H:%M:%S',
                filename = conf.get('LOG','filename'),
                filemode = conf.get('LOG','filemode')
                )

                myhost = conf.get('MYSQL','myhost')
                myport = int(conf.get('MYSQL','myport'))
                myuser = conf.get('MYSQL','myuser')
                mypasswd = conf.get('MYSQL','mypasswd')
                mydb = conf.get('MYSQL','mydb')
                apiurl = conf.get('PARAS','viooh_url')
                self.filepath = conf.get('PARAS','saturation_dir')
                self.re_queue = redis.Redis(host=conf.get('REDIS','rehost'),port=conf.get('REDIS','report'),db=int(conf.get('REDIS','redb')))
                self.conn=pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)



        def dayReportInfo(self):
                flist=[]
                files=os.listdir(self.filepath.decode('utf-8'))
                for file in files:
                        flist=set()
                        file=file.encode('utf-8')
                        playerId=file[0:-4]
                        with open(self.filepath+file,'r') as fh:
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
                                        self.re_queue.lpush(f,e)
                                        self.re_queue.expire(f,60)
                                        self.re_queue.expire(str(stra),60)
                                        self.re_queue.sadd(str(stra),str(c))
                                        flist.add(f)
                                for m in flist:
                                        q=0
                                        o=self.re_queue.lrange(m,0,-1)
                                        for p in o:
                                                q += int(p)
                                        j=m.split(':')[0]+':'+m.split(':')[2]
                                        self.re_queue.expire(j,60)
                                        self.re_queue.lpush(j,q)
                        logging.debug('%s文件已成功转换至缓存redis.'%file)

                cur=self.conn.cursor()
                cur.execute('delete from vioohDaysum where show_time = %s',(self.todayTime,))
                members=self.re_queue.smembers(self.todayTime)
                for member in members:
                        result=self.re_queue.lrange(str(member)+':'+str(self.todayTime),0,-1)
                        countAll=Counter(result)
                        for countNum,screenNum in countAll.items():
                                values=(member,countNum,screenNum,self.todayTime)
                                query='insert into vioohDaysum(campaign_id,count_num,screen_num,show_time) values(%s,%s,%s,%s)'
                                cur.execute(query,values)
                                self.conn.commit()
                        logging.debug('%s 广告活动报表数据已成功存入数据库' % member)
                cur.close()
                self.conn.close()
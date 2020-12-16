#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
@Author Sam Cheng
@desc this is a script for tomorrow plan of campaigns
@create 2020-09-15
@modify 2020-10-22
'''
import requests
import json
import pymysql
import redis
import re
from xml.dom import minidom
import os
from collections import Counter
import logging
import configparser


class crawlData():

        def __init__(self,playerId):

            self.playerId=playerId

        def savePlayerInfo(self):

            cur_path = os.path.dirname(os.path.realpath(__file__))
            config_path = os.path.join(cur_path,'config.ini')
            conf = configparser.ConfigParser()
            conf.read(config_path)

            logging.basicConfig(
                level = logging.DEBUG,
                format = '%(asctime)s:%(levelname)s%(message)s',
                datefmt = '%Y-%m-%d%A%H:%M:%S',
                filename = cur_path + str(self.playerId),
                filemode = conf.get('LOG','filemode')
            )

            omitList = str(conf.get('PARAS','ignorefile')).split(',')
            myhost = conf.get('MYSQL','myhost')
            myport = int(conf.get('MYSQL','myport'))
            myuser = conf.get('MYSQL','myuser')
            mypasswd = conf.get('MYSQL','mypasswd')
            mydb = conf.get('MYSQL','mydb')
            apiurl = conf.get('PARAS','viooh_url')
            filepath = conf.get('PARAS','saturation_dir')
            redisPool = redis.Redis(host=conf.get('REDIS','rehost'),port=conf.get('REDIS','report'),db=int(conf.get('REDIS','redb')))

            try:
                res = requests.get(apiurl+str(self.playerId),timeout=(5,10))
                if res.status_code == 200:
                    htmlDict = res.json()
                    newChecksum = htmlDict.get('data').get('attributes').get('files').get('saturation.xml').get('checksum')
                    oldChecksum = redisPool.getset(self.playerId,newChecksum)
                    if newChecksum == oldChecksum:
                        logging.debug('%s saturation文件未发生改变，不下载' % self.playerId)
                        pass
                    else:
                        downloadUrl = htmlDict.get('data').get('attributes').get('files').get('saturation.xml').get('path')
                        try:
                            downloadRes = requests.get(downloadUrl,timeout=(5,20))
                            if downloadRes.status_code == 200:
                                localPath = filepath + str(self.playerId) + '.xml'
                                with open(localPath,'wb') as file:
                                    file.write(downloadRes.content)
                                    logging.debug('%s saturation新文件下载成功.' % self.playerId)
                            else:
                                logging.debug('%s viooh-api已阻止saturation文件下载.' % self.playerId)
                                redisPool.set(self.playerId,'ffff')
                                redisPool.lpush('block_player',self.playerId)

                        except Exception as e:
                            logging.error('%s saturation文件下载失败,原因 %s' % (self.playerId,e))

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
                        logging.debug('%s 检查广告活动 %s 结束' % (self.playerId,campaign))
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
                                        cus_name=content.get('campaign').get('name').encode('utf-8').decode('unicode_escape')
                                        sb_num=content.get('campaign').get('booking_engine_id')
                                        starts_at=content.get('campaign').get('starts_at').split('T')[0]
                                        ends_at=content.get('campaign').get('ends_at').split('T')[0]
                                        #bs_id=content.get('campaign').get('broadsign_campaign_ids')[0]
                                        #print(bs_id)
                                        viooh_id=content.get('campaign').get('id')
                                        content_len=list(content.get('frames').values())[0].get('content_length')
                                        query='insert into vioohCampaign(viooh_id,sb_id,customer_name,start,end,content_length) values(%s,%s,%s,%s,%s,%s)'
                                        values=(viooh_id,sb_num,cus_name,starts_at,ends_at,content_len)
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
                        redisPool.lpush('block_player',self.playerId)
                        logging.debug('%s播放机已被阻止网络访问' % self.playerId)
            except Exception as e:
                logging.error('%s播放机无法连接，请检查网络' % self.playerId)
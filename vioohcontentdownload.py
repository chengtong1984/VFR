#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
@Author Sam Cheng
@desc this is a script for downloading content from viooh-api
@create 2020-12-09
@modify 2020-12-09
'''

import requests
import json
import os
import pymysql
import tarfile
import shutil

proxy='proxyrelay.jcd.priv:8000'

ignorefile = ['saturation.xml','meta.json.js','sc.3.0.0.js','sc.2.3.0.js','sc.2.4.0.js','sc.2.5.0.js','petal.json.js','campaign-wide-data.json.js','rulesets.json.js','playlist.json.js']

proxies={
    'http':'http://'+proxy,
    'https':'http://'+proxy,
}

api_url = 'https://content-api.prd.prd.viooh.com.cn/api/v2/players/'

homePath = '/home/apache/viooh/'

myhost = '10.179.245.222'
myport = 3306
myuser = 'STD-MO'
mypasswd = 'STdg123!'
mydb = 'BAB'
mycharset = 'utf8'

players = []

def readPlayerId():
        global players
        conn = pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)
        cursor_1 = conn.cursor()
        cursor_1.execute("SELECT ID FROM view_ip_search WHERE location LIKE '%人民广场-DP双联%' OR location LIKE '%人民广场-站台LED%' OR location LIKE '%徐家汇-DP双联%' OR location LIKE '%徐家汇-站台LED%'")
        rows = cursor_1.fetchall()
        for row in rows:
                player_id=row[0]
                players.append(str(player_id))
        cursor_1.close()
        conn.close()
        return  players

def fetchData(playerID):
    try:
        res=requests.get(api_url + str(playerID),proxies=proxies)
        manifestDict=res.json()
        fileList = manifestDict.get('data').get('attributes').get('files').keys()
        campaignList = list(set(fileList) - set(ignorefile))
        for campaign in campaignList:
            campaignUrl = manifestDict.get('data').get('attributes').get('files').get(campaign).get('path')
            res2=requests.get(campaignUrl,proxies=proxies)
            res2=res2.json()
            contents=res2.get('data').get('attributes').get('files').keys()
            contentList = list(set(contents) - set(ignorefile) )
            if os.path.exists(homePath + campaign + '/assets'):
                pass
            else:
                os.makedirs(homePath + campaign + '/assets')
            for content in contentList:
                contentName = homePath + campaign + '/' + content
                if os.path.exists(contentName):
                    pass
                else:
                    contentPath=res2.get('data').get('attributes').get('files').get(content).get('path')
                    downloadContent=requests.get(contentPath,proxies=proxies)
                    with open(contentName,'wb') as file:
                        file.write(downloadContent.content)
    except requests.exceptions.ConnectionError as e:
        print("Error",e.args)

def makeTarfile(outputfile,sourcedir):
    with tarfile.open(outputfile, "w") as tar:
        tar.add(sourcedir,arcname=os.path.basename(sourcedir))

def deleteHis():
    db_name = []
    deleteFile = []
    list_name = []
    conn = pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)
    cursor_1 = conn.cursor()
    cursor_1.execute("SELECT viooh_id from vioohCampaign WHERE START <= NOW() AND END >= NOW() AND sb_id != '忽略'")
    rows = cursor_1.fetchall()
    for row in rows:
        db_name.append(row[0])
    for file in os.listdir('/home/apache/viooh/'):
        list_name.append(file)
    deleteFiles = list(set(list_name) - set(db_name))
    print(deleteFiles)
    for deleteFile in deleteFiles:
        shutil.rmtree('/home/apache/viooh/' + deleteFile)




if __name__== '__main__':
    deleteHis()
    #readPlayerId()
    #for player in players:
    #    fetchData(player)
    #makeTarfile('/home/apache/viooh.tar',homePath)
    #shutil.copyfile('/home/apache/viooh.tar','/tmp/viooh.tar')



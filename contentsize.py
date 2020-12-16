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
import sqlite3
import time
import paramiko
from flask import Flask
from flask import render_template
from jinja2 import Environment,FileSystemLoader


pkey = '/home/STD-MO/.ssh/id_rsa'
key = paramiko.RSAKey.from_private_key_file(pkey)

lconn = sqlite3.connect('/home/apache/contentsize.db')

proxy='proxyrelay.jcd.priv:8000'

ignorefile = ['saturation.xml','meta.json.js','sc.3.0.0.js','sc.2.3.0.js','sc.2.4.0.js','sc.2.5.0.js','petal.json.js','campaign-wide-data.json.js','rulesets.json.js','playlist.json.js']

proxies={
    'http':'http://'+proxy,
    'https':'http://'+proxy,
}

api_url = 'https://content-api.prd.prd.viooh.com.cn/api/v2/players/'

homePath = '/home/apache/viooh/'

table_name = 'content' + time.strftime("%Y%m%d",time.localtime())

def ssh_check(campaign_path,content_path):
    linux_command = "du -b /var/opt/smartcontent/bsplayer/smartcontent_prodcn/" + campaign_path + '/' + content_path + '*'
    print(linux_command)
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect('10.199.3.47',username = 'STD-MO',pkey = key)
    stdin,stdout,stderr = ssh.exec_command(linux_command)
    value_size = str(stdout.readlines()[0]).split()[0]
    ssh.close()
    return value_size

def dayswitch():
    check_table_exists = "select name from sqlite_master where name = '%s'" % (table_name,)
    create_sql = "CREATE TABLE %s (content varchar(50),player_id int(16),campaign varchar(50),s_size int(32),n_size int(32),stat int(2));" % (table_name,)
    lcursor2 = lconn.cursor()
    if lcursor2.execute(check_table_exists).fetchone() is None:
        lcursor2.execute(create_sql)
    lcursor2.close()
    lconn.commit()


def fetchData(playerID):
    lcursor = lconn.cursor()
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
                player_size = ssh_check(campaign,content)
                contentPath=res2.get('data').get('attributes').get('files').get(content).get('path')
                contentSize=res2.get('data').get('attributes').get('files').get(content).get('size')
                if str(contentSize) == str(player_size):
                    stat_id = 1
                else:
                    stat_id = 0
                query = "select * from %s where content = '%s' and player_id = %d" % (table_name,content,int(playerID))
                sql = "insert into %s (content,player_id,campaign,s_size,n_size,stat) values ('%s',%d,'%s',%d,%d,%d)" % (table_name,content,int(playerID),campaign,int(contentSize),int(player_size),stat_id)
                #print("%s,%d,%s,%d" % (content,int(playerID),campaign,int(contentSize)))
                if lcursor.execute(query).fetchone() is None:
                    lcursor.execute(sql)
                if os.path.exists(contentName):
                    pass
                else:
                    
                    downloadContent=requests.get(contentPath,proxies=proxies)
                    with open(contentName,'wb') as file:
                        file.write(downloadContent.content)
    except requests.exceptions.ConnectionError as e:
        print("Error",e.args)
    lcursor.close()
    lconn.commit()

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

def generate_plan():
    result={}
    body=[]
    num=1
    cursor_1=lconn.cursor()
    cursor_1.execute('select * from %s' % (table_name,))
    rows=cursor_1.fetchall()
    for row in rows:
        result['content_name']=row[0]
        result['player_name']=row[1]
        result['campaign_name']=row[2]
        result['s_size']=row[3]
        result['n_size']=row[4]
        result['stat_id']=row[5]
        body.append(result.copy())
        cursor_1.close()

    env=Environment(loader=FileSystemLoader(os.path.dirname(os.path.realpath(__file__))))
    template=env.get_template('template_size.html')
    with open('/home/apache/contentsize.html','w+') as fout:
        html_content=template.render(body=body)
        fout.write(html_content)
    print('成功生成明日报表')



if __name__== '__main__':
    generate_plan()
    #dayswitch()
    #fetchData('86686')
    #ssh_check()



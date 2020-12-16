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

import crawl_viooh_api

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

def readPlayerId():
        players = []
        conn = pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)
        redisPool = redis.Redis(host=conf.get('REDIS','rehost'),port=conf.get('REDIS','report'),db=int(conf.get('REDIS','redb')))
        cursor_1 = conn.cursor()
        cursor_1.execute("select hostName,playerID from relationalTable where mediaType = 'DP' or mediaType = 'LED'")
        rows = cursor_1.fetchall()
        for row in rows:
                player_id=row[1]
                players.append(str(player_id))
        cursor_1.close()
        conn.close()
        bPlayers=redisPool.lrange('block_player',0,-1)
        players=list(set(players).difference(set(bPlayers)))
        players=bPlayers + players
        return  players

def main():
    readPlayerId()
        
main()


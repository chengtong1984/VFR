#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
@Author Sam Cheng
@desc this is a script for tomorrow plan of campaigns
@create 2020-09-15
@modify 2020-10-22
'''
import pymysql
import os
import logging
import configparser

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


class vfrMySQL:
    def __init__(self):
        self._dbhost = conf.get('MYSQL','myhost')
        self._dbuser = conf.get('MYSQL','myuser')
        self._dbpassword = conf.get('MYSQL','mypasswd')
        self._dbname = mydb = conf.get('MYSQL','mydb')
        self._dbcharset = 'utf8'
        self._dbport = int(conf.get('MYSQL','myport'))
        self._conn = self.connectMySQL()

        if (self._conn):
            self._cursor = self._conn.cursor()
            #self._cursor = self._conn.cursor(cursor=pymysql.cursors.DictCursor)
    
    def connectMySQL(self):
        try:
            conn = pymysql.connect(host=self._dbhost,
                                   user=self._dbuser,
                                   passwd=self._dbpassword,
                                   db=self._dbname,
                                   port=self._dbport,
                                   #cursorclass=pymysql.cursors.DictCursor,
                                   charset=self._dbcharset)
        except Exception as e:
            raise
            #print("数据库连接出错")
            conn = False
        return conn

    def close(self):
        if (self._conn):
            try:
                if (type(self._cursor) == 'object'):
                    self._conn.close()
                if (type(self._conn) == 'object'):
                    self._conn.close()
            except Exception:
                print("关闭数据库连接异常")

    def ExecQueryAll(self,sql,*args):
        """
        执行查询语句
        """
        res = ''
        if (self._conn):
            try:
                self._cursor.execute(sql,args)
                res = self._cursor.fetchall()
            except Exception:
                res = False
                print("查询异常")
            self.close()
        return res

    def ExecQueryOne(self,sql,*args):
        """
        执行查询语句
        """
        res = ''
        if (self._conn):
            try:
                self._cursor.execute(sql,args)
                res = self._cursor.fetchone()
            except Exception:
                res = False
                print("查询异常")
            self.close()
        return res

    def ExecInsert(self,sql,*args):
        """
        执行查询语句
        """
        res = ''
        if (self._conn):
            try:
                self._cursor.execute(sql,args)
                res = self._cursor.fetchone()
            except Exception:
                res = False
                print("查询异常")
            self.close()
        return res

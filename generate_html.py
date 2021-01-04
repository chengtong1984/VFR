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
import time
import os
import logging
import importlib
import configparser

class generate_html():


        def __init__(self):


                self.tomorrow=str(datetime.date.today()+datetime.timedelta(days=1))
                self.today=str(datetime.date.today())
                self.display_time=datetime.datetime.now().strftime('%Y-%m-%d%H:%M:%S')

                self.cur_path = os.path.dirname(os.path.realpath(__file__))
                config_path = os.path.join(self.cur_path,'config.ini')
                conf = configparser.ConfigParser()
                conf.read(config_path)
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
                self.send_file = conf.get('MAIL','send_file_path')
                self.send_tomorrow_warning = conf.get('MAIL','send_tomorrow_warning_path')
                self.send_today_warning = conf.get('MAIL','send_today_warning_path')
                self.send_player_update = conf.get('MAIL','send_player_update_path')
                self.conn=pymysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb,use_unicode=True,charset='utf8')

        def generate_plan(self):
                result={}
                body=[]
                num=1
                cursor_1=self.conn.cursor()
                cursor_1.execute('select a.campaign_id,b.customer_name,b.sb_id,b.start,b.end,a.count_num,a.screen_num,FORMAT(b.content_length/1000,0) from vioohDaysum a left join vioohCampaign b on a.campaign_id=b.campaign_id where a.show_time = %s order by a.campaign_id;',(self.tomorrow,))
                rows=cursor_1.fetchall()

                for row in rows:
                        cursor_2=self.conn.cursor()
                        cursor_2.execute('select count_num from vioohDaysum where show_time = %s and campaign_id = %s',(self.today,row[0],))
                        rows_2=cursor_2.fetchone()
 
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
                        cursor_2.close()

                env=Environment(loader=FileSystemLoader(self.cur_path))
                template=env.get_template('template_plan.html')
                with open(self.send_file,'w+') as fout:
                        html_content=template.render(tomorrow=self.tomorrow,body=body,display_time=self.display_time)
                        fout.write(html_content)
                print('成功生成明日报表')

        
        def generate_tomorrow_warning(self):
                result={}
                body=[]
                num=1
                sql_tomorrow = 'select campaignID,customerName,sb_id from view_saturation_tomorrow_check'
                cursor_1=self.conn.cursor()
                cursor_1.execute(sql_tomorrow)
                rows=cursor_1.fetchall()
                cursor_1.close()

                for row in rows:

                        result['campaign_id']=row[0]
                        result['customer_name']=row[1]
                        result['sb_id']=row[2]
                        result['num']=(num%2)

                        body.append(result.copy())
                        num=num+1

                env=Environment(loader=FileSystemLoader(self.cur_path))
                template=env.get_template('template_warn.html')
                with open(self.send_tomorrow_warning,'w+') as fout:
                        html_content=template.render(tomorrow=self.tomorrow,body=body,display_time=self.display_time)
                        fout.write(html_content)
                print('成功生成明日报警')


        def generate_today_warning(self):
                result={}
                body=[]
                num=1
                sql_tomorrow = 'select campaignID,customerName,sb_id from view_saturation_today_check'
                cursor_2=self.conn.cursor()
                cursor_2.execute(sql_tomorrow)
                rows=cursor_2.fetchall()
                cursor_2.close()

                for row in rows:

                        result['campaign_id']=row[0]
                        result['customer_name']=row[1]
                        result['sb_id']=row[2]
                        result['num']=(num%2)

                        body.append(result.copy())
                        num=num+1

                env=Environment(loader=FileSystemLoader(self.cur_path))
                template=env.get_template('template_warn.html')
                with open(self.send_today_warning,'w+') as fout:
                        html_content=template.render(tomorrow=self.tomorrow,body=body,display_time=self.display_time)
                        fout.write(html_content)
                print('成功生成今日报警')

        def generate_player_update(self):

                result={}
                body=[]
                num=1

                file_name = '/anaconda/git/player_list'
                with open(file_name) as file_obj:
                        for content in file_obj:
                                content = content.strip()

                                sql_player_info = "select PLAYER,LOCATION,computer_model,platform_version from view_ip_search where IP = '%s'" % (content,)
                                print(sql_player_info)
                                cursor_2=self.conn.cursor()
                                cursor_2.execute(sql_player_info)
                                rows=cursor_2.fetchall()
                                print(rows)
               

                                for row in rows:

                                        result['player']=row[0]
                                        result['location']=row[1]
                                        result['computer_model']=row[2]
                                        result['platform_version']=row[3]
                                        result['num']=(num%2)

                                        body.append(result.copy())
                                        num=num+1
 
                cursor_2.close()

                env=Environment(loader=FileSystemLoader(self.cur_path))
                template=env.get_template('template_update.html')
                with open(self.send_player_update,'w+') as fout:
                        html_content=template.render(tomorrow=self.today,body=body,display_time=self.display_time)
                        fout.write(html_content)
                print('成功生成今日升级报告')

        
        #def __del__(self):
                #try:
                #        self.conn.close()
                #except TypeError:
                #        pass

if __name__ == "__main__":
        aaaaa = generate_html()
        aaaaa.generate_player_update()
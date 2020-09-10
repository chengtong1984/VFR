# -*- coding: UTF-8 -*-

from pyecharts import Pie,WordCloud,Bar,Funnel
import pymysql

new_attr = []
new_v1 = []
num_sum = 0

def getData(table):
    global new_attr
    global new_v1
    new_attr = []
    new_v1 = []
    sql = 'select * from %s' % table
    conn = pymysql.connect(host='10.179.245.222',port=3306,user='STD-MO',passwd='STdg123!',db='BAB')
    cursor_1 = conn.cursor()
    cursor_1.execute(sql)
    result = cursor_1.fetchall()
    cursor_1.close()
    conn.close()
    for single in result:
        new_attr.append(single[0])
        new_v1.append(single[1])

def dms_pie():
    getData('view_dms_version')
    pie =Pie("总共%d台" % sum(new_v1))
    pie.add("", new_attr, new_v1, is_label_show=True)
    pie.show_config()
    pie.render('/var/www/html/bab_info/dms_version.html')

def model_bar():
    getData('view_computer_model')
    bar =Bar("总共%d台" % sum(new_v1))
    bar.add("播放机台数", new_attr, new_v1)
    bar.show_config()
    bar.render('/var/www/html/bab_info/computer_model.html')


def media_funnel():
    getData('view_media_type')
    funnel =Funnel("")
    funnel.add("", new_attr, new_v1)
    funnel.show_config()
    funnel.render('/var/www/html/bab_info/media_type.html')

def poweroff_cloud():
    getData('view_daily_poweroff')
    wordcloud =WordCloud(width=1300, height=620)
    wordcloud.add("", new_attr, new_v1, word_size_range=[20, 100],shape='diamond')
    wordcloud.show_config()
    wordcloud.render('/var/www/html/bab_info/daily_poweroff.html')


dms_pie()
model_bar()
media_funnel()
poweroff_cloud()

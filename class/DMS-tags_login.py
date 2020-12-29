# coding=utf-8
#author=sam

import requests
import ssl
import json

def add_tags(mac):
    cookies = ''
    ssl._create_default_https_context = ssl._create_unverified_context
    url = "https://dms-tags.jcdecaux.com/dologin.html"
    headers = {
    "Connection": "keep-alive",
    "Content-Length": "74",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36 Edg/87.0.664.66",
    "Content-Type": "application/x-www-form-urlencoded",
    "Accept": "*/*",
    "Origin": "https://dms-tags.jcdecaux.com",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": "https://dms-tags.jcdecaux.com/login.html?req=/master/overview",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6"
    }
    data = {"httpd_username":"sam.cheng_adm@asia.jcdecaux.org", "httpd_password":"Jcdsh12345"}
    url2="https://dms-tags.jcdecaux.com/api/tagging/v1/entities/mac_address/" + mac
    payload2 = {"$assign":{"tags":[{"allowed_assignment_fields":["not_before","not_after"],"color":"#1cb461","creation_date":{"$date":"2018-05-28T15:07:49.624000"},"description":"No effect until DMS 5.1-5","entity_type":"computer","icon":"glyphicon glyphicon-download-alt","label":"update_now+r","last_updated":{"$date":"2018-06-27T07:20:34.922000"},"name":"update_now","source":"manual","type":"config_delegate","value":"reboot_if_needed"}]}}
    s = requests.post(url,data=data,headers=headers,verify=False,allow_redirects=False)
    cookies = s.cookies.get_dict().get('DMS-tags-session')
    headers2 = {
    "Host": "dms-tags.jcdecaux.com",
    "Connection": "keep-alive",
    "Content-Length": "424",
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36 Edg/87.0.664.66",
    "Content-Type": "application/json",
    "Origin": "https://dms-tags.jcdecaux.com",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
    "Referer": "https://dms-tags.jcdecaux.com/master/entities/prod",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
    "Cookie": "DMS-tags-session=" + str(cookies)
    }
    r = requests.post(url2, data=json.dumps(payload2),headers=headers2,verify=False)

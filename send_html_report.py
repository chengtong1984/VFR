#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
@Author Sam Cheng
@desc this is a script for sending report
@create 2020-09-15
@modify 2020-10-22
'''
import smtplib
from email.mime.text import MIMEText
import configparser
import os

class sendHTMLReport():

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
                me = self.mail_user + "<" + self.mail_user + ">"
                msg = MIMEText(mail_content,_subtype = 'html',_charset = 'utf8')
                msg['Subject'] = self.subject
                msg['From'] = self.mail_user
                msg['To'] = ";".join(self.to_list)
                msg['Cc'] = ";".join(self.to_cc)
                try:
                        self.s = smtplib.SMTP()
                        self.s.connect(self.mail_host,587)
                        self.s.ehlo()
                        self.s.starttls()
                        self.s.login(self.mail_user,self.mail_pass)
                        self.s.sendmail(me,self.to_list+self.to_cc,msg.as_string())
                        self.s.close
                        return True
                except Exception as e:
                        return False
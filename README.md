# VFR version 4.1

provide a short report of the next day viooh campaign broadcast plan.

MO-ITS Support Utility for Viooh Campaign Release Notes
================================================================
July 30, 2020

VFR is a standalone script that performs a detailed scan and report of all players information to assist you with Viooh Campaign Schedule playback count. VFR produces an e-mail that can be sent, viewed and shared by the user.

Note:  No personal information is collected.  


Contents
==========================
- Operating System Support
- VFR Output file
- VFR for Linux Script File
- Root Access Requirement
- 3rd Party software
- VFR Parameter List & Definitions
- How to run VFR from the terminal & command line examples



Operating System Support
==========================
Linux RedHat CentOS Linux 7.6

VFR.py Output File
==========================
VFR Output file include two file.The output logging file can be viewed/opened in text editors.This provides the user the capability of viewing the whole script running process.The output html file can be sent to the users.This provides the report.

VFR for Linux Script File
==========================
VFR for Linux is a standalone script named VioohForecastReport.  The script require additional libraries or tools installed on the system.

Root Access Requirement
==========================
The VFR script verifies that the logged in user is root.

3rd Party software
==========================
python 2.7
redis 3.2.12
mysql 5.6.16





VFR Parameter List & Definitions
==========================
Usage: Parameter is in the config.ini.Please keep config.ini in the same dir.

[MYSQL]

#according to your database 

myhost = 
myport = 
myuser = 
mypasswd = 
mydb = 
mycharset = 

[REDIS]

rehost= 
report= 
redb= 

[MAIL]

mailto_list = []
mail_host = #mail_server_url
mail_user = #mail_account
mail_pass = 
send_file_path = 

[LOG]

filename=VFRrecord.log
filemode=w


[PARAS]

viooh_url = 
saturation_dir = 
ignorefile = saturation.xml,meta.json.js,sc.3.0.0.js,sc.2.3.0.js,sc.2.4.0.js,sc.2.5.0.js


How to run VFR from the terminal
==========================
Note:  The running of the VFR_script may take several minutes.
1.  Open SSH-Terminal
2.  Create two documents. One is for storing the saturation.xml.The other is for storing the report.html file
3.  Create the tables using the createTable.sql
4.  Finish the config.ini.Fill all the blank places.
5.  Put three files under the same document.VioohForecastReport.py,config.ini,template.html
6.  Run the VioohForecastReport.py file using this command:  python ./VioohForcastReport.py
 


script running examples (assume the script is in the current dir)
==========================
nohup python ./VioohForecastReport &




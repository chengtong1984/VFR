import paramiko
import time
import os
import sys
import logging

__author__ = 'sam.cheng'

logger = logging.getLogger()
logger.setLevel(logging.INFO)
rq = time.strftime('%Y%m%d%H%M', time.localtime(time.time()))
log_path = os.path.dirname(os.getcwd()) + '/Logs/'
log_name = log_path + rq + '.log'
logfile = log_name
fh = logging.FileHandler(logfile, mode='w')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
logger.addHandler(fh)

pkey = '/home/STD-MO/.ssh/id_rsa'
key = paramiko.RSAKey.from_private_key_file(pkey)

def test_paramiko_interact(playerIP):
    if(os.system('ping -c 1 -w 1 ' + str(playerIP)) == 0):
        print('OK')
        linux_command = 'sudo DMS-auto-update'
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(str(playerIP),username = 'STD-MO',pkey = key,timeout = 900)
        stdin,stdout,stderr = ssh.exec_command(linux_command)
        value_size = stdout.readlines()
        print(value_size)
        for values in value_size:
            if 'reboot' in values:
                print(values)
                stdin,stdout,stderr = ssh.exec_command('sudo reboot')
                ssh.close()
                logger.info(str(playerIP) + ' ' + str(values)gi)
                return True
            elif 'dl_failed' in values:
                print(values)
                logger.info(str(playerIP) + ' finish updating successfully.')
                ssh.close()
                return False
            elif 'removing' in values:
                print(values)
                logger.info(str(playerIP) + ' finish updating successfully.')
                ssh.close()
                return False
        ssh.close()
        return True
    else:
        print (str(playerIP) + ' connection failed.')
        logger.info(str(playerIP) + ' connection failed.')
        return True

def update_one(playerlist):
    result = True
    while result:
        result=test_paramiko_interact(playerlist)
        time.sleep(10)

def tags_pull(playerIP):
    if(os.system('ping -c 1 -w 1 ' + str(playerIP)) == 0):
        print('OK')
        linux_command = 'sudo DMS-tags pull'
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(str(playerIP),username = 'STD-MO',pkey = key,timeout = 900)
        while True:
            stdin,stdout,stderr = ssh.exec_command(linux_command)
            value_size = stdout.readlines()
            if value_size is None:
                time.sleep(10)
                return True
            else:
                return False

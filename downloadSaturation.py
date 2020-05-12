import requests,json
from multiprocessing.dummy import Pool as ThreadPool
import pymysql

players = []
myhost=''
myport=
myuser=''
mypasswd=''
mydb=''
apiurl=''
filepath=''

redisPool = redis.Redis(host='127.0.0.1',port='6379',db=0)


def readPlayerId():
    conn = py.mysql.connect(host=myhost,port=myport,user=myuser,passwd=mypasswd,db=mydb)
    cursor_1 = conn.cursor() 
    cursor_1.execute('select * from player_info')
    rows = cursor_1.fetchall()
    for row in rows:
        player_id=row[1]
        players.append(player_id)
    cursor_1.close()
    conn.close()
    
def saveSaturationFile(playerId):
    try:
        res = requests.get(apiurl + str(playerId))
    except:
        print('%s disconnected' % playerId)
        return -1
    htmlDict = res.json()
    newChecksum = htmlDict.get('data').get('attributes').get('files').get('saturation.xml').get('checksum')
    oldChecksum = redisPool.getset(playerId,newChecksum)
    if newChecksum == oldChecksum:
        pass
    else:
        downloadUrl = htmlDict.get('data').get('attributes').get('files').get('saturation.xml').get('path')
        downloadRes = requests.get(downloadUrl)
        localPath = filepath + str(playerId) + '.xml'
        with open(localPath,'wb') as file:
            file.write(downloadRes.content)
            
readPlayerId
processPool = ThreadPool(processes=4)
processPool.map(saveSaturationFile,players)
processPool.close()
processPool.join()

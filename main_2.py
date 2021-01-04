import dbconnect
import dms_tags_add
import player_ssh
import time

conn = dbconnect.vfrMySQL()

if __name__ == '__main__':
    file_name = '/anaconda/git/player_list'
    with open(file_name) as file_obj:
        for content in file_obj:
            content = content.strip()
            mac_addr = conn.ExecQueryOne("SELECT MAC FROM view_ip_search WHERE IP = %s",content)
            print(mac_addr)
            mac_addr = str(mac_addr[0])
            dms_tags_add.updatereset(mac_addr)
            time.sleep(10)
            player_ssh.tags_pull(content)
            player_ssh.update_one(content)




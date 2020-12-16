
import pymysql



def modifycampaignID():
        sql1 = 'SELECT sb_id FROM vioohCampaign WHERE campaign_id IS NULL'
        conn = pymysql.connect(host='10.179.245.222',port=3306,user='STD-MO',passwd='STdg123!',db='BAB')
        cursor_1 = conn.cursor()
        cursor_1.execute(sql1)
        nullCampaigns = cursor_1.fetchall()
        if nullCampaigns is not None:
            for nullCampaign in nullCampaigns:
                sql2 = "select campaignID from customerInfo where sbID = '%s'" % nullCampaign
                cursor_1.execute(sql2)
                campaignID = cursor_1.fetchone()
                if campaignID is not None:
                    campaignID = campaignID[0]
                    print(campaignID)
                    sql3 = "update vioohCampaign set campaign_id = '%s' where sb_id = '%s'" % (campaignID,nullCampaign[0],)
                    print(sql3)
                    cursor_1.execute(sql3)
                    conn.commit()
        cursor_1.close()
        conn.close()

if __name__ == '__main__':
    modifycampaignID()
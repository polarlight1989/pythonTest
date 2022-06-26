import urllib.request
import json
from db import SqlLib
import threading
import re
import redis
import time

def saveAdData(adAccountId,adsList):
    url = "https://sino-channel-api-gateway.meetsocial.cn/sino_channel_tiktok/BQOqyonoI0qA/open_api/v1.2/ad/review_info/?advertiser_id={adAccountId}&ad_ids=[{adsList}]".format(
        adAccountId = adAccountId
        ,adsList = ",".join(adsList)
    )
    req = urllib.request.Request(url=url, method='GET')
    response = urllib.request.urlopen(req).read()
    responseJson = json.loads(response)
    if responseJson['data']['ad_review_map']:
        db = SqlLib()
        for value in responseJson['data']['ad_review_map'].values():
            sql = "UPDATE TPGBR_1055 SET ad_audit_data = '{adData}' WHERE ad_id = '{adId}'".format(
                adId = value['id']
                ,adData = json.dumps(value).replace("\\","\\\\").replace("\"","\\\"").replace("'","\\'")
            )
            db.update(sql)
    print("saveAdData success!")
    return

def main():
   
    db = SqlLib()
    adList = db.select("select ad_account_id,ad_id from TPGBR_1055 where ad_audit_data is null order by  ad_account_id asc limit 1000")
    if len(adList) == 0:
        return True
    adAccountId = ""
    adsList = []
    for ad in adList:
        if adAccountId == "":
            adAccountId = ad[0]
        #切换广告账号或满
        if ad[0] != adAccountId or len(adsList) == 100:
            try:
                saveAdData(adAccountId,adsList)
            except Exception as e :
                print(e)
            adsList = []
            adAccountId = ""
        adsList.append(ad[1])
    return False

while True:
    r = True
    try:
        r = main()
    except Exception as e :
        print(e)
    if r:
        print("sleep")
        time.sleep(5)

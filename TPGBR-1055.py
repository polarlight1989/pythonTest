import urllib.request
import json
from db import SqlLib
import threading
import re
import redis

r = redis.StrictRedis(host='192.168.1.19', port=6379, db=0)
bcList = ["6535686934272934922","6965834406267224066","6970990049907195906","6970994103261659137","6986518091131846657"]


def saveAdAccountIds(bcId,page=1):
    dict = {"bc_id":bcId,"page":page}
    url = "https://sino-channel-api-gateway.meetsocial.cn/sino_channel_tiktok/BQOqyonoI0qA/open_api/v1.2/bc/asset/get/?bc_id={bc_id}&asset_type=ADVERTISER&page_size=50&page={page}".format(**dict)
    req = urllib.request.Request(url=url, method='GET')
    response = urllib.request.urlopen(req).read()
    responseJson = json.loads(response)
    # 循环保存
    for item in responseJson['data']['list']:
        db = SqlLib()
        data = {"ad_account_id":str(item['asset_id']),'bc_id':bcId}
        print(data)
        db.insert("tiktok_ad_account",data)
    if responseJson['data']['page_info']['total_page'] > page:
        page += 1
        saveAdAccountIds(bcId,page)
    return
def saveAdList(adAccountData,page=1):
    dict = {"advertiser_id":adAccountData['ad_account_id'],"page":page}
    url = "https://sino-channel-api-gateway.meetsocial.cn/sino_channel_tiktok/BQOqyonoI0qA/open_api/v1.2/ad/get/?advertiser_id={advertiser_id}&page={page}&page_size=50".format(**dict)
    req = urllib.request.Request(url=url, method='GET')
    response = urllib.request.urlopen(req).read()
    responseJson = json.loads(response)
    db = SqlLib()
    #db.update("DELETE FROM TPGBR_1055 WHERE ad_account_id = '{advertiser_id}'".format(advertiser_id = adAccountData['ad_account_id']))
    if responseJson['data']['list']:
        for item in responseJson['data']['list']:
            data = {
                "bc_id" : str(adAccountData['bc_id'])
                ,"ad_id" : str(item['ad_id'])
                ,"ad_account_id" : str(adAccountData['ad_account_id'])
                ,"ad_data" : json.dumps(item).replace("\\","\\\\").replace("\"","\\\"").replace("'","\\'")
            }
            db.update("DELETE FROM TPGBR_1055 WHERE ad_id = '{ad_id}'".format(ad_id = str(item['ad_id'])))
            db.insert("TPGBR_1055",data)
    if responseJson['data']['page_info']['total_page'] > page:
        page += 1
        saveAdList(adAccountData,page)
    return
db = SqlLib()
adAccountList = db.select("SELECT ad_account_id,bc_id FROM tiktok_ad_account ORDER BY ID ASC")
redisKey = "tiktok_skip__"
index = 0
for item in adAccountList:
    index += 1
    print("row:"+str(index))
    if r.get(redisKey+item[0]) != b"1":
        print("pull:"+item[0])
        try:
            saveAdList({"ad_account_id":item[0],"bc_id":item[1]})
            r.set(redisKey+item[0], '1')
        except Exception as e :
            print(e)
    else:
        print("skip:"+item[0])
    
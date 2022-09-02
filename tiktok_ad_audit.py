import urllib.request
import json
from db import SqlLib
import threading
import re
import redis
import time
import json
import pika

r = redis.StrictRedis(host='192.168.1.19', port=6379, db=0)
credentials = pika.PlainCredentials('admin','123456')

connection = pika.BlockingConnection(pika.ConnectionParameters(host='192.168.0.243',credentials=credentials))
channel = connection.channel()# 创建一个AMQP信道 
#声明队列，并设置durable为True，为了避免rabbitMq-server挂掉数据丢失，将durable设为True
queueName = "tiktok_ad_audit_lists"
channel.queue_declare(queueName,durable=True)
channel.queue_bind(queue=queueName,exchange="/",routing_key=queueName)

def mqPush(url,callbackData):
    data = {"url":url,"method":"GET","callback":{"routingKey":queueName,"exchange":"/","data":callbackData}}
    message = json.dumps(data)
    channel.basic_publish(exchange='/', routing_key='httpclient', body=message)
    return

def saveAdData(adAccountId,adsList):
    url = "https://sino-channel-api-gateway.meetsocial.cn/sino_channel_tiktok/BQOqyonoI0qA/open_api/v1.2/ad/review_info/?advertiser_id={adAccountId}&ad_ids=[{adsList}]".format(
        adAccountId = adAccountId
        ,adsList = ",".join(adsList)
    )
    mqPush(url,{})
    return
def callback(channel,method,properties,body):
    responseJson = json.loads(body.decode())["body"]
    try:
        if responseJson["code"]:
            print(responseJson)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return
        t = time.time()
        t1 = time.time() 
        if responseJson['data']['ad_review_map']:
            db = SqlLib()
            for value in responseJson['data']['ad_review_map'].values():
                sql = "UPDATE TPGBR_1055 SET ad_audit_data = '{adData}',has_ad_audit_data=1 WHERE ad_id = '{adId}'".format(
                    adId = value['id']
                    ,adData = json.dumps(value).replace("\\","\\\\").replace("\"","\\\"").replace("'","\\'")
                )
                db.update(sql)
            db.close()
        else:
            channel.basic_ack(delivery_tag=method.delivery_tag)
        print("入库 时间:" + str(time.time() - t1) )
        print(time.time())
        print("saveAdData success!")
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e :
        print(responseJson)
        print(e)
        channel.basic_nack(delivery_tag=method.delivery_tag)
def main(c = 0):
    db = SqlLib()
    p = 200
    start = (c ) * p
    if start > 100000 : 
        return
    print("start")
    sql = "select ad_account_id,ad_id from TPGBR_1055 where has_ad_audit_data = 0 order by  ad_account_id desc limit {start},{end}".format(start=start,end=str(p))
    print(sql)
    adList = db.select(sql)
    if len(adList) == 0:
        main(0)
        return 
    adAccountId = ""
    adsList = []
    for ad in adList:
        if adAccountId == "":
            adAccountId = ad[0]
        #切换广告账号或满
        if ad[0] != adAccountId or len(adsList) == 100:
            try:
                saveAdData(adAccountId,adsList)
                time.sleep(1)
            except Exception as e :
                print(e)
                db.close()
            adsList = []
            adAccountId = ""
        adsList.append(ad[1])
    main(c+1)
#doPull()
#ain(0)
#告诉rabbitmq,用callback来接收消息
channel.basic_consume(queueName,callback,auto_ack = False)
#开始接收信息，并进入阻塞状态，队列里有信息才会调用callback进行处理
channel.start_consuming()
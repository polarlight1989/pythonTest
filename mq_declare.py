import pika
import json
from db import SqlLib
import redis
import re

r = redis.StrictRedis(host='192.168.1.19', port=6379, db=0)
credentials = pika.PlainCredentials('admin','123456')

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))
channel = connection.channel()# 创建一个AMQP信道 
#声明队列，并设置durable为True，为了避免rabbitMq-server挂掉数据丢失，将durable设为True
queueName = "tiktok_ad_lists"
channel.queue_declare(queueName,durable=True)
channel.queue_bind(queue=queueName,exchange="/",routing_key=queueName)


def mqPush(url,callbackData):
    data = {"url":url,"method":"GET","callback":{"routingKey":queueName,"exchange":"/","data":callbackData}}
    message = json.dumps(data)
    channel.basic_publish(exchange='/', routing_key='httpclient', body=message)
    return
 

# 定义一个回调函数来处理消息队列中消息，这里是打印出来
def callback(ch,method,properties,body):
    responseJson = json.loads(body.decode())["body"] 
    adAccountData = json.loads(body.decode())["callBackData"]
     
    
    try:
        db = SqlLib()
        if responseJson["code"] == -1:
            print(responseJson)
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return
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
        if responseJson['data']['page_info']['total_page'] > responseJson['data']['page_info']['page']:
            saveAdList(adAccountData,responseJson['data']['page_info']['page'] + 1)
        db.close()
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as e :
        print(responseJson)
        print(e)
        
        channel.basic_nack(delivery_tag=method.delivery_tag)
def doPull():
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
    return

def saveAdList(adAccountData,page=1):
    dict = {"advertiser_id":adAccountData['ad_account_id'],"page":page}
    url = "https://sino-channel-api-gateway.meetsocial.cn/sino_channel_tiktok/BQOqyonoI0qA/open_api/v1.2/ad/get/?advertiser_id={advertiser_id}&page={page}&page_size=50".format(**dict)
    mqPush(url,adAccountData)
    return 

#doPull()
#告诉rabbitmq,用callback来接收消息
channel.basic_consume(queueName,callback,auto_ack = False)
#开始接收信息，并进入阻塞状态，队列里有信息才会调用callback进行处理
channel.start_consuming()
import pika
import json


credentials = pika.PlainCredentials('admin','123456')

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))
channel = connection.channel()# 创建一个AMQP信道 
#声明队列，并设置durable为True，为了避免rabbitMq-server挂掉数据丢失，将durable设为True
queueName = "tiktok_ad_list"
channel.queue_declare(queueName,durable=True)
channel.queue_bind(queue=queueName,exchange="/",routing_key="queueName")


# 定义一个回调函数来处理消息队列中消息，这里是打印出来
def callback(ch,method,properties,body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(body.decode())
#告诉rabbitmq,用callback来接收消息
channel.basic_consume(queueName,callback,auto_ack = False)
#开始接收信息，并进入阻塞状态，队列里有信息才会调用callback进行处理
channel.start_consuming()
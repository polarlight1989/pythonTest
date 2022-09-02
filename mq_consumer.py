import pika
import json


credentials = pika.PlainCredentials('admin','123456')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))

channel = connection.channel()
#声明消息队列，消息在这个队列中传递，如果不存在，则创建队列
channel.queue_declare(queue='1',durable=True)
# 定义一个回调函数来处理消息队列中消息，这里是打印出来
def callback(ch,method,properties,body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    print(body.decode())
#告诉rabbitmq,用callback来接收消息
channel.basic_consume('1',callback)
#开始接收信息，并进入阻塞状态，队列里有信息才会调用callback进行处理
channel.start_consuming()
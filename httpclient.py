import pika
import json
import urllib.request

credentials = pika.PlainCredentials('admin','123456')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))
queueName = "httpClientQueue2"

channel = connection.channel()
def callback(ch,method,properties,body):
    jsonData = json.loads(body.decode())
    print(jsonData['url'])
    req = urllib.request.Request(url=jsonData['url'], method='GET')
    response = urllib.request.urlopen(req).read()
    responseJson = json.loads(response)
    print(responseJson)
channel.basic_consume(queueName,callback)
channel.start_consuming()
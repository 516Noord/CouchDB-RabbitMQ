import pika
import couchdb
import sys
import json

couchserver = couchdb.Server("http://test:Test@10.1.2.10:5984/")

print('***************************************')
db = couchserver['mdktest']
count = 0
for docid in db.view('_all_docs'):
   i = docid['id']
   print(i)
   dbdoc = db[i]
#   print(dbdoc)
   json_data = json.dumps(dbdoc)
   print(json_data)
   print('***************************************')

   credentials = pika.PlainCredentials('admin', 'admin')
   parameters = pika.ConnectionParameters(credentials=credentials)
   connection = pika.BlockingConnection(pika.ConnectionParameters(host='10.1.2.10', credentials=credentials))
   channel = connection.channel()
   channel.queue_declare(queue='couchdb_queue', durable=True)
   message = json_data

   channel.basic_publish(exchange='',
                 routing_key='couchdb_queue',
                 body=json.dumps(json_data),
                 properties=pika.BasicProperties(
                 delivery_mode = 2,
   ))
   print("[*] Sent message")
   connection.close()

import datetime
import socket
import pika
import couchdb
import sys
import json

# define variables
hostname = '10.1.2.10'
couchserver = couchdb.Server('http://test:Test@10.1.2.10:5984/')
couchdb = couchserver['mdktest']
couchview = '_all_docs'
rabbitmq_server = '10.1.2.10'
rabbitmq_channel = ''
rabbitmq_queue = 'couchdb_doc_queue'
rabbitmq_user = 'admin'
rabbitmq_user_password = 'admin'
credentials = pika.PlainCredentials('admin', 'admin')
logfile = 'log_file'
#debug_status = 'off'
debug_status = 'on'

def logFile(log_entry):
   f_log = open(logfile, 'a')
   f_log.write(str(datetime.datetime.now()) + " " + hostname + " " + log_entry + "\n")
   f_log.close()

def debug_logFile(log_entry):
   if debug_status == 'on':
        f_log = open(logfile, 'a')
        f_log.write(str(datetime.datetime.now()) + " " + hostname + " DEBUG: " + log_entry + "\n")
        f_log.close()
   else:
        logentry = ''

# Obtain all document IDs from CouchDB
def get_all_docs():
   count = 0
   for docid in couchdb.view(couchview):
       i = docid['id']
       #print(i)
       debug_logFile("[*] All Docs - ID: " + str(i))

# Dump all documents from CouchDB
def dump_doc():
   count = 0
   for docid in couchdb.view(couchview):
        couchdb_docid = docid['id']
        debug_logFile("[*] All Docs - ID: " + str(couchdb_docid))
        couchdb_doc = couchdb[couchdb_docid]
        couchdb_doc_json = json.dumps(couchdb_doc)
        debug_logFile("[*] Doc JSON: " + str(couchdb_doc_json))

def evaluate_jsonMessage(message):
   if 'views' in message:
        output = 'couchdb_design_queue'
        debug_logFile("[*] Design doc output: " + str(output))
   else:
        output = 'couchdb_doc_queue'
        debug_logFile("[*] Std doc output: " + str(output))
   return output

# Process any message, based on provided value
def process_message(rabbitmq_queue, message):
   parameters = pika.ConnectionParameters(credentials=credentials)
   connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_server, credentials=credentials))
   channel = connection.channel()
   channel.queue_declare(queue=rabbitmq_queue, durable=True)
   channel.basic_publish(exchange=rabbitmq_channel,
        routing_key=rabbitmq_queue,
        body=json.dumps(message),
        properties=pika.BasicProperties(
        delivery_mode = 2,
        )
   )
   output = ("[*] Message Sent to queue " + rabbitmq_queue)
   debug_logFile(str(output))
   #print(output)
   return output

# Obtain messages, process messages
def produce_messages():
   count = 0
   for docid in couchdb.view(couchview):
        couchdb_docid = docid['id']
        #print(couchdb_docid)
        couchdb_doc = couchdb[couchdb_docid]
        couchdb_doc_json = json.dumps(couchdb_doc)
        debug_logFile("[*] JSON output: " + str(couchdb_doc_json))
        #print(couchdb_doc_json)
        #print('***************************************')
        evaluate_jsonMessage(couchdb_doc_json)
        process_message(evaluate_jsonMessage(couchdb_doc_json), couchdb_doc_json)

#get_all_docs()

#dump_doc()

produce_messages()


# Test message for process_message function
#process_message('{"trainer": "Ash", "name": "Froakie", "gender": "m", "_rev": "1-3f01651aca5d64074967b114e0f37364", "_id": "ac17866fb05670e47d09cb7b8e007329", "type": ["Water"], "owned": "2015-07-11"}')
#evaluate_jsonMessage('{"trainer": "Ash", "name": "Froakie", "gender": "m", "_rev": "1-3f01651aca5d64074967b114e0f37364", "_id": "ac17866fb05670e47d09cb7b8e007329", "type": ["Water"], "owned": "2015-07-11"}')

# Test message for evaluate_json function
#evaluate_jsonMessage('{"_id": "_design/pokemon","_rev": "1-70f06ea20b602ccfd04c98ba0f1e88b7","language": "javascript","views": {"emit_doc": {"map": "function(doc) {emit(null, doc);}"}}}')

import datetime
import socket
import MySQLdb as my
import pika
import json
import sys

#hostname = socket.gethostname()
hostname = '10.1.2.10'
rabbitmq_server = 'localhost'
rabbitmq_channel = ''
rabbitmq_queue = 'couchdb_doc_queue'
mysql_server = 'localhost'
mysql_database = 'python_test'
mysql_table = 'COUCHDB2'
mysql_user = 'test'
mysql_user_password = 'Test'
logfile = 'log_file'
debug_status = 'off'
#debug_status = 'on'

db = my.connect(host=mysql_server,
   user = mysql_user,
   passwd = mysql_user_password,
   db = mysql_database)

connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_server))
channel = connection.channel()
channel.queue_declare(queue=rabbitmq_queue, durable=True)

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

def mySQL_check():
   cursor = db.cursor()
   try:
        cursor.execute('SELECT VERSION()')
        results = cursor.fetchone()
        if results:
           print(results)
           log_entry = '[*] Database Connection OK, version: ' + str(results)
           logFile(log_entry)
           print(log_entry)
           cursor.close()
        else:
           log_entry = '[*] Database Connection NOT ok.'
           logFile(log_entry)
           print(log_entry)
           cursor.close()
   except my.Error, e:
        log_entry = "ERROR %d IN CONNECTION: %s" % (e.args[0], e.args[1])
        print "ERROR %d IN CONNECTION: %s" % (e.args[0], e.args[1])

def mySQL_insert(json_id, sql_data):
   debug_logFile("[*] mySQL function started ")
   debug_logFile("[*] mySQL: " + str(db))
   cursor = db.cursor()
   #logFile("[*] SQL Values: " + str(sql_data))
   #sql = "INSERT INTO mysql_table(trainer, name, gender, _rev, _id, owned) VALUES (%s, %s, %s, %s, %s, %s)"
   sql = "INSERT INTO " + mysql_table + "(_id, _rev, trainer, name, gender, owned) VALUES (%s, %s, %s, %s, %s, %s)"
   debug_logFile("[*] SQL Data: " + str(sql_data))
   debug_logFile("[*] SQL Command: " + str(sql) + "," + str(sql_data))
   #print(sql, sql_data)
   try:
        number_of_rows = cursor.executemany(sql, sql_data)
        #cursor.execute(sql, sql_data)
        #logFile("[*] Single command ")
   except my.Error,e:
        mysql_error = (e[0], e[1])
        print e[0], e[1]
        print(mysql_error)
        logFile("[*] SQL Error: " + str(mysql_error))
        cursor.close()
        db.close()
        sys.exit(2)

   db.commit()
   logFile("[*] SQL Record entered with ID: " + str(json_id))
   #logFile("[*] SQL Record entered.")
   #db.close()

def callback(ch, method, properties, body):
   debug_logFile("[*] Function: callback started")
   debug_logFile("[*] RabbitMQ Queue: " + rabbitmq_queue)
   debug_logFile("[*] Receiving : %r" % json.loads(body))
   parsed_json = json.loads(body)
   debug_logFile("[*] Parsed : %r" % json.loads(parsed_json))
   parsed_json = json.loads(parsed_json)
   json_trainer = parsed_json["trainer"]
   json_name = parsed_json["name"]
   json_gender = parsed_json["gender"]
   json_rev = parsed_json["_rev"]
   json_id = parsed_json["_id"]
   json_type = parsed_json["type"]
   json_owned = parsed_json["owned"]
   print("Id      : " + json_id)
   print("Revision: " + json_rev)
   print("Trainer : " + json_trainer)
   print("Name    : " + json_name)
   print("Gender  : " + json_gender)
   #print("Type    : ",  json_type)
   print("Owned   : " + json_owned)
   print("[*] Message Received")
   print
   #sql_data = (parsed_json['trainer'],parsed_json['name'],parsed_json["gender"],parsed_json["_rev"],parsed_json["_id"],parsed_json["type"],parsed_json["owned"])
   sql_data = [(parsed_json["_id"],parsed_json["_rev"],parsed_json['trainer'],parsed_json['name'],parsed_json["gender"],parsed_json["owned"])]
   #print
   #print(sql_data)
   #logFile("SQL Data to be used")
   debug_logFile("JSON Data: " + str(sql_data))
   debug_logFile("Calling mySQL function")
   #mySQL_insert(sql_data)
   mySQL_insert(json_id, sql_data)
   ch.basic_ack(delivery_tag = method.delivery_tag)


logFile("[*] Open connection to RabbitMQ Server: " + rabbitmq_server)
logFile("[*] Open queue: " + rabbitmq_queue)
logFile("[*] Waiting for messages. To exit press CTRL + C")
channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback,
                      queue=rabbitmq_queue)
channel.start_consuming()




#mySQL_check()
#callback()
#mySQL_insert()

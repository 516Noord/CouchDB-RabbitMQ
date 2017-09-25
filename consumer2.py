import datetime
import socket
import MySQLdb as my
import pika
import json
import sys

hostname = socket.gethostname()
rabbitmq_server = 'localhost'
rabbitmq_channel = ''
rabbitmq_queue = 'couchdb_doc_queue'
rabbitmq_error_queue = 'couchdb_error_queue'
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

# Log function
def logFile(log_entry):
   f_log = open(logfile, 'a')
   f_log.write(str(datetime.datetime.now()) + " " + hostname + " " + log_entry + "\n")
   f_log.close()

# Debug logging, only when debug_status = 'on'
def debug_logFile(log_entry):
   if debug_status == 'on':
        f_log = open(logfile, 'a')
        f_log.write(str(datetime.datetime.now()) + " " + hostname + " DEBUG: " + log_entry + "\n")
        f_log.close()
   else:
        logentry = ''

# Function to check if database is available
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

# Process any message, based on provided value
def process_message(json_id, body):
   connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_server))
   channel = connection.channel()
   channel.queue_declare(queue=rabbitmq_error_queue, durable=True)
   channel.basic_publish(exchange=rabbitmq_channel,
        routing_key=rabbitmq_error_queue,
        body=json.loads(body),
        properties=pika.BasicProperties(
        delivery_mode = 2,
        )
   )
   logFile("[*] Message ID: " + json_id + " sent to queue: " + rabbitmq_error_queue)
   print("[*] Message ID: " + json_id + " sent to queue: " + rabbitmq_error_queue)

# Check if ID already exists in database (function is not used)
def mysql_check_id(json_id):
   sql_check_id = "SELECT _id FROM " + mysql_table + " WHERE _id = '" + json_id + "'"
   #sql_check_id = "SELECT (1) FROM " + mysql_table + " WHERE _id = '" + json_id + "'"
   debug_logFile("[*] SQL Check ID: " + sql_check_id)
   cursor = db.cursor()
   cursor.execute(sql_check_id)
   #check_id_results = cursor.fetchall()
   row_count = cursor.rowcount
   if row_count == 1:
        debug_logFile("[*] Number of rows: " + format(row_count))
        debug_logFile("[*] SQL Record already exists")
        id_exists = 1
        print id_exists
   else:
        debug_logFile("[*] SQL Record does not exist")
        id_exists = 0
        print id_exists
   return(json_id, id_exists)

# Insert record in database
def mySQL_insert(json_id, sql_data, body):
   debug_logFile("[*] SQL function started ")
   debug_logFile("[*] SQL check if key exists ")
   sql_check_id = "SELECT _id FROM " + mysql_table + " WHERE _id = '" + json_id + "'"
   debug_logFile("[*] SQL Check ID: " + sql_check_id)
   cursor = db.cursor()
   cursor.execute(sql_check_id)
   row_count = cursor.rowcount  # check if query returns a row
   if row_count == 0:
        debug_logFile("[*] SQL Record does not exist")
        debug_logFile("[*] SQL: " + str(db))
        cursor = db.cursor()
        debug_logFile("[*] SQL Values: " + str(sql_data))
        sql = "INSERT INTO " + mysql_table + "(_id, _rev, trainer, name, gender, owned) VALUES (%s, %s, %s, %s, %s, %s)"
        debug_logFile("[*] SQL Data: " + str(sql_data))
        debug_logFile("[*] SQL Command: " + str(sql) + "," + str(sql_data))
        try:
           number_of_rows = cursor.executemany(sql, sql_data)
           db.commit()  # Commit record to database
           logFile("[*] SQL Record entered with ID: " + str(json_id))
        except my.Error,e:      # check if mySQL throws an error
           mysql_error = (e[0], e[1])
           print(json_id)
           print e[0]
           print e[0], e[1]
           print(mysql_error)
           logFile("[*] SQL Error: " + str(mysql_error))
           cursor.close()
   else:
        debug_logFile("[*] Number of rows: " + format(row_count))
        debug_logFile("[*] SQL Record already exists")
        process_message(json_id, body)
        #db.close()
        #sys.exit(2)    # Exit program if record already exists
   #db.commit() # Commit record to database
   #logFile("[*] SQL Record entered with ID: " + str(json_id))

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
   mySQL_insert(json_id, sql_data, body)
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

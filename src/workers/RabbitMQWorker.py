import asyncio
import json
from multiprocessing.connection import Connection
import threading
import uuid
import time
from  utils.log import log 
from utils.handleMessage import sendMessage, convertMessage
import traceback
import pika 

from .Worker import Worker
import threading


class RabbitMQWorker(Worker):
    ###############
    # dont edit this part
    ###############
    route_base = "/"
    conn:Connection
    
    string_connection:str
    connection: pika.BlockingConnection
    
    consumeQueue:str
    consumeChannel:pika.adapters.blocking_connection.BlockingChannel
    consumeCompensationQueue:str
    consumeComensationChannel:pika.adapters.blocking_connection.BlockingChannel
    
    produceQueue:str
    produceChannel:pika.adapters.blocking_connection.BlockingChannel
    produceCompensationChannel:pika.adapters.blocking_connection.BlockingChannel
    produceCompensationQueue:str
    def run(self, conn: Connection, config:dict):
        # assign here
        RabbitMQWorker.conn = conn

        #### add your worker initialization code here
        try:
          self.consumeQueue = config.get("consumeQueue", "consume_queue")
          self.consumeCompensationQueue = config.get("consumeCompensationQueue", "consume_compensation_queue")
          self.produceQueue = config.get("produceQueue", "produce_queue")
          self.produceCompensationQueue = config.get("produceCompensationQueue", "produce_compensation_queue")
          self.topicExchange = config.get("topicExchange", "topic_exchange")
          self.connection_string = config.get("connection_string", "amqp://guest:guest@localhost:5672/%2F")
          
          # Initialize RabbitMQ connection
          parameters = pika.URLParameters(config['connection_string'])
          self.connection = pika.BlockingConnection(parameters)
          self.consumeChannel = self.connection.channel()
          self.consumeComensationChannel = self.connection.channel()
          
          # Declare queues
          
          self.consumeChannel.queue_declare(queue=self.consumeQueue, durable=True)
          self.consumeComensationChannel.queue_declare(queue=self.consumeCompensationQueue, durable=True)
            
            
          t1 = threading.Thread(target=self.consumeMessage, args=(self.consumeQueue, ["PreprocessingWorker/prepare_preprocessing/"])).start()
          t2 = threading.Thread(target=self.consumeMessage, args=(self.consumeCompensationQueue, ["DatabaseInteractionWorker/removeContext/", "DatabaseInteractionWorker/removeDocument"])).start()
        
        except Exception as e:
          traceback.print_exc()

          print(e)
          
          log(f"Failed to connect to RabbitMQ: {e}", "error")
          return
        #### until this part
        # start background threads *before* blocking server
        # t3 = threading.Thread(target=self.listen_task, daemon=True).start()

        asyncio.run(self.listen_task())

    def listen_task(self):
        log("RabbitMQWorker is listening for messages...", "info")
        while True:
            try:
                if RabbitMQWorker.conn.poll(1):  # Check for messages with 1 second timeout
                    message = self.conn.recv()
                    dest = [
                        d
                        for d in message["destination"]
                        if d.split("/", 1)[0] == "RabbitMQWorker"
                    ]
                    destSplited = dest[0].split('/')
                    method = destSplited[1]
                    param= destSplited[2]
                    instance_method = getattr(self,method)
                    instance_method(message)
            except EOFError:
                break
            except Exception as e:
              print(e)
              log(f"Listener error: {e}",'error' )
              break

    def sendToOtherWorker(self, destination, messageId: str, data: dict = None) -> None:
      sendMessage(
          conn=RabbitMQWorker.conn,
          destination=destination,
          messageId=messageId,
          status="completed",
          reason="Message sent to other worker successfully.",
          data=data or {}
      )
    ##########################################
    # add your worker methods here
    ##########################################
    def consumeMessage(self, queueName, destination=None):
        try:
            # Each thread should have its own connection
            parameters = pika.URLParameters(self.connection_string)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()

            channel.queue_declare(queue=queueName, durable=True)
            channel.basic_qos(prefetch_count=1)

            print(f"[*] Waiting for messages in queue: {queueName}")

            def callback(ch, method, properties, body):
                print(f"[x] Received message: {body.decode()}")
                self.sendToOtherWorker(
                    destination=destination,
                    messageId=str(uuid.uuid4()),
                    data=convertMessage(body.decode())
                )
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(queue=queueName, on_message_callback=callback)
            channel.start_consuming()

        except Exception as e:
            log(f"Error in consuming from queue {queueName}: {e}", 'error')
            self.connection.close()
            self.sendToOtherWorker(
                destination=["RabbitMQWorker/consumeMessage/"],
                messageId=str(uuid.uuid4()),
                data={"error": str(e)},
                reason="Failed to consume message from RabbitMQ queue."
            )

    def produceMessage(self,data):
        try:
            print(f"[*] Producing message to queue: {self.produceQueue}")
            print(f"[*] Data: {json.dumps(data['data'])}")
            
            # Create a separate connection for producing (don't use self.connection)
            parameters = pika.URLParameters(self.connection_string)
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            
            channel.exchange_declare(exchange=self.produceQueue, durable=True, exchange_type='fanout')
            channel.basic_publish(
                exchange=self.produceQueue,
                routing_key='',
                body=json.dumps(data['data']),
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                )
            )
            print(f"[*] Message sent to queue: {self.produceQueue}")
            
            # Close the connection properly
            channel.close()
            connection.close()
            
        except Exception as e:
            log(f"Error producing message: {e}", 'error')

def main(conn: Connection, config: dict):
    worker = RabbitMQWorker()
    worker.run(conn, config)
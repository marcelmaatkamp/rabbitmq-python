#!/usr/bin/env python
from kombu import Connection, Exchange, Queue
from socket import *  
import time
import json

exchange_name = 'koppelvlak.exchange.anpr.arvoo.soap.inputCapture'
queue_name = 'koppelvlak.queue.anpr.soap.inputCapture.bof'

cs = socket(AF_INET, SOCK_DGRAM)  
cs.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)  
cs.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)  
cs.connect(('172.16.99.255', 5455))

exchange = Exchange(exchange_name, type='fanout', durable=True)
queue = Queue(queue_name, exchange=exchange, key=queue_name)

def process_stream(body, message):
  # print json.dumps(body, indent=4)
  cs.send("{ \"koppelvlak\": { \"exchange\": \""+exchange_name+"\", \"queue\": \"" + queue_name + "\", \"data\": " + json.dumps(body)+" } }\r\n"+'\x00')
  message.ack()

with Connection('amqp://guest:guest@*.*.*.*//') as conn:
  with conn.Consumer(queue, callbacks=[process_stream]) as consumer:
    while True:
      conn.drain_events()

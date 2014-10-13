#!/usr/bin/env python 

from kombu import Connection, Exchange, Queue
from socket import *  
import time
import re
import json

cs = socket(AF_INET, SOCK_DGRAM)  
cs.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)  
cs.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)  
cs.bind(('172.16.99.255', 5455))

exchange = None
queue = None

with Connection('amqp://guest:guest@*.*.*.*//') as conn:
  with conn.Producer(serializer='json') as producer:
    while True:
      data = cs.recv(65535)
      json_data=json.loads(data[:-1]) 
      # print json.dumps(json_data, sort_keys=False,indent=4,separators=(',', ': '))

      if exchange == None:
        exchange = Exchange(json_data['koppelvlak']['exchange'], type='fanout', durable=True)
        queue = Queue(json_data['koppelvlak']['queue'], exchange=exchange, key=json_data['koppelvlak']['queue'])

      producer.publish(
        json_data['koppelvlak']['data'], exchange=json_data['koppelvlak']['exchange'], routing_key='ddiode', declare=[queue]
      )

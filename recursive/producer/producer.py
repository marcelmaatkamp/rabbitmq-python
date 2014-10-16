#!/usr/bin/env python 

from kombu import Connection, Exchange, Queue
from kombu import Producer
from kombu.common import maybe_declare
from kombu.pools import producers
from socket import *
import time
import re
import json
import os

exchangeName = "data"
topdir = '.'
exten = '.xml'

exchange = Exchange('orca', type="fanout")

article = {'title': 'No cellular coverage on the tube for 2012', 'ingress': 'yadda yadda yadda'}

with Connection('amqp://guest:guest@rabbitmq:5672//') as connection:
  channel = connection.channel()
  with Producer(channel, exchange = exchange, serializer="json") as producer:
    while True:
      for dirpath, dirnames, files in os.walk(topdir):
        for name in files:
          if name.lower().endswith(exten):
            filename = os.path.join(dirpath, name)
            file = open( filename )
            data = file.read()
            headers = {"path": dirpath, "name": name}
            producer.publish(
              data, headers=headers, exchange=exchange, routing_key='orca', serializer='json'
            )
            os.remove(filename)

from Library.consumer import Consumer
import requests

import sys
import time

import threading

poll_interval = 0.2

name = sys.argv[1]
topics = sys.argv[2]

topics = topics.split(',')

consumer = Consumer('localhost', 5000, name)

for t in topics:
    consumer.register(t)

print(consumer.name, 'starting...')
consumer.run(30, 0.01, 5,'tests/log/')
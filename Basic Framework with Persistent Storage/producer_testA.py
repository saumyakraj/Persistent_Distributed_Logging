from Library.producer import Producer

import random

import sys
import time

max_timeout = 0.01
name = sys.argv[1]
topics = sys.argv[2]
log_filename = sys.argv[3]

topics = topics.split(',')

log_file = open(log_filename, 'r')

logs = log_file.readlines()

# run using sdk
producer = Producer('localhost', 5000, name)
for t in topics:
    producer.register(t)

# file for logging productions (helpful in verification)
file = open('tests/log/' + producer.name + '.txt', 'w')


print(producer.name, 'Starting...')
for line in logs:
    tokens = line.strip().split("\t")
    topic = tokens.pop()
    message = '\t'.join(tokens)
    while not producer.enqueue(topic, message):
        # failure... 
        # keep on quering producer size
        time.sleep(random.uniform(0, max_timeout))
    file.write(producer.name + ' enqueued ' + message + ' at ' + topic + '\n')
    time.sleep(random.uniform(0, max_timeout))

print(producer.name, 'finished producing!')
    
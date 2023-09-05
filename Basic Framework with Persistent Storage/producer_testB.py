from Library.producer import Producer

import random

import sys
import time

max_timeout = 1.0
max_logs = 50
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
line_count = 0
for line in logs[0:max_logs]:
    tokens = line.strip().split("\t")
    topic = tokens.pop()
    message = '\t'.join(tokens)
    line_count += 1
    while not producer.enqueue_with_name(topic, message):
        # failure... 
        # keep on quering producer size
        count = -1
        while count == -1:
            count = producer.get_count()
            time.sleep(random.uniform(0, max_timeout))
        if count == line_count:
            break
          
    file.write(producer.name + ' enqueued ' + message + ' at ' + topic + '\n')
    time.sleep(random.uniform(0, max_timeout))

print(producer.name, 'finished producing!')
    
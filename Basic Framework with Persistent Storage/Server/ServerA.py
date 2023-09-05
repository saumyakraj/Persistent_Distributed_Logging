from flask import Flask
from flask import request
import threading
from threading import Lock
import traceback

# queue data structures

# queue for each topic
class TopicQueue:
    def __init__(self, topic_name):
        # each topic queue has its own lock to ensure broker ordering
        self.lock = Lock()
        self.messages = []
        self.topic_name = topic_name

# stores producer information
class Producers:
    def __init__(self):
        self.count = 0       # count for assigning producer_id
        self.lock = Lock()   # lock for getting producer_id
        self.topics = dict() # stores topic: producer_id

class Consumers:
    def __init__(self):
        self.count = 0          # count for assigning consumer_id
        self.lock = Lock()      # lock for getting consumer_id
        self.topics = dict()    # stores topic: consumer_id
        self.offsets = dict()   # stores message offset for each consumer_id

app = Flask(__name__)

# debugging functions
def print_thread_id():
    print('Request handled by worker thread:', threading.get_native_id())

def return_message(status:str, message=None):
    content = dict()
    content['status'] = status
    if message is not None:
        content['message'] = message
    return content

# functions for handelling each endpoint

@app.route('/topics', methods=['POST'])
def topic_register_request():
    print_thread_id()
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')
    
    topic_name = None
    try:
        receive = request.json
        topic_name = receive['topic_name']
    except:
        return return_message('failure', 'Error While Parsing json')
    
            
    # lock the queues, we don't want to return the wrong status
    # amd perform an unecessary insert
    global queues
    global queues_lock
    with queues_lock:
        if topic_name not in queues:
            queues[topic_name] = TopicQueue(topic_name)
            return return_message('success', 'topic ' + topic_name + ' created sucessfully')
        else: 
            return return_message('failure', 'Topic already exists')
    
    # this is not  dead code, this can return if there's an exception in queues_lock
    return return_message('failure', 'Error while aquiring queue lock')

@app.route('/topics', methods=['GET'])
def topic_get_request():
    print_thread_id()
    topics = []
    try:
        global queues  
        # no need to lock queues, topics can't be deleted
        for key in queues:
            topics.append(key)
        return return_message('success', topics)
    except: 
       return return_message('failure', 'Error while listing topics')
        
@app.route('/producer/register',methods=['POST'])
def producer_register_request():
    print_thread_id()
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')
    
    topic = None
    try:
        receive = request.json
        topic = receive['topic']
    except:
        return return_message('failure', 'error while parsing request')

        # topic can't be deleted, no need to lock queues
    if topic not in queues:
        return return_message('failure', 'Topic not found')
    
    global producers
    new_id = -1
    with producers.lock:
        new_id = producers.count
        producers.count += 1
        producers.topics[new_id] = topic
    
    if new_id == -1:
        return return_message('failure', 'Can not assign new id')
    return {
        "status": "success",
        "producer_id": new_id
    }


@app.route('/consumer/register', methods=['POST'])
def consumer_register_request():
    print_thread_id()
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')
    
    topic = None
    try:
        receive = request.json
        topic = receive['topic']
    except:
        return return_message('failure', 'error while parsing request')

    global consumers
    new_id = -1
    with consumers.lock:
        new_id = consumers.count
        consumers.count += 1
        consumers.topics[new_id] = topic
        # maintain a seperate lock for each consumer offset
        consumers.offsets[new_id] = [0, Lock()]
    
    if new_id == -1:
        return return_message('failure', 'Can not assign new id')
    
    return {
        "status": "success",
        "consumer_id": new_id
    }


@app.route('/producer/produce',methods=['POST'])
def producer_enqueue():
    print_thread_id()
    content_type = request.headers.get('Content-Type')
    if content_type != 'application/json':
        return return_message('failure', 'Content-Type not supported')
    
    topic = None
    producer_id = None
    message = None
    try:
        receive = request.json
        topic = receive['topic']
        producer_id = receive['producer_id']
        message = receive['message']
    except:
        return return_message('failure', 'error while parsing request')
    
    global producers
    if producer_id not in producers.topics:
        return return_message('failure', 'producer_id does not exist')
    
    if producers.topics[producer_id] != topic:
        return return_message('failure', 'topic does not match for given producer_id')
    
    # lock queue for requested topic
    with queues[topic].lock:
        queues[topic].messages.append(message)
    
    return return_message('success')

@app.route('/consumer/consume',methods=['GET'])
def consumer_dequeue():
    print_thread_id()   
    try:
        topic = request.args.get('topic')
        consumer_id = request.args.get('consumer_id')
        consumer_id = int(consumer_id)
    except:
        return return_message('failure', 'error while parsing request')
        
    global consumers
    if consumer_id not in consumers.topics:
        return return_message('failure', 'consumer_id does not exist')
    if consumers.topics[consumer_id] != topic:
        return return_message('failure', 'topic does not match for given consumer_id')
    
    # retreive message
    message = None
    with consumers.offsets[consumer_id][1]:
        try:
            message = queues[topic].messages[consumers.offsets[consumer_id][0]]
            consumers.offsets[consumer_id][0] += 1
        except:
            return return_message('failure', 'no more logs')
        
    return {
        "status": "success",
        "message": message
    }

@app.route('/size',methods=['GET'])
def consumer_size():
    print_thread_id()   
    try:
        topic = request.args.get('topic')
        consumer_id = request.args.get('consumer_id')
        consumer_id = int(consumer_id)
    except:
        return return_message('failure', 'error while parsing request')
        
    global consumers
    if consumer_id not in consumers.topics:
        return return_message('failure', 'consumer_id does not exist')
    if consumers.topics[consumer_id] != topic:
        return return_message('failure', 'topic does not match for given consumer_id')
    
    messages_left = 0
    try:
        messages_left = len(queues[topic].messages) - consumers.offsets[consumer_id][0]
    except:
        return return_message('failure', 'an error occured')
    return{
        "status": "success",
        "size": messages_left
    }
            

if __name__ == "__main__":
    # queuing data structures
    queues_lock = Lock()
    queues = dict()

    # producer data structures
    producers = Producers()
    consumers = Consumers()

    app.run(debug=True, threaded=True, processes=1)
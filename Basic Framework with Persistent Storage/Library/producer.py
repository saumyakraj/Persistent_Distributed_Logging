from typing import Dict
import requests
import json
import traceback
import sys

class Producer:
    def __init__(self, host: str, port: int, name: str = '') -> None:
        self.hostname: str = 'http://' + host + ':' + str(port) + '/'
        self.ids : Dict[str, int] = {}
        self.name: str = name

    def eprint(self, *args, **kwargs):
        print(self.name, *args, file=sys.stderr, **kwargs)
    
    def register(self, topic: str) -> int:
        if topic in self.ids:
            self.eprint('already registered for the topic', topic)
            return -1
        try:
            res = requests.post(self.hostname + 'producer/register', json={"topic":topic})
            if res.ok:
                try:
                    response = res.json()
                    if response['status'] == 'success':
                        self.ids[topic] = response['producer_id']
                    else: self.erint(response)
                except :
                    self.eprint('Invalid Response:', res.text)
            else:
                self.eprint('received unexpected responce code', res.status_code)
        except:
            self.eprint('Error Can not make a post request')
        return -1
            

    def enqueue(self, topic: str, message: str) -> bool:
        if topic not in self.ids:
            self.eprint
            return False
        
        prod_id = self.ids[topic]
        
        try:
            res = requests.post(self.hostname + 'producer/produce', json={"producer_id": prod_id, "topic": topic,"message": message})
            if res.ok:
                try:
                    response = res.json()
                    if response['status'] == 'success':
                        return True  
                    else: self.eprint(response)
                except:
                    self.eprint('Invalid response:', res.text)
            else:
                self.eprint('received unexpected responce code', res.status_code)
        except:
            self.eprint('Can not make a post request')
        return False
    
    def enqueue_with_name(self, topic: str, message: str) -> bool:
        if topic not in self.ids:
            self.eprint
            return False
        
        prod_id = self.ids[topic]
        
        try:
            res = requests.post(self.hostname + 'producer/produce', json={"producer_id": prod_id, "topic": topic,"message": message, "prod_client": self.name})
            if res.ok:
                try:
                    response = res.json()
                    if response['status'] == 'success':
                        return True  
                    else: self.eprint(response)
                except:
                    self.eprint('Invalid response:', res.text)
            else:
                self.eprint('received unexpected responce code', res.status_code)
        except:
            self.eprint('Can not make a post request')
        return False
    
    def get_count(self) -> int:
        try:
            res = requests.get(self.hostname + 'producer/client_size', params={"prod_client":self.name})
            if res.ok:
                try:
                    response = res.json()
                    if response['status'] == 'success':
                        return response['count']
                except:
                    self.eprint('Invalid response:', res.text)
            else:
                self.eprint('received unexpected responce code', res.status_code)
        except:
            self.eprint('Can not make a post request')
        return -1
            

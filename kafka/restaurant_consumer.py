import sys
import csv
import os
import json
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient


TOPIC_NAME= 'restaurent-take-away-data'
API_KEY = 'VAORODEYH3FZ6EZI'
ENDPOINT_SCHEMA_URL  = 'https://psrc-q8qx7.us-central1.gcp.confluent.cloud'
API_SECRET_KEY = 'oSB+WEPznmcw6Hi+F9tr+QOI1wAV7fe5zGXGkxESVZiNDF6SxWKoUfUgcK96ILca'
BOOTSTRAP_SERVER = 'pkc-lzvrd.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'C7ORYTTYSJSKAAAN'
SCHEMA_REGISTRY_API_SECRET = '8RNhPME834hzhVv+u448eHS/gtRD2Nkngp2yLqoIQw/GbIgsUMUW51zhg1P9aZfH'
SCHEMA_ID = 100002


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf

def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }
    
class CSVWriter():

    filename = None
    fp = None
    writer = None

    def __init__(self, filename):
        self.filename = filename
        self.fp = open(self.filename, 'w', encoding='utf8')
        
        self.writer = csv.writer(self.fp, delimiter=',', lineterminator='\n')

    def close(self):
        self.fp.close()

    def write(self, elems):
        self.writer.writerow(elems)
    def truncate(self):
        self.fp.truncate(0)
    def size(self):
        return os.path.getsize(self.filename)

    def fname(self):
        return self.filename


class Restaurant:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
    
    def restaurant_to_dict(self):
        return self.record
    
    @staticmethod
    def dict_to_restaurant(data:dict,ctx):
        return Restaurant(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema_obj = schema_registry_client.get_schema(SCHEMA_ID)
    schema_str = schema_obj.schema_str
    
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Restaurant.dict_to_restaurant)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'mygroup',
                     'auto.offset.reset': "earliest"
                     })

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])

    i=0
    mycsv = CSVWriter('restaurant_orders.csv')
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(2.0)
            if msg is None:
                print('continue', i)
                continue
            i=i+1
            
            rest = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            
            if rest is not None:
                
                mycsv.write(rest.restaurant_to_dict().values())
                
                print("User record {}: rest: {}\n"
                      .format(msg.key(), rest))
            print('i', i)
            if(i == 10) :
                break
        except KeyboardInterrupt:
            break      
    consumer.close()
    mycsv.close()
    print("Written %d bytes to %s" % (mycsv.size(), mycsv.fname()))
main(TOPIC_NAME)
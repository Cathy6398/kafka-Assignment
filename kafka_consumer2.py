import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient


API_KEY = 'FHYBE5JNLZANSCOM'
ENDPOINT_SCHEMA_URL  = 'https://psrc-ldg31.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'h1hFdlEzSYJXocw5D7aoVdz3wagz5PxxmH+DcJA1yUGLW1WuNXpqKnppE6wRaplE'
BOOTSTRAP_SERVER = 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'HYU74JIYXUR2LLCP'
SCHEMA_REGISTRY_API_SECRET = 'hPvjfemQjOfKqnyhvaOWPHzFHjOlwHwsWvbEacJ9sL5uylsbWqZe/5/CpAkzWS5p'


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


class Restaurant:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_restaurant(data:dict,ctx):
        return Restaurant(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):

   count=0
   schema_registry_conf = schema_config()
   schema_registry_client= SchemaRegistryClient(schema_registry_conf)
   my_schema = schema_registry_client.get_latest_version(topic + '-value').schema.schema_str
   json_deserializer = JSONDeserializer(my_schema,
                                         from_dict=Restaurant.dict_to_restaurant)
   consumer_conf = sasl_conf()
   consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})
   consumer = Consumer(consumer_conf)
   consumer.subscribe([topic])
   while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            restaurant = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if restaurant is not None:
                count+=1
                print("User record {}: restaurant: {}\n"
                      .format(msg.key(), restaurant))
                print(f"{count} message consumed")
        except KeyboardInterrupt:
            break
        consumer.close()

main("restaurant-take-away-data")

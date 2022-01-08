from uuid import uuid4

from confluent_kafka import Producer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

def kafka_producer_user(username, user_id):

    class User(object):
        def __init__(self, username, user_identifier):
            self.username = username
            self.user_identifier = user_identifier

    def user_to_dict(user, _):
        return dict(username=user.username, user_identifier=user.user_identifier)

    topic = 'user_creation'
    schema = """
            {
              "$schema": "http://json-schema.org/draft-07/schema#",
              "title": "User",
              "description": "User creation",
              "type": "object",
              "properties": {
                "username": {
                  "description": "User's name",
                  "type": "string"
                },
                "user_identifier": {
                  "description": "User's Discord id",
                  "type": "str"
                }
              },
              "required": [ "username", "user_identifier" ]
            }
            """
    schema_registry_conf = {'url': ''}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    json_serializer = JSONSerializer(schema, schema_registry_client, user_to_dict)

    producer_conf = {'bootstrap.servers': 'kafka:9092',
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': json_serializer}

    producer = SerializingProducer(producer_conf)
    print("Producing user records to topic {}. ^C to exit.".format(topic))

    user = User(username=username, user_identifier=user_id)
    producer.produce(topic=topic, key=str(uuid4()), value=user,
                     on_delivery=delivery_report)

    print("\nFlushing records...")
    producer.flush()

from confluent_kafka import SerializingProducer
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

def step_one_user_creation(username, user_id, transaction_id):
    class User(object):
        def __init__(self, username, user_identifier):
            self.username = username
            self.user_identifier = user_identifier

    def user_to_dict(user, _):
        return dict(username=user.username, user_identifier=user.user_identifier)

    topic = 'user_creation'
    schema = """{
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
      "type": "string"
    }
  },
  "required": [
    "username",
    "user_identifier"
  ]
}
"""
    schema_registry_conf = {'url': 'http://schema-registry:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    json_serializer = JSONSerializer(schema, schema_registry_client, user_to_dict)

    producer_conf = {'bootstrap.servers': 'broker:29092',
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': json_serializer}

    producer = SerializingProducer(producer_conf)
    print("Producing user records to topic {}.".format(topic))

    user = User(username=username, user_identifier=user_id)
    producer.produce(topic=topic, key=transaction_id, value=user,
                     on_delivery=delivery_report)

    print("\nFlushing records...")
    producer.flush()


def step_three_parsing_csv(bet_data_list, transaction_id):
    class BetData(object):
        def __init__(self, bet_data_list):
            self.bet_data_list = bet_data_list

    def file_to_dict(betting, _):
        return dict(bet_data_list=betting.bet_data_list)

    topic = 'parsing_csv'
    schema = """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Parsing",
  "description": "Trasformation of a bet data list in a CSV file",
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "date": {
        "type": "string"
      },
      "match": {
        "type": "string"
      },
      "one": {
        "type": "string"
      },
      "ics": {
        "type": "string"
      },
      "two": {
        "type": "string"
      },
      "gol": {
        "type": "string"
      },
      "over": {
        "type": "string"
      },
      "under": {
        "type": "string"
      }
    }
  }
}
"""
    schema_registry_conf = {'url': 'http://schema-registry:8081'}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    json_serializer = JSONSerializer(schema, schema_registry_client, file_to_dict)

    producer_conf = {'bootstrap.servers': 'broker:29092',
                     'key.serializer': StringSerializer('utf_8'),
                     'value.serializer': json_serializer}

    producer = SerializingProducer(producer_conf)
    print("Producing user records to topic {}.".format(topic))

    bet_data_list = BetData(bet_data_list=bet_data_list)
    producer.produce(topic=topic, key=transaction_id, value=bet_data_list,
                     on_delivery=delivery_report)

    print("\nFlushing records...")
    producer.flush()

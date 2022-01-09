import concurrent.futures
import threading

from confluent_kafka import Consumer, DeserializingConsumer, Producer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer

from BetData.kafka_producers import step_one_user_creation


def step_one_user_creation_reply(transaction_id):
    class User(object):
        def __init__(self, username, user_identifier):
            self.username = username
            self.user_identifier = user_identifier

    def dict_to_user(obj, ctx):
        """
        Converts object literal(dict) to a User instance.
        Args:
            ctx (SerializationContext): Metadata pertaining to the serialization
                operation.
            obj (dict): Object literal(dict)
        """
        if obj is None:
            return None

        return User(username=obj['username'],
                    user_identifier=obj['user_identifier'])

    topic = 'user_creation_reply'

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
    json_deserializer = JSONDeserializer(schema,
                                         from_dict=dict_to_user)
    string_deserializer = StringDeserializer('utf_8')

    consumer_conf = {'bootstrap.servers': 'broker:29092',
                     'key.deserializer': string_deserializer,
                     'value.deserializer': json_deserializer,
                     'group.id': 'mygroup',
                     'auto.offset.reset': "earliest",
                     'enable.auto.commit': False}

    consumer = DeserializingConsumer(consumer_conf)
    consumer.subscribe([topic])

    res_fut = concurrent.futures.Future()

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            user: User = msg.value()
            if user is not None:
                if msg.key() != transaction_id:
                    threading.Thread(target=step_one_user_creation, args=[user.username, user.user_identifier, msg.key()]).start()
                    continue
                return res_fut.set_result(msg.value())
        except KeyboardInterrupt:
            break

    consumer.close()
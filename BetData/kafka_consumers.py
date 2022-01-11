import concurrent.futures
import threading

from confluent_kafka import DeserializingConsumer, Consumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer


class KafkaBetdataStepOneConsumer:
    instance = None

    @staticmethod
    def current_instance():
        if KafkaBetdataStepOneConsumer.instance is None:
            instance = KafkaBetdataStepOneConsumer()
            KafkaBetdataStepOneConsumer.instance = instance
        return KafkaBetdataStepOneConsumer.instance

    class User(object):
        def __init__(self, username, user_identifier):
            self.username = username
            self.user_identifier = user_identifier

    def __init__(self):
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

            return KafkaBetdataStepOneConsumer.User(username=obj['username'],
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
        self._consumer = consumer
        self._consumer_running_state = False
        self._kafka_consumer_futs = {}

    def _consume_data(self):
        self._consumer_running_state = True
        while True:
            if len(self._kafka_consumer_futs) == 0:
                break
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue

                user: KafkaBetdataStepOneConsumer.User = msg.value()
                if user is not None:
                    if self._kafka_consumer_futs.get(msg.key()) is None:
                        print('An incident has happened')
                        self._consumer.commit(msg)
                        continue
                    if not self._kafka_consumer_futs[msg.key()].set_running_or_notify_cancel():
                        print(f'Future canceled: key: {msg.key()}')
                        self._consumer.commit(msg)
                        del self._kafka_consumer_futs[msg.key()]
                        continue
                    try:
                        result = self._kafka_consumer_futs[msg.key()][1](msg)
                    except Exception as ex:
                        self._kafka_consumer_futs[msg.key()][0].set_exception(ex)
                        self._consumer.commit(msg)
                        del self._kafka_consumer_futs[msg.key()]
                        continue
                    self._kafka_consumer_futs[msg.key()].set_result(result)
                    self._consumer.commit(msg)
                    del self._kafka_consumer_futs[msg.key()]
                self._consumer.commit(msg)
            except KeyboardInterrupt:
                break
        self._consumer_running_state = False


    def fetch(self, transaction_id, cb):
        if cons_fut := self._kafka_consumer_futs.get(transaction_id) is not None:
            del self._kafka_consumer_futs[transaction_id]
            return cons_fut[0]
        else:
            fut = concurrent.futures.Future()
            self._kafka_consumer_futs[transaction_id] = [fut, cb]
            if not self._consumer_running_state:
                threading.Thread(target=self._consume_data).start()
            return fut

class KafkaBetdataStepThreeConsumer:
    instance = None

    @staticmethod
    def current_instance():
        if KafkaBetdataStepThreeConsumer.instance is None:
            KafkaBetdataStepThreeConsumer.instance = KafkaBetdataStepThreeConsumer()
        return KafkaBetdataStepThreeConsumer.instance

    class BetData(object):
        def __init__(self, bet_data_list):
            self.bet_data_list = bet_data_list

    def __init__(self):
        topic = 'parsing_csv_reply'

        consumer_conf = {'bootstrap.servers': 'broker:29092',
                         'group.id': 'mygroup',
                         'auto.offset.reset': "earliest",
                         'enable.auto.commit': False}

        consumer = Consumer(consumer_conf)
        consumer.subscribe([topic])
        self._consumer = consumer
        self._running_state = False
        self._kafKa_consumer_futs = {}


    def _consume_data(self):
        self._consumer_running_state = True
        while True:
            if len(self._kafKa_consumer_futs) == 0:
                break
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue

                csv = msg.value()
                if csv is not None:
                    if self._kafKa_consumer_futs.get(msg.key()) is None:
                        print('An incident has happened')
                        self._consumer.commit(msg)
                        del self._kafKa_consumer_futs[msg.key()]
                    if not self._kafKa_consumer_futs[msg.key()].set_running_or_notify_cancel():
                        print(f'Future canceled: key: {msg.key()}')
                        self._consumer.commit(msg)
                        del self._kafKa_consumer_futs[msg.key()]
                        continue
                    try:
                        result = self._kafKa_consumer_futs[msg.key()][1](msg)
                    except Exception as ex:
                        self._kafKa_consumer_futs[msg.key()][0].set_exception(ex)
                        self._consumer.commit(msg)
                        del self._kafKa_consumer_futs[msg.key()]
                        continue
                    self._kafKa_consumer_futs[msg.key()].set_result(result)
                    self._consumer.commit(msg)
                    del self._kafKa_consumer_futs[msg.key()]
                self._consumer.commit(msg)
            except KeyboardInterrupt:
                break
        self._consumer_running_state = False

def step_three_parsing_csv_reply(transaction_id):
    class BetData(object):
        def __init__(self, bet_data_list):
            self.bet_data_list = bet_data_list

    def dict_to_file(obj, ctx):
        """
        Converts object literal(dict) to a User instance.
        Args:
            ctx (SerializationContext): Metadata pertaining to the serialization
                operation.
            obj (dict): Object literal(dict)
        """
        if obj is None:
            return None

        return BetData(bet_data_list=obj)

    topic = 'parsing_csv_reply'

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
    json_deserializer = JSONDeserializer(schema,
                                         from_dict=dict_to_file)
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
    yield res_fut
    yield consumer

    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            bet_data: BetData = msg.value()
            if bet_data is not None:
                if msg.key() != transaction_id:
                    consumer.commit(msg)
                    threading.Thread(target=step_three_parsing_csv_reply,
                                     args=[bet_data.bet_data_list, msg.key()]).start()
                    continue
                return res_fut.set_result(msg)
            consumer.commit(msg)
        except KeyboardInterrupt:
            break

    consumer.close()
import asyncio
import threading
from abc import ABC, abstractmethod

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer


class GenericProducer(ABC):
    bootstrap_servers = 'broker:29092'
    schema_registry_conf = {'url': 'http://schema-registry:8081'}

    @abstractmethod
    def model_to_dict(self, obj, ctx):
        ...

    @property
    @abstractmethod
    def schema(self):
        ...

    @abstractmethod
    def produce(self, id, value):
        ...

    def _produce_data(self):
        while not self._cancelled:
            self._producer.poll(0.1)

    def produce_data(self):
        if not self._polling_thread.is_alive():
            self._polling_thread.start()

    def close(self):
        self._cancelled = True
        self._polling_thread.join()

    def reset_state(self):
        self._cancelled = False

    def __init__(self, loop=None):
        schema_registry_client = SchemaRegistryClient(self.schema_registry_conf)

        json_serializer = JSONSerializer(self.schema, schema_registry_client, to_dict=self.model_to_dict)

        producer_conf = {'bootstrap.servers': self.bootstrap_servers,
                         'key.serializer': StringSerializer('utf_8'),
                         'value.serializer': json_serializer
                         }
        self._loop = loop or asyncio.get_event_loop()
        self._producer = SerializingProducer(producer_conf)
        self._polling_thread = threading.Thread(target=self._produce_data)
        self._cancelled = False

# TODO: to be moved to the BOT module

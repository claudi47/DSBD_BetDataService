import asyncio
import datetime
import logging
import pytz
import threading
import traceback
from abc import ABC, abstractmethod
from confluent_kafka import DeserializingConsumer, Consumer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer

import app.db_utils.advanced_scheduler as scheduling
import app.db_utils.mongo_utils as database
from app.models import SearchDataPartialInDb, BetDataListUpdateInDb, PyObjectId


class GenericConsumer(ABC):
    bootstrap_servers = 'broker:29092'

    @property
    @abstractmethod
    def group_id(self):
        ...

    @property
    @abstractmethod
    def auto_offset_reset(self):
        ...

    @property
    @abstractmethod
    def auto_commit(self):
        ...

    @property
    @abstractmethod
    def topic(self):
        ...

    @property
    @abstractmethod
    def schema(self):
        ...

    @abstractmethod
    def dict_to_model(self, map, ctx):
        ...

    def close(self):
        self._cancelled = True
        self._polling_thread.join()

    def consume_data(self):
        if not self._polling_thread.is_alive():
            self._polling_thread.start()

    @abstractmethod
    def _consume_data(self):
        ...

    def reset_state(self):
        self._cancelled = False

    def __init__(self, loop=None, normal=False):
        if not normal:
            json_deserializer = JSONDeserializer(self.schema,
                                                 from_dict=self.dict_to_model)
            string_deserializer = StringDeserializer('utf_8')

            consumer_conf = {'bootstrap.servers': self.bootstrap_servers,
                             'key.deserializer': string_deserializer,
                             'value.deserializer': json_deserializer,
                             'group.id': self.group_id,
                             'auto.offset.reset': self.auto_offset_reset,
                             'enable.auto.commit': self.auto_commit,
                             'allow.auto.create.topics': True}
            self._consumer = DeserializingConsumer(consumer_conf)
        else:
            consumer_conf = {'bootstrap.servers': self.bootstrap_servers,
                             'group.id': self.group_id,
                             'auto.offset.reset': self.auto_offset_reset,
                             'enable.auto.commit': self.auto_commit,
                             'allow.auto.create.topics': True}
            self._consumer = Consumer(consumer_conf)
        self._loop = loop or asyncio.get_event_loop()
        self._cancelled = False
        self._consumer.subscribe([self.topic])
        self._polling_thread = threading.Thread(target=self._consume_data)


search_betdata_sync_lock = threading.Lock()
search_betdata_sync: dict[str, asyncio.Future] = {}

bet_data_update_sync_lock = threading.Lock()
bet_data_update_sync: dict[str, asyncio.Future] = {}

rollback_search_bet_lock = threading.Lock()


class PartialSearchEntryConsumer(GenericConsumer):

    @property
    def group_id(self):
        return 'my_group'

    @property
    def auto_offset_reset(self):
        return 'earliest'

    @property
    def auto_commit(self):
        return False

    @property
    def topic(self):
        return 'search_entry'

    @property
    def schema(self):
        return """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Partial Search data",
  "description": "Partial search data",
  "type": "object",
  "properties": {
    "web_site": {
      "description": "Website name",
      "type": "string"
    },
    "user_id": {
      "description": "User's Discord id",
      "type": "string"
    }
  },
  "required": [
    "web_site",
    "user_id"
  ]
}"""

    def dict_to_model(self, map, ctx):
        if map is None:
            return None

        return SearchDataPartialInDb(**map)

    @staticmethod
    @scheduling.async_repeat_deco(repeat_count=3, reschedule_count=3, always_reschedule=True)
    async def _rollback_data(id, tx_id):
        await database.mongo.db[SearchDataPartialInDb.collection_name].delete_many({'_id': id})
        await database.mongo.db[BetDataListUpdateInDb.collection_name].delete_many({'search_id': id})
        try:
            del search_betdata_sync[tx_id]
        except:
            pass

    def _consume_data(self):
        while not self._cancelled:
            try:
                msg = self._consumer.poll(0.1)
                if msg is None:
                    continue

                search_entry: SearchDataPartialInDb = msg.value()
                if search_entry is not None:
                    id_to_insert = search_entry.id

                    async def complete_partial_search():
                        if scheduling.transaction_scheduler.get_job(msg.key()) is None:
                            with rollback_search_bet_lock:
                                if scheduling.transaction_scheduler.get_job(msg.key()) is None:
                                    scheduling.transaction_scheduler.add_job(self._rollback_data, 'date',
                                                                             run_date=datetime.datetime.now(
                                                                                 pytz.utc) + datetime.timedelta(
                                                                                 seconds=20),
                                                                             args=[id_to_insert, msg.key()],
                                                                             id=msg.key(),
                                                                             misfire_grace_time=None,
                                                                             replace_existing=True
                                                                             )
                        scheduling.transaction_scheduler.pause_job(msg.key())

                        await database.mongo.db[SearchDataPartialInDb.collection_name].insert_one(
                            {**search_entry.dict(by_alias=True), 'tx_id': msg.key()})
                        scheduling.transaction_scheduler.reschedule_job(msg.key(), trigger='date',
                                                                        run_date=datetime.datetime.now(
                                                                            pytz.utc) + datetime.timedelta(seconds=20))
                    asyncio.run_coroutine_threadsafe(complete_partial_search(), self._loop).result(20)
                    if search_betdata_sync.get(msg.key()) is None:
                        with search_betdata_sync_lock:
                            if search_betdata_sync.get(msg.key()) is None:
                                search_betdata_sync[msg.key()] = self._loop.create_future()
                    self._loop.call_soon_threadsafe(search_betdata_sync[msg.key()].set_result, 'executed')

                    self._consumer.commit(msg)
                else:
                    logging.warning(f'Null value for the message: {msg.key()}')
                    self._consumer.commit(msg)
            except Exception as exc:
                traceback.print_exc()
                logging.error(exc)
                try:
                    scheduling.transaction_scheduler.reschedule_job(job_id=msg.key(), trigger='date')
                    self._consumer.commit(msg)
                except:
                    try:
                        self._consumer.commit(msg)
                    except:
                        pass

                # break

        self._consumer.close()


class BetDataApplyConsumer(GenericConsumer):

    @property
    def group_id(self):
        return 'my_group'

    @property
    def auto_offset_reset(self):
        return 'earliest'

    @property
    def auto_commit(self):
        return False

    @property
    def topic(self):
        return 'bet_data_apply'

    @property
    def schema(self):
        return """{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "CSV Generation Request",
  "description": "CSV Generation Kafka Request",
  "type": "object",
  "properties": {
    "data": {
      "description": "Bet Data",
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
  }
}"""

    def dict_to_model(self, map, ctx):
        if map is None:
            return None

        return BetDataListUpdateInDb(**map)

    async def _update_betdata_list(self, bet_data, tx_id):
        try:
            if search_betdata_sync.get(tx_id) is None:
                with search_betdata_sync_lock:
                    if search_betdata_sync.get(tx_id) is None:
                        search_betdata_sync[tx_id] = self._loop.create_future()
            await search_betdata_sync[tx_id]

            scheduling.transaction_scheduler.pause_job(tx_id)

            search_doc = await database.mongo.db[SearchDataPartialInDb.collection_name].find_one({'tx_id': tx_id})
            search_id = search_doc['_id']
            await database.mongo.db[BetDataListUpdateInDb.collection_name].insert_many({**data.dict(),
                                                                                           'search_id': PyObjectId(
                                                                                               search_id)} for data
                                                                                       in
                                                                                       bet_data)
            if bet_data_update_sync.get(tx_id) is None:
                with bet_data_update_sync_lock:
                    if bet_data_update_sync.get(tx_id) is None:
                        bet_data_update_sync[tx_id] = self._loop.create_future()
            bet_data_update_sync[tx_id].set_result('success')

            scheduling.transaction_scheduler.reschedule_job(tx_id, trigger='date',
                                                            run_date=datetime.datetime.now(
                                                                pytz.utc) + datetime.timedelta(seconds=20))
        except:
            logging.exception('')
            scheduling.transaction_scheduler.reschedule_job(tx_id)
        finally:
            search_betdata_sync[tx_id].cancel()
            del search_betdata_sync[tx_id]

    def _consume_data(self):
        while not self._cancelled:
            try:
                msg = self._consumer.poll(0.1)
                if msg is None:
                    continue

                bet_data: BetDataListUpdateInDb = msg.value()
                if bet_data is not None:
                    asyncio.run_coroutine_threadsafe(self._update_betdata_list(bet_data.data, msg.key()),
                                                     self._loop).result(20)

                    self._consumer.commit(msg)
                else:
                    logging.warning(f'Null value for the message: {msg.key()}')
                    self._consumer.commit(msg)
            except Exception as exc:
                logging.error(exc)
                try:
                    scheduling.transaction_scheduler.reschedule_job(job_id=msg.key(), trigger='date')
                    self._consumer.commit(msg)
                except:
                    try:
                        self._consumer.commit(msg)
                        search_betdata_sync[msg.key()].cancel()
                        del search_betdata_sync[msg.key()]
                    except:
                        pass

                # break

        self._consumer.close()


class BetDataFinishConsumer(GenericConsumer):

    @property
    def group_id(self):
        return 'my_group'

    @property
    def auto_offset_reset(self):
        return 'earliest'

    @property
    def auto_commit(self):
        return False

    @property
    def topic(self):
        return 'bet_data_finish'

    @property
    def schema(self):
        return None

    def dict_to_model(self, map, ctx):
        return None

    def _consume_data(self):
        while not self._cancelled:
            try:
                msg = self._consumer.poll(0.1)
                if msg is None:
                    continue

                async def complete_transaction():
                    if bet_data_update_sync.get(msg.key().decode('utf-8')) is None:
                        with bet_data_update_sync_lock:
                            if bet_data_update_sync.get(msg.key().decode('utf-8')) is None:
                                bet_data_update_sync[msg.key().decode('utf-8')] = self._loop.create_future()
                    await bet_data_update_sync[msg.key().decode('utf-8')]
                    scheduling.transaction_scheduler.remove_job(msg.key().decode('utf-8'))

                asyncio.run_coroutine_threadsafe(complete_transaction(), loop=self._loop).result(20)

                del bet_data_update_sync[msg.key().decode('utf-8')]
                self._consumer.commit(msg)
            except Exception as exc:
                logging.error(exc)
                try:
                    self._consumer.commit(msg)
                    bet_data_update_sync[msg.key().decode('utf-8')].cancel()
                    del bet_data_update_sync[msg.key().decode('utf-8')]
                except:
                    pass

                # break

        self._consumer.close()


search_entry_consumer: PartialSearchEntryConsumer
betdata_apply_consumer: BetDataApplyConsumer
betdata_finish_consumer: BetDataFinishConsumer


def initialize_consumers():
    global search_entry_consumer, betdata_apply_consumer, betdata_finish_consumer
    search_entry_consumer = PartialSearchEntryConsumer(loop=asyncio.get_running_loop())
    betdata_apply_consumer = BetDataApplyConsumer(loop=asyncio.get_running_loop())
    betdata_finish_consumer = BetDataFinishConsumer(loop=asyncio.get_running_loop(), normal=True)
    search_entry_consumer.consume_data()
    betdata_apply_consumer.consume_data()
    betdata_finish_consumer.consume_data()


def close_consumers():
    search_entry_consumer.close()
    betdata_apply_consumer.close()
    betdata_finish_consumer.close()

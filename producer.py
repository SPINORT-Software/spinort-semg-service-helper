from abc import ABCMeta, abstractmethod
from confluent_kafka import Producer

import logging

logger = logging.getLogger(__name__)


class KafkaProducer:
    def __init__(self, kafka_producer_configuration, topic, **kwargs):
        self._kafka_producer = Producer(kafka_producer_configuration)
        self._topic = topic

    def delivery_callback(self, err, msg):
        if err:
            logger.exception('ERROR: Message failed delivery: {}'.format(err))
        else:
            logger.info("Produced event to topic {topic}: ".format(topic=msg.topic()))

    def produce(self, message):
        self._kafka_producer.produce(self._topic, message.encode("utf-8"), callback=self.delivery_callback)
        self._kafka_producer.flush()


class KafkaProducerConfiguration(metaclass=ABCMeta):
    @abstractmethod
    def get_bootstrap_servers(self):
        pass

    @abstractmethod
    def get_flush_timeout(self):
        pass

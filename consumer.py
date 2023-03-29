import logging
import threading
from abc import ABCMeta, abstractmethod

from confluent_kafka import Consumer, OFFSET_BEGINNING, KafkaException, KafkaError

logger = logging.getLogger(__name__)
_KAFKA_MAX_INT_VALUE = 2147483647


class KafkaConsumerConfiguration(metaclass=ABCMeta):
    @abstractmethod
    def get_bootstrap_servers(self):
        pass

    @abstractmethod
    def get_consumer_group_id(self):
        pass

    @abstractmethod
    def get_consumer_timeout_ms(self):
        pass


class KafkaConsumer(threading.Thread):
    def __init__(
            self,
            kafka_consumer_configuration,
            env_config,
            topic: str,
            callback_function=lambda event: print(event),
            **kwargs
    ):
        threading.Thread.__init__(self, name=f"kafkaconsumer_[{topic}]")
        self._stop_event = threading.Event()
        self._topic = topic
        self._callback_function = callback_function
        self._kafka_consumer_configuration = kafka_consumer_configuration
        self._env_config = env_config
        if kwargs:
            self._kwargs = kwargs

    def stop(self):
        self._stop_event.set()

    def run(self):
        try:
            group_id = self._env_config.get_kafka_consumer_configuration().get_consumer_group_id()

            self._kafka_consumer_configuration["group.id"] = group_id
            self._kafka_consumer_configuration["auto.offset.reset"] = "latest"
            consumer = Consumer(self._kafka_consumer_configuration)
            consumer.subscribe([self._topic])
            logger.info(f"Subscribing to topic: [{self._topic}] with consumer group.id: {group_id}")

            while not self._stop_event.is_set():
                try:
                    message = consumer.poll()
                    if message is None: continue
                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            # End of partition event
                            logger.exception('%% %s [%d] reached end at offset %d\n' % (
                                message.topic(), message.partition(), message.offset()))
                        elif message.error():
                            raise KafkaException(message.error())
                    else:
                        logger.info(f"Consuming {message.topic()}-{message.partition()}-{message.offset()}")
                        consumer.commit(asynchronous=True)
                        self._callback_function(message)
                except Exception as exception:
                    logger.exception(f"The kafka event could not be consumed {exception}")
                if self._stop_event.is_set():
                    break

            logger.info(f"Stop event received for consumer of topic [{self._topic}]. Consumption will be stopped")
            consumer.close()
        except Exception as e:
            logger.exception(f"Exception {e}")

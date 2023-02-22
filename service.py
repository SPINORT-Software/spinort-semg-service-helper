import os
from dotenv import load_dotenv
import logging

from consumer import KafkaConsumer
from kafka_alert import KafkaAlertApi
from assemblers.mvn_data_assembler import MVNDataAssembler
from udp_consumer import UDPConsumer

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SERVICE_BASE_DIR = os.path.dirname(__file__)


class Service:
    def __init__(self, configuration, confluent_config):
        self.kafka_alert = KafkaAlertApi(configuration)
        self.configuration = configuration
        self.confluent_config = confluent_config

    def start_ipc_consumer_thread(self):
        consumer = KafkaConsumer(
            self.configuration.get_kafka_consumer_configuration(),
            self.configuration.get_kafka_ipc_topic(),
            callback_function=self.kafka_alert.accept_record
        )
        consumer.start()

    def start_udp_consumer_thread(self):
        mvn_data_assembler = MVNDataAssembler(self.configuration, self.confluent_config)
        udp_consumer = UDPConsumer(
            mvn_data_assembler=mvn_data_assembler
        )
        udp_consumer.start()

import os
from dotenv import load_dotenv
import logging

from consumer import KafkaConsumer
from kafka_alert import KafkaAlertApi
from assemblers.semg_data_assembler import SEMGDataAssembler
from udp_consumer import UDPConsumer
from localStoragePy import localStoragePy

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SERVICE_BASE_DIR = os.path.dirname(__file__)


class Service:
    def __init__(self, configuration, confluent_config):
        self.local_storage = localStoragePy(configuration.get_local_storage_workspace_name(),
                                            configuration.get_local_storage_workspace_backend())
        self.kafka_alert = KafkaAlertApi(configuration, self.local_storage)
        self.configuration = configuration
        self.confluent_config = confluent_config

    def start_ipc_consumer_thread(self):
        consumer = KafkaConsumer(
            self.confluent_config,
            self.configuration,
            self.configuration.get_kafka_ipc_topic(),
            callback_function=self.kafka_alert.accept_record
        )
        consumer.start()

    def start_udp_consumer_thread(self):
        semg_data_assembler = SEMGDataAssembler(self.configuration, self.confluent_config, self.local_storage)
        udp_consumer = UDPConsumer(
            semg_data_assembler=semg_data_assembler
        )
        udp_consumer.start()

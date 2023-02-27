import os
from dotenv import load_dotenv
from configuration import get_config
import logging
from configparser import ConfigParser

from service import Service
from udp_consumer import UDPConsumer

load_dotenv()
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

SERVICE_BASE_DIR = os.path.dirname(__file__)

environment = os.getenv("ENVIRONMENT")
logger.info(f"Setting up SEMG Sensor service helper for environment [{environment}]")
configuration = get_config(environment)

confluent_properties_file = open("getting_started.ini")
confluent_config_parser = ConfigParser()
confluent_config_parser.read_file(confluent_properties_file)
confluent_config = dict(confluent_config_parser['default'])

if __name__ == '__main__':
    service = Service(configuration, confluent_config)
    service.start_ipc_consumer_thread()  # Kafka IPC topic consumer thread
    service.start_udp_consumer_thread()  # Kafka UDP Server consumer thread


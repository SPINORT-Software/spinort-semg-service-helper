import logging
import json
import numpy as np
import struct
import os
from json import load

from producer import KafkaProducer
from session_type import SessionType
from kafka_alert import KafkaAlertApi

logger = logging.getLogger(__name__)
SERVICE_BASE_DIR = os.path.dirname(__file__)


class SEMGDataAssembler:
    def __init__(self, configuration, confluent_config):
        self.kafka_alert = KafkaAlertApi(configuration)
        self.formatted_data = []
        self.producer = KafkaProducer(confluent_config, configuration.get_kafka_semg_sensor_topic())
        self.dataframe = {}
        self.configuration = configuration
        self.confluent_config = confluent_config
        self.allow_sending_key = self.configuration.get_environ_name_data_send_allow()

    def _should_allow_message_send(self):
        """
        Check if the environment variable for is True.
        :return:
        """
        allow_sending = False
        if self.allow_sending_key in os.environ and os.getenv(self.allow_sending_key) in ('True', 'true', '1'):
            allow_sending = True

        logger.info(f"[SEMG Sensor Environment] Allow Message Send to Kafka Configured to: {allow_sending}")
        return allow_sending

    def _prepare_kafka_message(self, data):
        """
        Retrieve the required values from OS Environ
        Calibration: Session ID and Step ID
        Treatment: Session ID
        :param message:
        :return:
        """
        if not os.getenv(self.configuration.get_environ_name_session_type()) or not os.getenv(
                self.configuration.get_environ_name_session_id()):
            logger.info("No session type set in the environment. Ignoring message sending to Kafka.")
            return None

        elif os.getenv(self.configuration.get_environ_name_session_type()) == SessionType.CALIBRATION.name:
            kafka_message = {
                "type": "calibration",
                "session": os.getenv(self.configuration.get_environ_name_session_id()),
                "step": os.getenv(self.configuration.get_environ_name_calibration_step_id()),
                "data": data
            }
        else:
            kafka_message = {
                "type": "treatment",
                "session": os.getenv(self.configuration.get_environ_name_session_id()),
                "data": data
            }
        return kafka_message

    def send(self, data):
        """
        Do not produce to Kafka if the message is none or empty
        :param data: data from SEMG
        :return:
        """
        if not self._should_allow_message_send():
            logger.info("SEMG data frame message will not be sent to Kafka because the environment "
                        f"variable [{self.configuration.get_environ_name_data_send_allow()}] is not set or is false.")
            # Check if message is to be sent to Kafka. Environment variable is maintained by ipc_command_assembler process
            return False

        if not data:
            logger.info(f"Can't produce message to Kafka, data is null.", data)
            return False

        logger.info(f"Producing message to Kafka topic: {self.configuration.get_kafka_semg_sensor_topic()}")
        kafka_message = self._prepare_kafka_message(data)
        self.producer.produce(json.dumps(kafka_message))
        self.dataframe = {}  # Clear the dataframe for the next MVN frame that will arrive


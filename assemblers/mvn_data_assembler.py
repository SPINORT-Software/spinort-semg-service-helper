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


class MVNDataAssembler:
    def __init__(self, configuration, confluent_config):
        self.segment_base_data = self.read_segment_data_file()
        self.kafka_alert = KafkaAlertApi(configuration)
        self.formatted_data = []
        self.producer = KafkaProducer(confluent_config, configuration.get_kafka_inertial_sensor_topic())
        self.single_cycle_msg_types_rcvd = 0
        self.dataframe = {}
        self.configuration = configuration
        self.confluent_config = confluent_config
        self.allow_sending_key = self.configuration.get_environ_name_data_send_allow()

    def handle(self):
        """
        Overrides the handle() method
        :return:
        """
        logger.info("Received request from {}".format(self.client_address[0]))
        message = self.rfile.readline().strip()
        logger.info("Datagram received from client is:".format(message))
        logger.info(message)

    def test(self):
        return self._should_allow_message_send()

    def _should_allow_message_send(self):
        """
        Check if the environment variable for is True.
        :return:
        """
        allow_sending = False
        if self.allow_sending_key in os.environ and os.getenv(self.allow_sending_key) in ('True', 'true', '1'):
            allow_sending = True

        logger.info(f"[Inertial Sensor Environment] Allow Message Send to Kafka Configured to: {allow_sending}")
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
        :param data: data from MVN Analyze
        :return:
        """
        if not self._should_allow_message_send():
            logger.info("MVN data frame message will not be sent to Kafka because the environment "
                        f"variable [{self.configuration.get_environ_name_data_send_allow()}] is not set or is false.")
            # Check if message is to be sent to Kafka. Environment variable is maintained by ipc_command_assembler process
            return False

        if not data:
            logger.info(f"Can't produce message to Kafka, data is null.", data)
            return False

        logger.info(f"Producing message to Kafka topic: {self.configuration.get_kafka_inertial_sensor_topic()}")
        kafka_message = self._prepare_kafka_message(data)
        self.producer.produce(json.dumps(kafka_message))
        self.dataframe = {}  # Clear the dataframe for the next MVN frame that will arrive

    def mvn_parse(self, message):
        """
        Parsing function to parse and interpret MVN Analyze commnunication data packets.
        Currently only supports joint angles and position in x,y,z.

        X - flexion/extension (sagittal plane)
        Y – axial bending (transverse plane)
        Z – lateral bending (coronal plane)
        """
        message_id = message[:6]  # ID string
        message_type = int(message[4:6])  # Message Type
        sample_counter = int.from_bytes(message[6:10], byteorder='big') + 1  # Counts number of sent samples
        datagram_counter = int(message[10])  # Counts number of sent datagrams
        segments_count = int(message[11])  # number of items
        sample_time_code = int.from_bytes(message[12:16], byteorder='big')  # time code of the sample
        body_segments_count = int(message[17])

        if message_type == 20:
            self.single_cycle_msg_types_rcvd += 1
            packetSize = 20
            Ids = np.zeros((segments_count, 2))
            angles = np.zeros((body_segments_count, 3))

            for s in range(1, 7):
                try:
                    offset = s * packetSize

                    Ids[s, 0] = int.from_bytes(message[24 + offset:28 + offset], byteorder='big')
                    Ids[s, 1] = int.from_bytes(message[28 + offset:32 + offset], byteorder='big')

                    angles[s, 0] = struct.unpack('>f', message[32 + offset:36 + offset])[0]
                    angles[s, 1] = struct.unpack('>f', message[36 + offset:40 + offset])[0]
                    angles[s, 2] = struct.unpack('>f', message[40 + offset:44 + offset])[0]

                    segment_label_data = self.segment_base_data[str(s)]
                    self.dataframe[segment_label_data['0']['joint_label']] = angles[s, 0]
                    self.dataframe[segment_label_data['1']['joint_label']] = angles[s, 1]
                    self.dataframe[segment_label_data['2']['joint_label']] = angles[s, 2]

                    logger.info(f"Joint Angle Data = {self.dataframe}")

                except Exception as e:
                    logger.exception(e)

        if message_type == 24:
            self.single_cycle_msg_types_rcvd += 1

            try:
                self.dataframe['com_posx'] = struct.unpack('>f', message[24:28])[0]
                self.dataframe['com_posy'] = struct.unpack('>f', message[28:32])[0]
                self.dataframe['com_posz'] = struct.unpack('>f', message[32:36])[0]

                logger.info(f"COM Data= {self.dataframe}")

            except Exception as e:
                logger.exception(e)

        return self.dataframe

    def read_segment_data_file(self):
        try:
            file_name = f"all_joints_data.json"
            file_path = os.path.join(SERVICE_BASE_DIR, 'data', file_name)

            with open(file_path, 'r') as f:
                data_content = load(f)

            return data_content
        except Exception as e:
            pass

    def is_both_message_types_received(self):
        return self.single_cycle_msg_types_rcvd == 2

    def reset_message_types_received(self):
        self.single_cycle_msg_types_rcvd = 0

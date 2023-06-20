from .kafka_assembler import KafkaAssembler
from commands import Commands

import os, sys
import logging
import json

logger = logging.getLogger(__name__)


class IpcCommandAssembler(KafkaAssembler):
    def __init__(self, configuration, local_storage):
        self._configuration = configuration

        self._session_id_key = configuration.get_environ_name_session_id()
        self._session_type_key = configuration.get_environ_name_session_type()
        self._step_id_key = configuration.get_environ_name_calibration_step_id()
        self._data_send_allow_key = configuration.get_environ_name_data_send_allow()
        self._local_storage = local_storage

    def assemble(self, kafka_consumer_record):
        """
        Set the Environment Variables received in the Command from the Smartback Backend engine.
        In the calibration start Command:
        :param kafka_consumer_record:
        :return:
        """
        original = kafka_consumer_record.value().decode("utf-8")
        original_event = json.loads(original)

        try:
            if "command" not in original_event:
                logger.info(
                    f"Not enough data available in the command message to assemble. Dropping the message {original_event}")
                return False

            command = original_event.get("command")
            logger.info(f"Received command [{command}] from ipc topic.")

            if command in (Commands.calibration_start.name, Commands.treatment_start.name):
                """
                Set to the environ variables:
                - Calibration Session ID
                - Session Type
                - Data Send Allow = False  
                """
                session_id_value = original_event.get("session")
                session_type_value = original_event.get("session_type")

                self._local_storage.setItem(self._session_id_key, session_id_value)
                self._local_storage.setItem(self._session_type_key, session_type_value)
                self._local_storage.setItem(self._step_id_key, "")
                self._local_storage.setItem(self._data_send_allow_key, str(False))

            elif command == Commands.calibration_step_start.name:
                """
                Preprocess: Check if self._session_id_key is not empty. Set the Step ID only if it's not empty.
                Set to the environ variables:
                - Calibration Step ID 
                - Data Send Allow = True
                """
                if not self._local_storage.getItem(self._session_id_key):
                    logger.info("Error processing calibration_step_start - step ID for calibration session will not "
                                "be set to environment. First set Session ID in the environment before setting Step "
                                "ID.")
                    return False

                calibration_step_id_value = original_event.get("step", None)

                if not calibration_step_id_value:
                    logger.info("Error processing calibration_step_start - step ID not provided in the data")
                    return False

                logger.info(
                    f"Setting Calibration Step ID [{calibration_step_id_value}] to Session [{self._local_storage.getItem(self._session_id_key)}]")

                self._local_storage.setItem(self._step_id_key, calibration_step_id_value)
                self._local_storage.setItem(self._data_send_allow_key, str(True))

            elif command == Commands.calibration_end.name:
                """
                Clear from environ variables:
                - Session ID 
                - Calibration Step ID
                - Data Send Allow = False
                """
                logger.info("Calibration end command received.")
                self._local_storage.setItem(self._session_id_key, "")
                self._local_storage.setItem(self._session_type_key, "")
                self._local_storage.setItem(self._step_id_key, "")
                self._local_storage.setItem(self._data_send_allow_key, str(False))

            elif command == Commands.treatment_start_data_send.name:
                self._local_storage.setItem(self._data_send_allow_key, str(True))

            elif command == Commands.data_send_pause.name:
                self._local_storage.setItem(self._data_send_allow_key, str(False))

            elif command == Commands.treatment_one_min_end.name:
                self._local_storage.setItem(self._data_send_allow_key, str(False))
                # TODO: Very Important for Treatment one minute end command to work in sync with the engine
                #  Sleep for 1 minute and set the allow sending to True
                #   sleep(60)
                #   os.environ[self._data_send_allow_key] = str(True)

            elif command == Commands.treatment_end.name:
                self._local_storage.setItem(self._session_id_key, "")
                self._local_storage.setItem(self._session_type_key, "")
                self._local_storage.setItem(self._data_send_allow_key, str(False))

            elif command == Commands.calibration_pause.name:
                self._local_storage.setItem(self._data_send_allow_key, str(False))

            else:
                logger.info(f"An unrecognized command is provided. Command = [{command}]")

        except Exception as e:
            logger.info(f"There was an error processing the command: {str(e)}")
            logger.info(f"The original event is {original_event}")
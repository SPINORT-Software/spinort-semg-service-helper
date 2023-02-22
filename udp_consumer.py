import logging
import threading
import socket
from time import sleep

logger = logging.getLogger(__name__)
_KAFKA_MAX_INT_VALUE = 2147483647

from assemblers.mvn_data_assembler import MVNDataAssembler


class UDPConsumer(threading.Thread):
    def __init__(self, mvn_data_assembler: MVNDataAssembler):
        threading.Thread.__init__(self, name=f"udpconsumer")
        self._stop_event = threading.Event()
        self._mvn_data_assembler = mvn_data_assembler
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.connection.bind(('127.0.0.1', 20001))

    def stop(self):
        self._stop_event.set()

    def run(self):
        try:
            logger.info("UDP Consumer Connection created.")

            while not self._stop_event.is_set():
                message = self.connection.recv(2000)
                logger.info(f"MVN message received : {message}")
                try:
                    # parsed_message = self._mvn_data_assembler.mvn_parse(message)

                    print(message)

                    # if self._mvn_data_assembler.is_both_message_types_received():
                    #     logger.info(f"Dataframe to be sent to Kafka: {parsed_message}")
                    #     self._mvn_data_assembler.send(parsed_message)
                    #     self._mvn_data_assembler.reset_message_types_received()
                    # else:
                    #     logger.info("Waiting for joint angle/center of mass message from MVN...")
                except Exception as exception:
                    logger.exception(f"The message could not be consumed {exception}")
                if self._stop_event.is_set():
                    logger.info("Stop event received.")
                    break

            logger.info(f"Stop event received on UDP Consumer. Consumption will be stopped")
        except Exception as e:
            logger.exception(f"An exception occurred while processing message from MVN : {str(e)}")

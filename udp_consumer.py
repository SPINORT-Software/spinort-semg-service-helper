import logging
import threading
import socket
from time import sleep
import json

logger = logging.getLogger(__name__)
_KAFKA_MAX_INT_VALUE = 2147483647

from assemblers.semg_data_assembler import SEMGDataAssembler


class UDPConsumer(threading.Thread):
    def __init__(self, semg_data_assembler: SEMGDataAssembler):
        threading.Thread.__init__(self, name=f"udpconsumer")
        self._stop_event = threading.Event()
        self._semg_data_assembler = semg_data_assembler
        self.connection = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.connection.bind(('127.0.0.1', 20001))

    def stop(self):
        self._stop_event.set()

    def run(self):
        try:
            logger.info("UDP Consumer Connection created.")

            while not self._stop_event.is_set():
                message = self.connection.recv(2000).decode("utf-8")
                message = json.loads(message)
                logger.info(f"SEMG message received : {message}")
                try:
                    logger.info(f"Dataframe to be sent to Kafka: {message}")
                    self._semg_data_assembler.send(message)
                except Exception as exception:
                    logger.exception(f"The message could not be consumed: {exception}")
                if self._stop_event.is_set():
                    logger.info("Stop event received.")
                    break

            logger.info(f"Stop event received on UDP Consumer. Consumption will be stopped")
        except Exception as e:
            logger.exception(f"An exception occurred while processing message from SEMF : {str(e)}")

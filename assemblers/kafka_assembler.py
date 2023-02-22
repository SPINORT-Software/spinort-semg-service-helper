from abc import ABC, abstractmethod


class KafkaAssembler(ABC):
    @abstractmethod
    def assemble(self, kafka_message):
        pass
